/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.read;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.BufferedTsFileInput;
import org.apache.tsfile.read.reader.LocalTsFileInput;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TsFileSequenceReaderPerformanceTest {

  private static final int FILE_COUNT = 100;
  private static final int DEVICE_COUNT = 100;
  private static final int POINTS_PER_DEVICE = 100_000;
  private static final int[] POINT_COUNTS_PER_CHUNK = {100, 1_000, 10_000};
  private static final int LOCAL_BUFFER_SIZE = 0;
  private static final int[] BUFFER_SIZES = {4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024};
  private static final String RUN_PERFORMANCE_TEST_PROPERTY = "tsfile.runPerformanceTests";
  private static final File TEST_DIRECTORY =
      new File("target", "tsfile-sequence-reader-performance");

  @Test
  public void testReadTsFilesWithDifferentBufferAndChunkSizes() throws Exception {
    Assume.assumeTrue(
        "Set -Dtsfile.runPerformanceTests=true to run the performance test",
        Boolean.getBoolean(RUN_PERFORMANCE_TEST_PROPERTY));
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    int previousMaxPointsInPage = config.getMaxNumberOfPointsInPage();

    try {
      for (int pointsPerChunk : POINT_COUNTS_PER_CHUNK) {
        assertEquals(0, POINTS_PER_DEVICE % pointsPerChunk);
        int measurementCount = POINTS_PER_DEVICE / pointsPerChunk;
        config.setMaxNumberOfPointsInPage(pointsPerChunk);
        List<File> files = generateTsFiles(measurementCount, pointsPerChunk);

        List<ReadConfiguration> configurations = createReadConfigurations();
        for (int fileIndex = 0; fileIndex < files.size(); fileIndex++) {
          File file = files.get(fileIndex);
          int firstConfigurationIndex = fileIndex % configurations.size();
          for (int offset = 0; offset < configurations.size(); offset++) {
            ReadConfiguration configuration =
                configurations.get((firstConfigurationIndex + offset) % configurations.size());
            configuration.elapsedNanos +=
                readTsFile(
                    file,
                    configuration.bufferSize,
                    configuration.statistics,
                    measurementCount,
                    pointsPerChunk);
          }
        }

        long expectedDeviceCount = (long) FILE_COUNT * DEVICE_COUNT;
        long expectedChunkCount = expectedDeviceCount * measurementCount;
        long expectedPointCount = expectedChunkCount * pointsPerChunk;
        System.out.printf(
            "Chunk scenario: points per chunk=%,d, measurements per device=%,d%n",
            pointsPerChunk, measurementCount);
        for (ReadConfiguration configuration : configurations) {
          assertStatistics(
              configuration.statistics,
              expectedDeviceCount,
              expectedChunkCount,
              expectedPointCount);
          printStatistics(
              configuration.inputName, configuration.statistics, configuration.elapsedNanos);
        }

        ReadConfiguration localConfiguration = configurations.get(0);
        for (int configurationIndex = 1;
            configurationIndex < configurations.size();
            configurationIndex++) {
          ReadConfiguration bufferedConfiguration = configurations.get(configurationIndex);
          System.out.printf(
              "%s vs LocalTsFileInput: speedup=%.3fx, time reduction=%+.2f%%%n",
              bufferedConfiguration.inputName,
              localConfiguration.elapsedNanos / (double) bufferedConfiguration.elapsedNanos,
              (localConfiguration.elapsedNanos - bufferedConfiguration.elapsedNanos)
                  * 100.0
                  / localConfiguration.elapsedNanos);
        }
      }
    } finally {
      config.setMaxNumberOfPointsInPage(previousMaxPointsInPage);
      FileUtils.deleteDirectory(TEST_DIRECTORY);
    }
  }

  private List<File> generateTsFiles(int measurementCount, int pointsPerChunk) throws Exception {
    FileUtils.deleteDirectory(TEST_DIRECTORY);
    Files.createDirectories(TEST_DIRECTORY.toPath());

    List<IMeasurementSchema> schemas = createMeasurementSchemas(measurementCount);
    Tablet tablet = createTablet(schemas, pointsPerChunk);
    List<File> files = new ArrayList<>(FILE_COUNT);
    for (int fileIndex = 0; fileIndex < FILE_COUNT; fileIndex++) {
      File file = new File(TEST_DIRECTORY, "sequence-reader-" + fileIndex + ".tsfile");
      generateTsFile(file, schemas, tablet);
      files.add(file);
    }
    return files;
  }

  private List<ReadConfiguration> createReadConfigurations() {
    List<ReadConfiguration> configurations = new ArrayList<>(BUFFER_SIZES.length + 1);
    configurations.add(new ReadConfiguration("LocalTsFileInput", LOCAL_BUFFER_SIZE));
    for (int bufferSize : BUFFER_SIZES) {
      configurations.add(
          new ReadConfiguration(
              String.format("BufferedTsFileInput(%d KiB)", bufferSize / 1024), bufferSize));
    }
    return configurations;
  }

  private List<IMeasurementSchema> createMeasurementSchemas(int measurementCount) {
    List<IMeasurementSchema> schemas = new ArrayList<>(measurementCount);
    for (int measurementIndex = 0; measurementIndex < measurementCount; measurementIndex++) {
      schemas.add(
          new MeasurementSchema(
              "s" + measurementIndex, TSDataType.INT64, TSEncoding.TS_2DIFF, CompressionType.LZ4));
    }
    return schemas;
  }

  private Tablet createTablet(List<IMeasurementSchema> schemas, int pointsPerChunk) {
    Tablet tablet = new Tablet(null, schemas, pointsPerChunk);
    for (int pointIndex = 0; pointIndex < pointsPerChunk; pointIndex++) {
      tablet.addTimestamp(pointIndex, pointIndex);
    }
    for (int measurementIndex = 0; measurementIndex < schemas.size(); measurementIndex++) {
      for (int pointIndex = 0; pointIndex < pointsPerChunk; pointIndex++) {
        tablet.addValue(pointIndex, measurementIndex, (long) pointIndex);
      }
    }
    return tablet;
  }

  private void generateTsFile(File file, List<IMeasurementSchema> schemas, Tablet tablet)
      throws Exception {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      for (int deviceIndex = 0; deviceIndex < DEVICE_COUNT; deviceIndex++) {
        String device = "root.performance.d" + deviceIndex;
        IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(device);
        for (IMeasurementSchema schema : schemas) {
          writer.registerTimeseries(deviceID, schema);
        }
        tablet.setDeviceId(device);
        writer.writeTree(tablet);
      }
    }
  }

  private long readTsFile(
      File file,
      int bufferSize,
      ReadStatistics statistics,
      int measurementCount,
      int pointsPerChunk)
      throws IOException {
    long startTime = System.nanoTime();
    TsFileInput input =
        bufferSize == LOCAL_BUFFER_SIZE
            ? new LocalTsFileInput(file.toPath())
            : new BufferedTsFileInput(file.toPath(), bufferSize);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(input)) {
      reader.position(TSFileConfig.MAGIC_STRING.getBytes().length + 1L);
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            readChunk(reader, marker, statistics, pointsPerChunk);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            reader.readChunkGroupHeader();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      verifyMetadata(reader, statistics, measurementCount, pointsPerChunk);
    }
    return System.nanoTime() - startTime;
  }

  private void assertStatistics(
      ReadStatistics statistics,
      long expectedDeviceCount,
      long expectedChunkCount,
      long expectedPointCount) {
    assertEquals(expectedDeviceCount, statistics.deviceCount);
    assertEquals(expectedChunkCount, statistics.chunkCount);
    assertEquals(expectedPointCount, statistics.pointCount);
  }

  private void printStatistics(String inputName, ReadStatistics statistics, long elapsedNanos) {
    System.out.printf(
        "%s sequentially read %d TsFiles: devices=%,d, chunks=%,d, points=%,d, total time=%d ms (%.3f s)%n",
        inputName,
        FILE_COUNT,
        statistics.deviceCount,
        statistics.chunkCount,
        statistics.pointCount,
        TimeUnit.NANOSECONDS.toMillis(elapsedNanos),
        elapsedNanos / 1_000_000_000.0);
  }

  private void readChunk(
      TsFileSequenceReader reader, byte marker, ReadStatistics statistics, int pointsPerChunk)
      throws IOException {
    ChunkHeader chunkHeader = reader.readChunkHeader(marker);
    int remainingDataSize = chunkHeader.getDataSize();
    long pointsInChunk = 0;
    while (remainingDataSize > 0) {
      boolean hasStatistics = (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER;
      PageHeader pageHeader = reader.readPageHeader(chunkHeader.getDataType(), hasStatistics);
      ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
      Decoder valueDecoder =
          Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
      Decoder timeDecoder =
          Decoder.getDecoderByType(
              TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
              TSDataType.INT64);
      BatchData batchData =
          new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder)
              .getAllSatisfiedPageData();
      pointsInChunk += batchData.length();
      remainingDataSize -= pageHeader.getSerializedPageSize();
    }

    assertEquals(pointsPerChunk, pointsInChunk);
    statistics.chunkCount++;
    statistics.pointCount += pointsInChunk;
  }

  private void verifyMetadata(
      TsFileSequenceReader reader,
      ReadStatistics statistics,
      int measurementCount,
      int pointsPerChunk)
      throws IOException {
    List<IDeviceID> devices = reader.getAllDevices();
    assertEquals(DEVICE_COUNT, devices.size());
    for (IDeviceID device : devices) {
      Map<String, List<ChunkMetadata>> metadataByMeasurement =
          reader.readChunkMetadataInDevice(device);
      assertEquals(measurementCount, metadataByMeasurement.size());
      for (List<ChunkMetadata> chunkMetadata : metadataByMeasurement.values()) {
        assertEquals(1, chunkMetadata.size());
        assertEquals(pointsPerChunk, chunkMetadata.get(0).getNumOfPoints());
      }
    }
    statistics.deviceCount += devices.size();
  }

  private static class ReadStatistics {

    private long deviceCount;
    private long chunkCount;
    private long pointCount;
  }

  private static class ReadConfiguration {

    private final String inputName;
    private final int bufferSize;
    private final ReadStatistics statistics = new ReadStatistics();
    private long elapsedNanos;

    private ReadConfiguration(String inputName, int bufferSize) {
      this.inputName = inputName;
      this.bufferSize = bufferSize;
    }
  }
}
