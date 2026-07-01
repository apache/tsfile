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
package org.apache.tsfile.write;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.file.metadata.utils.TestHelper;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.MeasurementGroup;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TsFileIOWriterTest {

  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileIOWriterTest.tsfile");
  private static final IDeviceID DEVICE_1 = IDeviceID.Factory.DEFAULT_FACTORY.create("device1");
  private static final IDeviceID DEVICE_2 = IDeviceID.Factory.DEFAULT_FACTORY.create("device2");
  private static final String SENSOR_1 = "sensor1";

  private static final int CHUNK_GROUP_NUM = 2;

  @Before
  public void before() throws IOException {
    TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH));

    // file schema
    IMeasurementSchema measurementSchema = TestHelper.createSimpleMeasurementSchema(SENSOR_1);
    VectorMeasurementSchema vectorMeasurementSchema =
        new VectorMeasurementSchema(
            "", new String[] {"s1", "s2"}, new TSDataType[] {TSDataType.INT64, TSDataType.INT64});
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    MeasurementGroup group = new MeasurementGroup(true, schemas);

    Schema schema = new Schema();
    schema.registerTimeseries(new Path(DEVICE_1), measurementSchema);
    schema.registerMeasurementGroup(new Path(DEVICE_2), group);

    writeChunkGroup(writer, measurementSchema);
    writeVectorChunkGroup(writer, vectorMeasurementSchema);
    writer.setMinPlanIndex(100);
    writer.setMaxPlanIndex(10000);
    writer.writePlanIndices();
    // end file
    writer.endFile();
  }

  @After
  public void after() {
    File file = new File(FILE_PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void changeTypeCompressionTest() throws IOException, WriteProcessException {
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    CompressionType prevInt32Compression = config.getCompressor(TSDataType.INT32);
    CompressionType prevTextCompression = config.getCompressor(TSDataType.TEXT);
    config.setInt32Compression("UNCOMPRESSED");
    config.setTextCompression("GZIP");

    try (TsFileIOWriter ioWriter =
            new TsFileIOWriter(
                new File(
                    TestConstant.BASE_OUTPUT_PATH.concat("changeTypeCompressionTest.tsfile")));
        TsFileWriter fileWriter = new TsFileWriter(ioWriter)) {
      fileWriter.registerTimeseries(
          Factory.DEFAULT_FACTORY.create("root.db1.d1"),
          new MeasurementSchema("s1", TSDataType.INT32));
      fileWriter.registerTimeseries(
          Factory.DEFAULT_FACTORY.create("root.db1.d1"),
          new MeasurementSchema("s2", TSDataType.TEXT));
      TableSchema tableSchema =
          new TableSchema(
              "t1",
              Arrays.asList(
                  new ColumnSchema("s1", TSDataType.INT32, ColumnCategory.FIELD),
                  new ColumnSchema("s2", TSDataType.TEXT, ColumnCategory.FIELD)));
      fileWriter.registerTableSchema(tableSchema);

      Tablet treeTablet =
          new Tablet(
              "root.db1.d1",
              Arrays.asList(
                  new MeasurementSchema("s1", TSDataType.INT32),
                  new MeasurementSchema("s2", TSDataType.TEXT)));
      treeTablet.addTimestamp(0, 0);
      treeTablet.addValue(0, 0, 0);
      treeTablet.addValue(0, 1, "0");
      fileWriter.writeTree(treeTablet);

      Tablet tableTablet =
          new Tablet(
              "t1",
              Arrays.asList("s1", "s2"),
              Arrays.asList(TSDataType.INT32, TSDataType.TEXT),
              Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD));
      tableTablet.addTimestamp(0, 0);
      tableTablet.addValue(0, 0, 0);
      tableTablet.addValue(0, 1, "0");
      fileWriter.writeTable(tableTablet);
      fileWriter.flush();

      ChunkMetadata s1TreeChunkMeta =
          ioWriter.getChunkGroupMetadataList().get(0).getChunkMetadataList().get(0);
      ChunkMetadata s2TreeChunkMeta =
          ioWriter.getChunkGroupMetadataList().get(0).getChunkMetadataList().get(1);
      ChunkMetadata s1TableChunkMeta =
          ioWriter.getChunkGroupMetadataList().get(1).getChunkMetadataList().get(1);
      ChunkMetadata s2TableChunkMeta =
          ioWriter.getChunkGroupMetadataList().get(1).getChunkMetadataList().get(2);

      fileWriter.close();

      try (TsFileSequenceReader sequenceReader =
          new TsFileSequenceReader(
              TestConstant.BASE_OUTPUT_PATH.concat("changeTypeCompressionTest.tsfile"))) {
        Chunk chunk = sequenceReader.readMemChunk(s1TreeChunkMeta);
        assertEquals(CompressionType.UNCOMPRESSED, chunk.getHeader().getCompressionType());
        chunk = sequenceReader.readMemChunk(s2TreeChunkMeta);
        assertEquals(CompressionType.GZIP, chunk.getHeader().getCompressionType());
        chunk = sequenceReader.readMemChunk(s1TableChunkMeta);
        assertEquals(CompressionType.UNCOMPRESSED, chunk.getHeader().getCompressionType());
        chunk = sequenceReader.readMemChunk(s2TableChunkMeta);
        assertEquals(CompressionType.GZIP, chunk.getHeader().getCompressionType());
      }

    } finally {
      config.setInt32Compression(prevInt32Compression.name());
      config.setTextCompression(prevTextCompression.name());
    }
  }

  @Test
  public void endFileTest() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);

    // magic_string
    assertEquals(TSFileConfig.MAGIC_STRING, reader.readHeadMagic());
    assertEquals(TSFileConfig.VERSION_NUMBER, reader.readVersionNumber());
    assertEquals(TSFileConfig.MAGIC_STRING, reader.readTailMagic());

    reader.position(TSFileConfig.MAGIC_STRING.getBytes().length + 1);

    ChunkHeader header;
    ChunkGroupHeader chunkGroupHeader;
    for (int i = 0; i < CHUNK_GROUP_NUM; i++) {
      // chunk group header
      assertEquals(MetaMarker.CHUNK_GROUP_HEADER, reader.readMarker());
      chunkGroupHeader = reader.readChunkGroupHeader();
      assertEquals(DEVICE_1, chunkGroupHeader.getDeviceID());
      // ordinary chunk header
      assertEquals(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER, reader.readMarker());
      header = reader.readChunkHeader(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER);
      assertEquals(SENSOR_1, header.getMeasurementID());
    }

    for (int i = 0; i < CHUNK_GROUP_NUM; i++) {
      // chunk group header
      assertEquals(MetaMarker.CHUNK_GROUP_HEADER, reader.readMarker());
      chunkGroupHeader = reader.readChunkGroupHeader();
      assertEquals(DEVICE_2, chunkGroupHeader.getDeviceID());
      // vector chunk header (time)
      assertEquals(MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER, reader.readMarker());
      header = reader.readChunkHeader(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER);
      assertEquals("", header.getMeasurementID());
      // vector chunk header (values)
      assertEquals(MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER, reader.readMarker());
      header = reader.readChunkHeader(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER);
      assertEquals("s1", header.getMeasurementID());
      assertEquals(MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER, reader.readMarker());
      header = reader.readChunkHeader(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER);
      assertEquals("s2", header.getMeasurementID());
    }

    assertEquals(MetaMarker.OPERATION_INDEX_RANGE, reader.readMarker());
    reader.readPlanIndex();
    assertEquals(100, reader.getMinPlanIndex());
    assertEquals(10000, reader.getMaxPlanIndex());

    assertEquals(MetaMarker.SEPARATOR, reader.readMarker());

    // make sure timeseriesMetadata is only
    Map<IDeviceID, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
        reader.getAllTimeseriesMetadata(false);
    Set<String> pathSet = new HashSet<>();
    for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry :
        deviceTimeseriesMetadataMap.entrySet()) {
      for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
        String seriesPath = entry.getKey() + "." + timeseriesMetadata.getMeasurementId();
        Assert.assertFalse(pathSet.contains(seriesPath));
        pathSet.add(seriesPath);
      }
    }

    // FileMetaData
    TsFileMetadata metaData = reader.readFileMetadata();
    int cnt = 0;
    for (MetadataIndexNode node : metaData.getTableMetadataIndexNodeMap().values()) {
      cnt += node.getChildren().size();
    }
    assertEquals(2, cnt);
  }

  private void writeChunkGroup(TsFileIOWriter writer, IMeasurementSchema measurementSchema)
      throws IOException {
    for (int i = 0; i < CHUNK_GROUP_NUM; i++) {
      // chunk group
      writer.startChunkGroup(DEVICE_1);
      // ordinary chunk, chunk statistics
      Statistics statistics = Statistics.getStatsByType(measurementSchema.getType());
      statistics.updateStats(0L, 0L);
      writer.startFlushChunk(
          measurementSchema.getMeasurementName(),
          measurementSchema.getCompressor(),
          measurementSchema.getType(),
          measurementSchema.getEncodingType(),
          statistics,
          0,
          0,
          0);
      writer.endCurrentChunk();
      writer.endChunkGroup();
    }
  }

  private void writeVectorChunkGroup(
      TsFileIOWriter writer, VectorMeasurementSchema vectorMeasurementSchema) throws IOException {
    for (int i = 0; i < CHUNK_GROUP_NUM; i++) {
      // chunk group
      writer.startChunkGroup(DEVICE_2);
      // vector chunk (time)
      writer.startFlushChunk(
          vectorMeasurementSchema.getMeasurementName(),
          vectorMeasurementSchema.getTimeCompressor(),
          vectorMeasurementSchema.getType(),
          vectorMeasurementSchema.getTimeTSEncoding(),
          Statistics.getStatsByType(vectorMeasurementSchema.getType()),
          0,
          0,
          TsFileConstant.TIME_COLUMN_MASK);
      writer.endCurrentChunk();
      // vector chunk (values)
      for (int j = 0; j < vectorMeasurementSchema.getSubMeasurementsCount(); j++) {
        Statistics subStatistics =
            Statistics.getStatsByType(
                vectorMeasurementSchema.getSubMeasurementsTSDataTypeList().get(j));
        subStatistics.updateStats(0L, 0L);
        writer.startFlushChunk(
            vectorMeasurementSchema.getSubMeasurementsList().get(j),
            vectorMeasurementSchema.getValueCompressor(j),
            vectorMeasurementSchema.getSubMeasurementsTSDataTypeList().get(j),
            vectorMeasurementSchema.getSubMeasurementsTSEncodingList().get(j),
            subStatistics,
            0,
            0,
            TsFileConstant.VALUE_COLUMN_MASK);
        writer.endCurrentChunk();
      }
      writer.endChunkGroup();
    }
  }
}
