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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.reader.LocalTsFileInput;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.utils.NoSyncBufferedOutputStream;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileOutput;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class CsvReadWriteTest {

  private static final String PARENT_DIR = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
  private static final String INPUT_PARENT_DIR = PARENT_DIR + "dataset_tsfile/";
  // private static final String INPUT_PARENT_DIR = PARENT_DIR + "dataset_long/";
  private static final String OUTPUT_PARENT_DIR = PARENT_DIR + "result/tsfile_read_write/";
  private static final String TSFILE_OUTPUT_DIR = OUTPUT_PARENT_DIR + "tsfiles/";
  // private static final String RESULT_CSV_PATH = OUTPUT_PARENT_DIR + "write_time.csv";
  private static final String RESULT_CSV_PATH = OUTPUT_PARENT_DIR + "write_time2.csv";
  private static final String READ_RESULT_CSV_PATH = OUTPUT_PARENT_DIR + "read_time.csv";
  // private static final int REPEAT_TIMES = 100;
  private static final int REPEAT_TIMES = 200;
  private static final String DEVICE_NAME = "device_1";
  private static final String MEASUREMENT_NAME = "sensor_1";
  private static final int MAX_DECIMAL_PRECISION = 8;

  private static final List<TSEncoding> ENCODINGS =
      Arrays.asList(
          TSEncoding.TS_2DIFF,
          TSEncoding.RLE,
          TSEncoding.GORILLA,
          TSEncoding.CHIMP,
          TSEncoding.SUBCOLUMN
      );

  private final IDeviceID deviceID = Factory.DEFAULT_FACTORY.create(DEVICE_NAME);

  @Test
  public void benchmarkCsvWriteEncodings() throws Exception {
    File inputDir = new File(INPUT_PARENT_DIR);
    File outputDir = new File(OUTPUT_PARENT_DIR);
    File tsFileDir = new File(TSFILE_OUTPUT_DIR);
    if (!inputDir.exists()) {
      throw new IOException("Input dataset directory does not exist: " + INPUT_PARENT_DIR);
    }
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }
    if (!tsFileDir.exists()) {
      tsFileDir.mkdirs();
    }

    File[] csvFiles = inputDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
    if (csvFiles == null || csvFiles.length == 0) {
      throw new IOException("No CSV dataset files found under " + INPUT_PARENT_DIR);
    }
    Arrays.sort(csvFiles, Comparator.comparing(File::getName));

    CsvWriter writer = new CsvWriter(RESULT_CSV_PATH, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Write Total Time Nanos",
          "Dataset Read Time Nanos",
          "Write CPU Time Nanos",
          "Write IO Time Nanos",
          "Write IO Write Nanos",
          "Write IO Flush Nanos",
          "Write IO Force Nanos",
          "Points",
          "Max Decimal Precision",
          "Multiplier",
          "TsFile Size Bytes",
          // "TsFile Path"
        });

    try {
      for (File datasetFile : csvFiles) {
        DatasetProfile datasetProfile = analyzeDataset(datasetFile);
        int boundedPrecision = Math.min(datasetProfile.getMaxDecimalPrecision(), MAX_DECIMAL_PRECISION);
        long multiplier = getMultiplier(boundedPrecision);

        String datasetName = extractFileName(datasetFile.getName());
        System.out.printf(
            "Writing dataset=%s, points=%d, precision=%d%n",
            datasetName, datasetProfile.getPointCount(), boundedPrecision);
        for (TSEncoding encoding : ENCODINGS) {
          System.out.printf("  Encoding=%s%n", encoding.name());
          java.nio.file.Path tsFilePath =
              Paths.get(TSFILE_OUTPUT_DIR, datasetName + "_" + encoding.name().toLowerCase() + ".tsfile");
          WriteBenchmarkResult benchmarkResult =
              benchmarkWrite(tsFilePath.toFile(), encoding, datasetFile, multiplier);

          writer.writeRecord(
              new String[] {
                datasetName,
                encoding.name(),
                String.valueOf(benchmarkResult.getTotalTimeNanos()),
                String.valueOf(benchmarkResult.getDatasetReadTimeNanos()),
                String.valueOf(benchmarkResult.getCpuTimeNanos()),
                String.valueOf(benchmarkResult.getIoTimeNanos()),
                String.valueOf(benchmarkResult.getIoWriteNanos()),
                String.valueOf(benchmarkResult.getIoFlushNanos()),
                String.valueOf(benchmarkResult.getIoForceNanos()),
                String.valueOf(datasetProfile.getPointCount()),
                String.valueOf(boundedPrecision),
                String.valueOf(multiplier),
                String.valueOf(benchmarkResult.getTsFileSizeBytes()),
                // tsFilePath.toString()
              });
        }
      }
    } finally {
      writer.close();
    }
  }

  @Test
  public void benchmarkTsFileReadEncodings() throws Exception {
    File tsFileDir = new File(TSFILE_OUTPUT_DIR);
    if (!tsFileDir.exists()) {
      throw new IOException("TsFile directory does not exist: " + TSFILE_OUTPUT_DIR);
    }

    File[] tsFiles = tsFileDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".tsfile"));
    if (tsFiles == null || tsFiles.length == 0) {
      throw new IOException("No tsfile files found under " + TSFILE_OUTPUT_DIR);
    }
    Arrays.sort(tsFiles, Comparator.comparing(File::getName));

    CsvWriter writer = new CsvWriter(READ_RESULT_CSV_PATH, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Read Total Time Nanos",
          "Read CPU Time Nanos",
          "Read IO Time Nanos",
          "Read IO Read Nanos",
          "Points",
          "TsFile Size Bytes",
          // "TsFile Path"
        });

    try {
      for (File tsFile : tsFiles) {
        ReadBenchmarkResult benchmarkResult = benchmarkRead(tsFile);
        String fileName = extractFileName(tsFile.getName());
        int splitIndex = fileName.lastIndexOf('_');
        String datasetName = splitIndex >= 0 ? fileName.substring(0, splitIndex) : fileName;
        String encodingName = splitIndex >= 0 ? fileName.substring(splitIndex + 1) : "unknown";

        System.out.printf("Reading tsfile=%s%n", tsFile.getName());

        if (datasetName.endsWith("_ts")) {
          datasetName = datasetName.substring(0, datasetName.length() - 3);
        }

        if (encodingName.equals("2diff")) {
          encodingName = "ts_2diff";
        }

        writer.writeRecord(
            new String[] {
              datasetName,
              encodingName.toUpperCase(),
              String.valueOf(benchmarkResult.getTotalTimeNanos()),
              String.valueOf(benchmarkResult.getCpuTimeNanos()),
              String.valueOf(benchmarkResult.getIoTimeNanos()),
              String.valueOf(benchmarkResult.getIoReadNanos()),
              String.valueOf(benchmarkResult.getPointCount()),
              String.valueOf(benchmarkResult.getTsFileSizeBytes()),
              // tsFile.toPath().toString()
            });
      }
    } finally {
      writer.close();
    }
  }

  private WriteBenchmarkResult benchmarkWrite(
      File tsFile, TSEncoding encoding, File datasetFile, long multiplier)
      throws IOException, WriteProcessException {
    long totalTimeNanos = 0;
    long totalDatasetReadTimeNanos = 0;
    long totalCpuTimeNanos = 0;
    long totalIoWriteNanos = 0;
    long totalIoFlushNanos = 0;
    long totalIoForceNanos = 0;

    for (int repeat = 0; repeat < REPEAT_TIMES; repeat++) {
      // System.out.printf(
      //     "    Write repeat %d/%d, file=%s, encoding=%s%n",
      //     repeat + 1, REPEAT_TIMES, datasetFile.getName(), encoding.name());
      Files.deleteIfExists(tsFile.toPath());
      WriteBenchmarkResult singleRunResult = writeTsFile(tsFile, encoding, datasetFile, multiplier);
      totalTimeNanos += singleRunResult.getTotalTimeNanos();
      totalDatasetReadTimeNanos += singleRunResult.getDatasetReadTimeNanos();
      totalCpuTimeNanos += singleRunResult.getCpuTimeNanos();
      totalIoWriteNanos += singleRunResult.getIoWriteNanos();
      totalIoFlushNanos += singleRunResult.getIoFlushNanos();
      totalIoForceNanos += singleRunResult.getIoForceNanos();
    }

    long tsFileSizeBytes = Files.size(tsFile.toPath());
    return new WriteBenchmarkResult(
        totalTimeNanos / REPEAT_TIMES,
      totalDatasetReadTimeNanos / REPEAT_TIMES,
        totalCpuTimeNanos / REPEAT_TIMES,
        totalIoWriteNanos / REPEAT_TIMES,
        totalIoFlushNanos / REPEAT_TIMES,
        totalIoForceNanos / REPEAT_TIMES,
        tsFileSizeBytes);
  }

  private ReadBenchmarkResult benchmarkRead(File tsFile) throws IOException {
    long totalTimeNanos = 0;
    long totalCpuTimeNanos = 0;
    long totalIoReadNanos = 0;
    long pointCount = -1;

    for (int repeat = 0; repeat < REPEAT_TIMES; repeat++) {
      // System.out.printf("    Read repeat %d/%d, file=%s%n", repeat + 1, REPEAT_TIMES, tsFile.getName());
      ReadBenchmarkResult singleRunResult = readTsFile(tsFile);
      totalTimeNanos += singleRunResult.getTotalTimeNanos();
      totalCpuTimeNanos += singleRunResult.getCpuTimeNanos();
      totalIoReadNanos += singleRunResult.getIoReadNanos();
      pointCount = singleRunResult.getPointCount();
    }

    return new ReadBenchmarkResult(
        totalTimeNanos / REPEAT_TIMES,
        totalCpuTimeNanos / REPEAT_TIMES,
        totalIoReadNanos / REPEAT_TIMES,
        pointCount,
        Files.size(tsFile.toPath()));
  }

  private WriteBenchmarkResult writeTsFile(
      File tsFile, TSEncoding encoding, File datasetFile, long multiplier)
      throws IOException, WriteProcessException {
    ProfilingTsFileOutput profilingOutput = new ProfilingTsFileOutput(tsFile);
    long encodeAndWriteTimeNanos = 0;
    long datasetReadTimeNanos = 0;

    try (TsFileWriter tsFileWriter = new TsFileWriter(profilingOutput, new Schema())) {
      tsFileWriter.registerTimeseries(
          new Path(deviceID),
          new MeasurementSchema(MEASUREMENT_NAME, TSDataType.INT32, encoding));
      try (InputStream inputStream = Files.newInputStream(datasetFile.toPath())) {
        CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
        long timestamp = 1L;
        try {
          while (true) {
            long readStartNanos = System.nanoTime();
            boolean hasRecord = loader.readRecord();
            datasetReadTimeNanos += System.nanoTime() - readStartNanos;
            if (!hasRecord) {
              break;
            }

            String value = getFirstColumnValue(loader);
            if (value == null) {
              continue;
            }

            long writeStartNanos = System.nanoTime();
            TSRecord record = new TSRecord(deviceID, timestamp++);
            record.addTuple(new IntDataPoint(MEASUREMENT_NAME, scaleValue(value, multiplier)));
            tsFileWriter.writeRecord(record);
            encodeAndWriteTimeNanos += System.nanoTime() - writeStartNanos;
          }
        } finally {
          loader.close();
        }
      }
    }

    long cpuTimeNanos = Math.max(0L, encodeAndWriteTimeNanos - profilingOutput.getIoTimeNanos());
    long totalTimeNanos = datasetReadTimeNanos + cpuTimeNanos + profilingOutput.getIoTimeNanos();
    return new WriteBenchmarkResult(
        totalTimeNanos,
        datasetReadTimeNanos,
        cpuTimeNanos,
        profilingOutput.getIoWriteNanos(),
        profilingOutput.getIoFlushNanos(),
        profilingOutput.getIoForceNanos(),
        Files.size(tsFile.toPath()));
  }

  private ReadBenchmarkResult readTsFile(File tsFile) throws IOException {
    ProfilingTsFileInput profilingInput = new ProfilingTsFileInput(tsFile);
    long startNanos = System.nanoTime();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(profilingInput);
        TsFileReader tsFileReader = new TsFileReader(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path(deviceID, MEASUREMENT_NAME, true));
      QueryExpression queryExpression = QueryExpression.create(paths, null);
      QueryDataSet queryDataSet = tsFileReader.query(queryExpression);

      long pointCount = 0;
      while (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        if (!rowRecord.getFields().isEmpty() && rowRecord.getFields().get(0) != null) {
          pointCount++;
        }
      }
      long totalTimeNanos = System.nanoTime() - startNanos;
      long cpuTimeNanos = Math.max(0L, totalTimeNanos - profilingInput.getIoTimeNanos());
      return new ReadBenchmarkResult(
          totalTimeNanos,
          cpuTimeNanos,
          profilingInput.getIoReadNanos(),
          pointCount,
          Files.size(tsFile.toPath()));
    }
  }

  private static int getDecimalPrecision(String str) {
    int decimalIndex = str.indexOf('.');
    if (decimalIndex == -1) {
      return 0;
    }
    return str.substring(decimalIndex + 1).length();
  }

  private static DatasetProfile analyzeDataset(File datasetFile) throws IOException {
    long pointCount = 0;
    int maxDecimalPrecision = 0;

    try (InputStream inputStream = Files.newInputStream(datasetFile.toPath())) {
      CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
      try {
        while (loader.readRecord()) {
          String value = getFirstColumnValue(loader);
          if (value == null) {
            continue;
          }

          pointCount++;
          maxDecimalPrecision = Math.max(maxDecimalPrecision, getDecimalPrecision(value));
        }
      } finally {
        loader.close();
      }
    }

    return new DatasetProfile(pointCount, maxDecimalPrecision);
  }

  private static String getFirstColumnValue(CsvReader loader) throws IOException {
    String[] values = loader.getValues();
    if (values.length == 0) {
      return null;
    }

    String value = values[0].trim();
    return value.isEmpty() ? null : value;
  }

  private static long getMultiplier(int decimalPrecision) {
    long multiplier = 1L;
    for (int i = 0; i < decimalPrecision; i++) {
      multiplier *= 10L;
    }
    return multiplier;
  }

  private static int scaleValue(String rawValue, long multiplier) {
    return new BigDecimal(rawValue).multiply(BigDecimal.valueOf(multiplier)).intValue();
  }

  private static String extractFileName(String path) {
    File file = new File(path);
    String fileName = file.getName();
    int dotIndex = fileName.lastIndexOf('.');
    if (dotIndex <= 0) {
      return fileName;
    }
    return fileName.substring(0, dotIndex);
  }

  private static final class WriteBenchmarkResult {
    private final long totalTimeNanos;
    private final long datasetReadTimeNanos;
    private final long cpuTimeNanos;
    private final long ioWriteNanos;
    private final long ioFlushNanos;
    private final long ioForceNanos;
    private final long tsFileSizeBytes;

    private WriteBenchmarkResult(
        long totalTimeNanos,
        long datasetReadTimeNanos,
        long cpuTimeNanos,
        long ioWriteNanos,
        long ioFlushNanos,
        long ioForceNanos,
        long tsFileSizeBytes) {
      this.totalTimeNanos = totalTimeNanos;
      this.datasetReadTimeNanos = datasetReadTimeNanos;
      this.cpuTimeNanos = cpuTimeNanos;
      this.ioWriteNanos = ioWriteNanos;
      this.ioFlushNanos = ioFlushNanos;
      this.ioForceNanos = ioForceNanos;
      this.tsFileSizeBytes = tsFileSizeBytes;
    }

    private long getTotalTimeNanos() {
      return totalTimeNanos;
    }

    private long getDatasetReadTimeNanos() {
      return datasetReadTimeNanos;
    }

    private long getCpuTimeNanos() {
      return cpuTimeNanos;
    }

    private long getIoTimeNanos() {
      return ioWriteNanos + ioFlushNanos + ioForceNanos;
    }

    private long getIoWriteNanos() {
      return ioWriteNanos;
    }

    private long getIoFlushNanos() {
      return ioFlushNanos;
    }

    private long getIoForceNanos() {
      return ioForceNanos;
    }

    private long getTsFileSizeBytes() {
      return tsFileSizeBytes;
    }
  }

  private static final class DatasetProfile {
    private final long pointCount;
    private final int maxDecimalPrecision;

    private DatasetProfile(long pointCount, int maxDecimalPrecision) {
      this.pointCount = pointCount;
      this.maxDecimalPrecision = maxDecimalPrecision;
    }

    private long getPointCount() {
      return pointCount;
    }

    private int getMaxDecimalPrecision() {
      return maxDecimalPrecision;
    }
  }

  private static final class ReadBenchmarkResult {
    private final long totalTimeNanos;
    private final long cpuTimeNanos;
    private final long ioReadNanos;
    private final long pointCount;
    private final long tsFileSizeBytes;

    private ReadBenchmarkResult(
        long totalTimeNanos,
        long cpuTimeNanos,
        long ioReadNanos,
        long pointCount,
        long tsFileSizeBytes) {
      this.totalTimeNanos = totalTimeNanos;
      this.cpuTimeNanos = cpuTimeNanos;
      this.ioReadNanos = ioReadNanos;
      this.pointCount = pointCount;
      this.tsFileSizeBytes = tsFileSizeBytes;
    }

    private long getTotalTimeNanos() {
      return totalTimeNanos;
    }

    private long getCpuTimeNanos() {
      return cpuTimeNanos;
    }

    private long getIoTimeNanos() {
      return ioReadNanos;
    }

    private long getIoReadNanos() {
      return ioReadNanos;
    }

    private long getPointCount() {
      return pointCount;
    }

    private long getTsFileSizeBytes() {
      return tsFileSizeBytes;
    }
  }

  private static final class ProfilingTsFileOutput extends OutputStream implements TsFileOutput {
    private final FileOutputStream outputStream;
    private final OutputStream bufferedStream;

    private long position;
    private long ioWriteNanos;
    private long ioFlushNanos;
    private long ioForceNanos;

    private ProfilingTsFileOutput(File file) throws IOException {
      this.outputStream = new FileOutputStream(file);
      this.bufferedStream = new NoSyncBufferedOutputStream(new TimedOutputStream());
    }

    @Override
    public void write(int b) throws IOException {
      bufferedStream.write(b);
      position++;
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte b) throws IOException {
      bufferedStream.write(b);
      position++;
    }

    @Override
    public void write(byte[] b, int start, int offset) throws IOException {
      bufferedStream.write(b, start, offset);
      position += offset;
    }

    @Override
    public void write(ByteBuffer byteBuffer) throws IOException {
      int remaining = byteBuffer.remaining();
      if (byteBuffer.hasArray()) {
        bufferedStream.write(
            byteBuffer.array(),
            byteBuffer.arrayOffset() + byteBuffer.position(),
            remaining);
        byteBuffer.position(byteBuffer.limit());
      } else {
        byte[] bytes = new byte[remaining];
        byteBuffer.get(bytes);
        bufferedStream.write(bytes);
      }
      position += remaining;
    }

    @Override
    public long getPosition() {
      return position;
    }

    @Override
    public void close() throws IOException {
      bufferedStream.close();
      outputStream.close();
    }

    @Override
    public OutputStream wrapAsStream() {
      return this;
    }

    @Override
    public void flush() throws IOException {
      bufferedStream.flush();
      long ioStartNanos = System.nanoTime();
      outputStream.flush();
      ioFlushNanos += System.nanoTime() - ioStartNanos;
    }

    @Override
    public void truncate(long size) throws IOException {
      bufferedStream.flush();
      outputStream.getChannel().truncate(size);
      position = outputStream.getChannel().position();
    }

    @Override
    public void force() throws IOException {
      flush();
      long ioStartNanos = System.nanoTime();
      outputStream.getFD().sync();
      ioForceNanos += System.nanoTime() - ioStartNanos;
    }

    private final class TimedOutputStream extends OutputStream {

      @Override
      public void write(int b) throws IOException {
        long ioStartNanos = System.nanoTime();
        try {
          outputStream.write(b);
        } finally {
          ioWriteNanos += System.nanoTime() - ioStartNanos;
        }
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        long ioStartNanos = System.nanoTime();
        try {
          outputStream.write(b, off, len);
        } finally {
          ioWriteNanos += System.nanoTime() - ioStartNanos;
        }
      }

      @Override
      public void flush() throws IOException {
        outputStream.flush();
      }

      @Override
      public void close() throws IOException {
        outputStream.close();
      }
    }

    private long getIoWriteNanos() {
      return ioWriteNanos;
    }

    private long getIoTimeNanos() {
      return ioWriteNanos + ioFlushNanos + ioForceNanos;
    }

    private long getIoFlushNanos() {
      return ioFlushNanos;
    }

    private long getIoForceNanos() {
      return ioForceNanos;
    }
  }

  private static final class ProfilingTsFileInput implements TsFileInput {
    private final TsFileInput input;

    private long ioReadNanos;

    private ProfilingTsFileInput(File file) throws IOException {
      this.input = new LocalTsFileInput(file.toPath());
    }

    @Override
    public long size() throws IOException {
      return input.size();
    }

    @Override
    public long position() throws IOException {
      return input.position();
    }

    @Override
    public TsFileInput position(long newPosition) throws IOException {
      input.position(newPosition);
      return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      long ioStartNanos = System.nanoTime();
      try {
        return input.read(dst);
      } finally {
        ioReadNanos += System.nanoTime() - ioStartNanos;
      }
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
      long ioStartNanos = System.nanoTime();
      try {
        return input.read(dst, position);
      } finally {
        ioReadNanos += System.nanoTime() - ioStartNanos;
      }
    }

    @Override
    public InputStream wrapAsInputStream() throws IOException {
      return input.wrapAsInputStream();
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    @Override
    public String getFilePath() {
      return input.getFilePath();
    }

    private long getIoReadNanos() {
      return ioReadNanos;
    }

    private long getIoTimeNanos() {
      return ioReadNanos;
    }
  }

}