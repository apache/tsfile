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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TablePointCountPerformanceTest {

  private static final String MODE_PROPERTY = "tablePointCountEnabled";
  private static final int TABLE1_ROW_COUNT = 200_000;
  private static final int TABLE2_ROW_COUNT = 100_000;
  private static final int BATCH_SIZE = 100;
  private static final long TABLE1_POINT_COUNT = 333_333;
  private static final long TABLE2_POINT_COUNT = 100_000;

  /**
   * Measures one table point-count mode per JVM. Invoke this test twice with {@code
   * -DtablePointCountEnabled=false} and {@code true}; each JVM performs its own warm-up and writes
   * a mode-specific metrics file. Once both files exist, the test generates a comparison report
   * containing the two JVM process IDs, file sizes, and measured write durations.
   */
  @Test
  public void measureTablePointCountInDedicatedJvm() throws IOException, WriteProcessException {
    String configuredMode = System.getProperty(MODE_PROPERTY);
    Assume.assumeTrue(
        "Performance test requires -D" + MODE_PROPERTY + "=true|false", configuredMode != null);
    Assert.assertTrue(
        "Invalid -D" + MODE_PROPERTY + " value: " + configuredMode,
        "true".equals(configuredMode) || "false".equals(configuredMode));
    boolean enabled = Boolean.parseBoolean(configuredMode);

    TableSchema tableSchema1 =
        new TableSchema(
            "table1",
            Arrays.asList(
                new ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                new ColumnSchema("s1", TSDataType.INT32, ColumnCategory.FIELD),
                new ColumnSchema("s2", TSDataType.INT32, ColumnCategory.FIELD)));
    TableSchema tableSchema2 =
        new TableSchema(
            "table2",
            Arrays.asList(
                new ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                new ColumnSchema("s1", TSDataType.INT32, ColumnCategory.FIELD)));
    List<Tablet> table1Tablets = createTablets(tableSchema1, TABLE1_ROW_COUNT, true);
    List<Tablet> table2Tablets = createTablets(tableSchema2, TABLE2_ROW_COUNT, false);

    File targetDirectory = new File("target");
    File warmUpFile = new File(targetDirectory, "table-point-count-warm-up-" + enabled + ".tsfile");
    File measuredFile = new File(targetDirectory, "table-point-count-" + enabled + ".tsfile");
    try {
      writeFile(warmUpFile, enabled, tableSchema1, tableSchema2, table1Tablets, table2Tablets);
      Assert.assertTrue(warmUpFile.delete());

      long startNanos = System.nanoTime();
      writeFile(measuredFile, enabled, tableSchema1, tableSchema2, table1Tablets, table2Tablets);
      long elapsedNanos = System.nanoTime() - startNanos;
      Assert.assertTrue(elapsedNanos > 0);

      verifyProperties(measuredFile, enabled);
      long pid = ProcessHandle.current().pid();
      writeMetrics(targetDirectory, enabled, pid, measuredFile.length(), elapsedNanos);
      generateReportIfBothModesExist(targetDirectory);
      System.out.printf(
          "Table point count enabled=%s: pid=%d, size=%d bytes, write=%d ns (%.3f ms)%n",
          enabled, pid, measuredFile.length(), elapsedNanos, elapsedNanos / 1_000_000.0);
    } finally {
      warmUpFile.delete();
      measuredFile.delete();
    }
  }

  private void writeFile(
      File file,
      boolean enabled,
      TableSchema tableSchema1,
      TableSchema tableSchema2,
      List<Tablet> table1Tablets,
      List<Tablet> table2Tablets)
      throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(new TsFileIOWriter(file, enabled))) {
      writer.registerTableSchema(tableSchema1);
      writer.registerTableSchema(tableSchema2);
      for (Tablet tablet : table1Tablets) {
        writer.writeTable(tablet);
      }
      for (Tablet tablet : table2Tablets) {
        writer.writeTable(tablet);
      }
    }
  }

  private List<Tablet> createTablets(
      TableSchema tableSchema, int rowCount, boolean nullableSecondField) {
    List<String> columnNames =
        IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas());
    List<TSDataType> dataTypes = IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas());
    List<Tablet> tablets = new ArrayList<>((rowCount + BATCH_SIZE - 1) / BATCH_SIZE);
    for (int startRow = 0; startRow < rowCount; startRow += BATCH_SIZE) {
      int currentBatchSize = Math.min(BATCH_SIZE, rowCount - startRow);
      Tablet tablet =
          new Tablet(
              tableSchema.getTableName(),
              columnNames,
              dataTypes,
              tableSchema.getColumnTypes(),
              currentBatchSize);
      for (int row = 0; row < currentBatchSize; row++) {
        int globalRow = startRow + row;
        tablet.addTimestamp(row, globalRow);
        tablet.addValue("device", row, "d1");
        tablet.addValue("s1", row, globalRow);
        if (nullableSecondField) {
          tablet.addValue("s2", row, globalRow % 3 == 0 ? null : globalRow);
        }
      }
      tablets.add(tablet);
    }
    return tablets;
  }

  private void verifyProperties(File file, boolean enabled) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      Map<String, byte[]> properties = reader.getTsFileProperties();
      String table1Key = TsFileIOWriter.TABLE_POINT_COUNT_PROPERTY_PREFIX + "table1";
      String table2Key = TsFileIOWriter.TABLE_POINT_COUNT_PROPERTY_PREFIX + "table2";
      if (enabled) {
        Assert.assertArrayEquals(
            BytesUtils.longToBytes(TABLE1_POINT_COUNT), properties.get(table1Key));
        Assert.assertArrayEquals(
            BytesUtils.longToBytes(TABLE2_POINT_COUNT), properties.get(table2Key));
      } else {
        Assert.assertFalse(properties.containsKey(table1Key));
        Assert.assertFalse(properties.containsKey(table2Key));
      }
    }
  }

  private void writeMetrics(
      File targetDirectory, boolean enabled, long pid, long fileSize, long elapsedNanos)
      throws IOException {
    Properties metrics = new Properties();
    metrics.setProperty("enabled", Boolean.toString(enabled));
    metrics.setProperty("pid", Long.toString(pid));
    metrics.setProperty("fileSize", Long.toString(fileSize));
    metrics.setProperty("elapsedNanos", Long.toString(elapsedNanos));
    metrics.setProperty("rows", Integer.toString(TABLE1_ROW_COUNT + TABLE2_ROW_COUNT));
    metrics.setProperty("points", Long.toString(TABLE1_POINT_COUNT + TABLE2_POINT_COUNT));
    try (FileOutputStream output = new FileOutputStream(metricsFile(targetDirectory, enabled))) {
      metrics.store(output, "Table point count performance metrics");
    }
  }

  private void generateReportIfBothModesExist(File targetDirectory) throws IOException {
    File disabledMetricsFile = metricsFile(targetDirectory, false);
    File enabledMetricsFile = metricsFile(targetDirectory, true);
    if (!disabledMetricsFile.isFile() || !enabledMetricsFile.isFile()) {
      return;
    }
    Properties disabled = loadMetrics(disabledMetricsFile);
    Properties enabled = loadMetrics(enabledMetricsFile);
    long disabledPid = Long.parseLong(disabled.getProperty("pid"));
    long enabledPid = Long.parseLong(enabled.getProperty("pid"));
    Assert.assertNotEquals("Modes must run in separate JVMs", disabledPid, enabledPid);
    long disabledSize = Long.parseLong(disabled.getProperty("fileSize"));
    long enabledSize = Long.parseLong(enabled.getProperty("fileSize"));
    long disabledNanos = Long.parseLong(disabled.getProperty("elapsedNanos"));
    long enabledNanos = Long.parseLong(enabled.getProperty("elapsedNanos"));
    Assert.assertTrue(enabledSize > disabledSize);

    String report =
        String.format(
            "# Table Point Count Performance Report%n%n"
                + "## Dataset%n%n"
                + "- table1: %,d rows, %,d non-null FIELD points%n"
                + "- table2: %,d rows, %,d non-null FIELD points%n"
                + "- Batch size: %,d rows%n"
                + "- Total: %,d rows, %,d non-null FIELD points%n%n"
                + "## Results%n%n"
                + "| Point counting | JVM PID | File size (bytes) | Write time (ns) | Write time (ms) |%n"
                + "|---|---:|---:|---:|---:|%n"
                + "| Disabled | %d | %,d | %,d | %.3f |%n"
                + "| Enabled | %d | %,d | %,d | %.3f |%n%n"
                + "- File-size delta: %,d bytes%n"
                + "- File-size overhead: %.3f%%%n"
                + "- Write-time delta: %,d ns (%.3f ms)%n"
                + "- Write-time overhead: %.3f%%%n%n"
                + "> Each mode ran in a separate JVM and performed an independent warm-up. Write "
                + "time includes writer construction, schema registration, batched writes, "
                + "metadata serialization, force, and close. Timing is diagnostic and is not used "
                + "as a CI pass/fail threshold.%n",
            TABLE1_ROW_COUNT,
            TABLE1_POINT_COUNT,
            TABLE2_ROW_COUNT,
            TABLE2_POINT_COUNT,
            BATCH_SIZE,
            TABLE1_ROW_COUNT + TABLE2_ROW_COUNT,
            TABLE1_POINT_COUNT + TABLE2_POINT_COUNT,
            disabledPid,
            disabledSize,
            disabledNanos,
            disabledNanos / 1_000_000.0,
            enabledPid,
            enabledSize,
            enabledNanos,
            enabledNanos / 1_000_000.0,
            enabledSize - disabledSize,
            (enabledSize - disabledSize) * 100.0 / disabledSize,
            enabledNanos - disabledNanos,
            (enabledNanos - disabledNanos) / 1_000_000.0,
            (enabledNanos - disabledNanos) * 100.0 / disabledNanos);
    File reportFile = new File(targetDirectory, "table-point-count-performance-report.md");
    Files.write(reportFile.toPath(), report.getBytes(StandardCharsets.UTF_8));
    System.out.printf("Table point-count report: %s%n", reportFile.getAbsolutePath());
  }

  private Properties loadMetrics(File file) throws IOException {
    Properties metrics = new Properties();
    try (FileInputStream input = new FileInputStream(file)) {
      metrics.load(input);
    }
    return metrics;
  }

  private File metricsFile(File targetDirectory, boolean enabled) {
    return new File(targetDirectory, "table-point-count-" + enabled + ".properties");
  }
}
