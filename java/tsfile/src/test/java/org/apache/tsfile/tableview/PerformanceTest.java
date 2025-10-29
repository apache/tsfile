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

package org.apache.tsfile.tableview;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.ITsFileTreeReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.read.v4.TsFileTreeReaderBuilder;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.TsFileTreeWriter;
import org.apache.tsfile.write.v4.TsFileTreeWriterBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Performance test comparing TreeV4 and Table modes in TsFile with microsecond precision. */
public class PerformanceTest {

  // Test configuration constants - default values set to 10
  private final String testDir = "target" + File.separator + "tableViewTest";
  private int measurementSchemaCnt = 100; // Number of measurement schemas (data points) per device
  private int tableCnt = 10; // Number of tables in the database
  private int devicePerTable = 10; // Number of devices allocated per table
  private int tabletCnt = 10; // Number of tablets each device writes to
  private int pointPerSeries = 10; // Number of data points per time series within a single tablet
  private final int idSchemaCnt = 3; // Number of ID schemas

  private List<IMeasurementSchema> idSchemas;
  private List<IMeasurementSchema> measurementSchemas;

  public static void main(String[] args) throws Exception {
    final PerformanceTest test = new PerformanceTest();
    test.initSchemas();

    // Test different variable values
    int[] testValues = {10, 20, 30, 40, 50};

    System.out.println("=== Controlled Variable Performance Test (μs precision) ===");

    // Test measurementSchemaCnt impact
    System.out.println(
        "\n--- Testing measurementSchemaCnt impact (tableCnt=10, devicePerTable=10, pointPerSeries=10, tabletCnt=10) ---");
    for (int value : testValues) {
      test.measurementSchemaCnt = value;
      test.tableCnt = 10;
      test.devicePerTable = 10;
      test.pointPerSeries = 10;
      test.tabletCnt = 10;
      test.initSchemas(); // Reinitialize schemas with new measurement count
      test.runSingleTest("measurementSchemaCnt=" + value);
    }

    // Test tableCnt impact
    System.out.println(
        "\n--- Testing tableCnt impact (measurementSchemaCnt=100, devicePerTable=10, pointPerSeries=10, tabletCnt=10) ---");
    for (int value : testValues) {
      test.measurementSchemaCnt = 100;
      test.tableCnt = value;
      test.devicePerTable = 10;
      test.pointPerSeries = 10;
      test.tabletCnt = 10;
      test.initSchemas();
      test.runSingleTest("tableCnt=" + value);
    }

    // Test devicePerTable impact
    System.out.println(
        "\n--- Testing devicePerTable impact (measurementSchemaCnt=100, tableCnt=10, pointPerSeries=10, tabletCnt=10) ---");
    for (int value : testValues) {
      test.measurementSchemaCnt = 100;
      test.tableCnt = 10;
      test.devicePerTable = value;
      test.pointPerSeries = 10;
      test.tabletCnt = 10;
      test.initSchemas();
      test.runSingleTest("devicePerTable=" + value);
    }

    // Test pointPerSeries impact
    System.out.println(
        "\n--- Testing pointPerSeries impact (measurementSchemaCnt=100, tableCnt=10, devicePerTable=10, tabletCnt=10) ---");
    for (int value : testValues) {
      test.measurementSchemaCnt = 100;
      test.tableCnt = 10;
      test.devicePerTable = 10;
      test.pointPerSeries = value;
      test.tabletCnt = 10;
      test.initSchemas();
      test.runSingleTest("pointPerSeries=" + value);
    }

    // Test tabletCnt impact
    System.out.println(
        "\n--- Testing tabletCnt impact (measurementSchemaCnt=100, tableCnt=10, devicePerTable=10, pointPerSeries=10) ---");
    for (int value : testValues) {
      test.measurementSchemaCnt = 100;
      test.tableCnt = 10;
      test.devicePerTable = 10;
      test.pointPerSeries = 10;
      test.tabletCnt = value;
      test.initSchemas();
      test.runSingleTest("tabletCnt=" + value);
    }
  }

  /** Run single test and output results in microseconds */
  private void runSingleTest(String testCase) throws Exception {
    System.out.println("\nTest Configuration: " + testCase);
    System.out.printf(
        "measurementSchemaCnt=%d, tableCnt=%d, devicePerTable=%d, pointPerSeries=%d, tabletCnt=%d%n",
        measurementSchemaCnt, tableCnt, devicePerTable, pointPerSeries, tabletCnt);

    int repetitionCnt = 10;
    int warmupRuns = repetitionCnt / 2;

    // Test TreeV4 mode
    testMode("TreeV4", repetitionCnt, warmupRuns);

    // Test Table mode
    testMode("Table", repetitionCnt, warmupRuns);
  }

  /** Tests a specific mode for multiple repetitions */
  private void testMode(String mode, int totalRuns, int warmupRuns) throws Exception {
    List<Long> registerTimeList = new ArrayList<>();
    List<Long> writeTimeList = new ArrayList<>();
    List<Long> closeTimeList = new ArrayList<>();
    List<Long> queryTimeList = new ArrayList<>();
    List<Long> fileSizeList = new ArrayList<>();

    for (int i = 0; i < totalRuns; i++) {
      switch (mode) {
        case "TreeV4":
          testTreeV4(registerTimeList, writeTimeList, closeTimeList, queryTimeList, fileSizeList);
          break;
        case "Table":
          testTable(registerTimeList, writeTimeList, closeTimeList, queryTimeList, fileSizeList);
          break;
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    calculateAndPrintStats(
        mode,
        registerTimeList,
        writeTimeList,
        closeTimeList,
        queryTimeList,
        fileSizeList,
        warmupRuns,
        totalRuns);
  }

  /** Calculates and prints performance statistics in microseconds */
  private void calculateAndPrintStats(
      String mode,
      List<Long> registerList,
      List<Long> writeList,
      List<Long> closeList,
      List<Long> queryList,
      List<Long> sizeList,
      int warmupRuns,
      int totalRuns) {
    List<Long> registerSub = registerList.subList(warmupRuns, totalRuns);
    List<Long> writeSub = writeList.subList(warmupRuns, totalRuns);
    List<Long> closeSub = closeList.subList(warmupRuns, totalRuns);
    List<Long> querySub = queryList.subList(warmupRuns, totalRuns);
    List<Long> sizeSub = sizeList.subList(warmupRuns, totalRuns);

    final double registerTime = registerSub.stream().mapToLong(l -> l).average().orElse(0.0);
    final double writeTime = writeSub.stream().mapToLong(l -> l).average().orElse(0.0);
    final double closeTime = closeSub.stream().mapToLong(l -> l).average().orElse(0.0);
    final double queryTime = querySub.stream().mapToLong(l -> l).average().orElse(0.0);
    final double fileSize = sizeSub.stream().mapToLong(l -> l).average().orElse(0.0);

    System.out.printf(
        "[%s] Reg:%.1fμs Write:%.1fμs Close:%.1fμs Query:%.1fμs Size:%.1fKB%n",
        mode,
        registerTime / 1_000.0, // Convert ns to μs
        writeTime / 1_000.0,
        closeTime / 1_000.0,
        queryTime / 1_000.0,
        fileSize / 1024.0);
  }

  /** Tests the TreeV4 mode performance with microsecond timing */
  private void testTreeV4(
      List<Long> registerTimeList,
      List<Long> writeTimeList,
      List<Long> closeTimeList,
      List<Long> queryTimeList,
      List<Long> fileSizeList)
      throws IOException {
    long registerTimeSum = 0;
    long writeTimeSum = 0;
    long closeTimeSum = 0;
    long queryTimeSum = 0;
    long startTime;

    final File file = initFile("test_v4_tree");
    List<String> allDeviceIds = new ArrayList<>();

    try (TsFileTreeWriter writer = new TsFileTreeWriterBuilder().file(file).build()) {
      // Register schemas for all devices
      startTime = System.nanoTime();
      for (int tableNum = 0; tableNum < tableCnt; tableNum++) {
        for (int deviceNum = 0; deviceNum < devicePerTable; deviceNum++) {
          String deviceId = genTreeDeviceId(tableNum, deviceNum).toString();
          allDeviceIds.add(deviceId);
          writer.registerAlignedTimeseries(deviceId, measurementSchemas);
        }
      }
      registerTimeSum = System.nanoTime() - startTime;

      // Write data tablets for all devices
      Tablet tablet = new Tablet(null, measurementSchemas, pointPerSeries);
      for (int tableNum = 0; tableNum < tableCnt; tableNum++) {
        for (int deviceNum = 0; deviceNum < devicePerTable; deviceNum++) {
          for (int tabletNum = 0; tabletNum < tabletCnt; tabletNum++) {
            fillTreeTablet(tablet, tableNum, deviceNum, tabletNum);
            startTime = System.nanoTime();
            writer.write(tablet);
            writeTimeSum += System.nanoTime() - startTime;
          }
        }
      }

      // Measure close time
      startTime = System.nanoTime();
    } catch (WriteProcessException e) {
      throw new RuntimeException(e);
    }
    closeTimeSum = System.nanoTime() - startTime;

    long fileSize = file.length();

    // Query performance test
    startTime = System.nanoTime();
    try (ITsFileTreeReader reader = new TsFileTreeReaderBuilder().file(file).build()) {
      List<String> measurementNames =
          measurementSchemas.stream()
              .map(IMeasurementSchema::getMeasurementName)
              .collect(Collectors.toList());

      ResultSet resultSet =
          reader.query(allDeviceIds, measurementNames, Long.MIN_VALUE, Long.MAX_VALUE);
      while (resultSet.next()) {
        // Consume all results to ensure complete query execution
      }
    }
    queryTimeSum = System.nanoTime() - startTime;

    // Clean up test file
    file.delete();

    // Record metrics for this run
    registerTimeList.add(registerTimeSum);
    writeTimeList.add(writeTimeSum);
    closeTimeList.add(closeTimeSum);
    queryTimeList.add(queryTimeSum);
    fileSizeList.add(fileSize);
  }

  /** Tests the Table mode performance with microsecond timing */
  private void testTable(
      List<Long> registerTimeList,
      List<Long> writeTimeList,
      List<Long> closeTimeList,
      List<Long> queryTimeList,
      List<Long> fileSizeList)
      throws IOException, WriteProcessException, ReadProcessException {
    long registerTimeSum = 0;
    long writeTimeSum = 0;
    long closeTimeSum = 0;
    long queryTimeSum = 0;
    long startTime;

    final File file = initFile("test_table");
    TsFileWriter tsFileWriter = new TsFileWriter(file);
    try {
      // Register table schemas
      startTime = System.nanoTime();
      registerTable(tsFileWriter);
      registerTimeSum = System.nanoTime() - startTime;

      // Write data tablets
      Tablet tablet = initTableTablet();
      for (int tableNum = 0; tableNum < tableCnt; tableNum++) {
        for (int deviceNum = 0; deviceNum < devicePerTable; deviceNum++) {
          for (int tabletNum = 0; tabletNum < tabletCnt; tabletNum++) {
            fillTableTablet(tablet, tableNum, deviceNum, tabletNum);
            startTime = System.nanoTime();
            tsFileWriter.writeTable(
                tablet,
                Collections.singletonList(new Pair<>(tablet.getDeviceID(0), tablet.getRowSize())));
            writeTimeSum += System.nanoTime() - startTime;
          }
        }
      }
    } finally {
      // Measure close time
      startTime = System.nanoTime();
      tsFileWriter.close();
      closeTimeSum = System.nanoTime() - startTime;
    }
    long fileSize = file.length();

    // Query performance test
    startTime = System.nanoTime();
    for (int tableNum = 0; tableNum < tableCnt; tableNum++) {
      String tableName = genTableName(tableNum);
      List<String> columnNames = new ArrayList<>();
      idSchemas.stream().map(IMeasurementSchema::getMeasurementName).forEach(columnNames::add);
      measurementSchemas.stream()
          .map(IMeasurementSchema::getMeasurementName)
          .forEach(columnNames::add);

      try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build()) {
        ResultSet resultSet = reader.query(tableName, columnNames, Long.MIN_VALUE, Long.MAX_VALUE);
        while (resultSet.next()) {
          // Consume all results to ensure complete query execution
        }
      }
    }
    queryTimeSum = System.nanoTime() - startTime;

    // Clean up test file
    file.delete();

    // Record metrics for this run
    registerTimeList.add(registerTimeSum);
    writeTimeList.add(writeTimeSum);
    closeTimeList.add(closeTimeSum);
    queryTimeList.add(queryTimeSum);
    fileSizeList.add(fileSize);
  }

  // ============ Helper Methods ============

  /** Initialize schemas based on current configuration */
  private void initSchemas() {
    idSchemas = new ArrayList<>(idSchemaCnt);
    for (int i = 0; i < idSchemaCnt; i++) {
      idSchemas.add(
          new MeasurementSchema(
              "id" + i, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
    }

    measurementSchemas = new ArrayList<>();
    for (int i = 0; i < measurementSchemaCnt; i++) {
      measurementSchemas.add(
          new MeasurementSchema(
              "s" + i, TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4));
    }
  }

  private File initFile(String prefix) throws IOException {
    File dir = new File(testDir);
    dir.mkdirs();
    return new File(dir, prefix + "_" + System.currentTimeMillis() + ".tsfile");
  }

  private void fillTreeTablet(Tablet tablet, int tableNum, int deviceNum, int tabletNum) {
    tablet.setDeviceId(genTreeDeviceId(tableNum, deviceNum).toString());
    for (int i = 0; i < measurementSchemaCnt; i++) {
      for (int valNum = 0; valNum < pointPerSeries; valNum++) {
        tablet.addValue(valNum, i, (long) tabletNum * pointPerSeries + valNum);
      }
    }
    for (int valNum = 0; valNum < pointPerSeries; valNum++) {
      tablet.addTimestamp(valNum, tabletNum * pointPerSeries + valNum);
    }
  }

  private Tablet initTableTablet() {
    List<IMeasurementSchema> allSchema = new ArrayList<>(idSchemas);
    allSchema.addAll(measurementSchemas);

    List<ColumnCategory> columnCategories = new ArrayList<>();
    for (int i = 0; i < idSchemaCnt; i++) {
      columnCategories.add(ColumnCategory.TAG);
    }
    for (int i = 0; i < measurementSchemaCnt; i++) {
      columnCategories.add(ColumnCategory.FIELD);
    }

    return new Tablet(
        null,
        IMeasurementSchema.getMeasurementNameList(allSchema),
        IMeasurementSchema.getDataTypeList(allSchema),
        columnCategories,
        pointPerSeries);
  }

  private void fillTableTablet(Tablet tablet, int tableNum, int deviceNum, int tabletNum) {
    IDeviceID deviceID = genTableDeviceId(tableNum, deviceNum);
    tablet.setTableName(deviceID.segment(0).toString());
    for (int i = 0; i < idSchemaCnt; i++) {
      for (int rowNum = 0; rowNum < pointPerSeries; rowNum++) {
        tablet.addValue(rowNum, i, deviceID.segment(i + 1).toString());
      }
    }
    for (int i = 0; i < measurementSchemaCnt; i++) {
      for (int valNum = 0; valNum < pointPerSeries; valNum++) {
        tablet.addValue(valNum, i + idSchemaCnt, (long) (tabletNum * pointPerSeries + valNum));
      }
    }
    for (int valNum = 0; valNum < pointPerSeries; valNum++) {
      tablet.addTimestamp(valNum, tabletNum * pointPerSeries + valNum);
    }
  }

  private void registerTable(TsFileWriter writer) {
    for (int i = 0; i < tableCnt; i++) {
      TableSchema tableSchema = genTableSchema(i);
      writer.registerTableSchema(tableSchema);
    }
  }

  private TableSchema genTableSchema(int tableNum) {
    List<IMeasurementSchema> allSchemas = new ArrayList<>(idSchemas);
    allSchemas.addAll(measurementSchemas);

    List<ColumnCategory> columnCategories = new ArrayList<>();
    for (int i = 0; i < idSchemaCnt; i++) {
      columnCategories.add(ColumnCategory.TAG);
    }
    for (int i = 0; i < measurementSchemaCnt; i++) {
      columnCategories.add(ColumnCategory.FIELD);
    }

    return new TableSchema(genTableName(tableNum), allSchemas, columnCategories);
  }

  private String genTableName(int tableNum) {
    return "table_" + tableNum;
  }

  private IDeviceID genTableDeviceId(int tableNum, int deviceNum) {
    String[] idSegments = new String[idSchemaCnt + 1];
    idSegments[0] = genTableName(tableNum);
    for (int i = 0; i < idSchemaCnt; i++) {
      idSegments[i + 1] = "d0";
    }
    idSegments[idSchemaCnt] = "d" + deviceNum;
    return new StringArrayDeviceID(idSegments);
  }

  private IDeviceID genTreeDeviceId(int tableNum, int deviceNum) {
    String[] idSegments = new String[idSchemaCnt + 1];
    idSegments[0] = genTableName(tableNum);
    for (int i = 0; i < idSchemaCnt; i++) {
      idSegments[i + 1] = "d0";
    }
    idSegments[idSchemaCnt] = "d" + deviceNum;
    return new PlainDeviceID(String.join(".", idSegments));
  }
}
