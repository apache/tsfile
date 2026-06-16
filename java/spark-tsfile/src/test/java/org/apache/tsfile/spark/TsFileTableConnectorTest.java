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

package org.apache.tsfile.spark;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TsFileTableConnectorTest {

  private static SparkSession spark;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void startSpark() {
    Assume.assumeTrue(
        "Spark 3.x with Hadoop 3.3 cannot start on JDK "
            + Runtime.version().feature()
            + " because javax.security.auth.Subject.getSubject is unsupported",
        Runtime.version().feature() < 24);
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("tsfile-spark-connector-test")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
  }

  @AfterClass
  public static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void parseReadAndWriteOptions() {
    Map<String, String> readOptions = new HashMap<>();
    readOptions.put("path", "/tmp/table.tsfile");
    readOptions.put("model", "table");
    readOptions.put("table", "Weather");
    readOptions.put("timestampAs", "timestamp");
    readOptions.put("timestampPrecision", "us");

    TsFileTableOptions parsedRead =
        TsFileTableOptions.forRead(new CaseInsensitiveStringMap(readOptions));
    assertEquals("/tmp/table.tsfile", parsedRead.path());
    assertEquals("weather", parsedRead.table());
    assertEquals(TsFileTableOptions.TimestampAs.TIMESTAMP, parsedRead.timestampAs());
    assertEquals(TsFileTableOptions.TimestampPrecision.US, parsedRead.timestampPrecision());

    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put("path", "/tmp/out");
    writeOptions.put("table", "Weather");
    writeOptions.put("tagColumns", "City, Station");
    writeOptions.put("fieldColumns", "Temperature");
    writeOptions.put("nullTagPolicy", "error");
    TsFileTableOptions parsedWrite =
        TsFileTableOptions.forWrite(new CaseInsensitiveStringMap(writeOptions));
    assertEquals(Arrays.asList("City", "Station"), parsedWrite.tagColumns());
    assertEquals(Collections.singletonList("Temperature"), parsedWrite.fieldColumns());

    assertFailsContaining(
        "Unsupported TsFile connector option",
        () -> {
          Map<String, String> invalid = new HashMap<>(readOptions);
          invalid.put("unknown", "x");
          TsFileTableOptions.forRead(new CaseInsensitiveStringMap(invalid));
        });
    assertFailsContaining(
        "Only TsFile table model is supported",
        () -> {
          Map<String, String> invalid = new HashMap<>(readOptions);
          invalid.put("model", "tree");
          TsFileTableOptions.forRead(new CaseInsensitiveStringMap(invalid));
        });
  }

  @Test
  public void missingTableOnWriteFails() {
    Dataset<Row> rows = spark.createDataFrame(sampleRows(), sampleSchema());
    assertFailsContaining(
        "requires option \"table\"",
        () ->
            rows.write()
                .format("tsfile")
                .option("tagColumns", "city")
                .mode(SaveMode.Append)
                .save(temporaryFolder.newFolder("missing-table").getAbsolutePath()));
  }

  @Test
  public void singleTableFileCanInferTable() throws Exception {
    File file = temporaryFolder.newFile("single.tsfile");
    writeWeatherFile(file, "Weather", 0);

    Dataset<Row> df = spark.read().format("tsfile").load(file.getAbsolutePath());

    assertEquals(3, df.count());
    assertEquals(
        Arrays.asList("time", "city", "temperature", "humidity"),
        Arrays.asList(df.schema().fieldNames()));
  }

  @Test
  public void multiTableFileRequiresTableOption() throws Exception {
    File file = temporaryFolder.newFile("multi.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writeWeatherTable(writer, "weather", 0);
      writeWeatherTable(writer, "traffic", 10);
    }

    assertFailsContaining(
        "multiple tables",
        () -> spark.read().format("tsfile").load(file.getAbsolutePath()).count());
  }

  @Test
  public void lowerCaseDuplicateColumnsFail() {
    StructType schema =
        new StructType()
            .add("time", DataTypes.LongType, false)
            .add("City", DataTypes.StringType, false)
            .add("city", DataTypes.IntegerType, true);
    assertFailsContaining(
        "Duplicate DataFrame column",
        () -> TsFileTableWriteContext.build(writeOptions("/tmp/out", "City"), schema));
  }

  @Test
  public void nonStringTagColumnFails() {
    StructType schema =
        new StructType()
            .add("time", DataTypes.LongType, false)
            .add("city", DataTypes.IntegerType, false)
            .add("temperature", DataTypes.IntegerType, true);
    assertFailsContaining(
        "TAG column must be StringType",
        () -> TsFileTableWriteContext.build(writeOptions("/tmp/out", "city"), schema));
  }

  @Test
  public void nullTagWriteFails() {
    StructType schema =
        new StructType()
            .add("time", DataTypes.LongType, false)
            .add("city", DataTypes.StringType, true)
            .add("temperature", DataTypes.IntegerType, true);
    Dataset<Row> rows =
        spark.createDataFrame(Collections.singletonList(RowFactory.create(1L, null, 10)), schema);

    assertFailsContaining(
        "TAG column must not be null",
        () ->
            rows.write()
                .format("tsfile")
                .option("table", "weather")
                .option("tagColumns", "city")
                .mode(SaveMode.Append)
                .save(temporaryFolder.newFolder("null-tag").getAbsolutePath()));
  }

  @Test
  public void unsupportedCategoryFails() {
    TableSchema schema =
        tableSchema(
            "weather",
            new ColumnSpec("city", TSDataType.STRING, ColumnCategory.TAG),
            new ColumnSpec("attr", TSDataType.STRING, ColumnCategory.ATTRIBUTE));
    assertFailsContaining(
        "Column category ATTRIBUTE is not supported",
        () ->
            TsFileTableSchema.fromTableSchema(schema, "time", TsFileTableOptions.TimestampAs.LONG));
  }

  @Test
  public void readsSingleFileAndSparseFieldNulls() throws Exception {
    File file = temporaryFolder.newFile("sparse.tsfile");
    writeWeatherFile(file, "weather", 0);

    List<Row> rows =
        spark
            .read()
            .format("tsfile")
            .option("table", "weather")
            .load(file.getAbsolutePath())
            .orderBy("time")
            .collectAsList();

    assertEquals(3, rows.size());
    assertEquals(20, rows.get(0).getInt(rows.get(0).fieldIndex("temperature")));
    assertTrue(rows.get(1).isNullAt(rows.get(1).fieldIndex("temperature")));
    assertEquals(32L, rows.get(2).getLong(rows.get(2).fieldIndex("humidity")));
  }

  @Test
  public void readsDirectoryOfMultipleFiles() throws Exception {
    File directory = temporaryFolder.newFolder("multi-file");
    writeWeatherFile(new File(directory, "part-a.tsfile"), "weather", 0);
    writeWeatherFile(new File(directory, "part-b.tsfile"), "weather", 100);

    Dataset<Row> df =
        spark.read().format("tsfile").option("table", "weather").load(directory.getAbsolutePath());

    assertEquals(6, df.count());
    assertEquals(1, df.where("time = 100").count());
  }

  @Test
  public void incompatibleSchemaFails() throws Exception {
    File directory = temporaryFolder.newFolder("incompatible");
    writeWeatherFile(new File(directory, "part-a.tsfile"), "weather", 0);
    writeTsFile(
        new File(directory, "part-b.tsfile"),
        "weather",
        new ColumnSpec("city", TSDataType.STRING, ColumnCategory.TAG),
        new ColumnSpec("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD));

    assertFailsContaining(
        "Incompatible TsFile table schema",
        () ->
            spark
                .read()
                .format("tsfile")
                .option("table", "weather")
                .load(directory.getAbsolutePath())
                .count());
  }

  @Test
  public void supportsColumnPruningAndTimeTagOnlyProjection() throws Exception {
    File file = temporaryFolder.newFile("projection.tsfile");
    writeWeatherFile(file, "weather", 0);

    List<Row> projected =
        spark
            .read()
            .format("tsfile")
            .option("table", "weather")
            .load(file.getAbsolutePath())
            .select("time", "city")
            .orderBy("time")
            .collectAsList();

    assertEquals(2, projected.get(0).size());
    assertEquals(0L, projected.get(0).getLong(0));
    assertEquals("beijing", projected.get(0).getString(1));

    TsFileTableSchema tableSchema =
        TsFileTableSchemaInferer.infer(readOptions(file.getAbsolutePath(), "weather"))
            .tableSchema();
    StructType requiredSchema =
        new StructType().add("time", DataTypes.LongType, false).add("city", DataTypes.StringType);
    TsFileTableReadContext context =
        new TsFileTableReadContext(
            readOptions(file.getAbsolutePath(), "weather"),
            Collections.singletonList(file.getAbsolutePath()),
            tableSchema,
            requiredSchema,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            Collections.emptyMap());
    assertEquals(Arrays.asList("city", "temperature"), context.queryColumns());
    assertEquals("temperature", context.hiddenFieldColumn());
  }

  @Test
  public void roundTripWriteAndRead() throws Exception {
    File output = temporaryFolder.newFolder("round-trip");
    Dataset<Row> rows = spark.createDataFrame(sampleRows(), sampleSchema());

    rows.write()
        .format("tsfile")
        .option("table", "weather")
        .option("tagColumns", "city")
        .mode(SaveMode.Append)
        .save(output.getAbsolutePath());

    List<Row> readRows =
        spark
            .read()
            .format("tsfile")
            .option("table", "weather")
            .load(output.getAbsolutePath())
            .orderBy("time")
            .collectAsList();

    assertEquals(3, readRows.size());
    assertEquals("beijing", readRows.get(0).getString(readRows.get(0).fieldIndex("city")));
    assertEquals(20, readRows.get(0).getInt(readRows.get(0).fieldIndex("temperature")));
    assertTrue(readRows.get(1).isNullAt(readRows.get(1).fieldIndex("temperature")));
  }

  @Test
  public void writesOneTsFilePerNonEmptySparkPartition() throws Exception {
    File output = temporaryFolder.newFolder("partitioned");
    Dataset<Row> rows =
        spark
            .range(0, 4, 1, 2)
            .selectExpr(
                "id as time",
                "concat('city', cast(id % 2 as string)) as city",
                "cast(id as int) as temperature",
                "cast(id as bigint) as humidity");

    rows.write()
        .format("tsfile")
        .option("table", "weather")
        .option("tagColumns", "city")
        .mode(SaveMode.Append)
        .save(output.getAbsolutePath());

    File[] tsFiles = output.listFiles((dir, name) -> name.endsWith(".tsfile"));
    assertNotNull(tsFiles);
    assertEquals(2, tsFiles.length);
    assertFalse(new File(output, "_temporary").exists());
  }

  private static TsFileTableOptions readOptions(String path, String table) {
    Map<String, String> options = new HashMap<>();
    options.put("path", path);
    options.put("table", table);
    return TsFileTableOptions.forRead(new CaseInsensitiveStringMap(options));
  }

  private static TsFileTableOptions writeOptions(String path, String tagColumns) {
    Map<String, String> options = new HashMap<>();
    options.put("path", path);
    options.put("table", "weather");
    options.put("tagColumns", tagColumns);
    return TsFileTableOptions.forWrite(new CaseInsensitiveStringMap(options));
  }

  private static StructType sampleSchema() {
    return new StructType()
        .add("time", DataTypes.LongType, false)
        .add("city", DataTypes.StringType, false)
        .add("temperature", DataTypes.IntegerType, true)
        .add("humidity", DataTypes.LongType, true);
  }

  private static List<Row> sampleRows() {
    return Arrays.asList(
        RowFactory.create(0L, "beijing", 20, 30L),
        RowFactory.create(1L, "shanghai", null, 31L),
        RowFactory.create(2L, "beijing", 22, 32L));
  }

  private static void writeWeatherFile(File file, String tableName, long offset) throws Exception {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writeWeatherTable(writer, tableName, offset);
    }
  }

  private static void writeWeatherTable(TsFileWriter writer, String tableName, long offset)
      throws Exception {
    writeTable(
        writer,
        tableName,
        new Object[][] {
          {offset, "beijing", 20, 30L},
          {offset + 1, "shanghai", null, 31L},
          {offset + 2, "beijing", 22, 32L}
        },
        new ColumnSpec("city", TSDataType.STRING, ColumnCategory.TAG),
        new ColumnSpec("temperature", TSDataType.INT32, ColumnCategory.FIELD),
        new ColumnSpec("humidity", TSDataType.INT64, ColumnCategory.FIELD));
  }

  private static void writeTsFile(File file, String tableName, ColumnSpec... columns)
      throws Exception {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writeTable(writer, tableName, new Object[][] {{0L, "beijing", 20.0D}}, columns);
    }
  }

  private static void writeTable(
      TsFileWriter writer, String tableName, Object[][] rows, ColumnSpec... columns)
      throws Exception {
    writer.registerTableSchema(tableSchema(tableName, columns));
    List<String> columnNames = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    for (ColumnSpec column : columns) {
      columnNames.add(column.name.toLowerCase(Locale.ROOT));
      dataTypes.add(column.type);
      categories.add(column.category);
    }
    Tablet tablet =
        new Tablet(tableName, columnNames, dataTypes, categories, Math.max(1, rows.length));
    for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
      tablet.addTimestamp(rowIndex, ((Number) rows[rowIndex][0]).longValue());
      for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
        tablet.addValue(columnNames.get(columnIndex), rowIndex, rows[rowIndex][columnIndex + 1]);
      }
    }
    writer.writeTable(tablet);
  }

  private static TableSchema tableSchema(String tableName, ColumnSpec... columns) {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    for (ColumnSpec column : columns) {
      measurementSchemas.add(
          new MeasurementSchema(column.name.toLowerCase(Locale.ROOT), column.type));
      categories.add(column.category);
    }
    return new TableSchema(tableName, measurementSchemas, categories);
  }

  private static void assertFailsContaining(String expectedMessage, ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Throwable error) {
      if (!containsMessage(error, expectedMessage)) {
        AssertionError assertionError =
            new AssertionError("Expected failure containing: " + expectedMessage, error);
        throw assertionError;
      }
      return;
    }
    fail("Expected failure containing: " + expectedMessage);
  }

  private static boolean containsMessage(Throwable error, String expectedMessage) {
    Throwable current = error;
    while (current != null) {
      if (current.getMessage() != null && current.getMessage().contains(expectedMessage)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private interface ThrowingRunnable {
    void run() throws Throwable;
  }

  private static class ColumnSpec {
    private final String name;
    private final TSDataType type;
    private final ColumnCategory category;

    private ColumnSpec(String name, TSDataType type, ColumnCategory category) {
      this.name = name;
      this.type = type;
      this.category = category;
    }
  }
}
