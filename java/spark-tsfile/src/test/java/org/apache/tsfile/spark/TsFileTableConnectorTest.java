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
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

    assertEquals(3, projected.size());
    assertEquals(2, projected.get(0).size());
    assertEquals(0L, projected.get(0).getLong(0));
    assertEquals("beijing", projected.get(0).getString(1));
    assertEquals(1L, projected.get(1).getLong(0));
    assertEquals("shanghai", projected.get(1).getString(1));
    assertEquals(2L, projected.get(2).getLong(0));
    assertEquals("beijing", projected.get(2).getString(1));

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
  public void sqlTemporaryViewReadsTsFile() throws Exception {
    File file = temporaryFolder.newFile("sql-view.tsfile");
    writeWeatherFile(file, "weather", 0);

    spark.sql(
        "CREATE OR REPLACE TEMPORARY VIEW weather_sql USING tsfile OPTIONS (path '"
            + file.getAbsolutePath().replace("'", "\\'")
            + "', table 'weather')");

    List<Row> rows =
        spark
            .sql(
                "SELECT time, city, temperature FROM weather_sql "
                    + "WHERE city = 'beijing' ORDER BY time")
            .collectAsList();

    assertEquals(2, rows.size());
    assertEquals(0L, rows.get(0).getLong(0));
    assertEquals("beijing", rows.get(0).getString(1));
    assertEquals(22, rows.get(1).getInt(2));
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

  @Test
  public void appendTwiceCreatesDistinctTsFiles() throws Exception {
    File output = temporaryFolder.newFolder("append-twice");
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
    File[] firstWriteFiles = output.listFiles((dir, name) -> name.endsWith(".tsfile"));
    assertNotNull(firstWriteFiles);
    assertEquals(2, firstWriteFiles.length);

    rows.write()
        .format("tsfile")
        .option("table", "weather")
        .option("tagColumns", "city")
        .mode(SaveMode.Append)
        .save(output.getAbsolutePath());

    File[] secondWriteFiles = output.listFiles((dir, name) -> name.endsWith(".tsfile"));
    assertNotNull(secondWriteFiles);
    assertEquals(4, secondWriteFiles.length);
    assertEquals(
        8,
        spark
            .read()
            .format("tsfile")
            .option("table", "weather")
            .load(output.getAbsolutePath())
            .count());
  }

  @Test
  public void commitDoesNotOverwriteExistingOutputFile() throws Exception {
    File output = temporaryFolder.newFolder("commit-collision");
    Path outputPath = output.toPath();
    Path tempFile =
        outputPath.resolve("_temporary").resolve("query").resolve("part-query-0.tsfile");
    Path finalFile = outputPath.resolve("part-query-0.tsfile");
    Files.createDirectories(tempFile.getParent());
    Files.write(tempFile, "new".getBytes(StandardCharsets.UTF_8));
    Files.write(finalFile, "old".getBytes(StandardCharsets.UTF_8));
    TsFileTableBatchWrite batchWrite =
        new TsFileTableBatchWrite(
            TsFileTableWriteContext.build(
                writeOptions(output.getAbsolutePath(), "city"), sampleSchema()),
            false,
            "query");

    assertFailsContaining(
        "already exists",
        () ->
            batchWrite.commit(
                new TsFileTableWriterCommitMessage[] {
                  new TsFileTableWriterCommitMessage(tempFile.toString(), finalFile.toString())
                }));

    assertEquals("old", Files.readString(finalFile, StandardCharsets.UTF_8));
    assertEquals("new", Files.readString(tempFile, StandardCharsets.UTF_8));
  }

  @Test
  public void abortDoesNotDeleteExistingOutputFile() throws Exception {
    File output = temporaryFolder.newFolder("abort-collision");
    Path outputPath = output.toPath();
    Path tempFile =
        outputPath.resolve("_temporary").resolve("query").resolve("part-query-0.tsfile");
    Path finalFile = outputPath.resolve("part-query-0.tsfile");
    Files.createDirectories(tempFile.getParent());
    Files.write(tempFile, "new".getBytes(StandardCharsets.UTF_8));
    Files.write(finalFile, "old".getBytes(StandardCharsets.UTF_8));
    TsFileTableBatchWrite batchWrite =
        new TsFileTableBatchWrite(
            TsFileTableWriteContext.build(
                writeOptions(output.getAbsolutePath(), "city"), sampleSchema()),
            false,
            "query");

    batchWrite.abort(
        new TsFileTableWriterCommitMessage[] {
          new TsFileTableWriterCommitMessage(tempFile.toString(), finalFile.toString())
        });

    assertFalse(Files.exists(tempFile));
    assertTrue(Files.exists(finalFile));
    assertEquals("old", Files.readString(finalFile, StandardCharsets.UTF_8));
  }

  @Test
  public void externalReadSchemaIsValidatedAndUsed() throws Exception {
    assertTrue(new TsFileTableProvider().supportsExternalMetadata());
    File file = temporaryFolder.newFile("external-schema.tsfile");
    writeWeatherFile(file, "weather", 0);
    StructType projectionSchema =
        new StructType()
            .add("time", DataTypes.LongType, false)
            .add("city", DataTypes.StringType, false);

    Dataset<Row> projected =
        spark
            .read()
            .format("tsfile")
            .schema(projectionSchema)
            .option("table", "weather")
            .load(file.getAbsolutePath());
    assertEquals(Arrays.asList("time", "city"), Arrays.asList(projected.schema().fieldNames()));
    assertEquals(3, projected.count());

    StructType incompatibleSchema =
        new StructType()
            .add("time", DataTypes.LongType, false)
            .add("temperature", DataTypes.DoubleType, true);
    assertFailsContaining(
        "External Spark schema column temperature has type DoubleType",
        () ->
            spark
                .read()
                .format("tsfile")
                .schema(incompatibleSchema)
                .option("table", "weather")
                .load(file.getAbsolutePath())
                .count());
  }

  @Test
  public void pushesSupportedTimeRangeAndTagFilters() {
    TsFileTableFilterTranslator translator =
        new TsFileTableFilterTranslator(
            weatherSchema(), readOptions("/tmp/weather.tsfile", "weather"));
    Filter[] filters =
        new Filter[] {
          new And(
              new And(new GreaterThanOrEqual("time", 10L), new LessThanOrEqual("time", 20L)),
              new EqualTo("city", "beijing"))
        };

    Filter[] residuals = translator.pushFilters(filters);

    assertEquals(0, residuals.length);
    assertEquals(3, translator.pushedFilters().length);
    assertEquals(10L, translator.startTime());
    assertEquals(20L, translator.endTime());
    assertEquals(Collections.singletonMap("city", "beijing"), translator.tagEqualities());
  }

  @Test
  public void pushesTimeEqualityAndExclusiveBounds() {
    TsFileTableFilterTranslator translator =
        new TsFileTableFilterTranslator(
            weatherSchema(), readOptions("/tmp/weather.tsfile", "weather"));

    Filter[] residuals =
        translator.pushFilters(
            new Filter[] {
              new EqualTo("time", 100L), new GreaterThan("time", 99L), new LessThan("time", 101L)
            });

    assertEquals(0, residuals.length);
    assertEquals(3, translator.pushedFilters().length);
    assertEquals(100L, translator.startTime());
    assertEquals(100L, translator.endTime());
  }

  @Test
  public void leavesUnsupportedFiltersAsResiduals() {
    TsFileTableFilterTranslator translator =
        new TsFileTableFilterTranslator(
            weatherSchema(), readOptions("/tmp/weather.tsfile", "weather"));

    Filter[] residuals =
        translator.pushFilters(
            new Filter[] {
              new EqualTo("city", "beijing"), new EqualTo("temperature", 20), new IsNotNull("city")
            });

    assertEquals(2, residuals.length);
    assertEquals(1, translator.pushedFilters().length);
    assertEquals(Collections.singletonMap("city", "beijing"), translator.tagEqualities());
  }

  @Test
  public void pushdownCanBeDisabled() {
    Map<String, String> options = new HashMap<>();
    options.put("path", "/tmp/weather.tsfile");
    options.put("table", "weather");
    options.put("pushdown", "false");
    TsFileTableFilterTranslator translator =
        new TsFileTableFilterTranslator(
            weatherSchema(), TsFileTableOptions.forRead(new CaseInsensitiveStringMap(options)));
    Filter[] filters = new Filter[] {new EqualTo("city", "beijing"), new EqualTo("time", 1L)};

    Filter[] residuals = translator.pushFilters(filters);

    assertEquals(2, residuals.length);
    assertEquals(0, translator.pushedFilters().length);
    assertEquals(Long.MIN_VALUE, translator.startTime());
    assertEquals(Long.MAX_VALUE, translator.endTime());
    assertTrue(translator.tagEqualities().isEmpty());
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

  private static TsFileTableSchema weatherSchema() {
    return TsFileTableSchema.fromTableSchema(
        tableSchema(
            "weather",
            new ColumnSpec("city", TSDataType.STRING, ColumnCategory.TAG),
            new ColumnSpec("temperature", TSDataType.INT32, ColumnCategory.FIELD),
            new ColumnSpec("humidity", TSDataType.INT64, ColumnCategory.FIELD)),
        "time",
        TsFileTableOptions.TimestampAs.LONG);
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
