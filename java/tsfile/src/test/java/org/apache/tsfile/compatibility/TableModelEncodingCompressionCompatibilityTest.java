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

package org.apache.tsfile.compatibility;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Assume;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TableModelEncodingCompressionCompatibilityTest {

  private static final String GENERATE_DIR_PROPERTY = "tsfile.compat.generate.dir";
  private static final String VALIDATE_DIR_PROPERTY = "tsfile.compat.validate.dir";
  private static final String MANIFEST_FILE = "manifest.csv";
  private static final String MANIFEST_HEADER =
      "file,table,tagColumn,valueColumn,dataType,encoding,compression,rowCount";
  private static final String TABLE_NAME = "compat_table";
  private static final String TAG_COLUMN = "device";
  private static final String VALUE_COLUMN = "value";
  private static final String TAG_VALUE = "compat_device";
  private static final int ROW_COUNT = 32;

  private static final int[] INT_VALUES = {
    0,
    1,
    -1,
    7,
    -8,
    1024,
    -1024,
    123456,
    -654321,
    Integer.MAX_VALUE - 1024,
    Integer.MIN_VALUE + 1024,
    42,
    42,
    0,
    -8,
    2048
  };

  private static final long[] LONG_VALUES = {
    0L,
    1L,
    -1L,
    7L,
    -8L,
    1_234_567_890L,
    -987_654_321L,
    4_294_967_296L,
    -4_294_967_296L,
    1_234_567_890_123L,
    -1_234_567_890_123L,
    Long.MAX_VALUE - 4096,
    Long.MIN_VALUE + 4096,
    42L,
    42L,
    0L
  };

  private static final int[] FLOAT_BITS = {
    0x00000000,
    0x80000000,
    0x3f800000,
    0xbf800000,
    0x41280000,
    0xc0700000,
    0x44801000,
    0xc5002000,
    0x40490fdb,
    0xc02df854,
    0x3f800000,
    0x3f800000,
    0x00000000,
    0x80000000,
    0x42f6e979,
    0xc2f6e979
  };

  private static final long[] DOUBLE_BITS = {
    0x0000000000000000L,
    0x8000000000000000L,
    0x3ff0000000000000L,
    0xbff0000000000000L,
    0x4029000000000000L,
    0xc00c000000000000L,
    0x4090008000000000L,
    0xc0a0010000000000L,
    0x400921fb54442d18L,
    0xc005bf0a8b145769L,
    0x3ff0000000000000L,
    0x3ff0000000000000L,
    0x0000000000000000L,
    0x8000000000000000L,
    0x405edd2f1a9fbe77L,
    0xc05edd2f1a9fbe77L
  };

  private static final LocalDate[] DATE_VALUES = {
    LocalDate.of(1970, 1, 1),
    LocalDate.of(1999, 12, 31),
    LocalDate.of(2000, 2, 29),
    LocalDate.of(2024, 2, 29),
    LocalDate.of(2038, 1, 19),
    LocalDate.of(2050, 6, 15),
    LocalDate.of(1969, 7, 20),
    LocalDate.of(1980, 1, 6)
  };

  @Test
  public void generateFixtures() throws Exception {
    String outputDir = System.getProperty(GENERATE_DIR_PROPERTY);
    Assume.assumeTrue(
        GENERATE_DIR_PROPERTY + " is not set", outputDir != null && !outputDir.isEmpty());

    Path directory = Paths.get(outputDir);
    Files.createDirectories(directory);

    List<FixtureCase> cases = buildMatrix();
    for (FixtureCase fixtureCase : cases) {
      writeFixture(directory, fixtureCase);
    }
    writeManifest(directory, cases);
  }

  @Test
  public void validateFixtures() throws Exception {
    String inputDir = System.getProperty(VALIDATE_DIR_PROPERTY);
    Assume.assumeTrue(
        VALIDATE_DIR_PROPERTY + " is not set", inputDir != null && !inputDir.isEmpty());

    Path directory = Paths.get(inputDir);
    for (FixtureCase fixtureCase : readManifest(directory)) {
      validateFixture(directory, fixtureCase);
    }
  }

  private static List<FixtureCase> buildMatrix() {
    List<FixtureCase> cases = new ArrayList<>();
    List<CompressionType> compressions =
        Arrays.asList(CompressionType.UNCOMPRESSED, CompressionType.ZSTD, CompressionType.LZMA2);
    for (CompressionType compression : compressions) {
      for (TSDataType dataType :
          Arrays.asList(
              TSDataType.INT32,
              TSDataType.DATE,
              TSDataType.INT64,
              TSDataType.TIMESTAMP,
              TSDataType.FLOAT,
              TSDataType.DOUBLE)) {
        cases.add(new FixtureCase(dataType, TSEncoding.CHIMP, compression, ROW_COUNT));
        cases.add(new FixtureCase(dataType, TSEncoding.RLBE, compression, ROW_COUNT));
      }
      cases.add(new FixtureCase(TSDataType.DOUBLE, TSEncoding.CAMEL, compression, ROW_COUNT));
    }
    return cases;
  }

  private static void writeFixture(Path directory, FixtureCase fixtureCase) throws Exception {
    TableSchema tableSchema = tableSchema(fixtureCase);
    File file = directory.resolve(fixtureCase.fileName).toFile();
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.setGenerateTableSchema(true);
      writer.registerTableSchema(tableSchema);
      writer.writeTable(tablet(tableSchema, fixtureCase));
    }
  }

  private static void validateFixture(Path directory, FixtureCase fixtureCase) throws Exception {
    File file = directory.resolve(fixtureCase.fileName).toFile();
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build();
        ResultSet resultSet =
            reader.query(
                fixtureCase.tableName,
                Arrays.asList(fixtureCase.tagColumn, fixtureCase.valueColumn),
                Long.MIN_VALUE,
                Long.MAX_VALUE)) {
      ResultSetMetadata metadata = resultSet.getMetadata();
      assertEquals("Time", metadata.getColumnName(1));
      assertEquals(TSDataType.INT64, metadata.getColumnType(1));
      assertEquals(fixtureCase.tagColumn, metadata.getColumnName(2));
      assertEquals(TSDataType.STRING, metadata.getColumnType(2));
      assertEquals(fixtureCase.valueColumn, metadata.getColumnName(3));
      assertEquals(fixtureCase.dataType, metadata.getColumnType(3));

      int row = 0;
      while (resultSet.next()) {
        assertEquals("time at row " + row, row, resultSet.getLong(1));
        assertFalse("tag is null at row " + row, resultSet.isNull(2));
        assertEquals(TAG_VALUE, resultSet.getString(2));
        assertFalse("value is null at row " + row, resultSet.isNull(3));
        assertValue(fixtureCase, row, resultSet);
        row++;
      }
      assertEquals(fixtureCase.rowCount, row);
    }
  }

  private static TableSchema tableSchema(FixtureCase fixtureCase) {
    List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema(
                fixtureCase.tagColumn,
                TSDataType.STRING,
                TSEncoding.PLAIN,
                CompressionType.UNCOMPRESSED),
            new MeasurementSchema(
                fixtureCase.valueColumn,
                fixtureCase.dataType,
                fixtureCase.encoding,
                fixtureCase.compression));
    return new TableSchema(
        fixtureCase.tableName, schemas, Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
  }

  private static Tablet tablet(TableSchema tableSchema, FixtureCase fixtureCase) {
    Tablet tablet =
        new Tablet(
            tableSchema.getTableName(),
            IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
            IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
            tableSchema.getColumnTypes(),
            fixtureCase.rowCount);
    for (int row = 0; row < fixtureCase.rowCount; row++) {
      tablet.addTimestamp(row, row);
      tablet.addValue(TAG_COLUMN, row, TAG_VALUE);
      addValue(tablet, fixtureCase.dataType, row);
    }
    return tablet;
  }

  private static void addValue(Tablet tablet, TSDataType dataType, int row) {
    switch (dataType) {
      case INT32:
        tablet.addValue(row, VALUE_COLUMN, intValue(row));
        break;
      case DATE:
        tablet.addValue(row, VALUE_COLUMN, dateValue(row));
        break;
      case INT64:
      case TIMESTAMP:
        tablet.addValue(row, VALUE_COLUMN, longValue(row));
        break;
      case FLOAT:
        tablet.addValue(row, VALUE_COLUMN, floatValue(row));
        break;
      case DOUBLE:
        tablet.addValue(row, VALUE_COLUMN, doubleValue(row));
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private static void assertValue(FixtureCase fixtureCase, int row, ResultSet resultSet) {
    switch (fixtureCase.dataType) {
      case INT32:
        assertEquals("INT32 at row " + row, intValue(row), resultSet.getInt(3));
        break;
      case DATE:
        assertEquals("DATE at row " + row, dateValue(row), resultSet.getDate(3));
        break;
      case INT64:
      case TIMESTAMP:
        assertEquals("INT64/TIMESTAMP at row " + row, longValue(row), resultSet.getLong(3));
        break;
      case FLOAT:
        assertEquals(
            "FLOAT bits at row " + row,
            Float.floatToIntBits(floatValue(row)),
            Float.floatToIntBits(resultSet.getFloat(3)));
        break;
      case DOUBLE:
        assertEquals(
            "DOUBLE bits at row " + row,
            Double.doubleToLongBits(doubleValue(row)),
            Double.doubleToLongBits(resultSet.getDouble(3)));
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + fixtureCase.dataType);
    }
  }

  private static int intValue(int row) {
    return INT_VALUES[row % INT_VALUES.length];
  }

  private static long longValue(int row) {
    return LONG_VALUES[row % LONG_VALUES.length];
  }

  private static float floatValue(int row) {
    return Float.intBitsToFloat(FLOAT_BITS[row % FLOAT_BITS.length]);
  }

  private static double doubleValue(int row) {
    return Double.longBitsToDouble(DOUBLE_BITS[row % DOUBLE_BITS.length]);
  }

  private static LocalDate dateValue(int row) {
    return DATE_VALUES[row % DATE_VALUES.length];
  }

  private static void writeManifest(Path directory, List<FixtureCase> cases) throws IOException {
    try (BufferedWriter writer =
        Files.newBufferedWriter(directory.resolve(MANIFEST_FILE), StandardCharsets.UTF_8)) {
      writer.write(MANIFEST_HEADER);
      writer.newLine();
      for (FixtureCase fixtureCase : cases) {
        writer.write(fixtureCase.toManifestLine());
        writer.newLine();
      }
    }
  }

  private static List<FixtureCase> readManifest(Path directory) throws IOException {
    Path manifest = directory.resolve(MANIFEST_FILE);
    List<String> lines = Files.readAllLines(manifest, StandardCharsets.UTF_8);
    if (lines.isEmpty()) {
      return Collections.emptyList();
    }
    assertEquals(MANIFEST_HEADER, lines.get(0));
    List<FixtureCase> cases = new ArrayList<>();
    for (int i = 1; i < lines.size(); i++) {
      String line = lines.get(i).trim();
      if (line.isEmpty()) {
        continue;
      }
      cases.add(FixtureCase.fromManifestLine(line));
    }
    return cases;
  }

  private static class FixtureCase {
    private final String fileName;
    private final String tableName;
    private final String tagColumn;
    private final String valueColumn;
    private final TSDataType dataType;
    private final TSEncoding encoding;
    private final CompressionType compression;
    private final int rowCount;

    private FixtureCase(
        TSDataType dataType, TSEncoding encoding, CompressionType compression, int rowCount) {
      this(
          fileName(dataType, encoding, compression),
          TABLE_NAME,
          TAG_COLUMN,
          VALUE_COLUMN,
          dataType,
          encoding,
          compression,
          rowCount);
    }

    private FixtureCase(
        String fileName,
        String tableName,
        String tagColumn,
        String valueColumn,
        TSDataType dataType,
        TSEncoding encoding,
        CompressionType compression,
        int rowCount) {
      this.fileName = fileName;
      this.tableName = tableName;
      this.tagColumn = tagColumn;
      this.valueColumn = valueColumn;
      this.dataType = dataType;
      this.encoding = encoding;
      this.compression = compression;
      this.rowCount = rowCount;
    }

    private static FixtureCase fromManifestLine(String line) {
      String[] parts = line.split(",", -1);
      if (parts.length != 8) {
        throw new IllegalArgumentException("Bad manifest line: " + line);
      }
      return new FixtureCase(
          parts[0],
          parts[1],
          parts[2],
          parts[3],
          TSDataType.valueOf(parts[4]),
          TSEncoding.valueOf(parts[5]),
          CompressionType.valueOf(parts[6]),
          Integer.parseInt(parts[7]));
    }

    private String toManifestLine() {
      return String.join(
          ",",
          fileName,
          tableName,
          tagColumn,
          valueColumn,
          dataType.name(),
          encoding.name(),
          compression.name(),
          String.valueOf(rowCount));
    }

    private static String fileName(
        TSDataType dataType, TSEncoding encoding, CompressionType compression) {
      return encoding.name().toLowerCase(Locale.ROOT)
          + "_"
          + dataType.name().toLowerCase(Locale.ROOT)
          + "_"
          + compression.name().toLowerCase(Locale.ROOT)
          + ".tsfile";
    }
  }
}
