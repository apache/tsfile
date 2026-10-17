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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
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
import org.junit.runners.model.MultipleFailureException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableModelEncodingCompressionCompatibilityTest {

  private static final String GENERATE_DIR_PROPERTY = "tsfile.compat.generate.dir";
  private static final String VALIDATE_DIR_PROPERTY = "tsfile.compat.validate.dir";
  private static final String MANIFEST_FILE = "manifest.csv";
  private static final String MANIFEST_HEADER =
      "file,table,tagColumn,valueColumn,dataType,encoding,compression,rowCount,nullPattern,booleanEncoding";
  private static final String TABLE_NAME = "compat_table";
  private static final String TAG_COLUMN = "device";
  private static final String VALUE_COLUMN = "value";
  private static final String TAG_VALUE = "compat_device";

  private static final int NULL_PAGE_ROWS = 64;
  private static final String BOOLEAN_COLUMN = "boolean_value";
  private static final int[] ROW_COUNTS = {32, 4096, 1_000_000};
  private static final int NULL_PERCENT = 10;
  private static final String ANCHOR_COLUMN = "anchor";

  // Keep this contract in sync with the C++ compatibility test. MIXED has
  // empty pages at the start, in the middle (consecutive), and at the end;
  // page 4 mixes null/non-null rows and pages 1/5 are entirely non-null.
  private enum NullPattern {
    NONE,
    RANDOM,
    MIXED,
    ALL
  }

  private static boolean isNull(FixtureCase fixtureCase, int row) {
    if (fixtureCase.nullPattern == NullPattern.RANDOM) {
      return randomNull(row, 0x12345678);
    }
    if (fixtureCase.nullPattern == NullPattern.ALL) {
      return true;
    }
    if (fixtureCase.nullPattern == NullPattern.NONE) {
      return false;
    }
    int page = row / NULL_PAGE_ROWS;
    return page == 0 || page == 2 || page == 3 || page >= 6 || (page == 4 && row % 2 == 0);
  }

  // Fixed-seed 32-bit mixing, shared with C++. Separate salts make each
  // column's 10% NULL mask independent and reproducible in either language.
  private static int rowHash(int row, int salt) {
    int value = (row + 1) * 0x9e3779b9 ^ salt;
    value ^= value >>> 16;
    value *= 0x85ebca6b;
    value ^= value >>> 13;
    return value;
  }

  private static boolean randomNull(int row, int salt) {
    return Integer.remainderUnsigned(rowHash(row, salt), 100) < NULL_PERCENT;
  }

  private static boolean booleanNull(int row) {
    return randomNull(row, 0x87654321);
  }

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

  // FLOAT/DOUBLE + TS_2DIFF converts values to fixed-point with
  // maxPointNumber (10^mpn). The expected value is computed by applying the
  // same tri-state conversion the writer performs and then decoding it back,
  // so expectations reflect the encoding rules rather than the raw input
  // bits. Every chosen value has the same expected value whether the writer
  // used maxPointNumber 0 (explicit max_point_number property) or 2 (the
  // TSFileConfig.floatPrecision default shared with the C++ writer), so the
  // validating reader never needs to know which writer produced the file.
  // The values cover all page layouts the writers can produce:
  //  - plain integers (scaled form; at mpn = 0 every finite in-range value
  //    is scaled, so pages written at mpn = 0 contain no bitmap)
  //  - 1.5e9f / 1e18: the scaled product overflows while round(v) still
  //    fits -> scale-overflow form, single page-wide bitmap (reachable only
  //    at mpn > 0, i.e. the default writer configuration)
  //  - NaN and +/-Infinity (stored as raw IEEE bits -> two page-wide
  //    bitmaps; Java floatToIntBits canonicalizes NaN)
  private static final int[] TS_2DIFF_FLOAT_BITS = {
    0x00000000, 0x3f800000, 0xbf800000, 0x461c4000, 0xc61c4000, 0x4eb2d05e,
    0x7f800000, 0xff800000, 0x7fc00000, 0x3f800000, 0x00000000, 0x4a742400,
    0xca742400, 0x4e6e6b28, 0x3f800000, 0x00000000
  };

  private static final long[] TS_2DIFF_DOUBLE_BITS = {
    0x0000000000000000L, 0x3ff0000000000000L, 0xbff0000000000000L,
    0x40c3880000000000L, 0xc0c3880000000000L, 0x43abc16d674ec800L,
    0x7ff0000000000000L, 0xfff0000000000000L, 0x7ff8000000000000L,
    0x3ff0000000000000L, 0x0000000000000000L, 0x41f0000000000000L,
    0xc1cd6f3458800000L, 0x430c6bf526340000L, 0x3ff0000000000000L,
    0x0000000000000000L
  };

  // maxPointNumber used by the standard Java schema write path
  // (TSEncodingBuilder.Ts2Diff initFromProps -> TSFileConfig.floatPrecision,
  // current default 2); the C++ FloatTS2DIFFEncoder default matches.
  private static final int TS_2DIFF_MAX_POINT_NUMBER = 2;
  private static final double TS_2DIFF_MAX_POINT_VALUE =
      TS_2DIFF_MAX_POINT_NUMBER <= 0 ? 1.0 : Math.pow(10, TS_2DIFF_MAX_POINT_NUMBER);

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
    assertFalse("empty compatibility matrix", cases.isEmpty());
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
    List<FixtureCase> fixtures = readManifest(directory);
    Set<String> expectedCoverage = new HashSet<>();
    for (FixtureCase fixtureCase : buildMatrix()) {
      expectedCoverage.add(coverageKey(fixtureCase));
    }
    Set<String> actualCoverage = new HashSet<>();
    List<Throwable> failures = new ArrayList<>();
    for (FixtureCase fixtureCase : fixtures) {
      String compressionFilter = System.getProperty("tsfile.compat.compression", "");
      String rowsFilter = System.getProperty("tsfile.compat.rows", "");
      if ((!compressionFilter.isEmpty()
              && !fixtureCase.compression.name().equals(compressionFilter))
          || (!rowsFilter.isEmpty() && fixtureCase.rowCount != Integer.parseInt(rowsFilter))) {
        continue;
      }
      actualCoverage.add(coverageKey(fixtureCase));
      try {
        validateFixture(directory, fixtureCase);
      } catch (Exception | AssertionError failure) {
        failures.add(new AssertionError(fixtureCase.fileName, failure));
      }
    }
    assertFalse("empty compatibility coverage", expectedCoverage.isEmpty());
    assertEquals("writer/reader matrix coverage", expectedCoverage, actualCoverage);
    MultipleFailureException.assertEmpty(failures);
  }

  private static String coverageKey(FixtureCase fixtureCase) {
    return fixtureCase.dataType
        + "/"
        + fixtureCase.encoding
        + "/"
        + fixtureCase.compression
        + "/"
        + fixtureCase.rowCount
        + "/"
        + fixtureCase.nullPattern;
  }

  private static List<FixtureCase> buildMatrix() {
    List<FixtureCase> cases = new ArrayList<>();
    List<CompressionType> compressions =
        Arrays.asList(
            CompressionType.UNCOMPRESSED,
            CompressionType.SNAPPY,
            CompressionType.GZIP,
            CompressionType.LZ4,
            CompressionType.ZSTD,
            CompressionType.LZMA2);
    for (CompressionType compression : compressions) {
      for (TSDataType dataType :
          Arrays.asList(
              TSDataType.BOOLEAN,
              TSDataType.INT32,
              TSDataType.DATE,
              TSDataType.INT64,
              TSDataType.TIMESTAMP,
              TSDataType.FLOAT,
              TSDataType.DOUBLE,
              TSDataType.TEXT,
              TSDataType.STRING,
              TSDataType.BLOB)) {
        for (TSEncoding encoding : TSEncoding.values()) {
          if (cppSupports(dataType, encoding)) {
            assertTrue(dataType + "/" + encoding, TSEncoding.isSupported(dataType, encoding));
            cases.add(new FixtureCase(dataType, encoding, compression, ROW_COUNTS[0]));
          }
        }
        if (dataType == TSDataType.FLOAT || dataType == TSDataType.DOUBLE) {
          cases.add(
              new FixtureCase(
                  FixtureCase.fileName(dataType, TSEncoding.TS_2DIFF, compression) + ".mpn0",
                  TABLE_NAME,
                  TAG_COLUMN,
                  VALUE_COLUMN,
                  dataType,
                  TSEncoding.TS_2DIFF,
                  compression,
                  ROW_COUNTS[0]));
        }
      }
    }
    for (CompressionType compression :
        Arrays.asList(
            CompressionType.UNCOMPRESSED,
            CompressionType.LZ4,
            CompressionType.ZSTD,
            CompressionType.LZMA2)) {
      for (TSDataType dataType :
          Arrays.asList(
              TSDataType.INT32,
              TSDataType.DATE,
              TSDataType.INT64,
              TSDataType.TIMESTAMP,
              TSDataType.FLOAT,
              TSDataType.DOUBLE)) {
        for (NullPattern pattern : Arrays.asList(NullPattern.MIXED, NullPattern.ALL)) {
          cases.add(new FixtureCase(dataType, compression, NULL_PAGE_ROWS * 8, pattern));
        }
        cases.add(new FixtureCase(dataType, compression, NULL_PAGE_ROWS, NullPattern.ALL));
      }
    }
    List<FixtureCase> scaled = new ArrayList<>();
    for (FixtureCase fixtureCase : cases) {
      if (fixtureCase.nullPattern != NullPattern.NONE) {
        scaled.add(fixtureCase);
        continue;
      }
      for (int rows : ROW_COUNTS) {
        String name = fixtureCase.fileName.replace(".tsfile", "." + rows + ".tsfile");
        scaled.add(
            new FixtureCase(
                name,
                TABLE_NAME,
                TAG_COLUMN,
                VALUE_COLUMN,
                fixtureCase.dataType,
                fixtureCase.encoding,
                fixtureCase.compression,
                rows,
                NullPattern.RANDOM));
      }
    }
    String compressionFilter = System.getProperty("tsfile.compat.compression", "");
    String rowsFilter = System.getProperty("tsfile.compat.rows", "");
    scaled.removeIf(
        f ->
            (!compressionFilter.isEmpty() && !f.compression.name().equals(compressionFilter))
                || (!rowsFilter.isEmpty() && f.rowCount != Integer.parseInt(rowsFilter)));
    return scaled;
  }

  // Intersection with the implemented C++ encoder/decoder factories. Excludes
  // enum placeholders and Java-only FLOAT RLE/GORILLA_V1/REGULAR combinations.
  private static boolean cppSupports(TSDataType type, TSEncoding encoding) {
    boolean integer = type == TSDataType.INT32 || type == TSDataType.INT64;
    boolean integral = integer || type == TSDataType.DATE || type == TSDataType.TIMESTAMP;
    boolean floating = type == TSDataType.FLOAT || type == TSDataType.DOUBLE;
    switch (encoding) {
      case PLAIN:
        return true;
      case DICTIONARY:
        return type == TSDataType.TEXT || type == TSDataType.STRING;
      case RLE:
        return integral;
      case TS_2DIFF:
      case GORILLA:
      case CHIMP:
      case RLBE:
        return integral || floating;
      case ZIGZAG:
        return integer;
      case SPRINTZ:
        return integer || floating;
      case CAMEL:
        return type == TSDataType.DOUBLE;
      default:
        return false;
    }
  }

  private static void writeFixture(Path directory, FixtureCase fixtureCase) throws Exception {
    TableSchema tableSchema = tableSchema(fixtureCase);
    File file = directory.resolve(fixtureCase.fileName).toFile();
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    int oldPageRows = config.getMaxNumberOfPointsInPage();
    int oldPageBytes = config.getPageSizeInByte();
    try {
      if (fixtureCase.nullPattern != NullPattern.NONE) {
        config.setMaxNumberOfPointsInPage(
            fixtureCase.nullPattern == NullPattern.RANDOM ? 4096 : NULL_PAGE_ROWS);
        config.setPageSizeInByte(1024 * 1024);
      }
      try (TsFileWriter writer = new TsFileWriter(file)) {
        writer.setGenerateTableSchema(true);
        writer.registerTableSchema(tableSchema);
        writer.writeTable(tablet(tableSchema, fixtureCase));
      }
    } finally {
      config.setMaxNumberOfPointsInPage(oldPageRows);
      config.setPageSizeInByte(oldPageBytes);
    }
    if (fixtureCase.nullPattern == NullPattern.MIXED) {
      assertEmptyPages(file, fixtureCase);
    }
  }

  // Prove that the regression fixture actually contains Java's one-byte empty
  // page headers, rather than merely null values inside a non-empty page.
  private static void assertEmptyPages(File file, FixtureCase fixtureCase) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getPath())) {
      List<ChunkMetadata> chunks =
          reader.getChunkMetadataList(
              reader.getAllDevices().get(0), fixtureCase.valueColumn, false);
      assertEquals(1, chunks.size());
      Chunk chunk = reader.readMemChunk(chunks.get(0));
      ByteBuffer data = chunk.getData();
      int page = 0;
      while (data.hasRemaining()) {
        int start = data.position();
        PageHeader header = PageHeader.deserializeFrom(data, fixtureCase.dataType);
        boolean empty = page == 0 || page == 2 || page == 3 || page == 6 || page == 7;
        assertEquals("empty page " + page, empty, header.getUncompressedSize() == 0);
        if (empty) {
          assertEquals(1, data.position() - start);
        }
        data.position(data.position() + header.getCompressedSize());
        page++;
      }
      assertEquals(8, page);
    }
  }

  private static void validateFixture(Path directory, FixtureCase fixtureCase) throws Exception {
    assertOnWireCodecs(directory, fixtureCase);
    if (fixtureCase.nullPattern == NullPattern.RANDOM) {
      validateQuery(directory, fixtureCase, true, 0, fixtureCase.rowCount - 1);
      validateQuery(
          directory,
          fixtureCase,
          false,
          Math.max(0, fixtureCase.rowCount - 3),
          fixtureCase.rowCount - 1);
      return;
    }
    validateQuery(directory, fixtureCase, false, 0, fixtureCase.rowCount - 1);
    if (fixtureCase.nullPattern != NullPattern.NONE) {
      validateQuery(directory, fixtureCase, true, 0, fixtureCase.rowCount - 1);
      if (fixtureCase.rowCount > 383) {
        // Start inside/at page boundaries and query an entirely null interval.
        validateQuery(directory, fixtureCase, false, 63, 321);
        validateQuery(directory, fixtureCase, true, 128, 255);
        validateQuery(directory, fixtureCase, true, 320, 383);
      }
    }
  }

  private static void assertOnWireCodecs(Path directory, FixtureCase fixtureCase) throws Exception {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(directory.resolve(fixtureCase.fileName).toString())) {
      String[] columns = {fixtureCase.valueColumn, BOOLEAN_COLUMN};
      TSEncoding[] encodings = {fixtureCase.encoding, fixtureCase.booleanEncoding};
      for (int index = 0; index < columns.length; index++) {
        List<ChunkMetadata> chunks =
            reader.getChunkMetadataList(reader.getAllDevices().get(0), columns[index], false);
        assertFalse(columns[index] + " has no chunks", chunks.isEmpty());
        for (ChunkMetadata chunk : chunks) {
          Chunk data = reader.readMemChunk(chunk);
          assertEquals(encodings[index], data.getHeader().getEncodingType());
          assertEquals(fixtureCase.compression, data.getHeader().getCompressionType());
        }
      }
    }
  }

  private static void validateQuery(
      Path directory, FixtureCase fixtureCase, boolean withAnchor, int start, int end)
      throws Exception {
    File file = directory.resolve(fixtureCase.fileName).toFile();
    List<String> columns =
        new ArrayList<>(
            Arrays.asList(fixtureCase.tagColumn, fixtureCase.valueColumn, BOOLEAN_COLUMN));
    if (withAnchor) {
      columns.add(ANCHOR_COLUMN);
    }
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build();
        ResultSet resultSet = reader.query(fixtureCase.tableName, columns, start, end)) {
      ResultSetMetadata metadata = resultSet.getMetadata();
      assertEquals("Time", metadata.getColumnName(1));
      assertEquals(TSDataType.INT64, metadata.getColumnType(1));
      assertEquals(fixtureCase.tagColumn, metadata.getColumnName(2));
      assertEquals(TSDataType.STRING, metadata.getColumnType(2));
      assertEquals(fixtureCase.valueColumn, metadata.getColumnName(3));
      assertEquals(fixtureCase.dataType, metadata.getColumnType(3));
      assertEquals(BOOLEAN_COLUMN, metadata.getColumnName(4));
      assertEquals(TSDataType.BOOLEAN, metadata.getColumnType(4));
      int row = start;
      while (resultSet.next()) {
        assertTrue("unexpected row in " + fixtureCase.fileName, row <= end);
        assertEquals("time at row " + row, row, resultSet.getLong(1));
        assertFalse("tag is null at row " + row, resultSet.isNull(2));
        assertEquals(TAG_VALUE, resultSet.getString(2));
        assertEquals(
            fixtureCase.fileName + " null at row " + row,
            isNull(fixtureCase, row),
            resultSet.isNull(3));
        if (!isNull(fixtureCase, row)) {
          assertValue(fixtureCase, row, resultSet);
        }
        assertEquals("BOOLEAN NULL at row " + row, booleanNull(row), resultSet.isNull(4));
        if (!booleanNull(row)) {
          assertEquals("BOOLEAN at row " + row, booleanValue(row), resultSet.getBoolean(4));
        }
        if (withAnchor) {
          assertFalse(resultSet.isNull(5));
          assertEquals(row, resultSet.getLong(5));
        }
        row++;
      }
      assertEquals(fixtureCase.fileName + " row count", end + 1, row);
    }
  }

  private static TableSchema tableSchema(FixtureCase fixtureCase) {
    Map<String, String> props = null;
    if (isTs2DiffFloatCase(fixtureCase) && fixtureCase.fileName.endsWith(".mpn0")) {
      props = Collections.singletonMap(Encoder.MAX_POINT_NUMBER, "0");
    }
    MeasurementSchema valueSchema =
        new MeasurementSchema(
            fixtureCase.valueColumn,
            fixtureCase.dataType,
            fixtureCase.encoding,
            fixtureCase.compression,
            props);
    MeasurementSchema booleanSchema = new MeasurementSchema(BOOLEAN_COLUMN, TSDataType.BOOLEAN);
    assertEquals("Java default BOOLEAN encoding", TSEncoding.RLE, booleanSchema.getEncodingType());
    booleanSchema.setCompressionType(fixtureCase.compression);
    List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema(
                fixtureCase.tagColumn,
                TSDataType.STRING,
                TSEncoding.PLAIN,
                CompressionType.UNCOMPRESSED),
            valueSchema,
            booleanSchema);
    List<ColumnCategory> categories =
        new ArrayList<>(
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD));
    if (fixtureCase.nullPattern != NullPattern.NONE) {
      schemas = new ArrayList<>(schemas);
      schemas.add(
          new MeasurementSchema(
              ANCHOR_COLUMN, TSDataType.INT64, TSEncoding.PLAIN, fixtureCase.compression));
      categories.add(ColumnCategory.FIELD);
    }
    return new TableSchema(fixtureCase.tableName, schemas, categories);
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
      if (isNull(fixtureCase, row)) {
        tablet.addValue(VALUE_COLUMN, row, null);
      } else {
        addValue(tablet, fixtureCase, row);
      }
      tablet.addValue(BOOLEAN_COLUMN, row, booleanNull(row) ? null : booleanValue(row));
      if (fixtureCase.nullPattern != NullPattern.NONE) {
        tablet.addValue(ANCHOR_COLUMN, row, (long) row);
      }
    }
    return tablet;
  }

  private static void addValue(Tablet tablet, FixtureCase fixtureCase, int row) {
    TSDataType dataType = fixtureCase.dataType;
    switch (dataType) {
      case TEXT:
      case STRING:
      case BLOB:
        tablet.addValue(row, VALUE_COLUMN, binaryValue(row));
        break;
      case BOOLEAN:
        tablet.addValue(row, VALUE_COLUMN, booleanValue(row));
        break;
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
        tablet.addValue(row, VALUE_COLUMN, expectedFloatValue(fixtureCase, row));
        break;
      case DOUBLE:
        tablet.addValue(row, VALUE_COLUMN, expectedDoubleValue(fixtureCase, row));
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private static void assertValue(FixtureCase fixtureCase, int row, ResultSet resultSet) {
    switch (fixtureCase.dataType) {
      case TEXT:
      case STRING:
      case BLOB:
        assertArrayEquals("binary at row " + row, binaryValue(row), resultSet.getBinary(3));
        break;
      case BOOLEAN:
        assertEquals("BOOLEAN at row " + row, booleanValue(row), resultSet.getBoolean(3));
        break;
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
            Float.floatToIntBits(expectedFloatValue(fixtureCase, row)),
            Float.floatToIntBits(resultSet.getFloat(3)));
        break;
      case DOUBLE:
        assertEquals(
            "DOUBLE bits at row " + row,
            Double.doubleToLongBits(expectedDoubleValue(fixtureCase, row)),
            Double.doubleToLongBits(resultSet.getDouble(3)));
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type: " + fixtureCase.dataType);
    }
  }

  private static byte[] binaryValue(int row) {
    // Empty values, repeated dictionary entries, embedded NUL and non-ASCII bytes.
    return new byte[][] {
          new byte[0],
          {97, 108, 112, 104, 97},
          {0, 1, -1, 127},
          {97, 108, 112, 104, 97},
          {-28, -72, -83, -26, -106, -121}
        }
        [row % 5];
  }

  // Alternate bits and long true/false runs to cover both RLE packing modes.
  private static boolean booleanValue(int row) {
    return row % 192 < 64 ? row % 2 == 0 : row % 192 < 128;
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

  private static boolean isTs2DiffFloatCase(FixtureCase fixtureCase) {
    return fixtureCase.encoding == TSEncoding.TS_2DIFF
        && (fixtureCase.dataType == TSDataType.FLOAT || fixtureCase.dataType == TSDataType.DOUBLE);
  }

  // maxPointValue of the writer configuration for this fixture: the
  // "*.mpn0" fixtures carry an explicit max_point_number=0 property, all
  // others use the shared default (see TS_2DIFF_MAX_POINT_NUMBER).
  private static double ts2DiffMaxPointValue(FixtureCase fixtureCase) {
    return fixtureCase.fileName.endsWith(".mpn0") ? 1.0 : TS_2DIFF_MAX_POINT_VALUE;
  }

  // Mirror FloatEncoder/FloatDecoder at the writer's maxPointNumber:
  // scaled -> round(v * mpv) / mpv, scale-overflow -> round(v) / 1,
  // raw bits -> intBitsToFloat (NaN is canonicalized by floatToIntBits).
  private static float expectedFloatValue(FixtureCase fixtureCase, int row) {
    if (!isTs2DiffFloatCase(fixtureCase)) {
      return floatValue(row);
    }
    float value = Float.intBitsToFloat(TS_2DIFF_FLOAT_BITS[row % TS_2DIFF_FLOAT_BITS.length]);
    if (Float.isNaN(value)) {
      return Float.intBitsToFloat(0x7fc00000);
    }
    double mpv = ts2DiffMaxPointValue(fixtureCase);
    double scaled = (double) value * mpv;
    if (scaled > Integer.MAX_VALUE || scaled < Integer.MIN_VALUE) {
      // value itself stays in int range for the values used here
      // (Infinity is caught below), so this is the scale-overflow form.
      if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
        return Float.intBitsToFloat(Float.floatToIntBits(value));
      }
      return (float) ((double) Math.round(value) / 1.0);
    }
    return (float) ((double) Math.round(scaled) / mpv);
  }

  private static double expectedDoubleValue(FixtureCase fixtureCase, int row) {
    if (!isTs2DiffFloatCase(fixtureCase)) {
      return doubleValue(row);
    }
    double value = Double.longBitsToDouble(TS_2DIFF_DOUBLE_BITS[row % TS_2DIFF_DOUBLE_BITS.length]);
    if (Double.isNaN(value)) {
      return Double.longBitsToDouble(0x7ff8000000000000L);
    }
    double mpv = ts2DiffMaxPointValue(fixtureCase);
    double scaled = value * mpv;
    if (scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
      if (value > Long.MAX_VALUE || value < Long.MIN_VALUE) {
        return Double.longBitsToDouble(Double.doubleToLongBits(value));
      }
      return (double) Math.round(value) / 1.0;
    }
    return (double) Math.round(scaled) / mpv;
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
    private final NullPattern nullPattern;
    private TSEncoding booleanEncoding = TSEncoding.RLE;

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
      this(
          fileName,
          tableName,
          tagColumn,
          valueColumn,
          dataType,
          encoding,
          compression,
          rowCount,
          NullPattern.NONE);
    }

    private FixtureCase(
        TSDataType dataType, CompressionType compression, int rowCount, NullPattern nullPattern) {
      this(
          fileName(dataType, TSEncoding.PLAIN, compression)
              + "."
              + nullPattern.name().toLowerCase(Locale.ROOT)
              + "."
              + rowCount,
          TABLE_NAME,
          TAG_COLUMN,
          VALUE_COLUMN,
          dataType,
          TSEncoding.PLAIN,
          compression,
          rowCount,
          nullPattern);
    }

    private FixtureCase(
        String fileName,
        String tableName,
        String tagColumn,
        String valueColumn,
        TSDataType dataType,
        TSEncoding encoding,
        CompressionType compression,
        int rowCount,
        NullPattern nullPattern) {
      this.fileName = fileName;
      this.tableName = tableName;
      this.tagColumn = tagColumn;
      this.valueColumn = valueColumn;
      this.dataType = dataType;
      this.encoding = encoding;
      this.compression = compression;
      this.rowCount = rowCount;
      this.nullPattern = nullPattern;
    }

    private static FixtureCase fromManifestLine(String line) {
      String[] parts = line.split(",", -1);
      if (parts.length != 10) {
        throw new IllegalArgumentException("Bad manifest line: " + line);
      }
      FixtureCase fixtureCase =
          new FixtureCase(
              parts[0],
              parts[1],
              parts[2],
              parts[3],
              TSDataType.valueOf(parts[4]),
              TSEncoding.valueOf(parts[5]),
              CompressionType.valueOf(parts[6]),
              Integer.parseInt(parts[7]),
              NullPattern.valueOf(parts[8]));
      fixtureCase.booleanEncoding = TSEncoding.valueOf(parts[9]);
      return fixtureCase;
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
          String.valueOf(rowCount),
          nullPattern.name(),
          booleanEncoding.name());
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
