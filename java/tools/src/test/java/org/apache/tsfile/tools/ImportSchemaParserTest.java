/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.tools;

import org.apache.tsfile.enums.TSDataType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ImportSchemaParserTest {

  private final String testDir = "target" + File.separator + "schemaParserTest";

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
  }

  @After
  public void tearDown() {
    File dir = new File(testDir);
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        f.delete();
      }
    }
    dir.delete();
  }

  private String writeSchema(String content) throws IOException {
    String path = testDir + File.separator + "test.schema";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
      writer.write(content);
    }
    return path;
  }

  @Test
  public void testNewSchemaFormat() throws Exception {
    String path =
        writeSchema(
            "table_name=root.db1\n"
                + "time_precision=ms\n"
                + "has_header=true\n"
                + "separator=,\n"
                + "null_format=\\N\n"
                + "\n"
                + "tag_columns\n"
                + "Group DEFAULT Datang\n"
                + "Region\n"
                + "FactoryNumber\n"
                + "\n"
                + "time_column=Time\n"
                + "\n"
                + "source_columns\n"
                + "Region TEXT,\n"
                + "FactoryNumber TEXT,\n"
                + "SKIP,\n"
                + "Time INT64,\n"
                + "Temperature FLOAT,\n"
                + "Emission DOUBLE,\n");

    ImportSchema schema = ImportSchemaParser.parse(path);

    assertEquals("root.db1", schema.getTableName());
    assertEquals("ms", schema.getTimePrecision());
    assertTrue(schema.isHasHeader());
    assertEquals(",", schema.getSeparator());
    assertEquals("\\N", schema.getNullFormat());
    assertEquals("Time", schema.getTimeColumnName());

    List<ImportSchema.TagColumn> tags = schema.getTagColumns();
    assertEquals(3, tags.size());
    assertEquals("Group", tags.get(0).getName());
    assertTrue(tags.get(0).hasDefault());
    assertEquals("Datang", tags.get(0).getDefaultValue());
    assertTrue(tags.get(0).isVirtual());
    assertEquals("Region", tags.get(1).getName());
    assertFalse(tags.get(1).hasDefault());
    assertFalse(tags.get(1).isVirtual());
    assertEquals("FactoryNumber", tags.get(2).getName());

    List<ImportSchema.SourceColumn> srcCols = schema.getSourceColumns();
    assertEquals(6, srcCols.size());
    assertEquals("Region", srcCols.get(0).getName());
    assertEquals(TSDataType.TEXT, srcCols.get(0).getDataType());
    assertFalse(srcCols.get(0).isSkip());
    assertTrue(srcCols.get(2).isSkip());
    assertEquals("Time", srcCols.get(3).getName());
    assertEquals(TSDataType.INT64, srcCols.get(3).getDataType());
    assertEquals("Temperature", srcCols.get(4).getName());
    assertEquals(TSDataType.FLOAT, srcCols.get(4).getDataType());
    assertEquals("Emission", srcCols.get(5).getName());
    assertEquals(TSDataType.DOUBLE, srcCols.get(5).getDataType());

    List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
    assertEquals(2, fields.size());
    assertEquals("Temperature", fields.get(0).getName());
    assertEquals("Emission", fields.get(1).getName());
  }

  @Test
  public void testOldSchemaFormat() throws Exception {
    String path =
        writeSchema(
            "table_name=root.db1\n"
                + "time_precision=ms\n"
                + "has_header=true\n"
                + "separator=,\n"
                + "null_format=\\N\n"
                + "\n"
                + "id_columns\n"
                + "tmp1\n"
                + "\n"
                + "time_column=time\n"
                + "\n"
                + "csv_columns\n"
                + "time INT64,\n"
                + "tmp1 TEXT,\n"
                + "tmp2 FLOAT,\n"
                + "tmp3 FLOAT,\n"
                + "SKIP,\n"
                + "tmp5 FLOAT\n");

    ImportSchema schema = ImportSchemaParser.parse(path);

    assertEquals("root.db1", schema.getTableName());
    assertEquals("time", schema.getTimeColumnName());

    List<ImportSchema.TagColumn> tags = schema.getTagColumns();
    assertEquals(1, tags.size());
    assertEquals("tmp1", tags.get(0).getName());
    assertFalse(tags.get(0).hasDefault());

    List<ImportSchema.SourceColumn> srcCols = schema.getSourceColumns();
    assertEquals(6, srcCols.size());
    assertTrue(srcCols.get(4).isSkip());

    List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
    assertEquals(3, fields.size());
    assertEquals("tmp2", fields.get(0).getName());
    assertEquals(TSDataType.FLOAT, fields.get(0).getDataType());
    assertEquals("tmp3", fields.get(1).getName());
    assertEquals("tmp5", fields.get(2).getName());
  }

  @Test
  public void testFieldDerivationExcludesTimeTagSkip() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "time_column=ts\n"
                + "\n"
                + "tag_columns\n"
                + "device\n"
                + "\n"
                + "source_columns\n"
                + "ts INT64,\n"
                + "device TEXT,\n"
                + "SKIP,\n"
                + "value1 FLOAT,\n"
                + "value2 DOUBLE\n");

    ImportSchema schema = ImportSchemaParser.parse(path);

    List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
    assertEquals(2, fields.size());
    assertEquals("value1", fields.get(0).getName());
    assertEquals(TSDataType.FLOAT, fields.get(0).getDataType());
    assertEquals("value2", fields.get(1).getName());
    assertEquals(TSDataType.DOUBLE, fields.get(1).getDataType());
  }

  @Test
  public void testNoTagColumns() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "time_column=time\n"
                + "\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "temp FLOAT,\n"
                + "humidity DOUBLE\n");

    ImportSchema schema = ImportSchemaParser.parse(path);

    assertTrue(schema.getTagColumns().isEmpty());

    List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
    assertEquals(2, fields.size());
    assertEquals("temp", fields.get(0).getName());
    assertEquals("humidity", fields.get(1).getName());
  }

  @Test
  public void testDefaultTagColumn() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "time_column=time\n"
                + "\n"
                + "tag_columns\n"
                + "region DEFAULT beijing\n"
                + "site\n"
                + "\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "site TEXT,\n"
                + "value FLOAT\n");

    ImportSchema schema = ImportSchemaParser.parse(path);

    List<ImportSchema.TagColumn> tags = schema.getTagColumns();
    assertEquals(2, tags.size());

    assertTrue(tags.get(0).hasDefault());
    assertEquals("beijing", tags.get(0).getDefaultValue());
    assertTrue(tags.get(0).isVirtual());

    assertFalse(tags.get(1).hasDefault());
    assertFalse(tags.get(1).isVirtual());

    List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
    assertEquals(1, fields.size());
    assertEquals("value", fields.get(0).getName());
  }

  @Test
  public void testTabSeparator() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "separator=tab\n"
                + "time_column=time\n"
                + "\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "value FLOAT\n");

    ImportSchema schema = ImportSchemaParser.parse(path);
    assertEquals("\t", schema.getSeparator());
  }

  @Test
  public void testSemicolonSeparator() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "separator=;\n"
                + "time_column=time\n"
                + "\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "value FLOAT\n");

    ImportSchema schema = ImportSchemaParser.parse(path);
    assertEquals(";", schema.getSeparator());
  }

  @Test
  public void testMissingTableName() throws Exception {
    String path =
        writeSchema("time_column=time\n" + "source_columns\n" + "time INT64,\n" + "value FLOAT\n");

    try {
      ImportSchemaParser.parse(path);
      fail("Expected exception for missing table_name");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("table_name"));
    }
  }

  @Test
  public void testMissingTimeColumn() throws Exception {
    String path =
        writeSchema("table_name=test\n" + "source_columns\n" + "time INT64,\n" + "value FLOAT\n");

    try {
      ImportSchemaParser.parse(path);
      fail("Expected exception for missing time_column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("time_column"));
    }
  }

  @Test
  public void testTimeColumnNotInSourceColumns() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "time_column=missing_col\n"
                + "\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "value FLOAT\n");

    try {
      ImportSchemaParser.parse(path);
      fail("Expected exception for time_column not in source_columns");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("missing_col"));
    }
  }

  @Test
  public void testTagColumnNotInSourceColumns() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "time_column=time\n"
                + "\n"
                + "tag_columns\n"
                + "missing_tag\n"
                + "\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "value FLOAT\n");

    try {
      ImportSchemaParser.parse(path);
      fail("Expected exception for tag not in source_columns");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("missing_tag"));
    }
  }

  @Test
  public void testInvalidTimePrecision() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "time_precision=sec\n"
                + "time_column=time\n"
                + "\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "value FLOAT\n");

    try {
      ImportSchemaParser.parse(path);
      fail("Expected exception for invalid time_precision");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("time_precision"));
    }
  }

  @Test
  public void testNamedSkipForParquetArrow() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "time_column=time\n"
                + "\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "unused_col SKIP,\n"
                + "value FLOAT\n");

    ImportSchema schema = ImportSchemaParser.parse(path);

    List<ImportSchema.SourceColumn> srcCols = schema.getSourceColumns();
    assertEquals(3, srcCols.size());
    assertTrue(srcCols.get(1).isSkip());
    assertEquals("unused_col", srcCols.get(1).getName());

    List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
    assertEquals(1, fields.size());
    assertEquals("value", fields.get(0).getName());
  }

  @Test
  public void testAllDataTypes() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "time_column=ts\n"
                + "\n"
                + "source_columns\n"
                + "ts TIMESTAMP,\n"
                + "b BOOLEAN,\n"
                + "i32 INT32,\n"
                + "i64 INT64,\n"
                + "f FLOAT,\n"
                + "d DOUBLE,\n"
                + "s STRING,\n"
                + "t TEXT,\n"
                + "bl BLOB,\n"
                + "dt DATE\n");

    ImportSchema schema = ImportSchemaParser.parse(path);

    List<ImportSchema.SourceColumn> srcCols = schema.getSourceColumns();
    assertEquals(10, srcCols.size());
    assertEquals(TSDataType.TIMESTAMP, srcCols.get(0).getDataType());
    assertEquals(TSDataType.BOOLEAN, srcCols.get(1).getDataType());
    assertEquals(TSDataType.INT32, srcCols.get(2).getDataType());
    assertEquals(TSDataType.INT64, srcCols.get(3).getDataType());
    assertEquals(TSDataType.FLOAT, srcCols.get(4).getDataType());
    assertEquals(TSDataType.DOUBLE, srcCols.get(5).getDataType());
    assertEquals(TSDataType.STRING, srcCols.get(6).getDataType());
    assertEquals(TSDataType.TEXT, srcCols.get(7).getDataType());
    assertEquals(TSDataType.BLOB, srcCols.get(8).getDataType());
    assertEquals(TSDataType.DATE, srcCols.get(9).getDataType());
  }

  @Test
  public void testCommentsAndEmptyLines() throws Exception {
    String path =
        writeSchema(
            "// This is a comment\n"
                + "table_name=test\n"
                + "\n"
                + "// Another comment\n"
                + "time_column=time\n"
                + "\n"
                + "source_columns\n"
                + "// Column definitions\n"
                + "time INT64,\n"
                + "\n"
                + "value FLOAT\n");

    ImportSchema schema = ImportSchemaParser.parse(path);
    assertEquals("test", schema.getTableName());
    assertEquals(2, schema.getSourceColumns().size());
  }

  @Test
  public void testDefaultValues() throws Exception {
    String path =
        writeSchema(
            "table_name=test\n"
                + "time_column=time\n"
                + "\n"
                + "source_columns\n"
                + "time INT64,\n"
                + "value FLOAT\n");

    ImportSchema schema = ImportSchemaParser.parse(path);

    assertEquals("ms", schema.getTimePrecision());
    assertTrue(schema.isHasHeader());
    assertEquals(",", schema.getSeparator());
    assertNull(schema.getNullFormat());
  }

  @Test
  public void testNewSchemaEndToEndCsv() throws Exception {
    String schemaPath =
        writeSchema(
            "table_name=root.test\n"
                + "time_precision=ms\n"
                + "has_header=true\n"
                + "separator=,\n"
                + "\n"
                + "tag_columns\n"
                + "device\n"
                + "\n"
                + "time_column=ts\n"
                + "\n"
                + "source_columns\n"
                + "ts INT64,\n"
                + "device TEXT,\n"
                + "temperature FLOAT,\n"
                + "humidity DOUBLE\n");

    String csvPath = testDir + File.separator + "sensor.csv";
    try (BufferedWriter w = new BufferedWriter(new FileWriter(csvPath))) {
      w.write("ts,device,temperature,humidity\n");
      long ts = System.currentTimeMillis();
      for (int i = 0; i < 10; i++) {
        w.write((ts + i) + ",dev1," + (20.0f + i) + "," + (50.0 + i) + "\n");
      }
    }

    String targetPath = testDir + File.separator + "output";
    String[] args = new String[] {"-s" + csvPath, "-schema" + schemaPath, "-t" + targetPath};
    TsFileTool.main(args);

    String tsfilePath = targetPath + File.separator + "sensor.tsfile";
    assertTrue("TsFile should exist", new File(tsfilePath).exists());
  }
}
