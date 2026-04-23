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

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ParquetSourceReaderTest {

  private final String testDir = "target" + File.separator + "parquetReaderTest";

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
  }

  @After
  public void tearDown() {
    deleteRecursive(new File(testDir));
  }

  private void deleteRecursive(File dir) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteRecursive(f);
        }
        f.delete();
      }
    }
    dir.delete();
  }

  private File writeParquetFile(String name, MessageType schema, List<Group> rows)
      throws IOException {
    File file = new File(testDir, name);
    if (file.exists()) {
      file.delete();
    }

    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file.toPath()))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()) {
      for (Group row : rows) {
        writer.write(row);
      }
    }
    return file;
  }

  private MessageType buildBasicSchema() {
    return Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .named("time")
        .required(PrimitiveType.PrimitiveTypeName.FLOAT)
        .named("temperature")
        .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
        .named("humidity")
        .named("test");
  }

  @Test
  public void testAutoModeInferSchema() throws Exception {
    MessageType pqSchema = buildBasicSchema();
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(
        factory
            .newGroup()
            .append("time", 1000L)
            .append("temperature", 25.5f)
            .append("humidity", 60.0));
    rows.add(
        factory
            .newGroup()
            .append("time", 2000L)
            .append("temperature", 26.0f)
            .append("humidity", 55.0));

    File file = writeParquetFile("auto.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("auto", schema.getTableName());
      assertEquals("time", schema.getTimeColumnName());
      assertTrue(schema.getTagColumns().isEmpty());

      List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
      assertEquals(2, fields.size());
      assertEquals("temperature", fields.get(0).getName());
      assertEquals("humidity", fields.get(1).getName());
    }
  }

  @Test
  public void testAutoModeReadBatch() throws Exception {
    MessageType pqSchema = buildBasicSchema();
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(
          factory
              .newGroup()
              .append("time", 1000L + i)
              .append("temperature", 20.0f + i)
              .append("humidity", 50.0 + i));
    }

    File file = writeParquetFile("batch.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      reader.inferSchema();
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(5, batch.getRowCount());

      assertEquals(1000L, batch.getValue(0, 0));
      assertEquals(20.0f, (float) batch.getValue(0, 1), 0.001f);
      assertEquals(50.0, (double) batch.getValue(0, 2), 0.001);

      assertNull(reader.readBatch());
    }
  }

  @Test
  public void testAutoModeUppercaseTIME() throws Exception {
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("TIME")
            .required(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("value")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup().append("TIME", 1000L).append("value", 1.0f));

    File file = writeParquetFile("upper_time.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("TIME", schema.getTimeColumnName());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAutoModeMixedCaseTimeFails() throws Exception {
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("Time")
            .required(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("value")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup().append("Time", 1000L).append("value", 1.0f));

    File file = writeParquetFile("mixed_time.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      reader.inferSchema();
    }
  }

  @Test
  public void testAutoModeTableNameFromFilename() throws Exception {
    MessageType pqSchema = buildBasicSchema();
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(
        factory
            .newGroup()
            .append("time", 1000L)
            .append("temperature", 25.0f)
            .append("humidity", 60.0));

    File file = writeParquetFile("sensor_data.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("sensor_data", schema.getTableName());
    }
  }

  @Test
  public void testAutoModeTableNameOverride() throws Exception {
    MessageType pqSchema = buildBasicSchema();
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(
        factory
            .newGroup()
            .append("time", 1000L)
            .append("temperature", 25.0f)
            .append("humidity", 60.0));

    File file = writeParquetFile("data.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      reader.setOverrideTableName("custom_table");
      ImportSchema schema = reader.inferSchema();
      assertEquals("custom_table", schema.getTableName());
    }
  }

  @Test
  public void testAutoModeTimestampPrecisionMillis() throws Exception {
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("value")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup().append("time", 1000L).append("value", 1.0f));

    File file = writeParquetFile("ts_millis.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("ms", schema.getTimePrecision());
    }
  }

  @Test
  public void testAutoModeTimestampPrecisionMicros() throws Exception {
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("value")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup().append("time", 1000000L).append("value", 1.0f));

    File file = writeParquetFile("ts_micros.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("us", schema.getTimePrecision());
    }
  }

  @Test
  public void testSchemaMode() throws Exception {
    MessageType pqSchema = buildBasicSchema();
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(
        factory
            .newGroup()
            .append("time", 1000L)
            .append("temperature", 25.5f)
            .append("humidity", 60.0));
    rows.add(
        factory
            .newGroup()
            .append("time", 2000L)
            .append("temperature", 26.0f)
            .append("humidity", 55.0));

    File file = writeParquetFile("schema_mode.parquet", pqSchema, rows);

    ImportSchema importSchema = new ImportSchema();
    importSchema.setTableName("test_table");
    importSchema.setTimeColumnName("time");
    importSchema.setTimePrecision("ms");
    importSchema.setTagColumns(new ArrayList<ImportSchema.TagColumn>());
    importSchema.setSourceColumns(
        Arrays.asList(
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            new ImportSchema.SourceColumn("temperature", TSDataType.FLOAT),
            new ImportSchema.SourceColumn("humidity", TSDataType.DOUBLE)));

    try (ParquetSourceReader reader = new ParquetSourceReader(file, importSchema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
      assertEquals(1000L, batch.getValue(0, 0));
      assertEquals(25.5f, (float) batch.getValue(0, 1), 0.001f);
      assertEquals(60.0, (double) batch.getValue(0, 2), 0.001);

      assertNull(reader.readBatch());
    }
  }

  @Test
  public void testSchemaModeNamedSkip() throws Exception {
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("unused")
            .required(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("value")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup().append("time", 1000L).append("unused", "x").append("value", 3.14f));

    File file = writeParquetFile("skip.parquet", pqSchema, rows);

    ImportSchema importSchema = new ImportSchema();
    importSchema.setTableName("test");
    importSchema.setTimeColumnName("time");
    importSchema.setTimePrecision("ms");
    importSchema.setTagColumns(new ArrayList<ImportSchema.TagColumn>());
    importSchema.setSourceColumns(
        Arrays.asList(
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            ImportSchema.SourceColumn.skip("unused"),
            new ImportSchema.SourceColumn("value", TSDataType.FLOAT)));

    try (ParquetSourceReader reader = new ParquetSourceReader(file, importSchema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(1, batch.getRowCount());
    }
  }

  @Test
  public void testNativeNullHandling() throws Exception {
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .optional(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("value")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(factory.newGroup().append("time", 1000L).append("value", 3.14f));
    rows.add(factory.newGroup().append("time", 2000L));

    File file = writeParquetFile("nulls.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      reader.inferSchema();
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
      assertEquals(3.14f, (float) batch.getValue(0, 1), 0.001f);
      assertNull(batch.getValue(1, 1));
    }
  }

  @Test
  public void testTypeMapping() throws Exception {
    MessageType pqSchema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("flag")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("count")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(pqSchema);

    List<Group> rows = new ArrayList<>();
    rows.add(
        factory
            .newGroup()
            .append("time", 1000L)
            .append("flag", true)
            .append("count", 42)
            .append("name", "hello"));

    File file = writeParquetFile("types.parquet", pqSchema, rows);

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();

      List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
      assertEquals(3, fields.size());
      assertEquals(TSDataType.BOOLEAN, fields.get(0).getDataType());
      assertEquals(TSDataType.INT32, fields.get(1).getDataType());
      assertEquals(TSDataType.STRING, fields.get(2).getDataType());

      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(true, batch.getValue(0, 1));
      assertEquals(42, batch.getValue(0, 2));
      assertEquals("hello", batch.getValue(0, 3));
    }
  }

  @Test
  public void testEmptyFile() throws Exception {
    MessageType pqSchema = buildBasicSchema();

    File file = writeParquetFile("empty.parquet", pqSchema, new ArrayList<Group>());

    try (ParquetSourceReader reader = new ParquetSourceReader(file)) {
      reader.inferSchema();
      SourceBatch batch = reader.readBatch();
      assertNull(batch);
    }
  }
}
