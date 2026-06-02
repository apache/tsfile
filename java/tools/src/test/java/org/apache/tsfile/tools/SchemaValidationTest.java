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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the schema validation added to {@link ParquetSourceReader#readBatch()} and {@link
 * ArrowSourceReader#readBatch()}. Each format gets one positive case plus four negative cases:
 * column count too few, column count too many, unknown column name, and unnamed SKIP.
 */
public class SchemaValidationTest {

  private final String testDir = "target" + File.separator + "schemaValidationTest";
  private BufferAllocator allocator;

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
    allocator = new RootAllocator();
  }

  @After
  public void tearDown() {
    allocator.close();
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

  /** Build a 3-column Parquet file: time INT64, temperature FLOAT, humidity DOUBLE. */
  private File writeParquetFile(String name) throws IOException {
    MessageType schema =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("time")
            .required(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("temperature")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("humidity")
            .named("test");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    List<Group> rows = new ArrayList<>();
    rows.add(
        factory
            .newGroup()
            .append("time", 1L)
            .append("temperature", 25.0f)
            .append("humidity", 50.0));
    rows.add(
        factory
            .newGroup()
            .append("time", 2L)
            .append("temperature", 26.0f)
            .append("humidity", 51.0));

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

  /** Build a 3-column Arrow file: time INT64, temperature FLOAT, humidity DOUBLE. */
  private File writeArrowFile(String name) throws IOException {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("time", FieldType.notNullable(new ArrowType.Int(64, true)), null));
    fields.add(
        new Field(
            "temperature",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null));
    fields.add(
        new Field(
            "humidity",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null));
    Schema arrowSchema = new Schema(fields);

    File file = new File(testDir, name);
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        FileOutputStream fos = new FileOutputStream(file);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {
      writer.start();
      BigIntVector t = (BigIntVector) root.getVector("time");
      Float4Vector tp = (Float4Vector) root.getVector("temperature");
      Float8Vector hu = (Float8Vector) root.getVector("humidity");
      t.allocateNew(2);
      tp.allocateNew(2);
      hu.allocateNew(2);
      t.set(0, 1L);
      tp.set(0, 25.0f);
      hu.set(0, 50.0);
      t.set(1, 2L);
      tp.set(1, 26.0f);
      hu.set(1, 51.0);
      root.setRowCount(2);
      writer.writeBatch();
      writer.end();
    }
    return file;
  }

  private ImportSchema makeSchema(ImportSchema.SourceColumn... sourceColumns) {
    ImportSchema schema = new ImportSchema();
    schema.setTableName("root.test");
    schema.setTimeColumnName("time");
    schema.setTimePrecision("ms");
    schema.setSourceColumns(new ArrayList<>(Arrays.asList(sourceColumns)));
    return schema;
  }

  private ImportSchema.SourceColumn col(String name, TSDataType type) {
    return new ImportSchema.SourceColumn(name, type);
  }

  private void expectValidationError(Runnable action, String expectedSubstring) {
    try {
      action.run();
      fail("Expected IllegalArgumentException containing: " + expectedSubstring);
    } catch (IllegalArgumentException e) {
      assertNotNull("Exception message was null", e.getMessage());
      assertTrue(
          "Expected message to contain '" + expectedSubstring + "', got: " + e.getMessage(),
          e.getMessage().contains(expectedSubstring));
    }
  }

  // ===================== Parquet =====================

  @Test
  public void parquetSchemaValidPasses() throws Exception {
    File file = writeParquetFile("valid.parquet");
    ImportSchema schema =
        makeSchema(
            col("time", TSDataType.INT64),
            col("temperature", TSDataType.FLOAT),
            col("humidity", TSDataType.DOUBLE));
    try (ParquetSourceReader reader = new ParquetSourceReader(file, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      org.junit.Assert.assertEquals(2, batch.getRowCount());
    }
  }

  @Test
  public void parquetColumnCountTooFewThrows() throws Exception {
    final File file = writeParquetFile("count_few.parquet");
    final ImportSchema schema =
        makeSchema(col("time", TSDataType.INT64), col("temperature", TSDataType.FLOAT));
    expectValidationError(
        () -> {
          try (ParquetSourceReader reader = new ParquetSourceReader(file, schema)) {
            reader.readBatch();
          }
        },
        "Column count mismatch");
  }

  @Test
  public void parquetColumnCountTooManyThrows() throws Exception {
    final File file = writeParquetFile("count_many.parquet");
    final ImportSchema schema =
        makeSchema(
            col("time", TSDataType.INT64),
            col("temperature", TSDataType.FLOAT),
            col("humidity", TSDataType.DOUBLE),
            col("pressure", TSDataType.DOUBLE));
    expectValidationError(
        () -> {
          try (ParquetSourceReader reader = new ParquetSourceReader(file, schema)) {
            reader.readBatch();
          }
        },
        "Column count mismatch");
  }

  @Test
  public void parquetUnknownColumnNameThrows() throws Exception {
    final File file = writeParquetFile("unknown_name.parquet");
    final ImportSchema schema =
        makeSchema(
            col("time", TSDataType.INT64),
            col("tempo", TSDataType.FLOAT), // typo
            col("humidity", TSDataType.DOUBLE));
    expectValidationError(
        () -> {
          try (ParquetSourceReader reader = new ParquetSourceReader(file, schema)) {
            reader.readBatch();
          }
        },
        "tempo");
  }

  @Test
  public void parquetUnnamedSkipThrows() throws Exception {
    final File file = writeParquetFile("unnamed_skip.parquet");
    final ImportSchema schema =
        makeSchema(
            col("time", TSDataType.INT64),
            ImportSchema.SourceColumn.skip(), // unnamed
            col("humidity", TSDataType.DOUBLE));
    expectValidationError(
        () -> {
          try (ParquetSourceReader reader = new ParquetSourceReader(file, schema)) {
            reader.readBatch();
          }
        },
        "Unnamed SKIP");
  }

  // ===================== Arrow =====================

  @Test
  public void arrowSchemaValidPasses() throws Exception {
    File file = writeArrowFile("valid.arrow");
    ImportSchema schema =
        makeSchema(
            col("time", TSDataType.INT64),
            col("temperature", TSDataType.FLOAT),
            col("humidity", TSDataType.DOUBLE));
    try (ArrowSourceReader reader = new ArrowSourceReader(file, schema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      org.junit.Assert.assertEquals(2, batch.getRowCount());
    }
  }

  @Test
  public void arrowColumnCountTooFewThrows() throws Exception {
    final File file = writeArrowFile("count_few.arrow");
    final ImportSchema schema =
        makeSchema(col("time", TSDataType.INT64), col("temperature", TSDataType.FLOAT));
    expectValidationError(
        () -> {
          try (ArrowSourceReader reader = new ArrowSourceReader(file, schema)) {
            reader.readBatch();
          }
        },
        "Column count mismatch");
  }

  @Test
  public void arrowColumnCountTooManyThrows() throws Exception {
    final File file = writeArrowFile("count_many.arrow");
    final ImportSchema schema =
        makeSchema(
            col("time", TSDataType.INT64),
            col("temperature", TSDataType.FLOAT),
            col("humidity", TSDataType.DOUBLE),
            col("pressure", TSDataType.DOUBLE));
    expectValidationError(
        () -> {
          try (ArrowSourceReader reader = new ArrowSourceReader(file, schema)) {
            reader.readBatch();
          }
        },
        "Column count mismatch");
  }

  @Test
  public void arrowUnknownColumnNameThrows() throws Exception {
    final File file = writeArrowFile("unknown_name.arrow");
    final ImportSchema schema =
        makeSchema(
            col("time", TSDataType.INT64),
            col("tempo", TSDataType.FLOAT),
            col("humidity", TSDataType.DOUBLE));
    expectValidationError(
        () -> {
          try (ArrowSourceReader reader = new ArrowSourceReader(file, schema)) {
            reader.readBatch();
          }
        },
        "tempo");
  }

  @Test
  public void arrowUnnamedSkipThrows() throws Exception {
    final File file = writeArrowFile("unnamed_skip.arrow");
    final ImportSchema schema =
        makeSchema(
            col("time", TSDataType.INT64),
            ImportSchema.SourceColumn.skip(),
            col("humidity", TSDataType.DOUBLE));
    expectValidationError(
        () -> {
          try (ArrowSourceReader reader = new ArrowSourceReader(file, schema)) {
            reader.readBatch();
          }
        },
        "Unnamed SKIP");
  }
}
