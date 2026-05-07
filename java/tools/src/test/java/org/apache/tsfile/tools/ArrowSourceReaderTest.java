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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ArrowSourceReaderTest {

  private final String testDir = "target" + File.separator + "arrowReaderTest";
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

  private Schema buildBasicSchema() {
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
    return new Schema(fields);
  }

  private File writeArrowFile(String name, Schema schema, WriteCallback callback)
      throws IOException {
    File file = new File(testDir, name);
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        FileOutputStream fos = new FileOutputStream(file);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, fos.getChannel())) {
      writer.start();
      callback.write(root, writer);
      writer.end();
    }
    return file;
  }

  interface WriteCallback {
    void write(VectorSchemaRoot root, ArrowFileWriter writer) throws IOException;
  }

  @Test
  public void testAutoModeInferSchema() throws Exception {
    Schema arrowSchema = buildBasicSchema();
    File file =
        writeArrowFile(
            "auto.arrow",
            arrowSchema,
            (root, writer) -> {
              BigIntVector timeVec = (BigIntVector) root.getVector("time");
              Float4Vector tempVec = (Float4Vector) root.getVector("temperature");
              Float8Vector humVec = (Float8Vector) root.getVector("humidity");
              timeVec.allocateNew(2);
              tempVec.allocateNew(2);
              humVec.allocateNew(2);
              timeVec.set(0, 1000L);
              tempVec.set(0, 25.5f);
              humVec.set(0, 60.0);
              timeVec.set(1, 2000L);
              tempVec.set(1, 26.0f);
              humVec.set(1, 55.0);
              root.setRowCount(2);
              writer.writeBatch();
            });

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("auto", schema.getTableName());
      assertEquals("time", schema.getTimeColumnName());
      assertTrue(schema.getTagColumns().isEmpty());

      List<ImportSchema.SourceColumn> fields = schema.fieldColumns();
      assertEquals(2, fields.size());
      assertEquals("temperature", fields.get(0).getName());
      assertEquals(TSDataType.FLOAT, fields.get(0).getDataType());
      assertEquals("humidity", fields.get(1).getName());
      assertEquals(TSDataType.DOUBLE, fields.get(1).getDataType());
    }
  }

  @Test
  public void testAutoModeReadBatch() throws Exception {
    Schema arrowSchema = buildBasicSchema();
    File file =
        writeArrowFile(
            "batch.arrow",
            arrowSchema,
            (root, writer) -> {
              BigIntVector timeVec = (BigIntVector) root.getVector("time");
              Float4Vector tempVec = (Float4Vector) root.getVector("temperature");
              Float8Vector humVec = (Float8Vector) root.getVector("humidity");
              timeVec.allocateNew(3);
              tempVec.allocateNew(3);
              humVec.allocateNew(3);
              for (int i = 0; i < 3; i++) {
                timeVec.set(i, 1000L + i);
                tempVec.set(i, 20.0f + i);
                humVec.set(i, 50.0 + i);
              }
              root.setRowCount(3);
              writer.writeBatch();
            });

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
      reader.inferSchema();
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(3, batch.getRowCount());
      assertEquals(1000L, batch.getValue(0, 0));
      assertEquals(20.0f, (float) batch.getValue(0, 1), 0.001f);
      assertEquals(50.0, (double) batch.getValue(0, 2), 0.001);
      assertNull(reader.readBatch());
    }
  }

  @Test
  public void testAutoModeUppercaseTIME() throws Exception {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("TIME", FieldType.notNullable(new ArrowType.Int(64, true)), null));
    fields.add(
        new Field(
            "value",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null));
    Schema arrowSchema = new Schema(fields);

    File file =
        writeArrowFile(
            "upper.arrow",
            arrowSchema,
            (root, writer) -> {
              BigIntVector tv = (BigIntVector) root.getVector("TIME");
              Float4Vector vv = (Float4Vector) root.getVector("value");
              tv.allocateNew(1);
              vv.allocateNew(1);
              tv.set(0, 1000L);
              vv.set(0, 1.0f);
              root.setRowCount(1);
              writer.writeBatch();
            });

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("TIME", schema.getTimeColumnName());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAutoModeMixedCaseTimeFails() throws Exception {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("Time", FieldType.notNullable(new ArrowType.Int(64, true)), null));
    fields.add(
        new Field(
            "value",
            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null));

    File file =
        writeArrowFile(
            "mixed.arrow",
            new Schema(fields),
            (root, writer) -> {
              BigIntVector tv = (BigIntVector) root.getVector("Time");
              Float4Vector vv = (Float4Vector) root.getVector("value");
              tv.allocateNew(1);
              vv.allocateNew(1);
              tv.set(0, 1000L);
              vv.set(0, 1.0f);
              root.setRowCount(1);
              writer.writeBatch();
            });

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
      reader.inferSchema();
    }
  }

  @Test
  public void testAutoModeTableNameFromFilename() throws Exception {
    Schema arrowSchema = buildBasicSchema();
    File file =
        writeArrowFile(
            "sensor_data.arrow",
            arrowSchema,
            (root, writer) -> {
              BigIntVector tv = (BigIntVector) root.getVector("time");
              Float4Vector tp = (Float4Vector) root.getVector("temperature");
              Float8Vector hm = (Float8Vector) root.getVector("humidity");
              tv.allocateNew(1);
              tp.allocateNew(1);
              hm.allocateNew(1);
              tv.set(0, 1000L);
              tp.set(0, 25.0f);
              hm.set(0, 60.0);
              root.setRowCount(1);
              writer.writeBatch();
            });

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();
      assertEquals("sensor_data", schema.getTableName());
    }
  }

  @Test
  public void testAutoModeTableNameOverride() throws Exception {
    Schema arrowSchema = buildBasicSchema();
    File file =
        writeArrowFile(
            "data.arrow",
            arrowSchema,
            (root, writer) -> {
              BigIntVector tv = (BigIntVector) root.getVector("time");
              Float4Vector tp = (Float4Vector) root.getVector("temperature");
              Float8Vector hm = (Float8Vector) root.getVector("humidity");
              tv.allocateNew(1);
              tp.allocateNew(1);
              hm.allocateNew(1);
              tv.set(0, 1000L);
              tp.set(0, 25.0f);
              hm.set(0, 60.0);
              root.setRowCount(1);
              writer.writeBatch();
            });

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
      reader.setOverrideTableName("custom");
      ImportSchema schema = reader.inferSchema();
      assertEquals("custom", schema.getTableName());
    }
  }

  @Test
  public void testSchemaMode() throws Exception {
    Schema arrowSchema = buildBasicSchema();
    File file =
        writeArrowFile(
            "schema_mode.arrow",
            arrowSchema,
            (root, writer) -> {
              BigIntVector tv = (BigIntVector) root.getVector("time");
              Float4Vector tp = (Float4Vector) root.getVector("temperature");
              Float8Vector hm = (Float8Vector) root.getVector("humidity");
              tv.allocateNew(2);
              tp.allocateNew(2);
              hm.allocateNew(2);
              tv.set(0, 1000L);
              tp.set(0, 25.5f);
              hm.set(0, 60.0);
              tv.set(1, 2000L);
              tp.set(1, 26.0f);
              hm.set(1, 55.0);
              root.setRowCount(2);
              writer.writeBatch();
            });

    ImportSchema importSchema = new ImportSchema();
    importSchema.setTableName("test");
    importSchema.setTimeColumnName("time");
    importSchema.setTimePrecision("ms");
    importSchema.setTagColumns(new ArrayList<ImportSchema.TagColumn>());
    importSchema.setSourceColumns(
        Arrays.asList(
            new ImportSchema.SourceColumn("time", TSDataType.INT64),
            new ImportSchema.SourceColumn("temperature", TSDataType.FLOAT),
            new ImportSchema.SourceColumn("humidity", TSDataType.DOUBLE)));

    try (ArrowSourceReader reader = new ArrowSourceReader(file, importSchema)) {
      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(2, batch.getRowCount());
      assertEquals(1000L, batch.getValue(0, 0));
      assertEquals(25.5f, (float) batch.getValue(0, 1), 0.001f);
      assertNull(reader.readBatch());
    }
  }

  @Test
  public void testNativeNullHandling() throws Exception {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("time", FieldType.notNullable(new ArrowType.Int(64, true)), null));
    fields.add(
        new Field(
            "value",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null));

    File file =
        writeArrowFile(
            "nulls.arrow",
            new Schema(fields),
            (root, writer) -> {
              BigIntVector tv = (BigIntVector) root.getVector("time");
              Float4Vector vv = (Float4Vector) root.getVector("value");
              tv.allocateNew(2);
              vv.allocateNew(2);
              tv.set(0, 1000L);
              vv.set(0, 3.14f);
              tv.set(1, 2000L);
              vv.setNull(1);
              root.setRowCount(2);
              writer.writeBatch();
            });

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
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
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("time", FieldType.notNullable(new ArrowType.Int(64, true)), null));
    fields.add(new Field("flag", FieldType.notNullable(new ArrowType.Bool()), null));
    fields.add(new Field("count", FieldType.notNullable(new ArrowType.Int(32, true)), null));
    fields.add(new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null));

    File file =
        writeArrowFile(
            "types.arrow",
            new Schema(fields),
            (root, writer) -> {
              BigIntVector tv = (BigIntVector) root.getVector("time");
              BitVector bv = (BitVector) root.getVector("flag");
              IntVector iv = (IntVector) root.getVector("count");
              VarCharVector sv = (VarCharVector) root.getVector("name");
              tv.allocateNew(1);
              bv.allocateNew(1);
              iv.allocateNew(1);
              sv.allocateNew(1);
              tv.set(0, 1000L);
              bv.set(0, 1);
              iv.set(0, 42);
              sv.set(0, "hello".getBytes(StandardCharsets.UTF_8));
              root.setRowCount(1);
              writer.writeBatch();
            });

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
      ImportSchema schema = reader.inferSchema();
      List<ImportSchema.SourceColumn> schemaFields = schema.fieldColumns();
      assertEquals(3, schemaFields.size());
      assertEquals(TSDataType.BOOLEAN, schemaFields.get(0).getDataType());
      assertEquals(TSDataType.INT32, schemaFields.get(1).getDataType());
      assertEquals(TSDataType.STRING, schemaFields.get(2).getDataType());

      SourceBatch batch = reader.readBatch();
      assertNotNull(batch);
      assertEquals(true, batch.getValue(0, 1));
      assertEquals(42, batch.getValue(0, 2));
      assertEquals("hello", batch.getValue(0, 3));
    }
  }

  @Test
  public void testEmptyFile() throws Exception {
    Schema arrowSchema = buildBasicSchema();
    File file = writeArrowFile("empty.arrow", arrowSchema, (root, writer) -> {});

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
      reader.inferSchema();
      SourceBatch batch = reader.readBatch();
      assertNull(batch);
    }
  }

  @Test
  public void testMultipleRecordBatches() throws Exception {
    Schema arrowSchema = buildBasicSchema();
    File file =
        writeArrowFile(
            "multi.arrow",
            arrowSchema,
            (root, writer) -> {
              BigIntVector tv = (BigIntVector) root.getVector("time");
              Float4Vector tp = (Float4Vector) root.getVector("temperature");
              Float8Vector hm = (Float8Vector) root.getVector("humidity");

              tv.allocateNew(2);
              tp.allocateNew(2);
              hm.allocateNew(2);
              tv.set(0, 1000L);
              tp.set(0, 20.0f);
              hm.set(0, 50.0);
              tv.set(1, 2000L);
              tp.set(1, 21.0f);
              hm.set(1, 51.0);
              root.setRowCount(2);
              writer.writeBatch();

              tv.allocateNew(3);
              tp.allocateNew(3);
              hm.allocateNew(3);
              for (int i = 0; i < 3; i++) {
                tv.set(i, 3000L + i);
                tp.set(i, 30.0f + i);
                hm.set(i, 60.0 + i);
              }
              root.setRowCount(3);
              writer.writeBatch();
            });

    try (ArrowSourceReader reader = new ArrowSourceReader(file)) {
      reader.inferSchema();

      SourceBatch batch1 = reader.readBatch();
      assertNotNull(batch1);
      assertEquals(2, batch1.getRowCount());

      SourceBatch batch2 = reader.readBatch();
      assertNotNull(batch2);
      assertEquals(3, batch2.getRowCount());

      assertNull(reader.readBatch());
    }
  }
}
