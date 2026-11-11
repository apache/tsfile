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
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.ITsFileReader;
import org.apache.tsfile.read.v4.TsFileReaderBuilder;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TableSchemaEvolutionTest {

  private static final String testDir = "target" + File.separator + "tableSchemaEvolutionTest";

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(testDir));
  }

  private static TableSchema schema(String... fieldColumns) {
    final List<IMeasurementSchema> columns = new ArrayList<>();
    final List<ColumnCategory> categories = new ArrayList<>();
    columns.add(new MeasurementSchema("id1", TSDataType.STRING));
    categories.add(ColumnCategory.TAG);
    for (String field : fieldColumns) {
      columns.add(
          new MeasurementSchema(
              field, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
      categories.add(ColumnCategory.FIELD);
    }
    return new TableSchema("table1", columns, categories);
  }

  private static Tablet tablet(
      TableSchema tableSchema, int rowCount, int startTime, int valueOffset) {
    final List<String> columns = new ArrayList<>();
    final List<TSDataType> types = new ArrayList<>();
    for (IMeasurementSchema column : tableSchema.getColumnSchemas()) {
      columns.add(column.getMeasurementName());
      types.add(column.getType());
    }
    final Tablet tablet =
        new Tablet(tableSchema.getTableName(), columns, types, tableSchema.getColumnTypes());
    for (int i = 0; i < rowCount; i++) {
      tablet.addTimestamp(i, startTime + i);
      for (int c = 0; c < columns.size(); c++) {
        final String name = columns.get(c);
        if (tableSchema.getColumnTypes().get(c) == ColumnCategory.TAG) {
          tablet.addValue(name, i, "d");
        } else {
          tablet.addValue(name, i, valueOffset + i);
        }
      }
    }
    tablet.setRowSize(rowCount);
    return tablet;
  }

  @Test
  public void testRenameAcrossChunkGroups() throws Exception {
    final File file = new File(testDir, "rename.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(schema("s1"));
      writer.writeTable(tablet(schema("s1"), 10, 0, 0));
      writer.flush();
      writer.renameColumn("table1", "s1", "s2");
      writer.writeTable(tablet(schema("s2"), 10, 10, 100));
      writer.flush();
    }

    // The new name returns data written under both the old and the new name.
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build();
        ResultSet rs = reader.query("table1", Arrays.asList("s2"), 0, 20)) {
      final List<Integer> values = new ArrayList<>();
      while (rs.next()) {
        assertFalse(rs.isNull("s2"));
        values.add(rs.getInt("s2"));
      }
      assertEquals(20, values.size());
      for (int i = 0; i < 10; i++) {
        assertEquals(i, (int) values.get(i));
      }
      for (int i = 10; i < 20; i++) {
        assertEquals(100 + (i - 10), (int) values.get(i));
      }
    }

    // The old name is an alias and resolves to the same column.
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build();
        ResultSet rs = reader.query("table1", Arrays.asList("s1"), 0, 20)) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(20, count);
    }
  }

  @Test
  public void testRenameRejectsTagColumn() throws Exception {
    final File file = new File(testDir, "rename-tag.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(schema("s1"));
      assertThrows(
          IllegalArgumentException.class, () -> writer.renameColumn("table1", "id1", "id2"));
    }
  }

  @Test
  public void testRenameRejectsNameCollision() throws Exception {
    final File file = new File(testDir, "rename-collision.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(schema("s1", "s2"));
      // target collides with an existing column
      assertThrows(IllegalArgumentException.class, () -> writer.renameColumn("table1", "s1", "s2"));
      // target collides with a tombstone
      writer.dropColumn("table1", "s2");
      assertThrows(IllegalArgumentException.class, () -> writer.renameColumn("table1", "s1", "s2"));
    }
  }

  @Test
  public void testAddColumnDuplicateRejected() throws Exception {
    final File file = new File(testDir, "add-dup.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(schema("s1"));
      assertThrows(
          IllegalArgumentException.class, () -> writer.addColumn("table1", "s1", TSDataType.INT64));
    }
  }

  @Test
  public void testDropThenReAddWithDifferentType() throws Exception {
    final File file = new File(testDir, "drop-readd.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(schema("s1"));
      writer.writeTable(tablet(schema("s1"), 10, 0, 0));
      writer.flush();
      writer.dropColumn("table1", "s1");
      assertEquals(-1, writer.getSchema().getTableSchemaMap().get("table1").findColumnIndex("s1"));
      // re-adding a dropped column with a different type is allowed (the tombstone is removed)
      writer.addColumn(
          "table1", "s1", TSDataType.INT64, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
      assertTrue(writer.getSchema().getTableSchemaMap().get("table1").findColumnIndex("s1") >= 0);
      // the re-added column is now active, so a second add is a duplicate
      assertThrows(
          IllegalArgumentException.class, () -> writer.addColumn("table1", "s1", TSDataType.INT32));
    }
  }

  @Test
  public void testAddColumnReadsNullForOldChunkGroups() throws Exception {
    final File file = new File(testDir, "add.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(schema("s1"));
      writer.writeTable(tablet(schema("s1"), 10, 0, 0));
      writer.flush();
      writer.addColumn("table1", "s2", TSDataType.INT32);
      writer.writeTable(tablet(schema("s1", "s2"), 10, 10, 100));
      writer.flush();
    }
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build();
        ResultSet rs = reader.query("table1", Arrays.asList("s1", "s2"), 0, 20)) {
      final List<Integer> s1 = new ArrayList<>();
      final List<Boolean> s2Null = new ArrayList<>();
      final List<Integer> s2 = new ArrayList<>();
      while (rs.next()) {
        s1.add(rs.getInt("s1"));
        s2Null.add(rs.isNull("s2"));
        if (!rs.isNull("s2")) {
          s2.add(rs.getInt("s2"));
        }
      }
      assertEquals(20, s1.size());
      assertEquals(20, s2Null.size());
      // s1 has data in all 20 rows; s2 is null for the first 10 (pre-add) and populated for the
      // last 10.
      for (int i = 0; i < 10; i++) {
        assertTrue(s2Null.get(i));
      }
      for (int i = 10; i < 20; i++) {
        assertFalse(s2Null.get(i));
      }
      assertEquals(10, s2.size());
      for (int i = 0; i < 10; i++) {
        assertEquals(100 + i, (int) s2.get(i));
      }
    }
  }

  @Test
  public void testDropHidesColumn() throws Exception {
    final File file = new File(testDir, "drop.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(schema("s1", "s2"));
      writer.writeTable(tablet(schema("s1", "s2"), 10, 0, 0));
      writer.flush();
      writer.dropColumn("table1", "s1");
    }
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build()) {
      assertEquals(-1, reader.getTableSchemas("table1").get().findColumnIndex("s1"));
      assertEquals(0, reader.getTableSchemas("table1").get().findColumnIndex("id1"));
    }
  }

  @Test
  public void testNoEvolutionFileReadsUnchanged() throws Exception {
    final File file = new File(testDir, "plain.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTableSchema(schema("s1", "s2"));
      writer.writeTable(tablet(schema("s1", "s2"), 10, 0, 0));
    }
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build();
        ResultSet rs = reader.query("table1", Arrays.asList("s1", "s2"), 0, 10)) {
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals(10, count);
    }
    // A file with no evolution carries no schema-evolution property, so it stays byte-compatible
    // with files written before this feature.
    try (ITsFileReader reader = new TsFileReaderBuilder().file(file).build()) {
      assertFalse(reader.getTsFileProperties().containsKey("tableSchemaEvolution"));
    }
  }
}
