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

package org.apache.tsfile.read.query;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.filter.factory.TagFilterBuilder;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.DeviceTableModelReader;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;

public class ResultSetTest {

  private File tsfile;

  @Before
  public void setTsfile() {
    final String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.sg1", 0, 0, 1);
    tsfile = new File(filePath);
    if (!tsfile.getParentFile().exists()) {
      Assert.assertTrue(tsfile.getParentFile().mkdirs());
    }
  }

  @After
  public void deleteFile() throws IOException {
    if (tsfile != null) {
      Files.deleteIfExists(tsfile.toPath());
    }
  }

  @Test
  public void testQueryTable() throws Exception {
    TableSchema tableSchema =
        new TableSchema(
            "t1",
            Arrays.asList(
                new MeasurementSchema("id1", TSDataType.STRING),
                new MeasurementSchema("id2", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.BOOLEAN),
                new MeasurementSchema("s2", TSDataType.BOOLEAN)),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD));
    Tablet tablet =
        new Tablet(
            Arrays.asList("id1", "id2", "s1", "s2"),
            Arrays.asList(
                TSDataType.STRING, TSDataType.STRING, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
            1024);
    tablet.addTimestamp(0, 0);
    tablet.addValue("id1", 0, "id_field1");
    tablet.addValue("id2", 0, "id_field2");
    tablet.addValue("s1", 0, true);
    tablet.addValue("s2", 0, false);

    tablet.addTimestamp(1, 1);
    tablet.addValue("id1", 1, "id_field1_2");
    tablet.addValue("s2", 1, true);

    tablet.addTimestamp(2, 2);

    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(tsfile).tableSchema(tableSchema).build()) {
      writer.write(tablet);
    }

    try (DeviceTableModelReader tsFileReader = new DeviceTableModelReader(tsfile);
        ResultSet resultSet =
            tsFileReader.query("T1", Arrays.asList("ID1", "ID2", "S2", "S1"), 0, 2); ) {
      // id1 id2 s2 s1
      ResultSetMetadata resultSetMetadata = resultSet.getMetadata();
      // Time id1 id2 s2 s1
      Assert.assertEquals("Time", resultSetMetadata.getColumnName(1));
      Assert.assertEquals(TSDataType.INT64, resultSetMetadata.getColumnType(1));
      Assert.assertEquals("ID1", resultSetMetadata.getColumnName(2));
      Assert.assertEquals(TSDataType.STRING, resultSetMetadata.getColumnType(2));
      Assert.assertEquals("ID2", resultSetMetadata.getColumnName(3));
      Assert.assertEquals(TSDataType.STRING, resultSetMetadata.getColumnType(3));
      Assert.assertEquals("S2", resultSetMetadata.getColumnName(4));
      Assert.assertEquals(TSDataType.BOOLEAN, resultSetMetadata.getColumnType(4));
      Assert.assertEquals("S1", resultSetMetadata.getColumnName(5));
      Assert.assertEquals(TSDataType.BOOLEAN, resultSetMetadata.getColumnType(5));

      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(2, resultSet.getLong(1));
      Assert.assertTrue(resultSet.isNull(2));
      Assert.assertTrue(resultSet.isNull(3));
      Assert.assertTrue(resultSet.isNull(4));
      Assert.assertTrue(resultSet.isNull(5));

      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(0, resultSet.getLong(1));
      Assert.assertEquals("id_field1", resultSet.getString(2));
      Assert.assertEquals("id_field2", resultSet.getString(3));
      Assert.assertFalse(resultSet.getBoolean(4));
      Assert.assertTrue(resultSet.getBoolean(5));

      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(1, resultSet.getLong(1));
      Assert.assertEquals("id_field1_2", resultSet.getString(2));
      Assert.assertTrue(resultSet.isNull(3));
      Assert.assertTrue(resultSet.getBoolean(4));
      Assert.assertTrue(resultSet.isNull(5));
    }
  }

  @Test
  public void testQueryTableWithAllTagFilters() throws Exception {
    TableSchema tableSchema =
        new TableSchema(
            "t1",
            Arrays.asList(
                new MeasurementSchema("id1", TSDataType.STRING),
                new MeasurementSchema("id2", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.BOOLEAN),
                new MeasurementSchema("s2", TSDataType.BOOLEAN)),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD));

    Tablet tablet =
        new Tablet(
            Arrays.asList("id1", "id2", "s1", "s2"),
            Arrays.asList(
                TSDataType.STRING, TSDataType.STRING, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
            1024);

    // Row 0
    tablet.addTimestamp(0, 0);
    tablet.addValue("id1", 0, "id_01");
    tablet.addValue("id2", 0, "name_01");
    tablet.addValue("s1", 0, true);
    tablet.addValue("s2", 0, false);

    // Row 1
    tablet.addTimestamp(1, 1);
    tablet.addValue("id1", 1, "id_02");
    tablet.addValue("id2", 1, "name_02");
    tablet.addValue("s1", 1, false);
    tablet.addValue("s2", 1, true);

    // Row 2
    tablet.addTimestamp(2, 2);
    tablet.addValue("id1", 2, "id_03");
    tablet.addValue("id2", 2, "name_03");
    tablet.addValue("s1", 2, true);
    tablet.addValue("s2", 2, true);

    // Row 3
    tablet.addTimestamp(3, 3);
    tablet.addValue("id1", 3, "id_04");
    tablet.addValue("id2", 3, "name_04");
    tablet.addValue("s1", 3, false);
    tablet.addValue("s2", 3, false);

    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(tsfile).tableSchema(tableSchema).build()) {
      writer.write(tablet);
    }

    TagFilterBuilder filterBuilder = new TagFilterBuilder(tableSchema);

    // eq
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.eq("ID1", "id_02"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(1, rs.getLong(1));
      Assert.assertEquals("id_02", rs.getString(2));
      Assert.assertEquals("name_02", rs.getString(3));
    }

    // neq
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.neq("ID1", "id_02"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(0, rs.getLong(1));
      Assert.assertTrue(rs.next());
      Assert.assertEquals(2, rs.getLong(1));
      Assert.assertTrue(rs.next());
      Assert.assertEquals(3, rs.getLong(1));
    }

    // lt
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.lt("ID1", "id_03"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(0, rs.getLong(1));
      Assert.assertTrue(rs.next());
      Assert.assertEquals(1, rs.getLong(1));
    }

    // lteq
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.lteq("ID1", "id_03"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(0, rs.getLong(1));
      Assert.assertTrue(rs.next());
      Assert.assertEquals(1, rs.getLong(1));
      Assert.assertTrue(rs.next());
      Assert.assertEquals(2, rs.getLong(1));
    }

    // gt
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.gt("ID1", "id_02"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(2, rs.getLong(1));
      Assert.assertTrue(rs.next());
      Assert.assertEquals(3, rs.getLong(1));
    }

    // gteq
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.gteq("ID1", "id_02"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(1, rs.getLong(1));
      Assert.assertTrue(rs.next());
      Assert.assertEquals(2, rs.getLong(1));
      Assert.assertTrue(rs.next());
      Assert.assertEquals(3, rs.getLong(1));
    }

    // betweenAnd
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.betweenAnd("ID1", "id_02", "id_03"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(1, rs.getLong(1)); // id_02
      Assert.assertTrue(rs.next());
      Assert.assertEquals(2, rs.getLong(1)); // id_03
    }

    // notBetweenAnd
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.notBetweenAnd("ID1", "id_02", "id_03"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(0, rs.getLong(1)); // id_01
      Assert.assertTrue(rs.next());
      Assert.assertEquals(3, rs.getLong(1)); // id_04
    }

    // and
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.and(
                    filterBuilder.gteq("ID1", "id_02"), filterBuilder.lteq("ID1", "id_03")))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(1, rs.getLong(1)); // id_02
      Assert.assertTrue(rs.next());
      Assert.assertEquals(2, rs.getLong(1)); // id_03
    }

    // or
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.or(
                    filterBuilder.eq("ID1", "id_01"), filterBuilder.eq("ID1", "id_04")))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(0, rs.getLong(1)); // id_01
      Assert.assertTrue(rs.next());
      Assert.assertEquals(3, rs.getLong(1)); // id_04
    }

    // not
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.not(filterBuilder.eq("ID1", "id_02")))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(0, rs.getLong(1)); // id_01
      Assert.assertTrue(rs.next());
      Assert.assertEquals(2, rs.getLong(1)); // id_03
      Assert.assertTrue(rs.next());
      Assert.assertEquals(3, rs.getLong(1)); // id_04
    }

    // regExp
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.regExp("ID1", "id_0[23]"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(1, rs.getLong(1)); // id_02
      Assert.assertTrue(rs.next());
      Assert.assertEquals(2, rs.getLong(1)); // id_03
    }

    // notRegExp
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.notRegExp("ID1", "id_0[23]"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(0, rs.getLong(1)); // id_01
      Assert.assertTrue(rs.next());
      Assert.assertEquals(3, rs.getLong(1)); // id_04
    }

    // like
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.like("ID1", "id_0_"))) {
      Assert.assertTrue(rs.next());
      Assert.assertEquals(0, rs.getLong(1)); // id_01
      Assert.assertTrue(rs.next());
      Assert.assertEquals(1, rs.getLong(1)); // id_02
      Assert.assertTrue(rs.next());
      Assert.assertEquals(2, rs.getLong(1)); // id_03
      Assert.assertTrue(rs.next());
      Assert.assertEquals(3, rs.getLong(1)); // id_04
    }

    // notLike
    try (DeviceTableModelReader r = new DeviceTableModelReader(tsfile);
        ResultSet rs =
            r.query(
                "T1",
                Arrays.asList("ID1", "ID2", "S2", "S1"),
                0,
                3,
                filterBuilder.notLike("ID1", "id_0_"))) {
      Assert.assertFalse(rs.next());
    }
  }

  @Test
  public void testQueryTableByIterator() throws Exception {
    TableSchema tableSchema =
        new TableSchema(
            "t1",
            Arrays.asList(
                new MeasurementSchema("id1", TSDataType.STRING),
                new MeasurementSchema("id2", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.BOOLEAN),
                new MeasurementSchema("s2", TSDataType.BOOLEAN)),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD));
    Tablet tablet =
        new Tablet(
            Arrays.asList("id1", "id2", "s1", "s2"),
            Arrays.asList(
                TSDataType.STRING, TSDataType.STRING, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
            1024);
    tablet.addTimestamp(0, 0);
    tablet.addValue("id1", 0, "id_field1");
    tablet.addValue("id2", 0, "id_field2");
    tablet.addValue("s1", 0, true);
    tablet.addValue("s2", 0, false);

    tablet.addTimestamp(1, 1);
    tablet.addValue("id1", 1, "id_field1_2");
    tablet.addValue("s2", 1, true);

    tablet.addTimestamp(2, 2);

    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(tsfile).tableSchema(tableSchema).build()) {
      writer.write(tablet);
    }

    try (DeviceTableModelReader tsFileReader = new DeviceTableModelReader(tsfile);
        ResultSet resultSet =
            tsFileReader.query("T1", Arrays.asList("ID1", "ID2", "S2", "S1"), 0, 2); ) {
      Iterator<TSRecord> tsRecordIterator = resultSet.recordIterator();

      Assert.assertTrue(tsRecordIterator.hasNext());
      TSRecord tsRecord = tsRecordIterator.next();
      Assert.assertEquals(2, tsRecord.time);
      Assert.assertNull(tsRecord.dataPointList.get(0));
      Assert.assertNull(tsRecord.dataPointList.get(1));
      Assert.assertNull(tsRecord.dataPointList.get(2));
      Assert.assertNull(tsRecord.dataPointList.get(3));

      Assert.assertTrue(tsRecordIterator.hasNext());
      tsRecord = tsRecordIterator.next();
      Assert.assertEquals(0, tsRecord.time);
      Assert.assertEquals("id_field1", tsRecord.dataPointList.get(0).getValue().toString());
      Assert.assertEquals("id_field2", tsRecord.dataPointList.get(1).getValue().toString());
      Assert.assertFalse((Boolean) tsRecord.dataPointList.get(2).getValue());
      Assert.assertTrue((Boolean) tsRecord.dataPointList.get(3).getValue());

      Assert.assertTrue(tsRecordIterator.hasNext());
      tsRecord = tsRecordIterator.next();
      Assert.assertEquals(1, tsRecord.time);
      Assert.assertEquals("id_field1_2", tsRecord.dataPointList.get(0).getValue().toString());
      Assert.assertNull(tsRecord.dataPointList.get(1));
      Assert.assertTrue((Boolean) tsRecord.dataPointList.get(2).getValue());
      Assert.assertNull(tsRecord.dataPointList.get(3));

      Assert.assertFalse(tsRecordIterator.hasNext());
    }
  }

  @Test
  public void testQueryWithMaxValue() throws Exception {
    TableSchema tableSchema =
        new TableSchema(
            "t1",
            Arrays.asList(
                new MeasurementSchema("id1", TSDataType.STRING),
                new MeasurementSchema("id2", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.BOOLEAN),
                new MeasurementSchema("s2", TSDataType.BOOLEAN)),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD));
    Tablet tablet =
        new Tablet(
            Arrays.asList("id1", "id2", "s1", "s2"),
            Arrays.asList(
                TSDataType.STRING, TSDataType.STRING, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
            1024);
    tablet.addTimestamp(0, Long.MAX_VALUE - 1);
    tablet.addValue("id1", 0, "id_field1");
    tablet.addValue("id2", 0, "id_field2");
    tablet.addValue("s1", 0, true);
    tablet.addValue("s2", 0, false);

    tablet.addTimestamp(1, Long.MAX_VALUE);
    tablet.addValue("id1", 1, "id_field1");
    tablet.addValue("id2", 1, "id_field2");
    tablet.addValue("s1", 1, true);
    tablet.addValue("s2", 1, false);
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(tsfile).tableSchema(tableSchema).build()) {
      writer.write(tablet);
    }

    try (DeviceTableModelReader tsFileReader = new DeviceTableModelReader(tsfile);
        ResultSet resultSet =
            tsFileReader.query(
                "T1", Arrays.asList("id1", "id2", "S2", "S1"), 0, Long.MAX_VALUE); ) {
      // id1 id2 s2 s1
      ResultSetMetadata resultSetMetadata = resultSet.getMetadata();
      // Time id1 id2 s2 s1
      Assert.assertEquals("Time", resultSetMetadata.getColumnName(1));
      Assert.assertEquals(TSDataType.INT64, resultSetMetadata.getColumnType(1));
      Assert.assertEquals("id1", resultSetMetadata.getColumnName(2));
      Assert.assertEquals(TSDataType.STRING, resultSetMetadata.getColumnType(2));
      Assert.assertEquals("id2", resultSetMetadata.getColumnName(3));
      Assert.assertEquals(TSDataType.STRING, resultSetMetadata.getColumnType(3));
      Assert.assertEquals("S2", resultSetMetadata.getColumnName(4));
      Assert.assertEquals(TSDataType.BOOLEAN, resultSetMetadata.getColumnType(4));
      Assert.assertEquals("S1", resultSetMetadata.getColumnName(5));
      Assert.assertEquals(TSDataType.BOOLEAN, resultSetMetadata.getColumnType(5));

      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(Long.MAX_VALUE - 1, resultSet.getLong(1));
      Assert.assertEquals("id_field1", resultSet.getString(2));
      Assert.assertEquals("id_field2", resultSet.getString(3));
      Assert.assertFalse(resultSet.getBoolean(4));
      Assert.assertTrue(resultSet.getBoolean(5));

      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(Long.MAX_VALUE, resultSet.getLong(1));
      Assert.assertEquals("id_field1", resultSet.getString(2));
      Assert.assertEquals("id_field2", resultSet.getString(3));
      Assert.assertFalse(resultSet.getBoolean(4));
      Assert.assertTrue(resultSet.getBoolean(5));
    }
  }

  @Test
  public void testQueryTableWithPartialNullValueInChunk() throws Exception {
    TableSchema tableSchema =
        new TableSchema(
            "t1",
            Arrays.asList(
                new MeasurementSchema("id1", TSDataType.STRING),
                new MeasurementSchema("id2", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.BOOLEAN),
                new MeasurementSchema("s2", TSDataType.BOOLEAN)),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD));
    Tablet tablet =
        new Tablet(
            Arrays.asList("id1", "id2", "s1", "s2"),
            Arrays.asList(
                TSDataType.STRING, TSDataType.STRING, TSDataType.BOOLEAN, TSDataType.BOOLEAN),
            1024);
    tablet.addTimestamp(0, 0);
    tablet.addValue("id1", 0, "id_field1");
    tablet.addValue("id2", 0, "id_field2");
    tablet.addValue("s1", 0, true);
    tablet.addValue("s2", 0, false);

    tablet.addTimestamp(1, 1);
    tablet.addValue("id1", 1, "id_field1");
    tablet.addValue("id2", 1, "id_field2");
    tablet.addValue("s2", 1, false);

    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(tsfile).tableSchema(tableSchema).build()) {
      writer.write(tablet);
    }

    try (DeviceTableModelReader tsFileReader = new DeviceTableModelReader(tsfile);
        ResultSet resultSet =
            tsFileReader.query("T1", Arrays.asList("id1", "id2", "S2", "S1"), 0, 2); ) {
      // id1 id2 s2 s1
      ResultSetMetadata resultSetMetadata = resultSet.getMetadata();
      // Time id1 id2 s2 s1
      Assert.assertEquals("Time", resultSetMetadata.getColumnName(1));
      Assert.assertEquals(TSDataType.INT64, resultSetMetadata.getColumnType(1));
      Assert.assertEquals("id1", resultSetMetadata.getColumnName(2));
      Assert.assertEquals(TSDataType.STRING, resultSetMetadata.getColumnType(2));
      Assert.assertEquals("id2", resultSetMetadata.getColumnName(3));
      Assert.assertEquals(TSDataType.STRING, resultSetMetadata.getColumnType(3));
      Assert.assertEquals("S2", resultSetMetadata.getColumnName(4));
      Assert.assertEquals(TSDataType.BOOLEAN, resultSetMetadata.getColumnType(4));
      Assert.assertEquals("S1", resultSetMetadata.getColumnName(5));
      Assert.assertEquals(TSDataType.BOOLEAN, resultSetMetadata.getColumnType(5));

      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(0, resultSet.getLong(1));
      Assert.assertEquals("id_field1", resultSet.getString(2));
      Assert.assertEquals("id_field2", resultSet.getString(3));
      Assert.assertFalse(resultSet.getBoolean(4));
      Assert.assertTrue(resultSet.getBoolean(5));

      Assert.assertTrue(resultSet.next());
      Assert.assertEquals(1, resultSet.getLong(1));
      Assert.assertEquals("id_field1", resultSet.getString(2));
      Assert.assertEquals("id_field2", resultSet.getString(3));
      Assert.assertTrue(resultSet.isNull("S1"));
      Assert.assertFalse(resultSet.getBoolean("S2"));
    }
  }
}
