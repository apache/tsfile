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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.DeviceTableModelReader;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
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
                Tablet.ColumnCategory.ID,
                Tablet.ColumnCategory.ID,
                Tablet.ColumnCategory.MEASUREMENT,
                Tablet.ColumnCategory.MEASUREMENT));
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
                Tablet.ColumnCategory.ID,
                Tablet.ColumnCategory.ID,
                Tablet.ColumnCategory.MEASUREMENT,
                Tablet.ColumnCategory.MEASUREMENT));
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
