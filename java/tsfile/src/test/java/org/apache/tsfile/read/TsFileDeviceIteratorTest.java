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

package org.apache.tsfile.read;

import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TsFileDeviceIteratorTest {
  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileDeviceIterator.tsfile");

  @After
  public void teardown() {
    new File(FILE_PATH).delete();
  }

  @Test
  public void test() throws IOException {
    int totalDeviceNum = 0;
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH))) {
      for (int i = 1; i <= 10; i++) {
        String tableName = "table" + i;
        registerTableSchema(writer, tableName);
        int deviceNum = i;
        if (i % 2 == 0) {
          deviceNum *= 10000;
        } else {
          deviceNum *= 10;
        }
        totalDeviceNum += deviceNum;
        generateDevice(writer, tableName, deviceNum);
      }
      writer.endFile();
    }
    int deviceFromIterator = 0;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      Assert.assertNull(deviceIterator.current());
      IDeviceID previous = null;
      while (deviceIterator.hasNext()) {
        Pair<IDeviceID, Boolean> next = deviceIterator.next();
        Assert.assertEquals(next, deviceIterator.current());
        deviceFromIterator++;
        if (previous != null) {
          Assert.assertTrue(previous.compareTo(next.getLeft()) < 0);
        }
        previous = next.getLeft();
      }
      Assert.assertEquals(totalDeviceNum, deviceFromIterator);

      deviceFromIterator = 0;
      deviceIterator = reader.getTableDevicesIteratorWithIsAligned("table2", null);
      previous = null;
      while (deviceIterator.hasNext()) {
        Pair<IDeviceID, Boolean> next = deviceIterator.next();
        Assert.assertEquals(next, deviceIterator.current());
        deviceFromIterator++;
        if (previous != null) {
          Assert.assertTrue(previous.compareTo(next.getLeft()) < 0);
        }
        previous = next.getLeft();
      }
      Assert.assertEquals(20000, deviceFromIterator);
    }
  }

  @Test
  public void testIteratorWithSpecifiedDevices1() throws IOException {
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH))) {
      // create two tables with different device counts
      registerTableSchema(writer, "tableA");
      registerTableSchema(writer, "tableB");

      generateDevice(writer, "tableA", 100);
      generateDevice(writer, "tableB", 1000);

      writer.endFile();
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {

      // Prepare a filtered device list (only part of table)
      List<IDeviceID> filteredDevices =
          Arrays.asList(
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableA", "d10"}),
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableA", "d20"}),
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableA", "d30"}));

      LazyTsFileDeviceIteratorWithDevices iterator =
          new LazyTsFileDeviceIteratorWithDevices(reader, "tableA", null, filteredDevices);

      int count = 0;
      IDeviceID previous = null;

      while (iterator.hasNext()) {
        IDeviceID pair = iterator.next();
        Assert.assertEquals(pair, iterator.getCurrentDeviceID());

        if (previous != null) {
          Assert.assertTrue(previous.compareTo(pair) < 0);
        }
        previous = pair;
        count++;
      }

      // Only 3 filtered devices should be returned
      Assert.assertEquals(3, count);

      // Prepare a filtered device list (only part of table)
      filteredDevices =
          Arrays.asList(
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableB", "d10"}),
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableB", "d100"}),
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableB", "d20"}),
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableB", "d200"}),
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableB", "d30"}),
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableB", "d300"}),
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"tableB", "d800"}));

      iterator = new LazyTsFileDeviceIteratorWithDevices(reader, "tableB", null, filteredDevices);

      count = 0;
      previous = null;

      while (iterator.hasNext()) {
        IDeviceID pair = iterator.next();
        Assert.assertEquals(pair, iterator.getCurrentDeviceID());

        if (previous != null) {
          Assert.assertTrue(previous.compareTo(pair) < 0);
        }
        previous = pair;
        count++;
      }

      // Only 7 filtered devices should be returned
      Assert.assertEquals(7, count);

      iterator =
          new LazyTsFileDeviceIteratorWithDevices(reader, "tableA", null, Collections.emptyList());
      Assert.assertFalse(iterator.hasNext());
      iterator =
          new LazyTsFileDeviceIteratorWithDevices(reader, "tableB", null, Collections.emptyList());
      Assert.assertFalse(iterator.hasNext());
      iterator =
          new LazyTsFileDeviceIteratorWithDevices(reader, "tableC", null, Collections.emptyList());
      Assert.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testIteratorWithSpecifiedDevices2() throws IOException {
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH))) {
      // create two tables with different device counts
      registerTableSchema(writer, "tableA");
      registerTableSchema(writer, "tableB");

      generateDevice(writer, "tableA", 100);
      generateDevice(writer, "tableB", 1000);

      writer.endFile();
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {

      List<IDeviceID> filteredDevices =
          reader.getAllDevices(reader.readFileMetadata().getTableMetadataIndexNode("tableA"));

      LazyTsFileDeviceIteratorWithDevices iterator =
          new LazyTsFileDeviceIteratorWithDevices(reader, "tableA", null, filteredDevices);

      int count = 0;
      IDeviceID previous = null;

      while (iterator.hasNext()) {
        IDeviceID pair = iterator.next();
        Assert.assertEquals(pair, iterator.getCurrentDeviceID());

        if (previous != null) {
          Assert.assertTrue(previous.compareTo(pair) < 0);
        }
        previous = pair;
        count++;
      }

      Assert.assertEquals(100, count);

      filteredDevices =
          reader.getAllDevices(reader.readFileMetadata().getTableMetadataIndexNode("tableB"));

      iterator = new LazyTsFileDeviceIteratorWithDevices(reader, "tableB", null, filteredDevices);

      count = 0;
      previous = null;

      while (iterator.hasNext()) {
        IDeviceID pair = iterator.next();
        Assert.assertEquals(pair, iterator.getCurrentDeviceID());

        if (previous != null) {
          Assert.assertTrue(previous.compareTo(pair) < 0);
        }
        previous = pair;
        count++;
      }

      Assert.assertEquals(1000, count);
    }
  }

  private void registerTableSchema(TsFileIOWriter writer, String tableName) {
    List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema(
                "id", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED),
            new MeasurementSchema("s1", TSDataType.INT64),
            new MeasurementSchema("s2", TSDataType.INT64),
            new MeasurementSchema("s3", TSDataType.INT64),
            new MeasurementSchema("s4", TSDataType.INT64));
    List<ColumnCategory> columnCategories =
        Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD);
    TableSchema tableSchema = new TableSchema(tableName, schemas, columnCategories);
    writer.getSchema().registerTableSchema(tableSchema);
  }

  private void generateDevice(TsFileIOWriter writer, String tableName, int deviceNum)
      throws IOException {
    for (int i = 0; i < deviceNum; i++) {
      IDeviceID deviceID =
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {tableName, "d" + i});
      writer.startChunkGroup(deviceID);
      TsFileGeneratorForTest.generateSimpleInt64AlignedSeriesToCurrentDevice(
          writer, Arrays.asList("s1", "s2", "s3", "s4"), new TimeRange[] {new TimeRange(10, 20)});
      writer.endChunkGroup();
    }
  }
}
