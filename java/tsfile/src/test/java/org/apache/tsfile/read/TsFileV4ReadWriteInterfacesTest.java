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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.v4.DeviceTableModelReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TsFileV4ReadWriteInterfacesTest {

  @Test
  public void testWriteSomeColumns() throws IOException, WriteProcessException {
    String filePath = TsFileGeneratorForTest.getTestTsFilePath("db", 0, 0, 0);

    TableSchema tableSchema =
        new TableSchema(
            "t1",
            Arrays.asList(
                new MeasurementSchema("device", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.INT32),
                new MeasurementSchema("s2", TSDataType.INT32),
                new MeasurementSchema("s3", TSDataType.INT32)),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD));
    Tablet tablet1 =
        new Tablet(
            tableSchema.getTableName(),
            Arrays.asList("device", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
    for (int i = 0; i < 1000; i++) {
      tablet1.addTimestamp(i, i);
      tablet1.addValue("device", i, "d1");
      tablet1.addValue("s1", i, 0);
    }
    Tablet tablet2 =
        new Tablet(
            tableSchema.getTableName(),
            IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
            IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
            tableSchema.getColumnTypes());
    for (int i = 0; i < 1000; i++) {
      tablet2.addTimestamp(i, 1005 + i);
      tablet2.addValue("device", i, "d1");
      tablet2.addValue("s1", i, 1);
      tablet2.addValue("s2", i, 1);
      tablet2.addValue("s3", i, 1);
    }
    try (ITsFileWriter writer =
        new TsFileWriterBuilder()
            .file(new File(filePath))
            .tableSchema(tableSchema)
            .memoryThreshold(1)
            .build()) {
      writer.write(tablet1);
      writer.write(tablet2);
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      while (deviceIterator.hasNext()) {
        Pair<IDeviceID, Boolean> pair = deviceIterator.next();
        List<AbstractAlignedChunkMetadata> alignedChunkMetadataList =
            reader.getAlignedChunkMetadataByMetadataIndexNode(
                pair.getLeft(), deviceIterator.getFirstMeasurementNodeOfCurrentDevice(), false);
        Assert.assertFalse(alignedChunkMetadataList.isEmpty());
        Assert.assertEquals(3, alignedChunkMetadataList.get(0).getValueChunkMetadataList().size());
        Assert.assertEquals(
            1000,
            alignedChunkMetadataList
                .get(0)
                .getValueChunkMetadataList()
                .get(0)
                .getStatistics()
                .getCount());
        Assert.assertNull(alignedChunkMetadataList.get(0).getValueChunkMetadataList().get(1));
        Assert.assertNull(alignedChunkMetadataList.get(0).getValueChunkMetadataList().get(2));
        Assert.assertEquals(3, alignedChunkMetadataList.get(1).getValueChunkMetadataList().size());
        Assert.assertEquals(
            1000,
            alignedChunkMetadataList
                .get(1)
                .getValueChunkMetadataList()
                .get(0)
                .getStatistics()
                .getCount());
        Assert.assertEquals(
            1000,
            alignedChunkMetadataList
                .get(1)
                .getValueChunkMetadataList()
                .get(1)
                .getStatistics()
                .getCount());
        Assert.assertEquals(
            1000,
            alignedChunkMetadataList
                .get(1)
                .getValueChunkMetadataList()
                .get(2)
                .getStatistics()
                .getCount());
      }
    }
  }

  @Test
  public void testGetTableDeviceMethods() throws Exception {
    String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.testsg", 0, 0, 0);
    try {
      int deviceNum = 5;
      int measurementNum = 1;
      int pointNum = 10;
      long startTime = 1;
      int startValue = 1;
      int chunkGroupSize = 10;
      int pageSize = 100;
      File file =
          TsFileGeneratorUtils.generateAlignedTsFile(
              filePath,
              deviceNum,
              measurementNum,
              pointNum,
              startTime,
              startValue,
              chunkGroupSize,
              pageSize);
      List<IDeviceID> deviceIDList = new ArrayList<>();
      TableSchema tableSchema =
          new TableSchema(
              "t1",
              Arrays.asList(
                  new MeasurementSchema("id1", TSDataType.STRING),
                  new MeasurementSchema("id2", TSDataType.STRING),
                  new MeasurementSchema("id3", TSDataType.STRING),
                  new MeasurementSchema("s1", TSDataType.INT32)),
              Arrays.asList(
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.TAG,
                  ColumnCategory.FIELD));
      try (ITsFileWriter writer =
          new TsFileWriterBuilder().file(file).tableSchema(tableSchema).build()) {
        Tablet tablet =
            new Tablet(
                tableSchema.getTableName(),
                IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
                IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
                tableSchema.getColumnTypes());

        String[][] ids =
            new String[][] {
              {null, null, null},
              {null, null, "id3-4"},
              {null, "id2-1", "id3-1"},
              {null, "id2-5", null},
              {"id1-2", null, "id3-2"},
              {"id1-3", "id2-3", null},
              {"id1-6", null, null},
            };
        for (int i = 0; i < ids.length; i++) {
          tablet.addTimestamp(i, i);
          tablet.addValue("id1", i, ids[i][0]);
          tablet.addValue("id2", i, ids[i][1]);
          tablet.addValue("id3", i, ids[i][2]);
          deviceIDList.add(
              new StringArrayDeviceID(tableSchema.getTableName(), ids[i][0], ids[i][1], ids[i][2]));
          tablet.addValue("s1", i, i);
        }
        tablet.setRowSize(ids.length);
        writer.write(tablet);
      }
      try (DeviceTableModelReader tsFileReader = new DeviceTableModelReader(file)) {
        Assert.assertEquals("t1", tsFileReader.getAllTableSchema().get(0).getTableName());
        Assert.assertEquals(tableSchema, tsFileReader.getTableSchemas("t1").get());
      }
    } finally {
      Files.deleteIfExists(Paths.get(filePath));
    }
  }

  @Test
  public void testInvalidColumnNameOrTableName() throws Exception {
    String filePath = TsFileGeneratorForTest.getTestTsFilePath("db", 0, 0, 0);

    TableSchema tableSchema =
        new TableSchema(
            "",
            Arrays.asList(
                new MeasurementSchema("", TSDataType.STRING),
                new MeasurementSchema("id2", TSDataType.STRING),
                new MeasurementSchema("id3", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.INT32)),
            Arrays.asList(
                ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD));
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(new File(filePath)).tableSchema(tableSchema).build()) {
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("TableName must not be blank."));
    }
    tableSchema.setTableName("table1");
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().file(new File(filePath)).tableSchema(tableSchema).build()) {
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Column name must not be blank."));
    }
  }
}
