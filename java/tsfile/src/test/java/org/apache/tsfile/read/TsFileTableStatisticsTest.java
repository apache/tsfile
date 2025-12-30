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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.file.metadata.statistics.TableStatistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TsFileTableStatisticsTest {

  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileTableStatisticsTest.tsfile");

  @After
  public void teardown() {
    new File(FILE_PATH).delete();
  }

  @Test
  public void testTableModelTsFile() throws IOException {
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH))) {
      for (int i = 1; i <= 10; i++) {
        String tableName = "table" + i;
        registerTableSchema(writer, tableName, i);
        int deviceNum = i;
        if (i % 2 == 0) {
          deviceNum *= 10000;
        } else {
          deviceNum *= 10;
        }
        generateDevice(writer, tableName, deviceNum, i);
      }
      writer.endFile();
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      Assert.assertTrue(reader.hasTableStatistics());
      Optional<ITsFileTableStatisticsReader> optional = reader.getTsFileTableStatisticsReader();
      Assert.assertTrue(optional.isPresent());
      ITsFileTableStatisticsReader tsFileTableStatisticsReader = optional.get();
      Map<String, TableStatistics> allTableStatistics =
          tsFileTableStatisticsReader.getAllTableStatistics();
      Assert.assertEquals(10, allTableStatistics.size());
      for (int i = 1; i <= 10; i++) {
        String tableName = "table" + i;
        TableStatistics tableStatistics = allTableStatistics.get(tableName);
        int deviceNum = i;
        if (i % 2 == 0) {
          deviceNum *= 10000;
        } else {
          deviceNum *= 10;
        }
        Statistics<? extends Serializable> timeStatistics = tableStatistics.getStatistics("");
        Assert.assertEquals(deviceNum, timeStatistics.getCount());
        Assert.assertEquals(0L, timeStatistics.getMinValue());
        Assert.assertEquals((long) deviceNum - 1, timeStatistics.getMaxValue());
        Assert.assertEquals(0L, timeStatistics.getStartTime());
        Assert.assertEquals(deviceNum - 1, timeStatistics.getEndTime());
        int measurementNum = i;
        for (int j = 0; j < measurementNum; j++) {
          String measurement = "s" + j;
          Statistics<? extends Serializable> fieldStatistics =
              tableStatistics.getStatistics(measurement);
          Assert.assertEquals(deviceNum, fieldStatistics.getCount());
          Assert.assertEquals(0L, fieldStatistics.getMinValue());
          Assert.assertEquals((long) deviceNum - 1, fieldStatistics.getMaxValue());
          Assert.assertEquals(0L, fieldStatistics.getStartTime());
          Assert.assertEquals(deviceNum - 1, fieldStatistics.getEndTime());
          Assert.assertEquals(0L, fieldStatistics.getFirstValue());
          Assert.assertEquals((long) deviceNum - 1, fieldStatistics.getLastValue());
        }
      }
    }
  }

  @Test
  public void testTableAndTreeTsFile() throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(new File(FILE_PATH))) {
      writer.registerTimeseries("root.test.d1", new MeasurementSchema("s1", TSDataType.INT64));
      writer.registerTableSchema(
          new TableSchema(
              "t1",
              Arrays.asList(
                  new ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                  new ColumnSchema("s1", TSDataType.INT64, ColumnCategory.FIELD),
                  new ColumnSchema("s2", TSDataType.INT64, ColumnCategory.FIELD))));
      Tablet treeTablet =
          new Tablet(
              "root.test.d1",
              Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64)));
      treeTablet.addTimestamp(0, 1);
      treeTablet.addValue("s1", 0, 1L);
      writer.writeTree(treeTablet);
      Tablet tableTablet =
          new Tablet(
              "t1",
              Arrays.asList("device", "s1", "s2"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.INT64),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD));
      tableTablet.addTimestamp(0, 1);
      tableTablet.addValue("device", 0, new Binary("d1", TSFileConfig.STRING_CHARSET));
      tableTablet.addValue("s1", 0, 2L);
      tableTablet.addValue("s2", 0, 3L);
      writer.writeTable(tableTablet);
      writer.flush();
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      Assert.assertTrue(reader.hasTableStatistics());
      Optional<ITsFileTableStatisticsReader> optional = reader.getTsFileTableStatisticsReader();
      Assert.assertTrue(optional.isPresent());
      ITsFileTableStatisticsReader tsFileTableStatisticsReader = optional.get();
      Statistics timeStatistics =
          tsFileTableStatisticsReader.getTableStatistics("t1").getTimeStatistics();
      Assert.assertEquals(1, timeStatistics.getCount());
      Assert.assertEquals(1, timeStatistics.getStartTime());
      Assert.assertEquals(1, timeStatistics.getEndTime());
      LongStatistics s1Statistics =
          (LongStatistics)
              tsFileTableStatisticsReader
                  .getTableFieldColumnStatistics("t1", "s2")
                  .getStatistics("s2");
      Assert.assertEquals(1, s1Statistics.getCount());
      Assert.assertEquals(3L, s1Statistics.getMinValue().longValue());
      Assert.assertEquals(3L, s1Statistics.getMaxValue().longValue());
      Assert.assertNull(
          tsFileTableStatisticsReader
              .getTableFieldColumnStatistics("t1", "s2")
              .getStatistics("s1"));

      Map<String, TableStatistics> allTableStatistics =
          tsFileTableStatisticsReader.getAllTableStatistics();
      Assert.assertEquals(1, allTableStatistics.size());
    }
  }

  private void generateDevice(
      TsFileIOWriter writer, String tableName, int deviceNum, int measurementNum)
      throws IOException {
    List<String> measurements = new ArrayList<>(measurementNum);
    for (int i = 0; i < measurementNum; i++) {
      measurements.add("s" + i);
    }
    for (int i = 0; i < deviceNum; i++) {
      IDeviceID deviceID =
          IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {tableName, "d" + i});
      writer.startChunkGroup(deviceID);
      TsFileGeneratorForTest.generateSimpleInt64AlignedSeriesToCurrentDevice(
          writer, measurements, new TimeRange[] {new TimeRange(i, i)});
      writer.endChunkGroup();
    }
  }

  private void registerTableSchema(TsFileIOWriter writer, String tableName, int measurementNum) {
    List<IMeasurementSchema> schemaList = new ArrayList<>(measurementNum + 1);
    schemaList.add(
        new MeasurementSchema(
            "id", TSDataType.STRING, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
    List<ColumnCategory> columnCategories = new ArrayList<>(measurementNum + 1);
    columnCategories.add(ColumnCategory.TAG);
    for (int i = 0; i < measurementNum; i++) {
      schemaList.add(new MeasurementSchema("s" + i, TSDataType.INT64));
      columnCategories.add(ColumnCategory.FIELD);
    }
    TableSchema tableSchema = new TableSchema(tableName, schemaList, columnCategories);
    writer.getSchema().registerTableSchema(tableSchema);
  }
}
