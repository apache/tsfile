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
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.file.metadata.statistics.TableStatistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
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
