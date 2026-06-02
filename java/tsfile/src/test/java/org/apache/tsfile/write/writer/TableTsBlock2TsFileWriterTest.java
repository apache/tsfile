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

package org.apache.tsfile.write.writer;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.TableTsBlock2TsFileWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TableTsBlock2TsFileWriterTest {

  private String filePath = TsFileGeneratorForTest.getTestTsFilePath("db", 0, 0, 0);

  @After
  public void tearDown() throws Exception {
    Files.deleteIfExists(Paths.get(filePath));
  }

  @Test
  public void testWriteWithExistingTimeColumnAndTagColumns()
      throws IOException, WriteProcessException {
    TableSchema tableSchema = getTableSchema();
    TsBlock tsBlock = getTsBlock();
    TableTsBlock2TsFileWriter writer =
        new TableTsBlock2TsFileWriter(
            new File(filePath),
            tableSchema,
            32 * 1024 * 1024,
            false,
            0,
            new int[] {1},
            new int[] {2, 3, 4},
            new IMeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT32),
              new MeasurementSchema("s3", TSDataType.INT32),
            });
    writer.write(tsBlock);
    writer.close();

    Assert.assertEquals(100, writer.getRowCount());
    Assert.assertEquals(10, writer.getDeviceCount());

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      Assert.assertEquals(tableSchema, reader.getTableSchemaMap().get("t1"));
      Map<IDeviceID, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
          reader.getAllTimeseriesMetadata(false);
      Assert.assertEquals(10, deviceTimeseriesMetadataMap.size());
      for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry :
          deviceTimeseriesMetadataMap.entrySet()) {
        Assert.assertEquals(4, entry.getValue().size());
        Assert.assertEquals(0, entry.getValue().get(0).getStatistics().getStartTime());
        Assert.assertEquals(9, entry.getValue().get(1).getStatistics().getEndTime());
      }
    }
  }

  @Test
  public void testWriteWithExistingTagColumns() throws IOException, WriteProcessException {
    TableSchema tableSchema = getTableSchema();
    TableTsBlock2TsFileWriter writer =
        new TableTsBlock2TsFileWriter(
            new File(filePath),
            tableSchema,
            32 * 1024 * 1024,
            true,
            -1,
            new int[] {1},
            new int[] {0, 2, 3, 4},
            new IMeasurementSchema[] {
              new MeasurementSchema("t", TSDataType.TIMESTAMP),
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT32),
              new MeasurementSchema("s3", TSDataType.INT32),
            });
    writer.write(getTsBlock());
    writer.close();

    Assert.assertEquals(100, writer.getRowCount());
    Assert.assertEquals(10, writer.getDeviceCount());

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      Assert.assertEquals(tableSchema, reader.getTableSchemaMap().get("t1"));
      Map<IDeviceID, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
          reader.getAllTimeseriesMetadata(false);
      Assert.assertEquals(10, deviceTimeseriesMetadataMap.size());
      for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry :
          deviceTimeseriesMetadataMap.entrySet()) {
        Assert.assertEquals(5, entry.getValue().size());
        Assert.assertEquals(0, entry.getValue().get(0).getStatistics().getStartTime());
        Assert.assertEquals(9, entry.getValue().get(1).getStatistics().getEndTime());
      }
    }
  }

  @Test
  public void testWriteWithoutTimeColumnAndTagColumns() throws IOException, WriteProcessException {
    TableSchema tableSchema = getTableSchema();
    TableTsBlock2TsFileWriter writer =
        new TableTsBlock2TsFileWriter(
            new File(filePath),
            tableSchema,
            32 * 1024 * 1024,
            true,
            -1,
            new int[0],
            new int[] {0, 1, 2, 3, 4},
            new IMeasurementSchema[] {
              new MeasurementSchema("t", TSDataType.TIMESTAMP),
              new MeasurementSchema("device", TSDataType.STRING),
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT32),
              new MeasurementSchema("s3", TSDataType.INT32),
            });
    writer.write(getTsBlock());
    writer.close();

    Assert.assertEquals(100, writer.getRowCount());
    Assert.assertEquals(1, writer.getDeviceCount());

    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      Assert.assertEquals(tableSchema, reader.getTableSchemaMap().get("t1"));
      Map<IDeviceID, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
          reader.getAllTimeseriesMetadata(false);
      Assert.assertEquals(1, deviceTimeseriesMetadataMap.size());
      for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry :
          deviceTimeseriesMetadataMap.entrySet()) {
        Assert.assertEquals(6, entry.getValue().size());
        Assert.assertEquals(0, entry.getValue().get(0).getStatistics().getStartTime());
        Assert.assertEquals(99, entry.getValue().get(1).getStatistics().getEndTime());
      }
    }
  }

  private TableSchema getTableSchema() {
    return new TableSchema(
        "t1",
        Arrays.asList(
            new MeasurementSchema("device", TSDataType.STRING),
            new MeasurementSchema("s1", TSDataType.INT32),
            new MeasurementSchema("s2", TSDataType.INT32),
            new MeasurementSchema("s3", TSDataType.INT32)),
        Arrays.asList(
            ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD));
  }

  private TsBlock getTsBlock() {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(
            Arrays.asList(
                TSDataType.TIMESTAMP,
                TSDataType.STRING,
                TSDataType.INT32,
                TSDataType.INT32,
                TSDataType.INT32));
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        tsBlockBuilder.getTimeColumnBuilder().writeLong(0);
        tsBlockBuilder.getValueColumnBuilders()[0].writeLong(j);
        tsBlockBuilder.getValueColumnBuilders()[1].writeBinary(
            new Binary("device" + i, TSFileConfig.STRING_CHARSET));
        tsBlockBuilder.getValueColumnBuilders()[2].writeInt(j);
        tsBlockBuilder.getValueColumnBuilders()[3].writeInt(j);
        tsBlockBuilder.getValueColumnBuilders()[4].writeInt(j);
        tsBlockBuilder.declarePosition();
      }
    }
    return tsBlockBuilder.build();
  }
}
