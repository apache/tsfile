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
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

public class TsFileIOWriterFlushTempChunkMetadataTest {

  private static final String FILE_PATH1 =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileIOWriterFlushTempChunkMetadataTest1.tsfile");
  private static final String FILE_PATH2 =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileIOWriterFlushTempChunkMetadataTest2.tsfile");

  @After
  public void teardown() {
    new File(FILE_PATH1).delete();
    new File(FILE_PATH2).delete();
  }

  @Test
  public void testAligned() throws IOException, WriteProcessException {
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
            Arrays.asList("device", "s1", "s2", "s3"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32),
            Arrays.asList(
                ColumnCategory.TAG,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD,
                ColumnCategory.FIELD));
    for (int i = 0; i < 1000; i++) {
      tablet1.addTimestamp(i, i);
      tablet1.addValue("device", i, "d" + i);
      tablet1.addValue("s1", i, 0);
      tablet1.addValue("s2", i, 0);
      tablet1.addValue("s3", i, 0);
    }
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH1), 1)) {
      TsFileWriter tsFileWriter = new TsFileWriter(writer);
      tsFileWriter.registerTableSchema(tableSchema);
      tsFileWriter.writeTable(tablet1);
      tsFileWriter.flush();
      writer.checkMetadataSizeAndMayFlush();
      tsFileWriter.close();
    }
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH2))) {
      TsFileWriter tsFileWriter = new TsFileWriter(writer);
      tsFileWriter.registerTableSchema(tableSchema);
      tsFileWriter.writeTable(tablet1);
      tsFileWriter.flush();
      tsFileWriter.close();
    }
    byte[] file1Contents = Files.readAllBytes(new File(FILE_PATH1).toPath());
    byte[] file2Contents = Files.readAllBytes(new File(FILE_PATH2).toPath());
    Assert.assertArrayEquals(file1Contents, file2Contents);
  }

  @Test
  public void testNonAligned() throws IOException, WriteProcessException {
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH1), 1)) {
      TsFileWriter tsFileWriter = new TsFileWriter(writer);
      writeNonAlignedData(tsFileWriter, writer, true);
      tsFileWriter.close();
    }
    try (TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH2))) {
      TsFileWriter tsFileWriter = new TsFileWriter(writer);
      writeNonAlignedData(tsFileWriter, writer, false);
      tsFileWriter.close();
    }
    byte[] file1Contents = Files.readAllBytes(new File(FILE_PATH1).toPath());
    byte[] file2Contents = Files.readAllBytes(new File(FILE_PATH2).toPath());
    Assert.assertArrayEquals(file1Contents, file2Contents);
  }

  private void writeNonAlignedData(
      TsFileWriter tsFileWriter,
      TsFileIOWriter tsFileIOWriter,
      boolean flushChunkMetadataToTempFile)
      throws WriteProcessException, IOException {
    for (int i = 0; i < 10; i++) {
      Tablet tablet =
          new Tablet(
              new StringArrayDeviceID("root.test.d" + i),
              Arrays.asList("s1", "s2", "s3"),
              Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
      for (int j = 0; j < 1000; j++) {
        tablet.addTimestamp(j, j);
        tablet.addValue("s1", j, 0);
        tablet.addValue("s2", j, 0);
        tablet.addValue("s3", j, 0);
      }
      tsFileWriter.registerTimeseries(
          "root.test.d" + i, new MeasurementSchema("s1", TSDataType.INT32));
      tsFileWriter.registerTimeseries(
          "root.test.d" + i, new MeasurementSchema("s2", TSDataType.INT32));
      tsFileWriter.registerTimeseries(
          "root.test.d" + i, new MeasurementSchema("s3", TSDataType.INT32));
      tsFileWriter.writeTree(tablet);
      tsFileWriter.flush();
      if (flushChunkMetadataToTempFile) {
        tsFileIOWriter.checkMetadataSizeAndMayFlush();
      }
    }
  }
}
