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
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.DeviceMetadataIndexEntriesQueryResult;
import org.apache.tsfile.read.query.DeviceMetadataIndexNodeOffsetsQueryContext;
import org.apache.tsfile.utils.FileGenerator;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DeviceMetadataIndexEntriesQueryTest {
  private static final String FILE_PATH = FileGenerator.outputDataFile;

  @Before
  public void before() throws IOException {}

  @After
  public void after() throws IOException {
    Files.deleteIfExists(new File(FILE_PATH).toPath());
  }

  @Test
  public void test1() throws IOException, WriteProcessException {
    File file = new File(FILE_PATH);
    TableSchema tableSchema =
        new TableSchema(
            "t1",
            Arrays.asList(
                new MeasurementSchema("device", TSDataType.STRING),
                new MeasurementSchema("s1", TSDataType.INT32)),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
    try (ITsFileWriter writer =
        new TsFileWriterBuilder().tableSchema(tableSchema).file(file).build()) {
      Tablet tablet =
          new Tablet(
              Arrays.asList("device", "s1"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT32),
              10000);
      for (int i = 0; i < 10000; i++) {
        tablet.addTimestamp(i, i);
        tablet.addValue("device", i, "d" + i);
        tablet.addValue("s1", i, i);
      }
      writer.write(tablet);
    }

    List<IDeviceID> queriedDevices = new ArrayList<>();
    for (int i = 0; i < 20000; i++) {
      if (i >= 15000) {
        queriedDevices.add(new StringArrayDeviceID("t2.d" + i));
      } else {
        queriedDevices.add(new StringArrayDeviceID("t1.d" + i));
      }
    }
    queriedDevices.sort(IDeviceID::compareTo);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      DeviceMetadataIndexEntriesQueryResult offsets =
          reader.getDeviceMetadataIndexNodeOffsets(null, queriedDevices, null);
      for (int i = 0; i < queriedDevices.size(); i++) {
        IDeviceID deviceID = queriedDevices.get(i);
        int deviceNumber = Integer.parseInt(deviceID.toString().substring("t1.d".length()));
        long[] metadataIndexNodeOffsetOfCurDevice = offsets.getDeviceMetadataIndexNodeOffset(i);
        if (deviceNumber >= 10000) {
          Assert.assertNull(metadataIndexNodeOffsetOfCurDevice);
          continue;
        }
        MetadataIndexNode metadataIndexNode =
            reader.readMetadataIndexNode(
                metadataIndexNodeOffsetOfCurDevice[0],
                metadataIndexNodeOffsetOfCurDevice[1],
                false);
        List<AbstractAlignedChunkMetadata> alignedChunkMetadataList =
            reader.getAlignedChunkMetadataByMetadataIndexNode(deviceID, metadataIndexNode, true);
        Assert.assertEquals(1, alignedChunkMetadataList.size());

        Assert.assertEquals(deviceNumber, alignedChunkMetadataList.get(0).getStartTime());
      }

      Assert.assertEquals(
          0,
          reader.getDeviceMetadataIndexNodeOffsets("t1", Collections.emptyList(), null).length());
      offsets =
          reader.getDeviceMetadataIndexNodeOffsets(
              "t1", Collections.singletonList(new StringArrayDeviceID("t1.d")), null);
      Assert.assertNull(offsets.getDeviceMetadataIndexNodeOffset(0));
    }
  }

  @Test
  public void test2() {
    DeviceMetadataIndexNodeOffsetsQueryContext queryContext =
        new DeviceMetadataIndexNodeOffsetsQueryContext(1);
    queryContext.addDeviceMetadataIndexNodeOffset(0, 1, (long) Integer.MAX_VALUE + 10);
    DeviceMetadataIndexEntriesQueryResult result1 = queryContext.compact();
    long[] offsets = result1.getDeviceMetadataIndexNodeOffset(0);
    Assert.assertEquals(1, offsets[0]);
    Assert.assertEquals((long) Integer.MAX_VALUE + 10, offsets[1]);
  }

  @Test
  public void test3() {
    DeviceMetadataIndexNodeOffsetsQueryContext queryContext =
        new DeviceMetadataIndexNodeOffsetsQueryContext(10);
    for (int i = 0; i < 10; i++) {
      queryContext.addDeviceMetadataIndexNodeOffset(i, i + 1, 0XFFFFFFFFL + 10 + i);
    }
    DeviceMetadataIndexEntriesQueryResult result1 = queryContext.compact();
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(i + 1, result1.getDeviceMetadataIndexNodeOffset(i)[0]);
      Assert.assertEquals(0XFFFFFFFFL + 10 + i, result1.getDeviceMetadataIndexNodeOffset(i)[1]);
    }
    queryContext = new DeviceMetadataIndexNodeOffsetsQueryContext(10);
    queryContext.addDeviceMetadataIndexNodeOffset(0, 1, 0XFFFFFFFFL + 10 + 10);
    DeviceMetadataIndexEntriesQueryResult result2 = queryContext.compact();
    Assert.assertTrue(result1.ramBytesUsed() > result2.ramBytesUsed());
    Assert.assertEquals(1, result2.getDeviceMetadataIndexNodeOffset(0)[0]);
    Assert.assertEquals(0XFFFFFFFFL + 10 + 10, result2.getDeviceMetadataIndexNodeOffset(0)[1]);
  }

  @Test
  public void test4() {
    int length = (int) Short.MAX_VALUE * 2;
    DeviceMetadataIndexNodeOffsetsQueryContext queryContext =
        new DeviceMetadataIndexNodeOffsetsQueryContext(length);
    for (int i = 0; i < length; i++) {
      if (i % 2 == 0) {
        queryContext.addDeviceMetadataIndexNodeOffset(i, i + 1, 0XFFFFFFFFL + 10 + i);
      }
    }
    DeviceMetadataIndexEntriesQueryResult result1 = queryContext.compact();
    for (int i = 0; i < length; i++) {
      if (i % 2 != 0) {
        Assert.assertNull(result1.getDeviceMetadataIndexNodeOffset(i));
        continue;
      }
      Assert.assertEquals(i + 1, result1.getDeviceMetadataIndexNodeOffset(i)[0]);
      Assert.assertEquals(0XFFFFFFFFL + 10 + i, result1.getDeviceMetadataIndexNodeOffset(i)[1]);
    }
  }
}
