/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.constant.TestConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.FileGenerator;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TimeSeriesMetadataReadTest {

  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TimeseriesMetadataReadTest.tsfile");
  private final TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
  private int maxDegreeOfIndexNode;

  @Before
  public void before() throws IOException {
    int rowCount = 100;
    maxDegreeOfIndexNode = conf.getMaxDegreeOfIndexNode();
    conf.setMaxDegreeOfIndexNode(3);
    FileGenerator.generateFile(rowCount, 10000, FILE_PATH);
  }

  @After
  public void after() {
    FileGenerator.after();
    conf.setMaxDegreeOfIndexNode(maxDegreeOfIndexNode);
    File file = new File(FILE_PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testReadTimeseriesMetadata() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);
    Path path = new Path("d1", "s1", true);
    Set<String> set = new HashSet<>();
    set.add("s1");
    set.add("s2");
    set.add("s3");
    // the Max Degree Of Index Node is set to be 3, so the leaf node should only contains 3 sensors
    // s4 should not be returned as result
    set.add("s4");
    List<TimeseriesMetadata> timeseriesMetadataList =
        reader.readTimeseriesMetadata(path.getIDeviceID(), path.getMeasurement(), set, false, null);
    Assert.assertEquals(3, timeseriesMetadataList.size());
    for (int i = 1; i <= timeseriesMetadataList.size(); i++) {
      Assert.assertEquals("s" + i, timeseriesMetadataList.get(i - 1).getMeasurementId());
    }

    path = new Path("d1", "s5", true);
    set.clear();
    set.add("s5");
    set.add("s6");
    // this is a fake one, this file doesn't contain this measurement
    // so the result is not supposed to contain this measurement's timeseries metadata
    set.add("s8");
    timeseriesMetadataList =
        reader.readTimeseriesMetadata(path.getIDeviceID(), path.getMeasurement(), set, false, null);
    Assert.assertEquals(2, timeseriesMetadataList.size());
    for (int i = 5; i < 7; i++) {
      Assert.assertEquals("s" + i, timeseriesMetadataList.get(i - 5).getMeasurementId());
    }
  }

  @Test
  public void testReadTimeseriesMetadataInBatch() throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH)) {
      IDeviceID device = new Path("d1", "s1", true).getIDeviceID();
      Set<String> requested =
          new HashSet<>(Arrays.asList("s1", "s2", "s3", "s4", "s5", "s6", "s8"));

      Map<String, TimeseriesMetadata> batch =
          reader.readTimeseriesMetadata(device, requested, false);
      // s8 does not exist in the file
      Assert.assertEquals(6, batch.size());
      Assert.assertNull(batch.get("s8"));
      for (String measurement : Arrays.asList("s1", "s2", "s3", "s4", "s5", "s6")) {
        TimeseriesMetadata single = reader.readTimeseriesMetadata(device, measurement, false, null);
        TimeseriesMetadata batched = batch.get(measurement);
        Assert.assertNotNull(measurement, single);
        Assert.assertNotNull(measurement, batched);
        Assert.assertEquals(measurement, single.getMeasurementId(), batched.getMeasurementId());
        Assert.assertEquals(
            measurement, single.getStatistics().getCount(), batched.getStatistics().getCount());
        Assert.assertEquals(
            measurement,
            single.getStatistics().getStartTime(),
            batched.getStatistics().getStartTime());
        Assert.assertEquals(
            measurement, single.getStatistics().getEndTime(), batched.getStatistics().getEndTime());
        Assert.assertEquals(
            measurement,
            single.getChunkMetadataList().size(),
            batched.getChunkMetadataList().size());
      }

      // a single searched measurement can warm the cache with its group siblings
      Map<String, TimeseriesMetadata> withSiblings =
          reader.readTimeseriesMetadata(
              device,
              null,
              java.util.Collections.singletonList("s1"),
              new HashSet<>(Arrays.asList("s1", "s2", "s3")),
              false,
              null);
      Assert.assertEquals(new HashSet<>(Arrays.asList("s1", "s2", "s3")), withSiblings.keySet());
      for (Map.Entry<String, TimeseriesMetadata> entry : withSiblings.entrySet()) {
        Assert.assertEquals(
            entry.getKey(),
            batch.get(entry.getKey()).getStatistics().getCount(),
            entry.getValue().getStatistics().getCount());
      }

      // an absent device is tolerated only when ignoreNotExistDevice is set
      IDeviceID absentDevice = new Path("d_not_exist", "s1", true).getIDeviceID();
      Assert.assertTrue(reader.readTimeseriesMetadata(absentDevice, requested, true).isEmpty());
      Assert.assertThrows(
          IOException.class, () -> reader.readTimeseriesMetadata(absentDevice, requested, false));

      // the offset-addressed variant used by external tsfiles reads the same metadata
      LazyTsFileDeviceIterator iterator = new LazyTsFileDeviceIterator(reader);
      IDeviceID current = null;
      long[] offset = null;
      while (iterator.hasNext()) {
        IDeviceID next = iterator.next();
        if (next.equals(device)) {
          current = next;
          offset = iterator.getCurrentDeviceMeasurementNodeOffset();
          break;
        }
      }
      Assert.assertNotNull(offset);
      Map<String, TimeseriesMetadata> viaOffset =
          reader.readTimeseriesMetadata(current, offset, requested, false, null);
      Assert.assertEquals(batch.keySet(), viaOffset.keySet());
      for (String measurement : batch.keySet()) {
        Assert.assertEquals(
            measurement,
            batch.get(measurement).getStatistics().getCount(),
            viaOffset.get(measurement).getStatistics().getCount());
      }
    }
  }

  @Test
  public void testReadTimeseriesMetadataInBatchThroughInternalNode() throws Exception {
    File file = new File(TestConstant.BASE_OUTPUT_PATH.concat("BatchInternalNode.tsfile"));
    file.delete();
    IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1");
    int measurementCount = 20;
    List<IMeasurementSchema> schemas = new ArrayList<>(measurementCount);
    List<String> present = new ArrayList<>(measurementCount);
    for (int i = 0; i < measurementCount; i++) {
      String measurement = String.format("m%02d", i);
      present.add(measurement);
      schemas.add(new MeasurementSchema(measurement, TSDataType.INT32));
    }
    try (TsFileWriter writer = new TsFileWriter(file)) {
      for (IMeasurementSchema schema : schemas) {
        writer.registerTimeseries(device, schema);
      }
      for (int time = 0; time < 5; time++) {
        TSRecord record = new TSRecord(device, time);
        for (String measurement : present) {
          record.addPoint(measurement, time);
        }
        writer.writeRecord(record);
      }
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      long[] offset = null;
      LazyTsFileDeviceIterator iterator = new LazyTsFileDeviceIterator(reader);
      while (iterator.hasNext()) {
        IDeviceID current = iterator.next();
        if (current.equals(device)) {
          offset = iterator.getCurrentDeviceMeasurementNodeOffset();
          break;
        }
      }
      Assert.assertNotNull(offset);

      List<Long> allReads = new ArrayList<>();
      Map<String, TimeseriesMetadata> all =
          reader.readTimeseriesMetadata(device, offset, present, false, allReads::add);
      Assert.assertEquals(measurementCount, all.size());
      // the batch read resolves several measurements per node/group read
      Assert.assertFalse(allReads.isEmpty());
      Assert.assertTrue(allReads.size() < measurementCount);
      for (String measurement : present) {
        TimeseriesMetadata metadata = all.get(measurement);
        Assert.assertNotNull(measurement, metadata);
        Assert.assertEquals(measurement, metadata.getMeasurementId());
        Assert.assertNotNull(measurement, metadata.getChunkMetadataList());
        Assert.assertFalse(measurement, metadata.getChunkMetadataList().isEmpty());
      }

      // absent names must not add reads
      Set<String> withAbsent = new HashSet<>(present);
      withAbsent.add("a");
      withAbsent.add("m99");
      withAbsent.add("zzz");
      List<Long> readsWithAbsent = new ArrayList<>();
      Map<String, TimeseriesMetadata> withAbsentResult =
          reader.readTimeseriesMetadata(device, offset, withAbsent, false, readsWithAbsent::add);
      Assert.assertEquals(measurementCount, withAbsentResult.size());
      Assert.assertEquals(allReads, readsWithAbsent);

      // a single searched measurement can warm the siblings that share its leaf group
      List<Long> siblingReads = new ArrayList<>();
      Map<String, TimeseriesMetadata> siblings =
          reader.readTimeseriesMetadata(
              device,
              offset,
              Collections.singletonList("m00"),
              new HashSet<>(present),
              false,
              siblingReads::add);
      Assert.assertTrue(siblings.containsKey("m00"));
      Assert.assertTrue(present.containsAll(siblings.keySet()));
      Assert.assertTrue(siblingReads.size() < measurementCount);
    } finally {
      Assert.assertTrue(file.delete());
    }
  }
}
