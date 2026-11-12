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

package org.apache.tsfile.write;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertThrows;

public class AllowNonDecreasingTimestampsTest {

  private final File file = new File("target/AllowNonDecreasingTimestampsTest.tsfile");
  private final String deviceId = "root.sg.d1";
  private final MeasurementSchema s1 =
      new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.PLAIN);
  private final MeasurementSchema s2 =
      new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.PLAIN);
  private TSFileConfig config;

  @Before
  public void setUp() {
    config = TSFileDescriptor.getInstance().getConfig();
    config.setAllowNonDecreasingTimestamps(false);
    if (file.exists() && !file.delete()) {
      throw new RuntimeException("can not delete " + file.getAbsolutePath());
    }
  }

  @After
  public void tearDown() {
    config.setAllowNonDecreasingTimestamps(false);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void writeNonAlignedTabletWithDuplicates() throws IOException, WriteProcessException {
    config.setAllowNonDecreasingTimestamps(true);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path(deviceId), Arrays.asList(s1));
      Tablet tablet = new Tablet(deviceId, Collections.singletonList(s1), 4);
      long[] times = {0, 1, 1, 2};
      for (int row = 0; row < times.length; row++) {
        tablet.addTimestamp(row, times[row]);
        tablet.addValue("s1", row, row * 10);
      }
      writer.writeTree(tablet);
    }

    List<Long> readTimes = readTimestamps(deviceId, "s1");
    Assert.assertEquals(Arrays.asList(0L, 1L, 1L, 2L), readTimes);
  }

  @Test
  public void writeAlignedRecordWithDuplicates() throws IOException, WriteProcessException {
    config.setAllowNonDecreasingTimestamps(true);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerAlignedTimeseries(new Path(deviceId), Arrays.asList(s1, s2));
      long[] times = {5, 6, 6, 7};
      for (int i = 0; i < times.length; i++) {
        TSRecord record = new TSRecord(deviceId, times[i]);
        record.addTuple(new IntDataPoint("s1", i));
        record.addTuple(new IntDataPoint("s2", i * 2));
        writer.writeRecord(record);
      }
    }

    List<Long> readTimes = readTimestamps(deviceId, "s1");
    Assert.assertEquals(Arrays.asList(5L, 6L, 6L, 7L), readTimes);
  }

  @Test
  public void strictlyDecreasingStillRejectedWhenFlagOn()
      throws IOException, WriteProcessException {
    config.setAllowNonDecreasingTimestamps(true);
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path(deviceId), Arrays.asList(s1));
      Tablet tablet = new Tablet(deviceId, Collections.singletonList(s1), 3);
      long[] times = {0, 1, 0};
      for (int row = 0; row < times.length; row++) {
        tablet.addTimestamp(row, times[row]);
        tablet.addValue("s1", row, row);
      }
      assertThrows(WriteProcessException.class, () -> writer.writeTree(tablet));
    }
  }

  @Test
  public void duplicateStillRejectedWhenFlagOff() throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path(deviceId), Arrays.asList(s1));
      Tablet tablet = new Tablet(deviceId, Collections.singletonList(s1), 3);
      long[] times = {0, 1, 1};
      for (int row = 0; row < times.length; row++) {
        tablet.addTimestamp(row, times[row]);
        tablet.addValue("s1", row, row);
      }
      assertThrows(WriteProcessException.class, () -> writer.writeTree(tablet));
    }
  }

  @Test
  public void recordsPolicyPropertyWhenFlagOn() throws IOException, WriteProcessException {
    config.setAllowNonDecreasingTimestamps(true);
    writeSingleRecord();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      byte[] value =
          reader.getTsFileProperties().get(TsFileIOWriter.ALLOW_NON_DECREASING_TIMESTAMPS_PROPERTY);
      Assert.assertArrayEquals("true".getBytes(TSFileConfig.STRING_CHARSET), value);
    }
  }

  @Test
  public void omitsPolicyPropertyWhenFlagOff() throws IOException, WriteProcessException {
    writeSingleRecord();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath())) {
      Assert.assertFalse(
          reader
              .getTsFileProperties()
              .containsKey(TsFileIOWriter.ALLOW_NON_DECREASING_TIMESTAMPS_PROPERTY));
    }
  }

  private void writeSingleRecord() throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(new Path(deviceId), Arrays.asList(s1));
      TSRecord record = new TSRecord(deviceId, 1);
      record.addTuple(new IntDataPoint("s1", 1));
      writer.writeRecord(record);
    }
  }

  private List<Long> readTimestamps(String device, String measurement) throws IOException {
    List<Long> times = new ArrayList<>();
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(file.getAbsolutePath());
        TsFileReader reader = new TsFileReader(sequenceReader)) {
      QueryExpression query =
          QueryExpression.create(
              Collections.singletonList(new Path(device, measurement, false)), null);
      QueryDataSet dataSet = reader.query(query);
      while (dataSet.hasNext()) {
        RowRecord record = dataSet.next();
        times.add(record.getTimestamp());
      }
    }
    return times;
  }
}
