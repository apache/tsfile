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
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.ResultSetMetadata;
import org.apache.tsfile.read.v4.ITsFileTreeReader;
import org.apache.tsfile.read.v4.TsFileTreeReaderBuilder;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.TsFileTreeWriter;
import org.apache.tsfile.write.v4.TsFileTreeWriterBuilder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class TsFileTreeReaderTest {

  private static final String FILE_PATH = "test_v4_tree_reader.tsfile";
  private ITsFileTreeReader treeReader;

  @Before
  public void setUp() throws IOException {
    generateTestTsFile();
    treeReader = new TsFileTreeReaderBuilder().file(new File(FILE_PATH)).build();
  }

  @After
  public void tearDown() throws IOException {
    if (treeReader != null) {
      treeReader.close();
    }
    new File(FILE_PATH).delete();
  }

  // Test basic query functionality with device IDs and measurements
  @Test
  public void testBasicQuery() throws IOException {
    List<String> deviceIds = Arrays.asList("d1", "d2");
    List<String> measurements = Arrays.asList("s1", "s2");
    long startTime = Long.MIN_VALUE;
    long endTime = Long.MAX_VALUE;

    ResultSet resultSet = treeReader.query(deviceIds, measurements, startTime, endTime);

    int countVal = 0;
    while (resultSet.next()) {
      Assert.assertEquals(resultSet.getLong(1), countVal);
      Assert.assertEquals(resultSet.getInt(2), countVal);
      Assert.assertEquals(resultSet.getLong(3), countVal);
      Assert.assertEquals(resultSet.getInt(4), countVal);
      countVal++;
    }
    Assert.assertTrue("Should return at least one record", countVal > 0);
  }

  // Test iterator-based query functionality
  @Test
  public void testIteratorQuery() throws IOException {
    List<String> deviceIds = Arrays.asList("d2", "d1");
    List<String> measurements = Arrays.asList("s2", "s1");
    long startTime = Long.MIN_VALUE;
    long endTime = Long.MAX_VALUE;

    ResultSet resultSet = treeReader.query(deviceIds, measurements, startTime, endTime);
    Iterator<TSRecord> recordIterator = resultSet.recordIterator();

    int countVal = 0;
    while (recordIterator.hasNext()) {
      TSRecord record = recordIterator.next();
      if (record.deviceId.toString().equals("d1")) {
        Assert.assertEquals("s2", record.dataPointList.get(1).getMeasurementId());
        Assert.assertEquals("s1", record.dataPointList.get(2).getMeasurementId());
      } else if (record.deviceId.toString().equals("d2")) {
        Assert.assertEquals("s1", record.dataPointList.get(2).getMeasurementId());
        Assert.assertNull(record.dataPointList.get(1));
      } else {
        Assert.fail("Unexpected deviceId: " + record.deviceId.toString());
      }
      countVal++;
    }
    Assert.assertTrue("Should return at least one record", countVal > 0);
  }

  // Test retrieving all device IDs from the TsFile
  @Test
  public void testGetAllDeviceIds() throws IOException {
    List<String> deviceIds = treeReader.getAllDeviceIds();
    Assert.assertEquals(2, deviceIds.size());
    Assert.assertTrue(deviceIds.contains("d1"));
    Assert.assertTrue(deviceIds.contains("d2"));
  }

  // Test ResultSet metadata functionality
  @Test
  public void testResultSetMetadata() throws IOException {
    List<String> deviceIds = Arrays.asList("d1", "d2");
    List<String> measurements = Arrays.asList("s1", "s2");
    long startTime = Long.MIN_VALUE;
    long endTime = Long.MAX_VALUE;

    ResultSet resultSet = treeReader.query(deviceIds, measurements, startTime, endTime);
    ResultSetMetadata resultSetMetadata = resultSet.getMetadata();
    Assert.assertEquals("Time", resultSetMetadata.getColumnName(1));
    Assert.assertEquals("d1.s1", resultSetMetadata.getColumnName(2));
    Assert.assertEquals("d1.s2", resultSetMetadata.getColumnName(3));
    Assert.assertEquals("d2.s1", resultSetMetadata.getColumnName(4));
  }

  // Test retrieving schema information for a specific device
  @Test
  public void testGetDeviceSchema() throws IOException {
    List<MeasurementSchema> schemas = treeReader.getDeviceSchema("d1");
    List<String> measurementNames =
        schemas.stream().map(MeasurementSchema::getMeasurementName).collect(Collectors.toList());

    Assert.assertTrue(measurementNames.contains("s1"));
    Assert.assertTrue(measurementNames.contains("s2"));
  }

  // Helper method to generate test TsFile data
  private void generateTestTsFile() throws IOException {
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    File file = new File(TsFileTreeReaderTest.FILE_PATH);

    try (TsFileTreeWriter writer = new TsFileTreeWriterBuilder().file(file).build()) {
      // Register time series schemas
      IMeasurementSchema schema1 = new MeasurementSchema("s1", TSDataType.INT32);
      IMeasurementSchema schema2 = new MeasurementSchema("s2", TSDataType.INT64);

      // Schema for device d1
      writer.registerTimeseries("d1", schema1);
      writer.registerTimeseries("d1", schema2);

      // Schema for device d2
      writer.registerTimeseries("d2", schema1);

      // Write data for device d1 using Tablet
      Tablet d1Tablet = new Tablet("d1", Arrays.asList(schema1, schema2));
      for (int i = 0; i < 10; i++) {
        d1Tablet.addTimestamp(i, i);
        d1Tablet.addValue(i, 0, i);
        d1Tablet.addValue(i, 1, (long) i);
      }
      writer.write(d1Tablet);

      // Write data for device d2 using TSRecord
      TSRecord record;
      for (int i = 0; i < 10; i++) {
        record = new TSRecord("d2", i);
        record.addPoint("s1", i);
        writer.write(record);
      }
    } catch (WriteProcessException e) {
      throw new RuntimeException(e);
    }
  }
}
