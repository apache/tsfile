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

package org.apache.tsfile.read.reader;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TsFileLastReaderTest {
  private final String filePath = "target/test.tsfile";
  private final File file = new File(filePath);

  private void createFile(int deviceNum, int measurementNum, int seriesPointNum)
      throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
      for (int j = 0; j < measurementNum; j++) {
        measurementSchemaList.add(new MeasurementSchema("s" + j, TSDataType.INT64));
      }
      for (int i = 0; i < deviceNum; i++) {
        writer.registerAlignedTimeseries("device" + i, measurementSchemaList);
      }

      for (int i = 0; i < deviceNum; i++) {
        Tablet tablet = new Tablet("device" + i, measurementSchemaList, seriesPointNum);
        for (int k = 0; k < seriesPointNum; k++) {
          tablet.addTimestamp(k, k);
        }
        for (int j = 0; j < measurementNum; j++) {
          for (int k = 0; k < seriesPointNum; k++) {
            tablet.addValue(k, j, k);
          }
        }
        writer.writeTree(tablet);
      }
    }
  }

  private void doReadLast(int deviceNum, int measurementNum, int seriesPointNum) throws Exception {
    long startTime = System.currentTimeMillis();
    Set<IDeviceID> devices = new HashSet<>();
    try (TsFileLastReader lastReader = new TsFileLastReader(filePath, false)) {
      while (lastReader.hasNext()) {
        Set<String> measurements = new HashSet<>();
        Pair<IDeviceID, List<Pair<String, TimeValuePair>>> next = lastReader.next();
        assertFalse(devices.contains(next.left));
        devices.add(next.left);

        // time column included
        assertEquals(measurementNum + 1, next.getRight().size());
        next.right.forEach(
            pair -> {
              measurements.add(pair.getLeft());
              assertEquals(seriesPointNum - 1, pair.getRight().getTimestamp());
              assertEquals(seriesPointNum - 1, pair.getRight().getValue().getLong());
            });
        assertEquals(measurementNum + 1, measurements.size());
      }
    }
    assertEquals(deviceNum, devices.size());
    System.out.printf("Last point iteration takes %dms%n", System.currentTimeMillis() - startTime);
  }

  private void testReadLast(int deviceNum, int measurementNum, int seriesPointNum)
      throws Exception {
    createFile(deviceNum, measurementNum, seriesPointNum);
    doReadLast(deviceNum, measurementNum, seriesPointNum);
    file.delete();
  }

  @Test
  public void testSmall() throws Exception {
    testReadLast(10, 10, 10);
  }

  @Test
  public void testManyDevices() throws Exception {
    testReadLast(10000, 10, 10);
  }

  @Test
  public void testManyMeasurement() throws Exception {
    testReadLast(10, 10000, 10);
  }

  @Test
  public void testManyPoints() throws Exception {
    testReadLast(100, 10, 10000);
  }

  @Test
  public void testManyMany() throws Exception {
    testReadLast(1000, 1000, 1000);
  }

  @Ignore("Performance")
  @Test
  public void testManyRead() throws Exception {
    int deviceNum = 10000;
    int measurementNum = 1000;
    int seriesPointNum = 1;
    createFile(deviceNum, measurementNum, seriesPointNum);
    for (int i = 0; i < 10; i++) {
      doReadLast(deviceNum, measurementNum, seriesPointNum);
    }
    file.delete();
  }
}
