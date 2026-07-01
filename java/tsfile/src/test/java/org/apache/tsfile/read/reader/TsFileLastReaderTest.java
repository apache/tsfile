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
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.utils.WriteUtils.TabletAddValueFunction;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({"ResultOfMethodCallIgnored", "SameParameterValue"})
public class TsFileLastReaderTest {

  private static final List<TSDataType> dataTypes =
      Arrays.asList(TSDataType.INT64, TSDataType.BLOB);
  private static final Map<TSDataType, TabletAddValueFunction> typeAddValueFunctions =
      new HashMap<>();

  static {
    typeAddValueFunctions.put(
        TSDataType.INT64, ((tablet, row, column) -> tablet.addValue(row, column, (long) row)));
    typeAddValueFunctions.put(
        TSDataType.BLOB,
        ((tablet, row, column) ->
            tablet.addValue(
                row, column, Long.toBinaryString(row).getBytes(StandardCharsets.UTF_8))));
  }

  private final String filePath = "target/test.tsfile";
  private final File file = new File(filePath);

  private void createFile(int deviceNum, int measurementNum, int seriesPointNum)
      throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
      for (int j = 0; j < measurementNum; j++) {
        TSDataType tsDataType = dataTypes.get(j % dataTypes.size());
        measurementSchemaList.add(new MeasurementSchema("s" + j, tsDataType));
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
          TSDataType tsDataType = dataTypes.get(j % dataTypes.size());
          for (int k = 0; k < seriesPointNum; k++) {
            typeAddValueFunctions.get(tsDataType).addValue(tablet, k, j);
          }
        }
        writer.writeTree(tablet);
      }
    }
  }

  // the second half measurements will have an emtpy last chunk each
  private void createFileWithLastEmptyChunks(int deviceNum, int measurementNum, int seriesPointNum)
      throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(file)) {
      List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
      for (int j = 0; j < measurementNum; j++) {
        TSDataType tsDataType = dataTypes.get(j % dataTypes.size());
        measurementSchemaList.add(new MeasurementSchema("s" + j, tsDataType));
      }
      for (int i = 0; i < deviceNum; i++) {
        writer.registerAlignedTimeseries("device" + i, measurementSchemaList);
      }

      // the first half seriesPointNum points are not null for all series
      int batchPointNum = seriesPointNum / 2;
      for (int i = 0; i < deviceNum; i++) {
        Tablet tablet = new Tablet("device" + i, measurementSchemaList, batchPointNum);
        for (int k = 0; k < batchPointNum; k++) {
          tablet.addTimestamp(k, k);
        }
        for (int j = 0; j < measurementNum; j++) {
          TSDataType tsDataType = dataTypes.get(j % dataTypes.size());
          for (int k = 0; k < batchPointNum; k++) {
            typeAddValueFunctions.get(tsDataType).addValue(tablet, k, j);
          }
        }
        writer.writeTree(tablet);
      }
      writer.flush();

      // the second half series have no value for the remaining points
      batchPointNum = seriesPointNum - batchPointNum;
      for (int i = 0; i < deviceNum; i++) {
        Tablet tablet = new Tablet("device" + i, measurementSchemaList, seriesPointNum);
        for (int k = 0; k < batchPointNum; k++) {
          tablet.addTimestamp(k, k + seriesPointNum / 2);
        }
        for (int j = 0; j < measurementNum / 2; j++) {
          TSDataType tsDataType = dataTypes.get(j % dataTypes.size());
          for (int k = 0; k < seriesPointNum; k++) {
            switch (tsDataType) {
              case INT64:
                tablet.addValue(k, j, (long) k + seriesPointNum / 2);
                break;
              case BLOB:
                tablet.addValue(
                    k,
                    j,
                    Long.toBinaryString(k + seriesPointNum / 2).getBytes(StandardCharsets.UTF_8));
                break;
              default:
                throw new IllegalArgumentException("Unsupported TSDataType " + tsDataType);
            }
          }
        }
        writer.writeTree(tablet);
      }
    }
  }

  private void doReadLastWithEmpty(int deviceNum, int measurementNum, int seriesPointNum)
      throws Exception {
    long startTime = System.currentTimeMillis();
    Set<IDeviceID> devices = new HashSet<>();
    try (TsFileLastReader lastReader = new TsFileLastReader(filePath, true, false)) {
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
              // the time column is regarded as the first half
              int measurementIndex =
                  pair.left.isEmpty() ? -1 : Integer.parseInt(pair.getLeft().substring(1));

              if (measurementIndex < measurementNum / 2) {
                assertEquals(seriesPointNum - 1, pair.getRight().getTimestamp());
                TsPrimitiveType value = pair.getRight().getValue();
                if (value.getDataType() == TSDataType.INT64) {
                  assertEquals(seriesPointNum - 1, value.getLong());
                } else {
                  assertEquals(
                      new Binary(Long.toBinaryString(seriesPointNum - 1), StandardCharsets.UTF_8),
                      value.getBinary());
                }
              } else {
                assertEquals(seriesPointNum / 2 - 1, pair.getRight().getTimestamp());
                TsPrimitiveType value = pair.getRight().getValue();
                if (value.getDataType() == TSDataType.INT64) {
                  assertEquals(seriesPointNum / 2 - 1, value.getLong());
                } else {
                  assertEquals(
                      new Binary(
                          Long.toBinaryString(seriesPointNum / 2 - 1), StandardCharsets.UTF_8),
                      value.getBinary());
                }
              }
            });
        assertEquals(measurementNum + 1, measurements.size());
      }
    }
    assertEquals(deviceNum, devices.size());
    System.out.printf("Last point iteration takes %dms%n", System.currentTimeMillis() - startTime);
  }

  private void doReadLast(int deviceNum, int measurementNum, int seriesPointNum, boolean ignoreBlob)
      throws Exception {
    long startTime = System.currentTimeMillis();
    Set<IDeviceID> devices = new HashSet<>();
    try (TsFileLastReader lastReader = new TsFileLastReader(filePath, true, ignoreBlob)) {
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
              // the time column is regarded as the first half
              int measurementIndex =
                  pair.left.isEmpty() ? -1 : Integer.parseInt(pair.getLeft().substring(1));
              TSDataType tsDataType =
                  measurementIndex == -1
                      ? TSDataType.INT64
                      : dataTypes.get(measurementIndex % dataTypes.size());

              if (tsDataType == TSDataType.BLOB && ignoreBlob) {
                assertNull(pair.getRight());
                return;
              }

              assertEquals(seriesPointNum - 1, pair.getRight().getTimestamp());
              if (pair.getRight() == null) {
                assertTrue(ignoreBlob);
              } else {
                TsPrimitiveType value = pair.getRight().getValue();
                if (value.getDataType() == TSDataType.INT64) {
                  assertEquals(seriesPointNum - 1, value.getLong());
                } else {
                  assertEquals(
                      new Binary(Long.toBinaryString(seriesPointNum - 1), StandardCharsets.UTF_8),
                      value.getBinary());
                }
              }
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
    doReadLast(deviceNum, measurementNum, seriesPointNum, false);
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
    testReadLast(100, 100, 100);
  }

  @Test
  public void testLastEmptyChunks() throws Exception {
    createFileWithLastEmptyChunks(100, 100, 100);
    doReadLastWithEmpty(100, 100, 100);
  }

  @Test
  public void testLastEmptyPage() throws Exception {
    try (TsFileIOWriter ioWriter = new TsFileIOWriter(file)) {
      ioWriter.startChunkGroup(Factory.DEFAULT_FACTORY.create("root.db1.d1"));
      List<IMeasurementSchema> measurementSchemaList =
          Arrays.asList(
              new MeasurementSchema("s1", TSDataType.INT64),
              new MeasurementSchema("s2", TSDataType.BLOB));
      AlignedChunkWriterImpl alignedChunkWriter = new AlignedChunkWriterImpl(measurementSchemaList);
      alignedChunkWriter.write(
          0,
          new TsPrimitiveType[] {
            TsPrimitiveType.getByType(TSDataType.INT64, 0L),
            TsPrimitiveType.getByType(
                TSDataType.BLOB, new Binary("0".getBytes(StandardCharsets.UTF_8)))
          });
      alignedChunkWriter.sealCurrentPage();
      alignedChunkWriter.write(
          1, new TsPrimitiveType[] {TsPrimitiveType.getByType(TSDataType.INT64, 1L), null});
      alignedChunkWriter.writeToFileWriter(ioWriter);
      ioWriter.endChunkGroup();

      ioWriter.endFile();
    }

    try (TsFileLastReader lastReader = new TsFileLastReader(filePath)) {
      Pair<IDeviceID, List<Pair<String, TimeValuePair>>> next = lastReader.next();
      assertEquals(Factory.DEFAULT_FACTORY.create("root.db1.d1"), next.getLeft());
      assertEquals(3, next.getRight().size());
      assertEquals("s1", next.getRight().get(1).left);
      assertEquals("s2", next.getRight().get(2).left);
      assertEquals(1, next.getRight().get(1).right.getTimestamp());
      assertEquals(1, next.getRight().get(1).right.getValue().getLong());
      assertEquals(0, next.getRight().get(2).right.getTimestamp());
      assertEquals("0", next.getRight().get(2).right.getValue().getStringValue());
    }
  }

  @Test
  public void testIgnoreBlob() throws Exception {
    createFile(10, 10, 10);
    doReadLast(10, 10, 10, true);
    file.delete();
  }

  @Ignore("Performance")
  @Test
  public void testManyRead() throws Exception {
    int deviceNum = 10000;
    int measurementNum = 1000;
    int seriesPointNum = 1;
    createFile(deviceNum, measurementNum, seriesPointNum);
    for (int i = 0; i < 10; i++) {
      doReadLast(deviceNum, measurementNum, seriesPointNum, false);
    }
    file.delete();
  }

  @Test
  public void testCreateButNotRead() throws Exception {
    createFile(10, 10, 10);
    try (TsFileLastReader ignored = new TsFileLastReader(filePath)) {}
  }
}
