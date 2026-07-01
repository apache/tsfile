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

package org.apache.tsfile.write.record;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TabletTest {

  @Test
  public void testAddValue() {
    Tablet tablet =
        new Tablet(
            "root.testsg.d1",
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.BOOLEAN),
                new MeasurementSchema("s2", TSDataType.BOOLEAN)));
    tablet.addTimestamp(0, 0);
    tablet.addValue("s1", 0, true);
    tablet.addValue("s2", 0, true);
    tablet.addTimestamp(1, 1);
    tablet.addValue(1, 0, false);
    tablet.addValue(1, 1, true);
    tablet.addTimestamp(2, 2);
    tablet.addValue(2, 0, true);

    Assert.assertEquals(tablet.getRowSize(), 3);
    Assert.assertTrue((Boolean) tablet.getValue(0, 0));
    Assert.assertTrue((Boolean) tablet.getValue(0, 1));
    Assert.assertFalse((Boolean) tablet.getValue(1, 0));
    Assert.assertTrue((Boolean) tablet.getValue(1, 1));
    Assert.assertTrue((Boolean) tablet.getValue(2, 0));
    Assert.assertFalse(tablet.getBitMaps()[0].isMarked(0));
    Assert.assertFalse(tablet.getBitMaps()[0].isMarked(1));
    Assert.assertFalse(tablet.getBitMaps()[0].isMarked(2));
    Assert.assertFalse(tablet.getBitMaps()[1].isMarked(0));
    Assert.assertFalse(tablet.getBitMaps()[1].isMarked(1));
    Assert.assertTrue(tablet.getBitMaps()[1].isMarked(2));

    tablet.addTimestamp(9, 9);
    Assert.assertEquals(10, tablet.getRowSize());

    tablet.reset();
    Assert.assertEquals(0, tablet.getRowSize());
    Assert.assertTrue(tablet.getBitMaps()[0].isAllMarked());
    Assert.assertTrue(tablet.getBitMaps()[0].isAllMarked());
    Assert.assertTrue(tablet.getBitMaps()[0].isAllMarked());
  }

  @Test
  public void testSerializationAndDeSerialization() {
    final String deviceId = "root.sg";
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));

    final int rowSize = 100;
    final long[] timestamps = new long[rowSize];
    final Object[] values = new Object[2];
    values[0] = new int[rowSize];
    values[1] = new long[rowSize];

    for (int i = 0; i < rowSize; i++) {
      timestamps[i] = i;
      ((int[]) values[0])[i] = 1;
      ((long[]) values[1])[i] = 1;
    }

    final Tablet tablet =
        new Tablet(
            deviceId,
            measurementSchemas,
            timestamps,
            values,
            new BitMap[] {new BitMap(1024), new BitMap(1024)},
            rowSize);
    try {
      final ByteBuffer byteBuffer = tablet.serialize();
      final Tablet newTablet = Tablet.deserialize(byteBuffer);
      assertEquals(tablet, newTablet);
      for (int i = 0; i < rowSize; i++) {
        for (int j = 0; j < tablet.getSchemas().size(); j++) {
          assertEquals(tablet.getValue(i, j), newTablet.getValue(i, j));
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSerializationAndDeSerializationWithMoreData() {
    final String deviceId = "root.sg";
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s3", TSDataType.DOUBLE, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s6", TSDataType.STRING, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s7", TSDataType.BLOB, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s8", TSDataType.TIMESTAMP, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s9", TSDataType.DATE, TSEncoding.PLAIN));

    final int rowSize = 1000;
    final Tablet tablet = new Tablet(deviceId, measurementSchemas);
    tablet.setRowSize(rowSize);
    tablet.initBitMaps();
    for (int i = 0; i < rowSize - 1; i++) {
      tablet.addTimestamp(i, i);
      tablet.addValue(measurementSchemas.get(0).getMeasurementName(), i, i);
      tablet.addValue(measurementSchemas.get(1).getMeasurementName(), i, (long) i);
      tablet.addValue(measurementSchemas.get(2).getMeasurementName(), i, (float) i);
      tablet.addValue(measurementSchemas.get(3).getMeasurementName(), i, (double) i);
      tablet.addValue(measurementSchemas.get(4).getMeasurementName(), i, (i % 2) == 0);
      tablet.addValue(measurementSchemas.get(5).getMeasurementName(), i, String.valueOf(i));
      tablet.addValue(measurementSchemas.get(6).getMeasurementName(), i, String.valueOf(i));
      tablet.addValue(
          measurementSchemas.get(7).getMeasurementName(),
          i,
          new Binary(String.valueOf(i), TSFileConfig.STRING_CHARSET));
      tablet.addValue(measurementSchemas.get(8).getMeasurementName(), i, (long) i);
      tablet.addValue(
          measurementSchemas.get(9).getMeasurementName(),
          i,
          LocalDate.of(2000 + i, i / 100 + 1, i / 100 + 1));

      tablet.getBitMaps()[i % measurementSchemas.size()].mark(i);
    }

    // Test add null
    tablet.addTimestamp(rowSize - 1, rowSize - 1);
    tablet.addValue(measurementSchemas.get(0).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(1).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(2).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(3).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(4).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(5).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(6).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(7).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(8).getMeasurementName(), rowSize - 1, null);
    tablet.addValue(measurementSchemas.get(9).getMeasurementName(), rowSize - 1, null);

    try {
      final ByteBuffer byteBuffer = tablet.serialize();
      final Tablet newTablet = Tablet.deserialize(byteBuffer);
      assertEquals(tablet, newTablet);
      for (int i = 0; i < rowSize; i++) {
        for (int j = 0; j < tablet.getSchemas().size(); j++) {
          assertEquals(tablet.getValue(i, j), newTablet.getValue(i, j));
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSerializationAndDeSerializationNull() {
    final String deviceId = "root.sg";
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s3", TSDataType.DOUBLE, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s6", TSDataType.STRING, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s7", TSDataType.BLOB, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s8", TSDataType.TIMESTAMP, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s9", TSDataType.DATE, TSEncoding.PLAIN));

    final int rowSize = 1000;
    final Tablet tablet = new Tablet(deviceId, measurementSchemas);
    tablet.setRowSize(rowSize);
    tablet.initBitMaps();
    for (int i = 0; i < rowSize; i++) {
      tablet.addTimestamp(i, i);
      tablet.addValue(measurementSchemas.get(0).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(1).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(2).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(3).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(4).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(5).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(6).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(7).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(8).getMeasurementName(), i, null);
      tablet.addValue(measurementSchemas.get(9).getMeasurementName(), i, null);
    }

    try {
      final ByteBuffer byteBuffer = tablet.serialize();
      final Tablet newTablet = Tablet.deserialize(byteBuffer);
      assertEquals(tablet, newTablet);
      for (int i = 0; i < rowSize; i++) {
        for (int j = 0; j < tablet.getSchemas().size(); j++) {
          assertNull(tablet.getValue(i, j));
          assertNull(newTablet.getValue(i, j));
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testWriteWrongType() {
    final String deviceId = "root.sg";
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s3", TSDataType.DOUBLE, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s6", TSDataType.STRING, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s7", TSDataType.BLOB, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s8", TSDataType.TIMESTAMP, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s9", TSDataType.DATE, TSEncoding.PLAIN));

    Tablet tablet = new Tablet(deviceId, measurementSchemas);
    addValueWithException(tablet, "s0", 0, 1L);
    addValueWithException(tablet, "s1", 0, 1);
    addValueWithException(tablet, "s2", 0, 0.1d);
    addValueWithException(tablet, "s3", 0, 0.1f);
    addValueWithException(tablet, "s3", 0, "1");
    addValueWithException(tablet, "s5", 0, 1L);
    addValueWithException(tablet, "s6", 0, 1L);
    addValueWithException(tablet, "s7", 0, 1L);
    addValueWithException(tablet, "s8", 0, "str");
    addValueWithException(tablet, "s9", 0, 1L);
  }

  private void addValueWithException(Tablet tablet, String column, int rowIndex, Object value) {
    try {
      tablet.addValue(column, rowIndex, value);
    } catch (IllegalArgumentException e) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testSerializeDateColumnWithNullValue() throws IOException {
    final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema("s1", TSDataType.DATE, TSEncoding.PLAIN));
    measurementSchemas.add(new MeasurementSchema("s2", TSDataType.DATE, TSEncoding.PLAIN));
    Tablet tablet = new Tablet("root.testsg.d1", measurementSchemas);
    tablet.addTimestamp(0, 0);
    tablet.addValue(0, 0, LocalDate.now());
    tablet.addTimestamp(1, 1);
    tablet.addValue(1, 1, LocalDate.now());
    ByteBuffer serialized = tablet.serialize();
    Tablet deserializeTablet = Tablet.deserialize(serialized);
    Assert.assertEquals(tablet.getValue(0, 0), deserializeTablet.getValue(0, 0));
    Assert.assertTrue(deserializeTablet.isNull(0, 1));
    Assert.assertEquals(tablet.getValue(1, 1), deserializeTablet.getValue(1, 1));
    Assert.assertTrue(deserializeTablet.isNull(1, 0));
  }

  @Test
  public void testAppendInconsistent() {
    Tablet t1 =
        new Tablet(
            "table1",
            Arrays.asList("tag1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));

    Tablet tWrongTable =
        new Tablet(
            "table2",
            Arrays.asList("tag1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
    assertFalse(t1.append(tWrongTable));

    Tablet tWrongColName =
        new Tablet(
            "table1",
            Arrays.asList("tag2", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
    assertFalse(t1.append(tWrongColName));

    Tablet tWrongColType =
        new Tablet(
            "table1",
            Arrays.asList("tag1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD));
    assertFalse(t1.append(tWrongColType));

    Tablet tWrongColCategory =
        new Tablet(
            "table1",
            Arrays.asList("tag1", "s1"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT32),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.TAG));
    assertFalse(t1.append(tWrongColCategory));
  }

  private void fillTablet(Tablet t, int valueOffset, int length) {
    for (int i = 0; i < length; i++) {
      t.addTimestamp(i, i + valueOffset);
      for (int j = 0; j < t.getSchemas().size(); j++) {
        switch (t.getSchemas().get(j).getType()) {
          case INT32:
            t.addValue(i, j, i + valueOffset);
            break;
          case TIMESTAMP:
          case INT64:
            t.addValue(i, j, (long) (i + valueOffset));
            break;
          case FLOAT:
            t.addValue(i, j, (i + valueOffset) * 1.0f);
            break;
          case DOUBLE:
            t.addValue(i, j, (i + valueOffset) * 1.0);
            break;
          case BOOLEAN:
            t.addValue(i, j, (i + valueOffset) % 2 == 0);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            t.addValue(i, j, String.valueOf(i + valueOffset));
            break;
          case DATE:
            t.addValue(i, j, LocalDate.of(i + valueOffset, 1, 1));
            break;
        }
      }
    }
  }

  private final List<String> colNamesForAppendTest =
      Arrays.asList(
          "tag1",
          TSDataType.INT32.name(),
          TSDataType.INT64.name(),
          TSDataType.FLOAT.name(),
          TSDataType.DOUBLE.name(),
          TSDataType.BOOLEAN.name(),
          TSDataType.TEXT.name(),
          TSDataType.STRING.name(),
          TSDataType.BLOB.name(),
          TSDataType.TIMESTAMP.name(),
          TSDataType.DATE.name());
  private final List<TSDataType> dataTypesForAppendTest =
      Arrays.asList(
          TSDataType.STRING,
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE,
          TSDataType.BOOLEAN,
          TSDataType.TEXT,
          TSDataType.STRING,
          TSDataType.BLOB,
          TSDataType.TIMESTAMP,
          TSDataType.DATE);
  private final List<ColumnCategory> categoriesForAppendTest =
      Arrays.asList(
          ColumnCategory.TAG,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD,
          ColumnCategory.FIELD);

  @Test
  public void testAppendNoNull() {
    Tablet t1 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);

    int t1Size = 100;
    fillTablet(t1, 0, t1Size);

    int t2Size = 100;
    Tablet t2 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);
    fillTablet(t2, t1Size, t2Size);

    assertTrue(t1.append(t2));
    checkAppendedTablet(t1, t1Size + t2Size, null);
  }

  @Test
  public void testPreferredCapacity() {
    Tablet t1 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);

    int t1Size = 100;
    fillTablet(t1, 0, t1Size);

    int t2Size = 100;
    Tablet t2 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);
    fillTablet(t2, t1Size, t2Size);

    assertTrue(t1.append(t2, 10000));
    checkAppendedTablet(t1, t1Size + t2Size, null);
    assertEquals(10000, t1.getMaxRowNumber());
  }

  @Test
  public void testAppendNullPoints() {
    Set<Pair<Integer, Integer>> nullPositions = new HashSet<>();
    int nullPointNum = 10;
    Random random = new Random();

    Tablet t1 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);

    int t1Size = 100;
    fillTablet(t1, 0, t1Size);
    for (int i = 0; i < nullPointNum; i++) {
      int rowIndex = random.nextInt(t1Size);
      int columnIndex = random.nextInt(colNamesForAppendTest.size());
      nullPositions.add(new Pair<>(rowIndex, columnIndex));
      t1.getBitMaps()[columnIndex].mark(rowIndex);
    }

    int t2Size = 100;
    Tablet t2 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);
    fillTablet(t2, t1Size, t2Size);
    for (int i = 0; i < nullPointNum; i++) {
      int rowIndex = random.nextInt(t1Size);
      int columnIndex = random.nextInt(colNamesForAppendTest.size());
      nullPositions.add(new Pair<>(rowIndex + t1Size, columnIndex));
      t2.getBitMaps()[columnIndex].mark(rowIndex);
    }

    assertTrue(t1.append(t2));
    checkAppendedTablet(t1, t1Size + t2Size, nullPositions);
  }

  @Test
  public void testAppendNullBitMapColumn() {
    int nullBitMapNum = 5;
    Random random = new Random();

    Tablet t1 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);

    int t1Size = 100;
    fillTablet(t1, 0, t1Size);
    for (int i = 0; i < nullBitMapNum; i++) {
      int columnIndex = random.nextInt(colNamesForAppendTest.size());
      t1.getBitMaps()[columnIndex] = null;
    }

    int t2Size = 100;
    Tablet t2 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);
    fillTablet(t2, t1Size, t2Size);
    for (int i = 0; i < nullBitMapNum; i++) {
      int columnIndex = random.nextInt(colNamesForAppendTest.size());
      t2.getBitMaps()[columnIndex] = null;
    }

    assertTrue(t1.append(t2));
    assertEquals(t1Size + t2Size, t1.getRowSize());

    checkAppendedTablet(t1, t1Size + t2Size, null);
  }

  @Test
  public void testAppendThisNullBitMap() {

    Tablet t1 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);

    int t1Size = 100;
    fillTablet(t1, 0, t1Size);
    t1.setBitMaps(null);

    int t2Size = 100;
    Tablet t2 =
        new Tablet(
            "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);
    fillTablet(t2, t1Size, t2Size);

    assertTrue(t1.append(t2));
    checkAppendedTablet(t1, t1Size + t2Size, null);
  }

  @Test
  public void testMultipleAppend() {
    List<Tablet> tablets = new ArrayList<>();
    int tabletNum = 10;
    int singleTabletSize = 100;
    for (int i = 0; i < tabletNum; i++) {
      Tablet tablet =
          new Tablet(
              "table1", colNamesForAppendTest, dataTypesForAppendTest, categoriesForAppendTest);
      fillTablet(tablet, i * singleTabletSize, singleTabletSize);
      tablets.add(tablet);
    }
    for (int i = 1; i < tabletNum; i++) {
      assertTrue(tablets.get(0).append(tablets.get(i)));
    }
    checkAppendedTablet(tablets.get(0), singleTabletSize * tabletNum, null);
  }

  private void checkAppendedTablet(
      Tablet result, int totalSize, Set<Pair<Integer, Integer>> nullPositions) {
    assertEquals(totalSize, result.getRowSize());

    for (int i = 0; i < totalSize; i++) {
      assertEquals(i, result.getTimestamp(i));
      for (int j = 0; j < result.getSchemas().size(); j++) {
        if (nullPositions != null && nullPositions.contains(new Pair<>(i, j))) {
          assertTrue(result.isNull(i, j));
          continue;
        }

        assertFalse(result.isNull(i, j));
        switch (result.getSchemas().get(j).getType()) {
          case INT32:
            assertEquals(i, result.getValue(i, j));
            break;
          case TIMESTAMP:
          case INT64:
            assertEquals((long) i, result.getValue(i, j));
            break;
          case FLOAT:
            assertEquals(i * 1.0f, (float) result.getValue(i, j), 0.0001f);
            break;
          case DOUBLE:
            assertEquals(i * 1.0, (double) result.getValue(i, j), 0.0001);
            break;
          case BOOLEAN:
            assertEquals(i % 2 == 0, result.getValue(i, j));
            break;
          case TEXT:
          case BLOB:
          case STRING:
            assertEquals(
                new Binary(String.valueOf(i).getBytes(StandardCharsets.UTF_8)),
                result.getValue(i, j));
            break;
          case DATE:
            assertEquals(LocalDate.of(i, 1, 1), result.getValue(i, j));
            break;
        }
      }
    }
  }
}
