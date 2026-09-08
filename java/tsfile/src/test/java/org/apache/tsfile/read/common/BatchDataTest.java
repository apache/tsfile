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

package org.apache.tsfile.read.common;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BatchDataTest {

  private static final TypeService<SerializedValueAsserter> ASSERT_SERIALIZED_VALUE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (inputStream, expectedValue) ->
                    assertEquals(expectedValue, inputStream.readBoolean());
            case INT32, DATE ->
                (inputStream, expectedValue) -> assertEquals(expectedValue, inputStream.readInt());
            case INT64, TIMESTAMP ->
                (inputStream, expectedValue) -> assertEquals(expectedValue, inputStream.readLong());
            case FLOAT ->
                (inputStream, expectedValue) ->
                    assertEquals((float) expectedValue, inputStream.readFloat(), 0);
            case DOUBLE ->
                (inputStream, expectedValue) ->
                    assertEquals((double) expectedValue, inputStream.readDouble(), 0);
            case TEXT, STRING, BLOB, OBJECT ->
                (inputStream, expectedValue) -> {
                  Binary binary = (Binary) expectedValue;
                  assertArrayEquals(
                      binary.getValues(), inputStream.readNBytes(inputStream.readInt()));
                };
            case ROW, UNKNOWN, VECTOR ->
                (inputStream, expectedValue) -> fail("Unexpected data type: " + type.getTypeEnum());
          };

  static {
    ASSERT_SERIALIZED_VALUE_SERVICE.check();
  }

  @Test
  public void testInt() {
    BatchData batchData = new BatchData(TSDataType.INT32);
    assertTrue(batchData.isEmpty());
    int value = 0;
    for (long time = 0; time < 10; time++) {
      batchData.putAnObject(time, value);
      value++;
    }
    assertEquals(TSDataType.INT32, batchData.getDataType());
    int res = 0;
    long time = 0;
    while (batchData.hasCurrent()) {
      assertEquals(time, batchData.currentTime());
      assertEquals(res, (int) batchData.currentValue());
      assertEquals(res, batchData.currentTsPrimitiveType().getInt());
      batchData.next();
      res++;
      time++;
    }
    batchData.resetBatchData();

    IPointReader reader = batchData.getBatchDataIterator();
    try {
      res = 0;
      time = 0;
      while (reader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = reader.nextTimeValuePair();
        assertEquals(time, timeValuePair.getTimestamp());
        assertEquals(res, timeValuePair.getValue().getInt());
        res++;
        time++;
      }
    } catch (IOException e) {
      fail();
    }
  }

  @Test
  public void testSignal() {
    BatchData batchData = SignalBatchData.getInstance();
    try {
      batchData.hasCurrent();
    } catch (UnsupportedOperationException e) {
      return;
    }
    fail();
  }

  @Test
  public void testSerializeData() throws IOException {
    Binary binary = new Binary("test", StandardCharsets.UTF_8);
    Object[][] testCases = {
      {TSDataType.BOOLEAN, true},
      {TSDataType.INT32, 1},
      {TSDataType.DATE, 20260714},
      {TSDataType.INT64, 1L},
      {TSDataType.TIMESTAMP, 2L},
      {TSDataType.FLOAT, 1.0F},
      {TSDataType.DOUBLE, 2.0D},
      {TSDataType.TEXT, binary},
      {TSDataType.STRING, binary},
      {TSDataType.BLOB, binary},
      {TSDataType.OBJECT, binary}
    };

    for (Object[] testCase : testCases) {
      TSDataType dataType = (TSDataType) testCase[0];
      Object value = testCase[1];
      BatchData batchData = new BatchData(dataType);
      batchData.putAnObject(100L, value);

      try (DataInputStream inputStream = serialize(batchData)) {
        assertEquals(100L, inputStream.readLong());
        assertSerializedValue(inputStream, dataType, value);
        assertEquals(-1, inputStream.read());
      }
    }
  }

  @Test
  public void testSerializeVectorData() throws IOException {
    Binary binary = new Binary("test", StandardCharsets.UTF_8);
    TsPrimitiveType[] values = {
      null,
      new TsPrimitiveType.TsBoolean(true),
      new TsPrimitiveType.TsInt(1),
      new TsPrimitiveType.TsInt(20260714, TSDataType.DATE),
      new TsPrimitiveType.TsLong(1L),
      new TsPrimitiveType.TsFloat(1.0F),
      new TsPrimitiveType.TsDouble(2.0D),
      new TsPrimitiveType.TsBinary(binary)
    };
    BatchData batchData = new BatchData(TSDataType.VECTOR);
    batchData.putAnObject(100L, values);

    try (DataInputStream inputStream = serialize(batchData)) {
      assertEquals(100L, inputStream.readLong());
      assertEquals(values.length, inputStream.readInt());
      for (TsPrimitiveType value : values) {
        if (value == null) {
          assertEquals(0, inputStream.readByte());
          continue;
        }
        assertEquals(1, inputStream.readByte());
        assertEquals(value.getDataType().serialize(), inputStream.readByte());
        assertSerializedValue(inputStream, value.getDataType(), value.getValue());
      }
      assertEquals(-1, inputStream.read());
    }
  }

  @Test
  public void testDescReadWriteExpansion() {
    TSDataType[] dataTypes = {
      TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.DATE, TSDataType.INT64,
      TSDataType.TIMESTAMP, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT,
      TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT, TSDataType.VECTOR
    };
    // Exercise array growth, the first prepended block, and multiple prepended blocks.
    int[] sizes = {
      BatchData.CAPACITY_THRESHOLD,
      BatchData.CAPACITY_THRESHOLD + 1,
      2 * BatchData.CAPACITY_THRESHOLD + 1
    };
    for (TSDataType dataType : dataTypes) {
      for (int size : sizes) {
        BatchData batchData = new DescReadWriteBatchData(dataType);
        for (int i = size; i > 0; i--) {
          batchData.putAnObject(i, expansionValue(dataType, i));
        }
        assertEquals(size, batchData.length());
        batchData.flip();
        for (int i = size; i > 0; i--) {
          assertTrue(batchData.hasCurrent());
          assertEquals(i, batchData.currentTime());
          Object expected = expansionValue(dataType, i);
          if (expected instanceof TsPrimitiveType[] vector) {
            assertArrayEquals(vector, (TsPrimitiveType[]) batchData.currentValue());
          } else {
            assertEquals(expected, batchData.currentValue());
          }
          batchData.next();
        }
        assertFalse(batchData.hasCurrent());
      }
    }
  }

  private Object expansionValue(TSDataType dataType, int value) {
    return switch (dataType) {
      case BOOLEAN -> value % 2 == 0;
      case INT32, DATE -> value;
      case INT64, TIMESTAMP -> (long) value;
      case FLOAT -> (float) value;
      case DOUBLE -> (double) value;
      case TEXT, STRING, BLOB, OBJECT ->
          new Binary(Integer.toString(value), StandardCharsets.UTF_8);
      case VECTOR -> new TsPrimitiveType[] {new TsPrimitiveType.TsInt(value)};
      case UNKNOWN -> throw new AssertionError("Unexpected data type: " + dataType);
    };
  }

  private DataInputStream serialize(BatchData batchData) throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    try (DataOutputStream outputStream = new DataOutputStream(byteStream)) {
      batchData.serializeData(outputStream);
    }
    return new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
  }

  private void assertSerializedValue(
      DataInputStream inputStream, TSDataType dataType, Object expectedValue) throws IOException {
    ASSERT_SERIALIZED_VALUE_SERVICE
        .call(Type.fromTsDataType(dataType))
        .assertValue(inputStream, expectedValue);
  }

  @FunctionalInterface
  private interface SerializedValueAsserter {

    void assertValue(DataInputStream inputStream, Object expectedValue) throws IOException;
  }
}
