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

package org.apache.tsfile.read.common.type;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TypeTest {

  private static final int OFFSET = 2;
  private static final int VALUE_LENGTH = 10;

  @Test
  public void testToBytes() {
    byte[] valueBytes = new byte[16];

    Type.fromTsDataType(TSDataType.BOOLEAN)
        .toBytes(new TsPrimitiveType.TsBoolean(true), valueBytes, OFFSET);
    Assert.assertTrue(BytesUtils.bytesToBool(valueBytes, OFFSET));

    Type.fromTsDataType(TSDataType.DATE)
        .toBytes(new TsPrimitiveType.TsInt(20260713, TSDataType.DATE), valueBytes, OFFSET);
    Assert.assertEquals(20260713, BytesUtils.bytesToInt(valueBytes, OFFSET));

    Type.fromTsDataType(TSDataType.TIMESTAMP)
        .toBytes(new TsPrimitiveType.TsLong(123456789L), valueBytes, OFFSET);
    Assert.assertEquals(
        123456789L, BytesUtils.bytesToLongFromOffset(valueBytes, Long.BYTES, OFFSET));

    Type.fromTsDataType(TSDataType.FLOAT)
        .toBytes(new TsPrimitiveType.TsFloat(1.25F), valueBytes, OFFSET);
    Assert.assertEquals(1.25F, BytesUtils.bytesToFloat(valueBytes, OFFSET), 0);

    Type.fromTsDataType(TSDataType.DOUBLE)
        .toBytes(new TsPrimitiveType.TsDouble(2.5D), valueBytes, OFFSET);
    Assert.assertEquals(2.5D, BytesUtils.bytesToDouble(valueBytes, OFFSET), 0);

    Binary binary = new Binary("test", StandardCharsets.UTF_8);
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Type.fromTsDataType(dataType)
          .toBytes(new TsPrimitiveType.TsBinary(binary), valueBytes, OFFSET);
      Assert.assertEquals(binary.getLength(), BytesUtils.bytesToInt(valueBytes, OFFSET));
      Assert.assertArrayEquals(
          binary.getValues(),
          Arrays.copyOfRange(
              valueBytes, OFFSET + Integer.BYTES, OFFSET + Integer.BYTES + binary.getLength()));
    }
  }

  @Test
  public void testCalcTypeSize() {
    Assert.assertEquals(
        Byte.BYTES,
        Type.fromTsDataType(TSDataType.BOOLEAN).calcTypeSize(new TsPrimitiveType.TsBoolean(true)));
    Assert.assertEquals(
        Integer.BYTES,
        Type.fromTsDataType(TSDataType.INT32).calcTypeSize(new TsPrimitiveType.TsInt(1)));
    Assert.assertEquals(
        Integer.BYTES,
        Type.fromTsDataType(TSDataType.DATE)
            .calcTypeSize(new TsPrimitiveType.TsInt(20260713, TSDataType.DATE)));
    Assert.assertEquals(
        Long.BYTES,
        Type.fromTsDataType(TSDataType.INT64).calcTypeSize(new TsPrimitiveType.TsLong(1L)));
    Assert.assertEquals(
        Long.BYTES,
        Type.fromTsDataType(TSDataType.TIMESTAMP).calcTypeSize(new TsPrimitiveType.TsLong(1L)));
    Assert.assertEquals(
        Float.BYTES,
        Type.fromTsDataType(TSDataType.FLOAT).calcTypeSize(new TsPrimitiveType.TsFloat(1.0F)));
    Assert.assertEquals(
        Double.BYTES,
        Type.fromTsDataType(TSDataType.DOUBLE).calcTypeSize(new TsPrimitiveType.TsDouble(1.0D)));

    Binary binary = new Binary("test", StandardCharsets.UTF_8);
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Assert.assertEquals(
          Integer.BYTES + binary.getLength(),
          Type.fromTsDataType(dataType).calcTypeSize(new TsPrimitiveType.TsBinary(binary)));
    }
  }

  @Test
  public void testToString() {
    Object[][] testCases = {
      {TSDataType.BOOLEAN, true, "true"},
      {TSDataType.INT32, 1, "1"},
      {TSDataType.DATE, 20260714, "20260714"},
      {TSDataType.INT64, 2L, "2"},
      {TSDataType.TIMESTAMP, 3L, "3"},
      {TSDataType.FLOAT, 1.25F, "1.25"},
      {TSDataType.DOUBLE, 2.5D, "2.5"},
      {TSDataType.TEXT, new Binary("text", StandardCharsets.UTF_8), "text"},
      {TSDataType.STRING, new Binary("string", StandardCharsets.UTF_8), "string"},
      {TSDataType.BLOB, new Binary(new byte[] {0x01, 0x23, (byte) 0xFF}), "0x0123ff"},
      {TSDataType.OBJECT, new Binary(BytesUtils.longToBytes(1L)), "(Object) 1 B"}
    };

    for (Object[] testCase : testCases) {
      TSDataType dataType = (TSDataType) testCase[0];
      Field field = Field.getField(testCase[1], dataType);
      Assert.assertEquals(testCase[2], Type.fromTsDataType(dataType).toString(field));
      Assert.assertEquals(testCase[2], field.getStringValue());
    }

    Assert.assertEquals("null", new Field(null).getStringValue());

    for (TSDataType dataType : new TSDataType[] {TSDataType.VECTOR, TSDataType.UNKNOWN}) {
      try {
        new Field(dataType).getStringValue();
        Assert.fail("Expected UnSupportedDataTypeException");
      } catch (UnSupportedDataTypeException ignored) {
        // Expected.
      }
    }
  }

  @Test
  public void testCalcTypeSizeFromObject() {
    Binary binary = new Binary("test", StandardCharsets.UTF_8);
    Object[][] testCases = {
      {TSDataType.BOOLEAN, true, Byte.BYTES},
      {TSDataType.INT32, 1, Integer.BYTES},
      {TSDataType.DATE, 20260713, Integer.BYTES},
      {TSDataType.INT64, 1L, Long.BYTES},
      {TSDataType.TIMESTAMP, 1L, Long.BYTES},
      {TSDataType.FLOAT, 1.0F, Float.BYTES},
      {TSDataType.DOUBLE, 1.0D, Double.BYTES},
      {TSDataType.TEXT, binary, Integer.BYTES + binary.getLength()},
      {TSDataType.STRING, binary, Integer.BYTES + binary.getLength()},
      {TSDataType.BLOB, binary, Integer.BYTES + binary.getLength()},
      {TSDataType.OBJECT, binary, Integer.BYTES + binary.getLength()}
    };

    for (Object[] testCase : testCases) {
      TSDataType dataType = (TSDataType) testCase[0];
      Object value = testCase[1];
      int expectedSize = (int) testCase[2];
      Assert.assertEquals(expectedSize, Type.fromTsDataType(dataType).calcTypeSize(value));
    }
  }

  @Test
  public void testEstimateArraySize() {
    int size = 10;
    Object[][] testCases = {
      {TSDataType.BOOLEAN, RamUsageEstimator.sizeOfBooleanArray(size)},
      {TSDataType.INT32, RamUsageEstimator.sizeOfIntArray(size)},
      {TSDataType.DATE, RamUsageEstimator.sizeOfObjectArray(size)},
      {TSDataType.INT64, RamUsageEstimator.sizeOfLongArray(size)},
      {TSDataType.TIMESTAMP, RamUsageEstimator.sizeOfLongArray(size)},
      {TSDataType.FLOAT, RamUsageEstimator.sizeOfFloatArray(size)},
      {TSDataType.DOUBLE, RamUsageEstimator.sizeOfDoubleArray(size)},
      {TSDataType.TEXT, RamUsageEstimator.sizeOfObjectArray(size)},
      {TSDataType.STRING, RamUsageEstimator.sizeOfObjectArray(size)},
      {TSDataType.BLOB, RamUsageEstimator.sizeOfObjectArray(size)},
      {TSDataType.OBJECT, RamUsageEstimator.sizeOfObjectArray(size)},
      {TSDataType.VECTOR, RamUsageEstimator.sizeOfLongArray(size)}
    };

    for (Object[] testCase : testCases) {
      Assert.assertEquals(
          testCase[1], Type.fromTsDataType((TSDataType) testCase[0]).estimateArraySize(size));
    }

    try {
      Type.fromTsDataType(TSDataType.UNKNOWN).estimateArraySize(size);
      Assert.fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException ignored) {
      // Expected.
    }
  }

  @Test
  public void testGetOneItemMaxSize() {
    Object[][] testCases = {
      {TSDataType.BOOLEAN, Byte.BYTES},
      {TSDataType.INT32, Integer.BYTES},
      {TSDataType.DATE, Integer.BYTES},
      {TSDataType.INT64, Long.BYTES},
      {TSDataType.TIMESTAMP, Long.BYTES},
      {TSDataType.FLOAT, Float.BYTES},
      {TSDataType.DOUBLE, Double.BYTES},
      {TSDataType.TEXT, Integer.BYTES + TSFileConfig.BYTE_SIZE_PER_CHAR * VALUE_LENGTH},
      {TSDataType.STRING, Integer.BYTES + TSFileConfig.BYTE_SIZE_PER_CHAR * VALUE_LENGTH},
      {TSDataType.BLOB, Integer.BYTES + TSFileConfig.BYTE_SIZE_PER_CHAR * VALUE_LENGTH},
      {TSDataType.OBJECT, Integer.BYTES + TSFileConfig.BYTE_SIZE_PER_CHAR * VALUE_LENGTH}
    };

    for (Object[] testCase : testCases) {
      TSDataType dataType = (TSDataType) testCase[0];
      int expectedSize = (int) testCase[1];
      Assert.assertEquals(
          expectedSize, Type.fromTsDataType(dataType).getOneItemMaxSize(VALUE_LENGTH));
      Assert.assertEquals(
          expectedSize, new PlainEncoder(dataType, VALUE_LENGTH).getOneItemMaxSize());
    }

    for (TSDataType dataType : new TSDataType[] {TSDataType.VECTOR, TSDataType.UNKNOWN}) {
      try {
        new PlainEncoder(dataType, VALUE_LENGTH).getOneItemMaxSize();
        Assert.fail("Expected UnsupportedOperationException");
      } catch (UnsupportedOperationException ignored) {
        // Expected.
      }
    }
  }

  @Test
  public void testUpdateStatistics() {
    Binary binary = new Binary("test", StandardCharsets.UTF_8);
    Object[][] testCases = {
      {TSDataType.BOOLEAN, new TsPrimitiveType.TsBoolean(true)},
      {TSDataType.INT32, new TsPrimitiveType.TsInt(1)},
      {TSDataType.DATE, new TsPrimitiveType.TsInt(20260714, TSDataType.DATE)},
      {TSDataType.INT64, new TsPrimitiveType.TsLong(1L)},
      {TSDataType.TIMESTAMP, new TsPrimitiveType.TsLong(1L)},
      {TSDataType.FLOAT, new TsPrimitiveType.TsFloat(1.0F)},
      {TSDataType.DOUBLE, new TsPrimitiveType.TsDouble(1.0D)},
      {TSDataType.TEXT, new TsPrimitiveType.TsBinary(binary)},
      {TSDataType.STRING, new TsPrimitiveType.TsBinary(binary)},
      {TSDataType.BLOB, new TsPrimitiveType.TsBinary(binary)},
      {TSDataType.OBJECT, new TsPrimitiveType.TsBinary(binary)}
    };

    for (Object[] testCase : testCases) {
      TSDataType dataType = (TSDataType) testCase[0];
      TsPrimitiveType value = (TsPrimitiveType) testCase[1];
      Type type = Type.fromTsDataType(dataType);
      Statistics<?> statistics = Statistics.getStatsByType(dataType);
      type.update(statistics, 100L, value);
      Assert.assertEquals(1, statistics.getCount());
      Assert.assertEquals(100L, statistics.getStartTime());
      Assert.assertEquals(100L, statistics.getEndTime());
      Assert.assertFalse(statistics.isEmpty());

      BatchData batchData = new BatchData(dataType);
      batchData.putAnObject(200L, value.getValue());
      Assert.assertEquals(value.getValue(), type.getCurrentValue(batchData));
      Assert.assertEquals(value.getValue(), batchData.currentValue());
      Statistics<?> batchStatistics = Statistics.getStatsByType(dataType);
      type.update(batchStatistics, batchData);
      Assert.assertEquals(1, batchStatistics.getCount());
      Assert.assertEquals(200L, batchStatistics.getStartTime());
      Assert.assertEquals(200L, batchStatistics.getEndTime());
      Assert.assertFalse(batchStatistics.isEmpty());
    }
  }

  @Test
  public void testGetCurrentValueForVectorAndUnknown() {
    TsPrimitiveType[] vector = {new TsPrimitiveType.TsLong(1L)};
    BatchData batchData = new BatchData(TSDataType.VECTOR);
    batchData.putAnObject(1L, vector);
    Assert.assertSame(vector, Type.fromTsDataType(TSDataType.VECTOR).getCurrentValue(batchData));
    Assert.assertSame(vector, batchData.currentValue());
    Assert.assertNull(Type.fromTsDataType(TSDataType.UNKNOWN).getCurrentValue(null));
  }
}
