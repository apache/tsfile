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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
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
  public void testGetDataPoint() {
    Object[][] testCases = {
      {TSDataType.BOOLEAN, "true", true},
      {TSDataType.INT32, "1", 1},
      {TSDataType.DATE, "2026-07-15", 20260715},
      {TSDataType.INT64, "2", 2L},
      {TSDataType.TIMESTAMP, "3", 3L},
      {TSDataType.FLOAT, "1.25", 1.25F},
      {TSDataType.DOUBLE, "2.5", 2.5D},
      {TSDataType.TEXT, "text", new Binary("text", TSFileConfig.STRING_CHARSET)},
      {TSDataType.STRING, "string", new Binary("string", TSFileConfig.STRING_CHARSET)},
      {TSDataType.BLOB, "blob", new Binary("blob", TSFileConfig.STRING_CHARSET)},
      {TSDataType.OBJECT, "object", new Binary("object", TSFileConfig.STRING_CHARSET)}
    };

    for (Object[] testCase : testCases) {
      TSDataType dataType = (TSDataType) testCase[0];
      String value = (String) testCase[1];
      Object expectedValue = testCase[2];

      DataPoint typeDataPoint = Type.fromTsDataType(dataType).getDataPoint("s", value);
      Assert.assertEquals("s", typeDataPoint.getMeasurementId());
      Assert.assertEquals(expectedValue, typeDataPoint.getValue());

      DataPoint factoryDataPoint = DataPoint.getDataPoint(dataType, "s", value);
      Assert.assertEquals(typeDataPoint.getClass(), factoryDataPoint.getClass());
      Assert.assertEquals(expectedValue, factoryDataPoint.getValue());
    }

    try {
      DataPoint.getDataPoint(TSDataType.INT32, "s", "invalid");
      Assert.fail("Expected UnSupportedDataTypeException");
    } catch (UnSupportedDataTypeException ignored) {
      // Expected.
    }

    try {
      DataPoint.getDataPoint(TSDataType.VECTOR, "s", "1");
      Assert.fail("Expected UnSupportedDataTypeException");
    } catch (UnSupportedDataTypeException ignored) {
      // Expected.
    }
  }

  @Test
  public void testWriteValueChunk() {
    ValueChunkWriter writer = Mockito.mock(ValueChunkWriter.class);

    Type.fromTsDataType(TSDataType.BOOLEAN).write(writer, 1L, true, false);
    Mockito.verify(writer).write(1L, true, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.INT32).write(writer, 2L, 1, false);
    Mockito.verify(writer).write(2L, 1, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.DATE).write(writer, 3L, null, true);
    Mockito.verify(writer).write(3L, 0, true);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.TIMESTAMP).write(writer, 4L, 2L, false);
    Mockito.verify(writer).write(4L, 2L, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.FLOAT).write(writer, 5L, 1.25F, false);
    Mockito.verify(writer).write(5L, 1.25F, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.DOUBLE).write(writer, 6L, 2.5D, false);
    Mockito.verify(writer).write(6L, 2.5D, false);
    Mockito.reset(writer);

    Binary binary = new Binary("value", TSFileConfig.STRING_CHARSET);
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Type.fromTsDataType(dataType).write(writer, 7L, binary, false);
      Mockito.verify(writer).write(7L, binary, false);
      Mockito.reset(writer);
    }

    try {
      Type.fromTsDataType(TSDataType.VECTOR).write(writer, 8L, null, true);
      Assert.fail("Expected UnSupportedDataTypeException");
    } catch (UnSupportedDataTypeException ignored) {
      // Expected.
    }
  }

  @Test
  public void testWriteTsPrimitiveValueChunk() {
    ValueChunkWriter writer = Mockito.mock(ValueChunkWriter.class);

    Type.fromTsDataType(TSDataType.BOOLEAN).write(writer, 1L, new TsPrimitiveType.TsBoolean(true));
    Mockito.verify(writer).write(1L, true, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.INT32).write(writer, 2L, new TsPrimitiveType.TsInt(1));
    Mockito.verify(writer).write(2L, 1, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.TIMESTAMP).write(writer, 3L, new TsPrimitiveType.TsLong(2L));
    Mockito.verify(writer).write(3L, 2L, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.FLOAT).write(writer, 4L, new TsPrimitiveType.TsFloat(1.25F));
    Mockito.verify(writer).write(4L, 1.25F, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.DOUBLE).write(writer, 5L, new TsPrimitiveType.TsDouble(2.5D));
    Mockito.verify(writer).write(5L, 2.5D, false);
    Mockito.reset(writer);

    Binary binary = new Binary("value", TSFileConfig.STRING_CHARSET);
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Type.fromTsDataType(dataType).write(writer, 6L, new TsPrimitiveType.TsBinary(binary));
      Mockito.verify(writer).write(6L, binary, false);
      Mockito.reset(writer);
    }

    Type.fromTsDataType(TSDataType.INT32).write(writer, 7L, (TsPrimitiveType) null);
    Mockito.verify(writer).write(7L, Integer.MAX_VALUE, true);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.TIMESTAMP).write(writer, 8L, (TsPrimitiveType) null);
    Mockito.verify(writer).write(8L, Long.MAX_VALUE, true);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.FLOAT).write(writer, 9L, (TsPrimitiveType) null);
    Mockito.verify(writer).write(9L, Float.MAX_VALUE, true);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.DOUBLE).write(writer, 10L, (TsPrimitiveType) null);
    Mockito.verify(writer).write(10L, Double.MAX_VALUE, true);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.BOOLEAN).write(writer, 11L, (TsPrimitiveType) null);
    Mockito.verify(writer).write(11L, false, true);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.TEXT).write(writer, 12L, (TsPrimitiveType) null);
    Mockito.verify(writer).write(12L, Binary.EMPTY_VALUE, true);
    Mockito.reset(writer);

    try {
      Type.fromTsDataType(TSDataType.VECTOR).write(writer, 13L, (TsPrimitiveType) null);
      Assert.fail("Expected UnSupportedDataTypeException");
    } catch (UnSupportedDataTypeException ignored) {
      // Expected.
    }
  }

  @Test
  public void testWriteTabletValueChunk() {
    ValueChunkWriter writer = Mockito.mock(ValueChunkWriter.class);

    Type.fromTsDataType(TSDataType.BOOLEAN)
        .write(writer, 1L, new boolean[] {false, true}, 1, false);
    Mockito.verify(writer).write(1L, true, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.INT32).write(writer, 2L, new int[] {0, 1}, 1, false);
    Mockito.verify(writer).write(2L, 1, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.DATE)
        .write(
            writer,
            3L,
            new LocalDate[] {LocalDate.of(2026, 7, 14), LocalDate.of(2026, 7, 15)},
            1,
            false);
    Mockito.verify(writer).write(3L, 20260715, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.TIMESTAMP).write(writer, 4L, new long[] {0L, 2L}, 1, false);
    Mockito.verify(writer).write(4L, 2L, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.FLOAT).write(writer, 5L, new float[] {0F, 1.25F}, 1, false);
    Mockito.verify(writer).write(5L, 1.25F, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.DOUBLE).write(writer, 6L, new double[] {0D, 2.5D}, 1, false);
    Mockito.verify(writer).write(6L, 2.5D, false);
    Mockito.reset(writer);

    Binary binary = new Binary("value", TSFileConfig.STRING_CHARSET);
    Binary[] binaries = {Binary.EMPTY_VALUE, binary};
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Type.fromTsDataType(dataType).write(writer, 7L, binaries, 1, false);
      Mockito.verify(writer).write(7L, binary, false);
      Mockito.reset(writer);
    }

    try {
      Type.fromTsDataType(TSDataType.VECTOR).write(writer, 8L, new long[] {1L}, 0, false);
      Assert.fail("Expected UnSupportedDataTypeException");
    } catch (UnSupportedDataTypeException ignored) {
      // Expected.
    }
  }

  @Test
  public void testWriteTsBlockValueChunk() {
    ValueChunkWriter writer = Mockito.mock(ValueChunkWriter.class);
    Column column = Mockito.mock(Column.class);
    Binary binary = new Binary("value", TSFileConfig.STRING_CHARSET);
    Mockito.when(column.getBoolean(1)).thenReturn(true);
    Mockito.when(column.getInt(1)).thenReturn(1);
    Mockito.when(column.getLong(1)).thenReturn(2L);
    Mockito.when(column.getFloat(1)).thenReturn(1.25F);
    Mockito.when(column.getDouble(1)).thenReturn(2.5D);
    Mockito.when(column.getBinary(1)).thenReturn(binary);

    Type.fromTsDataType(TSDataType.BOOLEAN).write(writer, 1L, column, 1, false);
    Mockito.verify(writer).write(1L, true, false);
    Mockito.reset(writer);

    for (TSDataType dataType : new TSDataType[] {TSDataType.INT32, TSDataType.DATE}) {
      Type.fromTsDataType(dataType).write(writer, 2L, column, 1, false);
      Mockito.verify(writer).write(2L, 1, false);
      Mockito.reset(writer);
    }

    for (TSDataType dataType : new TSDataType[] {TSDataType.INT64, TSDataType.TIMESTAMP}) {
      Type.fromTsDataType(dataType).write(writer, 3L, column, 1, false);
      Mockito.verify(writer).write(3L, 2L, false);
      Mockito.reset(writer);
    }

    Type.fromTsDataType(TSDataType.FLOAT).write(writer, 4L, column, 1, false);
    Mockito.verify(writer).write(4L, 1.25F, false);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.DOUBLE).write(writer, 5L, column, 1, false);
    Mockito.verify(writer).write(5L, 2.5D, false);
    Mockito.reset(writer);

    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB}) {
      Type.fromTsDataType(dataType).write(writer, 6L, column, 1, false);
      Mockito.verify(writer).write(6L, binary, false);
      Mockito.reset(writer);
    }

    Type.fromTsDataType(TSDataType.INT32).write(writer, 7L, column, 1, true);
    Mockito.verify(writer).write(7L, 0, true);
    Mockito.reset(writer);

    for (TSDataType dataType : new TSDataType[] {TSDataType.OBJECT, TSDataType.VECTOR}) {
      try {
        Type.fromTsDataType(dataType).write(writer, 8L, column, 1, false);
        Assert.fail("Expected UnSupportedDataTypeException");
      } catch (UnSupportedDataTypeException ignored) {
        // Expected.
      }
    }
  }

  @Test
  public void testWriteNonAlignedTabletValueChunk() {
    ChunkWriterImpl writer = Mockito.mock(ChunkWriterImpl.class);

    Type.fromTsDataType(TSDataType.BOOLEAN).write(writer, 1L, new boolean[] {false, true}, 1);
    Mockito.verify(writer).write(1L, true);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.INT32).write(writer, 2L, new int[] {0, 1}, 1);
    Mockito.verify(writer).write(2L, 1);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.DATE)
        .write(
            writer, 3L, new LocalDate[] {LocalDate.of(2026, 7, 14), LocalDate.of(2026, 7, 15)}, 1);
    Mockito.verify(writer).write(3L, 20260715);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.TIMESTAMP).write(writer, 4L, new long[] {0L, 2L}, 1);
    Mockito.verify(writer).write(4L, 2L);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.FLOAT).write(writer, 5L, new float[] {0F, 1.25F}, 1);
    Mockito.verify(writer).write(5L, 1.25F);
    Mockito.reset(writer);

    Type.fromTsDataType(TSDataType.DOUBLE).write(writer, 6L, new double[] {0D, 2.5D}, 1);
    Mockito.verify(writer).write(6L, 2.5D);
    Mockito.reset(writer);

    Binary binary = new Binary("value", TSFileConfig.STRING_CHARSET);
    Binary[] binaries = {Binary.EMPTY_VALUE, binary};
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Type.fromTsDataType(dataType).write(writer, 7L, binaries, 1);
      Mockito.verify(writer).write(7L, binary);
      Mockito.reset(writer);
    }

    try {
      Type.fromTsDataType(TSDataType.VECTOR).write(writer, 8L, new long[] {1L}, 0);
      Assert.fail("Expected UnSupportedDataTypeException");
    } catch (UnSupportedDataTypeException ignored) {
      // Expected.
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
  public void testGetValueFromField() {
    Object[][] testCases = {
      {TSDataType.BOOLEAN, true, true},
      {TSDataType.INT32, 1, 1},
      {TSDataType.DATE, 20260714, LocalDate.of(2026, 7, 14)},
      {TSDataType.INT64, 2L, 2L},
      {TSDataType.TIMESTAMP, 3L, 3L},
      {TSDataType.FLOAT, 1.25F, 1.25F},
      {TSDataType.DOUBLE, 2.5D, 2.5D},
      {
        TSDataType.TEXT,
        new Binary("text", StandardCharsets.UTF_8),
        new Binary("text", StandardCharsets.UTF_8)
      },
      {
        TSDataType.STRING,
        new Binary("string", StandardCharsets.UTF_8),
        new Binary("string", StandardCharsets.UTF_8)
      },
      {TSDataType.BLOB, new Binary(new byte[] {0x01, 0x23}), new Binary(new byte[] {0x01, 0x23})},
      {TSDataType.OBJECT, new Binary(BytesUtils.longToBytes(1L)), "(Object) 1 B"}
    };

    for (Object[] testCase : testCases) {
      TSDataType dataType = (TSDataType) testCase[0];
      Type type = Type.fromTsDataType(dataType);
      Field typeField = type.getField(testCase[1]);
      Assert.assertEquals(dataType, typeField.getDataType());
      Assert.assertEquals(testCase[2], type.getValue(typeField));

      Field field = Field.getField(testCase[1], dataType);
      Assert.assertEquals(dataType, field.getDataType());
      Assert.assertEquals(testCase[2], type.getValue(field));
      Assert.assertEquals(testCase[2], field.getObjectValue(dataType));
    }

    Assert.assertNull(Field.getField(null, TSDataType.INT32));
    Assert.assertNull(new Field(null).getObjectValue(TSDataType.INT32));
    Assert.assertEquals("1", Field.getField(1, TSDataType.INT32).getObjectValue(TSDataType.OBJECT));

    for (TSDataType dataType : new TSDataType[] {TSDataType.VECTOR, TSDataType.UNKNOWN}) {
      try {
        new Field(dataType).getObjectValue(dataType);
        Assert.fail("Expected UnSupportedDataTypeException");
      } catch (UnSupportedDataTypeException ignored) {
        // Expected.
      }

      try {
        Type.fromTsDataType(dataType).getField(new Object());
        Assert.fail("Expected UnSupportedDataTypeException");
      } catch (UnSupportedDataTypeException ignored) {
        // Expected.
      }
    }
  }

  @Test
  public void testSetToField() {
    Object[][] testCases = {
      {TSDataType.BOOLEAN, new TsPrimitiveType.TsBoolean(true), true},
      {TSDataType.INT32, new TsPrimitiveType.TsInt(1), 1},
      {
        TSDataType.DATE,
        new TsPrimitiveType.TsInt(20260714, TSDataType.DATE),
        LocalDate.of(2026, 7, 14)
      },
      {TSDataType.INT64, new TsPrimitiveType.TsLong(2L), 2L},
      {TSDataType.TIMESTAMP, new TsPrimitiveType.TsLong(3L), 3L},
      {TSDataType.FLOAT, new TsPrimitiveType.TsFloat(1.25F), 1.25F},
      {TSDataType.DOUBLE, new TsPrimitiveType.TsDouble(2.5D), 2.5D},
      {
        TSDataType.TEXT,
        new TsPrimitiveType.TsBinary(new Binary("text", StandardCharsets.UTF_8)),
        new Binary("text", StandardCharsets.UTF_8)
      },
      {
        TSDataType.STRING,
        new TsPrimitiveType.TsBinary(new Binary("string", StandardCharsets.UTF_8)),
        new Binary("string", StandardCharsets.UTF_8)
      },
      {
        TSDataType.BLOB,
        new TsPrimitiveType.TsBinary(new Binary(new byte[] {0x01, 0x23})),
        new Binary(new byte[] {0x01, 0x23})
      },
      {
        TSDataType.OBJECT,
        new TsPrimitiveType.TsBinary(new Binary(BytesUtils.longToBytes(1L))),
        "(Object) 1 B"
      }
    };

    for (Object[] testCase : testCases) {
      TSDataType dataType = (TSDataType) testCase[0];
      TsPrimitiveType from = (TsPrimitiveType) testCase[1];
      Type type = Type.fromTsDataType(dataType);

      Field directTarget = new Field(dataType);
      type.setTo(from, directTarget);
      Assert.assertEquals(testCase[2], type.getValue(directTarget));

      if (from.getDataType() == dataType) {
        Field factoryTarget = new Field(dataType);
        Field.setTsPrimitiveValue(from, factoryTarget);
        Assert.assertEquals(testCase[2], type.getValue(factoryTarget));
      }
    }

    TsPrimitiveType vector = new TsPrimitiveType.TsVector(new TsPrimitiveType[0]);
    for (TSDataType dataType : new TSDataType[] {TSDataType.VECTOR, TSDataType.UNKNOWN}) {
      try {
        Type.fromTsDataType(dataType).setTo(vector, new Field(dataType));
        Assert.fail("Expected UnSupportedDataTypeException");
      } catch (UnSupportedDataTypeException ignored) {
        // Expected.
      }
    }

    try {
      Field.setTsPrimitiveValue(vector, new Field(TSDataType.VECTOR));
      Assert.fail("Expected UnSupportedDataTypeException");
    } catch (UnSupportedDataTypeException ignored) {
      // Expected.
    }
  }

  @Test
  public void testAddPoint() {
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    IDeviceID deviceID = Mockito.mock(IDeviceID.class);
    Mockito.when(resultSet.getBoolean("value")).thenReturn(true);
    Mockito.when(resultSet.getInt("value")).thenReturn(1);
    Mockito.when(resultSet.getLong("value")).thenReturn(2L);
    Mockito.when(resultSet.getFloat("value")).thenReturn(1.25F);
    Mockito.when(resultSet.getDouble("value")).thenReturn(2.5D);
    Mockito.when(resultSet.getString("value")).thenReturn("text");
    Mockito.when(resultSet.getBinary("value")).thenReturn(new byte[] {0x01, 0x23});

    Object[][] testCases = {
      {TSDataType.BOOLEAN, true},
      {TSDataType.INT32, 1},
      {TSDataType.DATE, 1},
      {TSDataType.INT64, 2L},
      {TSDataType.TIMESTAMP, 2L},
      {TSDataType.FLOAT, 1.25F},
      {TSDataType.DOUBLE, 2.5D},
      {TSDataType.TEXT, new Binary("text", StandardCharsets.UTF_8)},
      {TSDataType.STRING, new Binary("text", StandardCharsets.UTF_8)},
      {TSDataType.BLOB, new Binary(new byte[] {0x01, 0x23})},
      {TSDataType.OBJECT, new Binary(new byte[] {0x01, 0x23})}
    };

    for (Object[] testCase : testCases) {
      TSRecord record = new TSRecord(deviceID, 1L);
      Type.fromTsDataType((TSDataType) testCase[0]).addPoint(record, "value", resultSet);
      Assert.assertEquals(1, record.dataPointList.size());
      Assert.assertEquals(testCase[1], record.dataPointList.get(0).getValue());
    }

    for (TSDataType dataType : new TSDataType[] {TSDataType.VECTOR, TSDataType.UNKNOWN}) {
      TSRecord record = new TSRecord(deviceID, 1L);
      try {
        Type.fromTsDataType(dataType).addPoint(record, "value", resultSet);
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
  public void testDeserializeColumn() {
    boolean[] nullIndicators = {true, false};

    ByteBuffer booleanBuffer = ByteBuffer.wrap(new byte[] {(byte) 0b1000_0000, 0x01});
    Column booleanColumn =
        (Column)
            Type.fromTsDataType(TSDataType.BOOLEAN)
                .deserializeColumn(booleanBuffer, 2, nullIndicators);
    Assert.assertTrue(booleanColumn.isNull(0));
    Assert.assertTrue(booleanColumn.getBoolean(1));
    Assert.assertEquals(1, booleanBuffer.position());

    for (TSDataType dataType : new TSDataType[] {TSDataType.INT32, TSDataType.DATE}) {
      Column column =
          (Column)
              Type.fromTsDataType(dataType)
                  .deserializeColumn(intBuffer(20260714), 2, nullIndicators);
      Assert.assertEquals(dataType, column.getDataType());
      Assert.assertTrue(column.isNull(0));
      Assert.assertEquals(20260714, column.getInt(1));
    }

    for (TSDataType dataType : new TSDataType[] {TSDataType.INT64, TSDataType.TIMESTAMP}) {
      Column column =
          (Column)
              Type.fromTsDataType(dataType)
                  .deserializeColumn(longBuffer(123456789L), 2, nullIndicators);
      Assert.assertTrue(column.isNull(0));
      Assert.assertEquals(123456789L, column.getLong(1));
    }

    Column floatColumn =
        (Column)
            Type.fromTsDataType(TSDataType.FLOAT)
                .deserializeColumn(intBuffer(Float.floatToIntBits(1.25F)), 2, nullIndicators);
    Assert.assertTrue(floatColumn.isNull(0));
    Assert.assertEquals(1.25F, floatColumn.getFloat(1), 0);

    Column doubleColumn =
        (Column)
            Type.fromTsDataType(TSDataType.DOUBLE)
                .deserializeColumn(longBuffer(Double.doubleToLongBits(2.5D)), 2, nullIndicators);
    Assert.assertTrue(doubleColumn.isNull(0));
    Assert.assertEquals(2.5D, doubleColumn.getDouble(1), 0);

    Binary binary = new Binary("test", StandardCharsets.UTF_8);
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Column column =
          (Column)
              Type.fromTsDataType(dataType)
                  .deserializeColumn(binaryBuffer(binary), 2, nullIndicators);
      Assert.assertTrue(column.isNull(0));
      Assert.assertEquals(binary, column.getBinary(1));
    }

    for (TSDataType dataType : new TSDataType[] {TSDataType.VECTOR, TSDataType.UNKNOWN}) {
      try {
        Type.fromTsDataType(dataType).deserializeColumn(ByteBuffer.allocate(0), 0, null);
        Assert.fail("Expected UnsupportedOperationException");
      } catch (UnsupportedOperationException ignored) {
        // Expected.
      }
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
  public void testEstimateValueSize() {
    Object[][] testCases = {
      {TSDataType.BOOLEAN, (long) Byte.BYTES},
      {TSDataType.INT32, (long) Integer.BYTES},
      {TSDataType.DATE, (long) Integer.BYTES},
      {TSDataType.INT64, (long) Long.BYTES},
      {TSDataType.TIMESTAMP, (long) Long.BYTES},
      {TSDataType.FLOAT, (long) Float.BYTES},
      {TSDataType.DOUBLE, (long) Double.BYTES},
      {TSDataType.TEXT, (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF},
      {TSDataType.STRING, (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF},
      {TSDataType.BLOB, (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF},
      {TSDataType.OBJECT, (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF},
      {TSDataType.VECTOR, (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF}
    };

    for (Object[] testCase : testCases) {
      Assert.assertEquals(
          testCase[1], Type.fromTsDataType((TSDataType) testCase[0]).estimateValueSize());
    }

    try {
      Type.fromTsDataType(TSDataType.UNKNOWN).estimateValueSize();
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

  private static ByteBuffer intBuffer(int value) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(value);
    return buffer.flip();
  }

  private static ByteBuffer longBuffer(long value) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(value);
    return buffer.flip();
  }

  private static ByteBuffer binaryBuffer(Binary value) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + value.getLength());
    buffer.putInt(value.getLength());
    buffer.put(value.getValues());
    return buffer.flip();
  }
}
