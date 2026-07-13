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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.TsPrimitiveType;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TypeTest {

  private static final int OFFSET = 2;

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
        Type.fromTsDataType(TSDataType.BOOLEAN)
            .calcTypeSize(TSDataType.BOOLEAN, new TsPrimitiveType.TsBoolean(true)));
    Assert.assertEquals(
        Integer.BYTES,
        Type.fromTsDataType(TSDataType.INT32)
            .calcTypeSize(TSDataType.INT32, new TsPrimitiveType.TsInt(1)));
    Assert.assertEquals(
        Integer.BYTES,
        Type.fromTsDataType(TSDataType.DATE)
            .calcTypeSize(TSDataType.DATE, new TsPrimitiveType.TsInt(20260713, TSDataType.DATE)));
    Assert.assertEquals(
        Long.BYTES,
        Type.fromTsDataType(TSDataType.INT64)
            .calcTypeSize(TSDataType.INT64, new TsPrimitiveType.TsLong(1L)));
    Assert.assertEquals(
        Long.BYTES,
        Type.fromTsDataType(TSDataType.TIMESTAMP)
            .calcTypeSize(TSDataType.TIMESTAMP, new TsPrimitiveType.TsLong(1L)));
    Assert.assertEquals(
        Float.BYTES,
        Type.fromTsDataType(TSDataType.FLOAT)
            .calcTypeSize(TSDataType.FLOAT, new TsPrimitiveType.TsFloat(1.0F)));
    Assert.assertEquals(
        Double.BYTES,
        Type.fromTsDataType(TSDataType.DOUBLE)
            .calcTypeSize(TSDataType.DOUBLE, new TsPrimitiveType.TsDouble(1.0D)));

    Binary binary = new Binary("test", StandardCharsets.UTF_8);
    for (TSDataType dataType :
        new TSDataType[] {TSDataType.TEXT, TSDataType.STRING, TSDataType.BLOB, TSDataType.OBJECT}) {
      Assert.assertEquals(
          Integer.BYTES + binary.getLength(),
          Type.fromTsDataType(dataType)
              .calcTypeSize(dataType, new TsPrimitiveType.TsBinary(binary)));
    }
  }
}
