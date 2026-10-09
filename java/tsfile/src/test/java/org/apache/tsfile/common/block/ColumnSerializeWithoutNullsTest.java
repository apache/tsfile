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

package org.apache.tsfile.common.block;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.NullColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class ColumnSerializeWithoutNullsTest {

  private static final boolean[] NULL_INDICATORS = {false, true, false, true};

  @Test
  public void testSerializeWithoutNulls() throws IOException {
    ByteBuffer buffer =
        serialize(
            new BooleanColumn(
                4, Optional.of(NULL_INDICATORS), new boolean[] {true, true, false, true}));
    Assert.assertEquals((byte) 0b1000_0000, buffer.get());
    Assert.assertFalse(buffer.hasRemaining());

    buffer = serialize(new IntColumn(4, Optional.of(NULL_INDICATORS), new int[] {1, 2, 3, 4}));
    Assert.assertEquals(1, buffer.getInt());
    Assert.assertEquals(3, buffer.getInt());
    Assert.assertFalse(buffer.hasRemaining());

    buffer =
        serialize(new LongColumn(4, Optional.of(NULL_INDICATORS), new long[] {1L, 2L, 3L, 4L}));
    Assert.assertEquals(1L, buffer.getLong());
    Assert.assertEquals(3L, buffer.getLong());
    Assert.assertFalse(buffer.hasRemaining());

    buffer =
        serialize(new FloatColumn(4, Optional.of(NULL_INDICATORS), new float[] {1F, 2F, 3F, 4F}));
    Assert.assertEquals(1F, Float.intBitsToFloat(buffer.getInt()), 0);
    Assert.assertEquals(3F, Float.intBitsToFloat(buffer.getInt()), 0);
    Assert.assertFalse(buffer.hasRemaining());

    buffer =
        serialize(new DoubleColumn(4, Optional.of(NULL_INDICATORS), new double[] {1D, 2D, 3D, 4D}));
    Assert.assertEquals(1D, Double.longBitsToDouble(buffer.getLong()), 0);
    Assert.assertEquals(3D, Double.longBitsToDouble(buffer.getLong()), 0);
    Assert.assertFalse(buffer.hasRemaining());

    buffer =
        serialize(
            new BinaryColumn(
                4,
                Optional.of(NULL_INDICATORS),
                new Binary[] {binary("a"), binary("b"), binary("cc"), binary("d")}));
    Assert.assertEquals(binary("a"), readBinary(buffer));
    Assert.assertEquals(binary("cc"), readBinary(buffer));
    Assert.assertFalse(buffer.hasRemaining());

    buffer = serialize(new TimeColumn(4, new long[] {1L, 2L, 3L, 4L}));
    Assert.assertEquals(1L, buffer.getLong());
    Assert.assertEquals(2L, buffer.getLong());
    Assert.assertEquals(3L, buffer.getLong());
    Assert.assertEquals(4L, buffer.getLong());
    Assert.assertFalse(buffer.hasRemaining());

    Assert.assertFalse(serialize(new NullColumn(4)).hasRemaining());
  }

  private static ByteBuffer serialize(Column column) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    column.serializeWithoutNulls(new DataOutputStream(output));
    return ByteBuffer.wrap(output.toByteArray());
  }

  private static Binary binary(String value) {
    return new Binary(value, StandardCharsets.UTF_8);
  }

  private static Binary readBinary(ByteBuffer buffer) {
    byte[] value = new byte[buffer.getInt()];
    buffer.get(value);
    return new Binary(value);
  }
}
