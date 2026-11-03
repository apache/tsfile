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

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.encoder.DoubleSprintzEncoder;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.FloatSprintzEncoder;
import org.apache.tsfile.encoding.encoder.IntSprintzEncoder;
import org.apache.tsfile.encoding.encoder.LongSprintzEncoder;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SprintzDecoderTest {
  private static final double DELTA = 0;
  private static final int FLOAT_MAX_POINT_VALUE = 10000;
  private static final long DOUBLE_MAX_POINT_VALUE = 1000000000000000L;

  private static final List<Float> floatList = new ArrayList<>();
  private static final List<Double> doubleList = new ArrayList<>();
  private static final List<Integer> intList = new ArrayList<>();
  private static final List<Long> longList = new ArrayList<>();

  private static final List<Integer> iterations = new ArrayList<>();

  // Byte-for-byte identical to the C++ encoder's output for the regression sequence.
  private static final String[] CPP_SPRINTZ_GOLDEN = {
    "0100ff882320030800000000800000007fffffffffffffff00000001c00000003fffffff0000002a810401030100",
    "010000000000000000ff8843400308000000000000000080000000000000007fffffffffffffffffffffffffffffff0000000000000001c0000000000000003fffffffffffffff000000000000002a810401030100",
    "1f00000000fdfffffe03fffffc03fffff807fffff007ffffe00fffffc01fffff803fffff8800000000c00e0fffbfffffd000000017fbfffff5000000015fc00000d0000000160200000081422a0000c4cf7a80",
    "3f0000000000000000ffbffffffffffffe007ffffffffffffc007ffffffffffff800fffffffffffff000ffffffffffffe001ffffffffffffc003ffffffffffff8007ffffffffffff880000000000000000c00381ffff7ffffffffffffd00000000000000017ff7ffffffffffff500000000000000015ff8000000000000d0000000000000001600400000000000000814045400000000000c23ffbd4",
  };

  private static String toHex(byte[] bytes) {
    StringBuilder hex = new StringBuilder();
    for (byte value : bytes) {
      hex.append(Character.forDigit((value >>> 4) & 15, 16));
      hex.append(Character.forDigit(value & 15, 16));
    }
    return hex.toString();
  }

  @Test
  public void testIntFullBlocksAndExtremeTransitions() throws IOException {
    String method = "fire";
    int[] edgeValues = {
      0,
      Integer.MIN_VALUE,
      Integer.MAX_VALUE,
      -1,
      1,
      Integer.MIN_VALUE / 2,
      Integer.MAX_VALUE / 2,
      42,
      0
    };
    for (int count : new int[] {1, 8, 9, 10, 16, 18, 32, 143, 144, 145, 4096}) {
      IntSprintzEncoder encoder = new IntSprintzEncoder();
      for (int round = 0; round < 2; round++) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < count; i++) {
          encoder.encode(i < 9 ? (int) i : edgeValues[i % edgeValues.length], out);
        }
        encoder.flush(out);
        if (count == 18) {
          assertEquals(CPP_SPRINTZ_GOLDEN[0], toHex(out.toByteArray()));
        }
        ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
        IntSprintzDecoder decoder = new IntSprintzDecoder();
        for (int i = 0; i < count; i++) {
          assertTrue(method + ":" + count + ":" + i, decoder.hasNext(buffer));
          int actual = decoder.readInt(buffer);
          int expected = i < 9 ? (int) i : edgeValues[i % edgeValues.length];
          assertEquals(expected, actual);
        }
        assertFalse(decoder.hasNext(buffer));
      }
    }
  }

  @Test
  public void testLongFullBlocksAndExtremeTransitions() throws IOException {
    String method = "fire";
    long[] edgeValues = {
      0, Long.MIN_VALUE, Long.MAX_VALUE, -1, 1, Long.MIN_VALUE / 2, Long.MAX_VALUE / 2, 42, 0
    };
    for (int count : new int[] {1, 8, 9, 10, 16, 18, 32, 143, 144, 145, 4096}) {
      LongSprintzEncoder encoder = new LongSprintzEncoder();
      for (int round = 0; round < 2; round++) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < count; i++) {
          encoder.encode(i < 9 ? (long) i : edgeValues[i % edgeValues.length], out);
        }
        encoder.flush(out);
        if (count == 18) {
          assertEquals(CPP_SPRINTZ_GOLDEN[1], toHex(out.toByteArray()));
        }
        ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
        LongSprintzDecoder decoder = new LongSprintzDecoder();
        for (int i = 0; i < count; i++) {
          assertTrue(method + ":" + count + ":" + i, decoder.hasNext(buffer));
          long actual = decoder.readLong(buffer);
          long expected = i < 9 ? (long) i : edgeValues[i % edgeValues.length];
          assertEquals(expected, actual);
        }
        assertFalse(decoder.hasNext(buffer));
      }
    }
  }

  @Test
  public void testFloatFullBlocksAndExtremeTransitions() throws IOException {
    String method = "fire";
    float[] edgeValues = {
      0.0f,
      -0.0f,
      Float.MAX_VALUE,
      -Float.MAX_VALUE,
      Float.MIN_VALUE,
      -Float.MIN_VALUE,
      1,
      -1,
      42.5f
    };
    for (int count : new int[] {1, 8, 9, 10, 16, 18, 32, 143, 144, 145, 4096}) {
      FloatSprintzEncoder encoder = new FloatSprintzEncoder();
      for (int round = 0; round < 2; round++) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < count; i++) {
          encoder.encode(i < 9 ? (float) i : edgeValues[i % edgeValues.length], out);
        }
        encoder.flush(out);
        if (count == 18) {
          assertEquals(CPP_SPRINTZ_GOLDEN[2], toHex(out.toByteArray()));
        }
        ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
        FloatSprintzDecoder decoder = new FloatSprintzDecoder();
        for (int i = 0; i < count; i++) {
          assertTrue(method + ":" + count + ":" + i, decoder.hasNext(buffer));
          float actual = decoder.readFloat(buffer);
          float expected = i < 9 ? (float) i : edgeValues[i % edgeValues.length];
          assertEquals(Float.floatToIntBits(expected), Float.floatToIntBits(actual));
        }
        assertFalse(decoder.hasNext(buffer));
      }
    }
  }

  @Test
  public void testDoubleFullBlocksAndExtremeTransitions() throws IOException {
    String method = "fire";
    double[] edgeValues = {
      0.0,
      -0.0,
      Double.MAX_VALUE,
      -Double.MAX_VALUE,
      Double.MIN_VALUE,
      -Double.MIN_VALUE,
      1,
      -1,
      42.5
    };
    for (int count : new int[] {1, 8, 9, 10, 16, 18, 32, 143, 144, 145, 4096}) {
      DoubleSprintzEncoder encoder = new DoubleSprintzEncoder();
      for (int round = 0; round < 2; round++) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < count; i++) {
          encoder.encode(i < 9 ? (double) i : edgeValues[i % edgeValues.length], out);
        }
        encoder.flush(out);
        if (count == 18) {
          assertEquals(CPP_SPRINTZ_GOLDEN[3], toHex(out.toByteArray()));
        }
        ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
        DoubleSprintzDecoder decoder = new DoubleSprintzDecoder();
        for (int i = 0; i < count; i++) {
          assertTrue(method + ":" + count + ":" + i, decoder.hasNext(buffer));
          double actual = decoder.readDouble(buffer);
          double expected = i < 9 ? (double) i : edgeValues[i % edgeValues.length];
          assertEquals(Double.doubleToLongBits(expected), Double.doubleToLongBits(actual));
        }
        assertFalse(decoder.hasNext(buffer));
      }
    }
  }

  @BeforeClass
  public static void setUp() {
    int hybridCount = 11;
    int hybridNum = 50;
    int hybridStart = 2000;
    for (int i = 0; i < hybridNum; i++) {
      for (int j = 0; j < hybridCount; j++) {
        floatList.add((float) hybridStart / FLOAT_MAX_POINT_VALUE);
        doubleList.add((double) hybridStart / DOUBLE_MAX_POINT_VALUE);
        intList.add(hybridStart);
        longList.add((long) hybridStart);
      }
      for (int j = 0; j < hybridCount; j++) {
        floatList.add((float) hybridStart / FLOAT_MAX_POINT_VALUE);
        doubleList.add((double) hybridStart / DOUBLE_MAX_POINT_VALUE);
        intList.add(hybridStart);
        longList.add((long) hybridStart);
        hybridStart += 3;
      }
      hybridCount += 2;
    }

    iterations.add(1);
    iterations.add(3);
    iterations.add(8);
    iterations.add(16);
    iterations.add(1000);
    iterations.add(10000);
  }

  @Test
  public void testIntSingleValue() throws IOException {
    Encoder encoder = new IntSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    encoder.encode(777, baos);
    encoder.flush(baos);

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

    Decoder decoder = new IntSprintzDecoder();
    if (decoder.hasNext(buffer)) {
      assertEquals(777, decoder.readInt(buffer));
    }
    if (decoder.hasNext(buffer)) {
      fail();
    }
  }

  @Test
  public void testLongSingleValue() throws IOException {
    Encoder encoder = new LongSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    encoder.encode((long) Integer.MAX_VALUE + 10, baos);
    encoder.flush(baos);

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

    Decoder decoder = new LongSprintzDecoder();
    if (decoder.hasNext(buffer)) {
      assertEquals((long) Integer.MAX_VALUE + 10, decoder.readLong(buffer));
    }
    if (decoder.hasNext(buffer)) {
      fail();
    }
  }

  @Test
  public void testIntZeroNumber() throws IOException {
    Encoder encoder = new IntSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int value = 0;
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    for (int i = 0; i < 2; i++) {
      Decoder decoder = new IntSprintzDecoder();
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readInt(buffer), DELTA);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readInt(buffer), DELTA);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readInt(buffer), DELTA);
      }
    }
  }

  @Test
  public void testLongZeroNumber() throws IOException {
    Encoder encoder = new LongSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    long value = 0;
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    for (int i = 0; i < 2; i++) {
      Decoder decoder = new LongSprintzDecoder();
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readLong(buffer), DELTA);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readLong(buffer), DELTA);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readLong(buffer), DELTA);
      }
    }
  }

  @Test
  public void testInteger() throws IOException {
    for (Integer num : iterations) {
      Encoder encoder = new IntSprintzEncoder();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      int value = 7;
      for (int i = 0; i < num; i++) {
        encoder.encode(value + 2 * i, baos);
      }
      encoder.flush(baos);

      ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

      Decoder decoder = new IntSprintzDecoder();
      for (int i = 0; i < num; i++) {
        if (decoder.hasNext(buffer)) {
          assertEquals(value + 2 * i, decoder.readInt(buffer));
          continue;
        }
        fail();
      }
      if (decoder.hasNext(buffer)) {
        fail();
      }
    }
  }

  @Test
  public void testLong() throws IOException {
    for (Integer num : iterations) {
      Encoder encoder = new LongSprintzEncoder();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      long value = 7;
      for (int i = 0; i < num; i++) {
        encoder.encode(value + 2 * i, baos);
      }
      encoder.flush(baos);

      ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

      Decoder decoder = new LongSprintzDecoder();
      for (int i = 0; i < num; i++) {
        if (decoder.hasNext(buffer)) {
          assertEquals(value + 2 * i, decoder.readLong(buffer));
          continue;
        }
        fail();
      }
      if (decoder.hasNext(buffer)) {
        fail();
      }
    }
  }

  @Test
  public void testIntegerRepeat() throws Exception {
    for (int i = 1; i <= 10; i++) {
      testInteger(i);
    }
  }

  private void testInteger(int repeatCount) throws Exception {
    Encoder encoder = new IntSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (int value : SprintzDecoderTest.intList) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

    for (int i = 0; i < repeatCount; i++) {
      Decoder decoder = new IntSprintzDecoder();
      for (int expected : SprintzDecoderTest.intList) {
        if (decoder.hasNext(buffer)) {
          int actual = decoder.readInt(buffer);
          assertEquals(expected, actual);
          continue;
        }
        fail();
      }
    }

    encoder = new IntSprintzEncoder();
    baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (int value : SprintzDecoderTest.intList) {
        encoder.encode(-value, baos);
      }
      encoder.flush(baos);
    }

    buffer = ByteBuffer.wrap(baos.toByteArray());

    for (int i = 0; i < repeatCount; i++) {
      Decoder decoder = new IntSprintzDecoder();
      for (int expected : SprintzDecoderTest.intList) {
        if (decoder.hasNext(buffer)) {
          int actual = decoder.readInt(buffer);
          assertEquals(expected, -actual);
          continue;
        }
        fail();
      }
    }
  }

  @Test
  public void testLongRepeat() throws Exception {
    for (int i = 1; i <= 10; i++) {
      testLong(i);
    }
  }

  private void testLong(int repeatCount) throws Exception {
    Encoder encoder = new LongSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (long value : SprintzDecoderTest.longList) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

    for (int i = 0; i < repeatCount; i++) {
      Decoder decoder = new LongSprintzDecoder();
      for (long expected : SprintzDecoderTest.longList) {
        if (decoder.hasNext(buffer)) {
          long actual = decoder.readLong(buffer);
          assertEquals(expected, actual);
          continue;
        }
        fail();
      }
    }

    encoder = new LongSprintzEncoder();
    baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (long value : SprintzDecoderTest.longList) {
        encoder.encode(-value, baos);
      }
      encoder.flush(baos);
    }

    buffer = ByteBuffer.wrap(baos.toByteArray());

    for (int i = 0; i < repeatCount; i++) {
      Decoder decoder = new LongSprintzDecoder();
      for (long expected : SprintzDecoderTest.longList) {
        if (decoder.hasNext(buffer)) {
          long actual = decoder.readLong(buffer);
          assertEquals(expected, -actual);
          continue;
        }
        fail();
      }
    }
  }

  @Test
  public void testFloatSingleValue() throws IOException {
    Encoder encoder = new FloatSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    encoder.encode(Float.MAX_VALUE, baos);
    encoder.flush(baos);

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

    Decoder decoder = new FloatSprintzDecoder();
    if (decoder.hasNext(buffer)) {
      assertEquals(Float.MAX_VALUE, decoder.readFloat(buffer), DELTA);
    }
    if (decoder.hasNext(buffer)) {
      fail();
    }
  }

  @Test
  public void testDoubleSingleValue() throws IOException {
    Encoder encoder = new DoubleSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    encoder.encode(Double.MAX_VALUE, baos);
    encoder.flush(baos);

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

    Decoder decoder = new DoubleSprintzDecoder();
    if (decoder.hasNext(buffer)) {
      assertEquals(Double.MAX_VALUE, decoder.readDouble(buffer), DELTA);
    }
    if (decoder.hasNext(buffer)) {
      fail();
    }
  }

  @Test
  public void testFloatZeroNumber() throws IOException {
    Encoder encoder = new FloatSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    float value = 0f;
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    for (int i = 0; i < 2; i++) {
      Decoder decoder = new FloatSprintzDecoder();
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readFloat(buffer), DELTA);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readFloat(buffer), DELTA);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readFloat(buffer), DELTA);
      }
    }
  }

  @Test
  public void testDoubleZeroNumber() throws IOException {
    Encoder encoder = new DoubleSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    double value = 0f;
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.encode(value, baos);
    encoder.flush(baos);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    for (int i = 0; i < 2; i++) {
      Decoder decoder = new DoubleSprintzDecoder();
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readDouble(buffer), DELTA);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readDouble(buffer), DELTA);
      }
      if (decoder.hasNext(buffer)) {
        assertEquals(value, decoder.readDouble(buffer), DELTA);
      }
    }
  }

  @Test
  public void testFloat() throws IOException {
    for (Integer num : iterations) {
      Encoder encoder = new FloatSprintzEncoder();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      float value = 7.101f;
      for (int i = 0; i < num; i++) {
        encoder.encode(value + 2 * i, baos);
      }
      encoder.flush(baos);

      ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

      Decoder decoder = new FloatSprintzDecoder();
      for (int i = 0; i < num; i++) {
        if (decoder.hasNext(buffer)) {
          assertEquals(value + 2 * i, decoder.readFloat(buffer), DELTA);
          continue;
        }
        fail();
      }
      if (decoder.hasNext(buffer)) {
        fail();
      }
    }
  }

  @Test
  public void testDouble() throws IOException {
    for (Integer num : iterations) {
      Encoder encoder = new DoubleSprintzEncoder();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      double value = 7.101f;
      for (int i = 0; i < num; i++) {
        encoder.encode(value + 2 * i, baos);
      }
      encoder.flush(baos);

      ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

      Decoder decoder = new DoubleSprintzDecoder();
      for (int i = 0; i < num; i++) {
        if (decoder.hasNext(buffer)) {
          assertEquals(value + 2 * i, decoder.readDouble(buffer), DELTA);
          continue;
        }
        fail();
      }
      if (decoder.hasNext(buffer)) {
        fail();
      }
    }
  }

  @Test
  public void testFloatRepeat() throws Exception {
    for (int i = 1; i <= 10; i++) {
      testFloat(i);
    }
  }

  private void testFloat(int repeatCount) throws Exception {
    Encoder encoder = new FloatSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (float value : SprintzDecoderTest.floatList) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

    for (int i = 0; i < repeatCount; i++) {
      Decoder decoder = new FloatSprintzDecoder();
      for (float expected : SprintzDecoderTest.floatList) {
        if (decoder.hasNext(buffer)) {
          float actual = decoder.readFloat(buffer);
          assertEquals(expected, actual, DELTA);
          continue;
        }
        fail();
      }
    }

    encoder = new FloatSprintzEncoder();
    baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (float value : SprintzDecoderTest.floatList) {
        encoder.encode(-value, baos);
      }
      encoder.flush(baos);
    }

    buffer = ByteBuffer.wrap(baos.toByteArray());

    for (int i = 0; i < repeatCount; i++) {
      Decoder decoder = new FloatSprintzDecoder();
      for (float expected : SprintzDecoderTest.floatList) {
        if (decoder.hasNext(buffer)) {
          float actual = decoder.readFloat(buffer);
          assertEquals(expected, -actual, DELTA);
          continue;
        }
        fail();
      }
    }
  }

  @Test
  public void testDoubleRepeat() throws Exception {
    for (int i = 1; i <= 10; i++) {
      testDouble(i);
    }
  }

  private void testDouble(int repeatCount) throws Exception {
    Encoder encoder = new DoubleSprintzEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (double value : SprintzDecoderTest.doubleList) {
        encoder.encode(value, baos);
      }
      encoder.flush(baos);
    }

    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());

    for (int i = 0; i < repeatCount; i++) {
      Decoder decoder = new DoubleSprintzDecoder();
      for (double expected : SprintzDecoderTest.doubleList) {
        if (decoder.hasNext(buffer)) {
          double actual = decoder.readDouble(buffer);
          assertEquals(expected, actual, DELTA);
          continue;
        }
        fail();
      }
    }

    encoder = new DoubleSprintzEncoder();
    baos = new ByteArrayOutputStream();
    for (int i = 0; i < repeatCount; i++) {
      for (double value : SprintzDecoderTest.doubleList) {
        encoder.encode(-value, baos);
      }
      encoder.flush(baos);
    }

    buffer = ByteBuffer.wrap(baos.toByteArray());

    for (int i = 0; i < repeatCount; i++) {
      Decoder decoder = new DoubleSprintzDecoder();
      for (double expected : SprintzDecoderTest.doubleList) {
        if (decoder.hasNext(buffer)) {
          double actual = decoder.readDouble(buffer);
          assertEquals(expected, -actual, DELTA);
          continue;
        }
        fail();
      }
    }
  }
}
