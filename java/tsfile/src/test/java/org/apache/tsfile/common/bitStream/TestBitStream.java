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

package org.apache.tsfile.common.bitStream;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import static org.apache.tsfile.common.bitStream.BitInputStream.readVarLong;
import static org.apache.tsfile.common.bitStream.BitOutputStream.writeVarInt;
import static org.apache.tsfile.common.bitStream.BitOutputStream.writeVarLong;

public class TestBitStream {

  @Test
  public void testWriteAndReadInt() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BitOutputStream out = new BitOutputStream(bout);

    out.writeInt(0, 0); // No-op write
    out.writeInt(0x78563412, 32); // Full int
    out.writeInt(2, 4); // Partial int
    out.writeInt(3, 3);
    out.writeInt(0, 1);
    out.writeInt(0xA8, 8); // One byte
    out.writeInt(0x11, 6); // 6 bits
    out.close();

    byte[] expected = new byte[] {0x78, 0x56, 0x34, 0x12, 0x26, (byte) 0xA8, 0x44};
    Assert.assertArrayEquals(expected, bout.toByteArray());
  }

  @Test
  public void testBitInputWithMarkAndEOF() throws IOException {
    byte[] data = new byte[] {0x12, 0x34, 0x56, 0x78, 0x32, (byte) 0xA8, 0x11};
    BitInputStream in = new BitInputStream(new ByteArrayInputStream(data), data.length * 8);

    Assert.assertTrue(in.markSupported());
    Assert.assertEquals(56, in.availableBits());
    Assert.assertEquals(0, in.readInt(0));
    Assert.assertEquals(0x12345678, in.readInt(32));
    Assert.assertEquals(3, in.readInt(4));

    in.mark(200);
    Assert.assertEquals(1, in.readInt(3));
    Assert.assertEquals(0, in.readInt(1));
    Assert.assertEquals(0xA8, in.readInt(8));

    in.reset();
    Assert.assertEquals(2, in.readInt(4));
    Assert.assertEquals(0xA8, in.readInt(8));

    Assert.assertEquals(8, in.availableBits());
    Assert.assertEquals(0x110, in.readInt(12));

    try {
      in.readInt(1);
      Assert.fail("Expected EOFException");
    } catch (EOFException ignored) {
    }

    in.close();
  }

  @Test
  public void testWriteAndReadLong() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BitOutputStream out = new BitOutputStream(bout);

    long[] values = {0L, 1L, 0xFFFFFFFFL, 0x123456789ABCDEFL, Long.MAX_VALUE, Long.MIN_VALUE};
    int[] bits = {1, 2, 32, 60, 64, 64};

    for (int i = 0; i < values.length; i++) {
      out.writeLong(values[i], bits[i]);
    }
    out.close();

    BitInputStream in =
        new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());
    for (int i = 0; i < values.length; i++) {
      long actual = in.readLong(bits[i]);
      Assert.assertEquals("Mismatch at index " + i, values[i], actual);
    }
    in.close();
  }

  @Test
  public void testWriteAndReadBits() throws IOException {
    boolean[] bits = {true, false, true, true, false, false, false, true, false, true};

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BitOutputStream out = new BitOutputStream(bout);
    for (boolean b : bits) {
      out.writeBit(b);
    }
    out.close();

    BitInputStream in =
        new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());
    for (int i = 0; i < bits.length; i++) {
      boolean actual = in.readBit();
      Assert.assertEquals("Bit mismatch at index " + i, bits[i], actual);
    }

    try {
      in.readBit();
      Assert.fail("Expected EOFException");
    } catch (EOFException ignored) {
    }

    in.close();
  }

  @Test
  public void testLongBitWidths() throws IOException {
    for (int bits = 1; bits <= 64; bits++) {
      long value = (1L << (bits - 1)) | 1L;

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      BitOutputStream out = new BitOutputStream(bout);
      out.writeLong(value, bits);
      out.close();

      BitInputStream in =
          new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());
      long result = in.readLong(bits);
      Assert.assertEquals("Failed at bit width = " + bits, value, result);
      in.close();
    }
  }

  @Test
  public void testAllZerosAndAllOnesLong() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BitOutputStream out = new BitOutputStream(bout);

    out.writeLong(0L, 64);
    out.writeLong(-1L, 64);
    out.close();

    BitInputStream in =
        new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());
    Assert.assertEquals(0L, in.readLong(64));
    Assert.assertEquals(-1L, in.readLong(64));
    in.close();
  }

  @Test
  public void testBitBoundaryCrossing() throws IOException {
    boolean[] bits = {
      false,
      true,
      true,
      false,
      true,
      false,
      false,
      true, // first byte
      true,
      true,
      false,
      true,
      false,
      true,
      true // crosses byte
    };

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BitOutputStream out = new BitOutputStream(bout);
    for (boolean b : bits) {
      out.writeBit(b);
    }
    out.close();

    BitInputStream in =
        new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());
    for (int i = 0; i < bits.length; i++) {
      Assert.assertEquals("Mismatch at bit index " + i, bits[i], in.readBit());
    }
    in.close();
  }

  @Test
  public void testMixedLongAndBit() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BitOutputStream out = new BitOutputStream(bout);

    out.writeLong(0x1FL, 5); // 11111
    out.writeBit(true); // 1
    out.writeBit(false); // 0
    out.writeBit(true); // 1
    out.close();

    BitInputStream in =
        new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());

    Assert.assertEquals(0x1F, in.readLong(5));
    Assert.assertTrue(in.readBit());
    Assert.assertFalse(in.readBit());
    Assert.assertTrue(in.readBit());

    in.close();
  }

  @Test
  public void testVarLongSymmetry() throws IOException {
    long[] testValues = {
      0, 1, -1, 63, -63, 64, -64, 128, -128, 1024, -1024, Long.MAX_VALUE, Long.MIN_VALUE
    };

    for (long original : testValues) {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      BitOutputStream out = new BitOutputStream(bout);
      writeVarLong(original, out);
      out.close();
      BitInputStream in =
          new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());
      long decoded = readVarLong(in);
      in.close();

      Assert.assertEquals("Mismatch for value: " + original, original, decoded);
    }
  }

  @Test
  public void testVarLongContinuousRange() throws IOException {
    for (int value = -10000; value <= 10000; value++) {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      BitOutputStream out = new BitOutputStream(bout);
      writeVarLong(value, out);
      out.close();

      BitInputStream in =
          new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());
      long decoded = readVarLong(in);
      in.close();

      Assert.assertEquals("Mismatch in range test for: " + value, value, decoded);
    }
  }

  @Test
  public void testVarLongBitLengthGrowth() throws IOException {
    long[] values = {0, 1, 2, 64, 128, 8192, 1 << 20, Long.MAX_VALUE / 2};
    int lastBits = 0;

    for (long value : values) {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      BitOutputStream out = new BitOutputStream(bout);
      int bits = writeVarLong(value, out);
      out.close();

      Assert.assertTrue("Bit length didn't increase for " + value, bits >= lastBits);
      lastBits = bits;
    }
  }

  @Test
  public void testVarIntSymmetry() throws IOException {
    int[] values = {
      0,
      1,
      -1,
      63,
      -63,
      127,
      -128,
      255,
      -256,
      1023,
      -1023,
      16384,
      -16384,
      Integer.MAX_VALUE,
      Integer.MIN_VALUE
    };

    for (int value : values) {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      BitOutputStream out = new BitOutputStream(bout);
      writeVarInt(value, out);
      out.close();

      BitInputStream in =
          new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());
      int decoded = BitInputStream.readVarInt(in);
      in.close();

      Assert.assertEquals("Mismatch for value: " + value, value, decoded);
    }
  }

  @Test
  public void testVarIntRange() throws IOException {
    for (int value = -10000; value <= 10000; value++) {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      BitOutputStream out = new BitOutputStream(bout);
      writeVarInt(value, out);
      out.close();

      BitInputStream in =
          new BitInputStream(new ByteArrayInputStream(bout.toByteArray()), out.getBitsWritten());
      int decoded = in.readVarInt(in);
      in.close();

      Assert.assertEquals("Mismatch in range for value: " + value, value, decoded);
    }
  }

  @Test
  public void testBitLengthGrowth() throws IOException {
    int[] values = {0, 1, 2, 64, 128, 1024, 16384, 1 << 20};
    int lastBits = 0;

    for (int value : values) {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      BitOutputStream out = new BitOutputStream(bout);
      int bits = writeVarInt(value, out);
      out.close();

      Assert.assertTrue("Bit length not increasing for value: " + value, bits >= lastBits);
      lastBits = bits;
    }
  }
}
