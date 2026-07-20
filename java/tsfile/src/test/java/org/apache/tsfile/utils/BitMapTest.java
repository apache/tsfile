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
package org.apache.tsfile.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BitMapTest {

  @Test
  public void testMarkAndUnMark() {
    BitMap bitmap = new BitMap(100);
    assertEquals(100, bitmap.getSize());
    assertTrue(bitmap.isAllUnmarked());
    assertFalse(bitmap.isAllMarked());
    for (int i = 0; i < 100; i++) {
      bitmap.mark(i);
      assertTrue(bitmap.isMarked(i));
      if (i == 50) {
        assertFalse(bitmap.isAllMarked());
        assertFalse(bitmap.isAllUnmarked());
      }
    }
    assertTrue(bitmap.isAllMarked());
    assertFalse(bitmap.isAllUnmarked());
    for (int i = 0; i < 100; i++) {
      bitmap.unmark(i);
      assertFalse(bitmap.isMarked(i));
    }
    assertTrue(bitmap.isAllUnmarked());
    assertFalse(bitmap.isAllMarked());
  }

  @Test
  public void testInitFromBytes() {
    BitMap bitmap1 = new BitMap(100);
    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) {
        bitmap1.mark(i);
      }
    }
    BitMap bitmap2 = new BitMap(bitmap1.getSize(), bitmap1.getByteArray());
    assertEquals(100, bitmap2.getSize());
    for (int i = 0; i < 100; i++) {
      assertEquals(bitmap1.isMarked(i), bitmap2.isMarked(i));
    }
  }

  @Test
  public void testImplementationSelectionAndExtension() {
    assertTrue(new BitMap(0).getImplementation() instanceof BitMapLongImpl);
    assertTrue(new BitMap(64).getImplementation() instanceof BitMapLongImpl);
    assertTrue(new BitMap(65).getImplementation() instanceof BitMapArrayImpl);

    BitMap bitMap = new BitMap(64);
    bitMap.mark(0);
    bitMap.mark(63);
    bitMap.extend(65);

    assertTrue(bitMap.getImplementation() instanceof BitMapArrayImpl);
    assertEquals(65, bitMap.getSize());
    assertTrue(bitMap.isMarked(0));
    assertTrue(bitMap.isMarked(63));
    assertFalse(bitMap.isMarked(64));
  }

  @Test
  public void testLongImplementationByteArrayCompatibility() {
    byte[] bytes = {
      (byte) 0b10101010,
      (byte) 0b01010101,
      (byte) 0b11110000,
      (byte) 0b00001111,
      (byte) 0b11001100,
      (byte) 0b00110011,
      (byte) 0b10000001,
      (byte) 0b01111110,
      0
    };
    BitMap bitMap = new BitMap(64, bytes);

    assertArrayEquals(bytes, bitMap.getByteArray());
    assertEquals(bitMap, bitMap.clone());
    assertEquals(bitMap.hashCode(), bitMap.clone().hashCode());
  }

  @Test
  public void testLongImplementationFullRange() {
    BitMap rangeBitMap = new BitMap(64);
    BitMap singleBitMap = new BitMap(64);

    rangeBitMap.markRange(0, 64);
    for (int i = 0; i < 64; i++) {
      singleBitMap.mark(i);
    }
    assertTrue(rangeBitMap.isAllMarked());
    assertArrayEquals(singleBitMap.getByteArray(), rangeBitMap.getByteArray());

    rangeBitMap.unmarkRange(0, 64);
    assertTrue(rangeBitMap.isAllUnmarked());
  }

  @Test
  public void testLongImplementationMarkAllByteCompatibility() {
    BitMap bitMap = new BitMap(32);
    bitMap.markAll();

    assertArrayEquals(
        new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        bitMap.getByteArray());

    bitMap.extend(64);
    for (int i = 0; i < 64; i++) {
      assertTrue(bitMap.isMarked(i));
    }
  }

  @Test
  public void testIsAllUnmarkedInRange() {
    BitMap bitMap = new BitMap(16);
    assertTrue(bitMap.isAllUnmarked(6));
    assertTrue(bitMap.isAllUnmarked(8));
    assertTrue(bitMap.isAllUnmarked(9));
    assertTrue(bitMap.isAllUnmarked(16));

    bitMap.mark(3);
    assertTrue(bitMap.isAllUnmarked(2));
    assertTrue(bitMap.isAllUnmarked(3));
    assertFalse(bitMap.isAllUnmarked(4));
    assertFalse(bitMap.isAllUnmarked(16));
    bitMap.unmark(3);

    bitMap.mark(9);
    assertTrue(bitMap.isAllUnmarked(9));
    assertFalse(bitMap.isAllUnmarked(10));
  }

  @Test
  public void testGetTruncatedByteArray() {
    BitMap bitMap = new BitMap(16);
    assertArrayEquals(new byte[2], bitMap.getTruncatedByteArray(13));
    assertArrayEquals(new byte[3], bitMap.getTruncatedByteArray(16));

    bitMap.mark(3);
    byte[] truncatedArray = bitMap.getTruncatedByteArray(12);
    assertEquals(2, truncatedArray.length);

    assertEquals((byte) 0b00001000, truncatedArray[0]);
    assertEquals((byte) 0b00000000, truncatedArray[1]);

    truncatedArray = bitMap.getTruncatedByteArray(8);
    assertEquals(2, truncatedArray.length);

    assertEquals((byte) 0b00001000, truncatedArray[0]);
  }

  @Test
  public void exhaustiveMergeTest() {
    int maxLen = 96;
    int maxSize = 128;
    for (int i = 1; i <= maxLen; i++) {
      for (int j = i; j <= maxSize; j++) {
        for (int k = 0; k <= j - i; k++) {
          for (int m = 0; m <= maxSize - i; m++) {
            runOneCase(j, k, maxSize, m, i);
          }
        }
      }
    }
  }

  private static void runOneCase(int srcSize, int srcStart, int destSize, int destStart, int len) {
    Random r = new Random();
    BitMap src = new BitMap(srcSize);
    BitMap dst = new BitMap(destSize);

    for (int i = 0; i < src.getSize(); i++) {
      if (r.nextBoolean()) {
        src.mark(i);
      }
    }

    for (int i = 0; i < dst.getSize(); i++) {
      if (r.nextBoolean()) {
        dst.mark(i);
      }
    }

    BitMap copy =
        new BitMap(destSize, Arrays.copyOf(dst.getByteArray(), dst.getByteArray().length));

    for (int i = 0; i < len; i++) {
      if (src.isMarked(srcStart + i)) {
        copy.mark(destStart + i);
      }
    }

    dst.merge(src, srcStart, destStart, len);
    assertArrayEquals(copy.getByteArray(), dst.getByteArray());
  }

  @Test
  public void emptyRange() {
    BitMap map = new BitMap(1);
    map.markRange(0, 0);
    assertEquals((byte) 0x00, map.getByteArray()[0]);
  }

  @Test
  public void singleByteAllBits() {
    for (int i = 0; i < 8; i++) {
      for (int j = 0; j <= 8 - i; j++) {
        doTest(8, i, j);
      }
    }
  }

  @Test
  public void twoBytesHeadTail() {
    for (int i = 0; i < 64; i++) {
      for (int j = 0; j <= 64 - i; j += 8) {
        doTest(64, i, j);
      }
    }
  }

  @Test
  public void twoBytesPartialHead() {
    for (int i = 0; i < 64; i += 8) {
      for (int j = 0; j <= 64 - i; j += 8) {
        doTest(64, i, j);
      }
    }
  }

  @Test
  public void twoBytesPartialTail() {
    int size = 64;
    for (int i = 0; i < size; i += 8) {
      for (int j = 1; j <= size - i; j++) {
        doTest(size, i, j);
      }
    }
  }

  private void doTest(int size, int start, int length) {
    BitMap map = new BitMap(size);
    BitMap bitMap = new BitMap(size);
    map.markRange(start, length);
    for (int i = start; i < start + length; i++) {
      bitMap.mark(i);
    }
    assertArrayEquals(bitMap.getByteArray(), map.getByteArray());

    map.unmarkRange(start, length);
    for (int i = start; i < start + length; i++) {
      bitMap.unmark(i);
    }
    System.out.println(start + "        " + length);
    assertArrayEquals(bitMap.getByteArray(), map.getByteArray());
  }
}
