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

import java.util.Arrays;
import java.util.Objects;

public class BitMap {
  private static final byte[] BIT_UTIL = new byte[] {1, 2, 4, 8, 16, 32, 64, -128};
  private static final byte[] UNMARK_BIT_UTIL =
      new byte[] {
        (byte) 0XFE, // 11111110
        (byte) 0XFD, // 11111101
        (byte) 0XFB, // 11111011
        (byte) 0XF7, // 11110111
        (byte) 0XEF, // 11101111
        (byte) 0XDF, // 11011111
        (byte) 0XBF, // 10111111
        (byte) 0X7F // 01111111
      };

  private final byte[] bits;
  private final int size;

  /** Initialize a BitMap with given size. */
  public BitMap(int size) {
    this.size = size;
    bits = new byte[getSizeOfBytes(size)];
    Arrays.fill(bits, (byte) 0);
  }

  /** Initialize a BitMap with given size and bytes. */
  public BitMap(int size, byte[] bits) {
    this.size = size;
    this.bits = bits;
  }

  public byte[] getByteArray() {
    return this.bits;
  }

  public int getSize() {
    return this.size;
  }

  /** returns the value of the bit with the specified index. */
  public boolean isMarked(int position) {
    return (bits[position / Byte.SIZE] & BIT_UTIL[position % Byte.SIZE]) != 0;
  }

  /** mark as 1 at all positions. */
  public void markAll() {
    Arrays.fill(bits, (byte) 0XFF);
  }

  /** mark as 1 at the given bit position. */
  public void mark(int position) {
    bits[position / Byte.SIZE] |= BIT_UTIL[position % Byte.SIZE];
  }

  /** mark as 0 at all positions. */
  public void reset() {
    Arrays.fill(bits, (byte) 0);
  }

  public void unmark(int position) {
    bits[position / Byte.SIZE] &= UNMARK_BIT_UTIL[position % Byte.SIZE];
  }

  /** whether all bits are zero, i.e., no Null value */
  public boolean isAllUnmarked() {
    int j;
    for (j = 0; j < size / Byte.SIZE; j++) {
      if (bits[j] != (byte) 0) {
        return false;
      }
    }
    for (j = 0; j < size % Byte.SIZE; j++) {
      if ((bits[size / Byte.SIZE] & BIT_UTIL[j]) != 0) {
        return false;
      }
    }
    return true;
  }

  // whether all bits in the range are unmarked
  public boolean isAllUnmarked(int rangeSize) {
    int j;
    for (j = 0; j < rangeSize / Byte.SIZE; j++) {
      if (bits[j] != (byte) 0) {
        return false;
      }
    }
    int remainingBits = rangeSize % Byte.SIZE;
    if (remainingBits > 0) {
      byte mask = (byte) (0xFF >> (Byte.SIZE - remainingBits));
      if ((bits[rangeSize / Byte.SIZE] & mask) != 0) {
        return false;
      }
    }
    return true;
  }

  /** whether all bits are one, i.e., all are Null */
  public boolean isAllMarked() {
    int j;
    for (j = 0; j < size / Byte.SIZE; j++) {
      if (bits[j] != (byte) 0XFF) {
        return false;
      }
    }
    for (j = 0; j < size % Byte.SIZE; j++) {
      if ((bits[size / Byte.SIZE] & BIT_UTIL[j]) == 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder();
    for (int i = 0; i < size; i++) {
      res.append(isMarked(i) ? 1 : 0);
    }
    return res.toString();
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(size);
    result = 31 * result + Arrays.hashCode(bits);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof BitMap)) {
      return false;
    }
    BitMap other = (BitMap) obj;
    return this.size == other.size && Arrays.equals(this.bits, other.bits);
  }

  public boolean equalsInRange(Object obj, int rangeSize) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof BitMap)) {
      return false;
    }
    BitMap other = (BitMap) obj;
    if (rangeSize > size || rangeSize > other.size) {
      throw new IllegalArgumentException(
          "range size "
              + rangeSize
              + " should <= the minimal bitmap size "
              + Math.min(this.size, other.size));
    }

    int byteSize = rangeSize / Byte.SIZE;
    for (int i = 0; i < byteSize; i++) {
      if (this.bits[i] != other.bits[i]) {
        return false;
      }
    }
    int remainingBits = rangeSize % Byte.SIZE;
    if (remainingBits > 0) {
      byte mask = (byte) (0xFF >> (Byte.SIZE - remainingBits));
      if ((this.bits[byteSize] & mask) != (other.bits[byteSize] & mask)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public BitMap clone() {
    byte[] cloneBytes = new byte[this.bits.length];
    System.arraycopy(this.bits, 0, cloneBytes, 0, this.bits.length);
    return new BitMap(this.size, cloneBytes);
  }

  /**
   * Copies a bitmap from the specified source bitmap, beginning at the specified position, to the
   * specified position of the destination bitmap. A subsequence of bits are copied from the source
   * bitmap referenced by src to the destination bitmap referenced by dest. The number of bits
   * copied is equal to the length argument. The bits at positions srcPos through srcPos+length-1 in
   * the source bitmap are copied into positions destPos through destPos+length-1, respectively, of
   * the destination bitmap.
   *
   * @param src the source bitmap.
   * @param srcPos starting position in the source bitmap.
   * @param dest the destination bitmap.
   * @param destPos starting position in the destination bitmap.
   * @param length the number of bits to be copied.
   * @throws IndexOutOfBoundsException if copying would cause access of data outside bitmap bounds.
   */
  public static void copyOfRange(BitMap src, int srcPos, BitMap dest, int destPos, int length) {
    if (srcPos + length > src.size) {
      throw new IndexOutOfBoundsException(
          (srcPos + length - 1) + " is out of src range " + src.size);
    } else if (destPos + length > dest.size) {
      throw new IndexOutOfBoundsException(
          (destPos + length - 1) + " is out of dest range " + dest.size);
    }
    for (int i = 0; i < length; ++i) {
      if (src.isMarked(srcPos + i)) {
        dest.mark(destPos + i);
      } else {
        dest.unmark(destPos + i);
      }
    }
  }

  public BitMap getRegion(int positionOffset, int length) {
    BitMap newBitMap = new BitMap(length);
    copyOfRange(this, positionOffset, newBitMap, 0, length);
    return newBitMap;
  }

  public static int getSizeOfBytes(int size) {
    // Regardless of whether it is divisible here, add 1 byte.
    // Should not modify this place, as many codes are already using the same method to calculate
    // bitmap size.
    // Precise calculation of size may cause those codes to throw IndexOutOfBounds or
    // BufferUnderFlow
    // exceptions.
    return size / Byte.SIZE + 1;
  }

  public byte[] getTruncatedByteArray(int size) {
    return Arrays.copyOf(this.bits, getSizeOfBytes(size));
  }
}
