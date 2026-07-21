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

import org.apache.tsfile.i18n.Messages;

import java.util.Arrays;

public class BitMap {

  private BitMapImpl implementation;

  /** Initialize an array-backed BitMap with the given size. */
  public BitMap(int size) {
    implementation = new BitMapArrayImpl(size);
  }

  /** Initialize an array-backed BitMap with the given size and bytes. */
  public BitMap(int size, byte[] bits) {
    implementation = new BitMapArrayImpl(size, bits);
  }

  BitMap(BitMapImpl implementation) {
    this.implementation = implementation;
  }

  public byte[] getByteArray() {
    return implementation.getByteArray();
  }

  public int getSize() {
    return implementation.getSize();
  }

  /** returns the value of the bit with the specified index. */
  public boolean isMarked(int position) {
    return implementation.isMarked(position);
  }

  /** mark as 1 at all positions. */
  public void markAll() {
    implementation.markAll();
  }

  /** mark as 1 at the given bit position. */
  public void mark(int position) {
    implementation.mark(position);
  }

  public void markRange(int startPosition, int length) {
    implementation.markRange(startPosition, length);
  }

  /** mark as 0 at all positions. */
  public void reset() {
    implementation.reset();
  }

  public void unmark(int position) {
    implementation.unmark(position);
  }

  public void unmarkRange(int startPosition, int length) {
    implementation.unmarkRange(startPosition, length);
  }

  public void merge(BitMap src, int srcStart, int destStart, int len) {
    implementation.merge(src.implementation, srcStart, destStart, len);
  }

  /** whether all bits are zero, i.e., no Null value */
  public boolean isAllUnmarked() {
    return implementation.isAllUnmarked();
  }

  // whether all bits in the range are unmarked
  public boolean isAllUnmarked(int rangeSize) {
    return implementation.isAllUnmarked(rangeSize);
  }

  /** whether all bits are one, i.e., all are Null */
  public boolean isAllMarked() {
    return implementation.isAllMarked();
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < getSize(); i++) {
      result.append(isMarked(i) ? 1 : 0);
    }
    return result.toString();
  }

  @Override
  public int hashCode() {
    int result = 31 + getSize();
    result = 31 * result + implementation.contentHashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof BitMap)) {
      return false;
    }
    BitMap other = (BitMap) obj;
    return getSize() == other.getSize() && implementation.contentEquals(other.implementation);
  }

  public boolean equalsInRange(Object obj, int rangeSize) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof BitMap)) {
      return false;
    }
    BitMap other = (BitMap) obj;
    if (rangeSize > getSize() || rangeSize > other.getSize()) {
      throw new IllegalArgumentException(
          Messages.format(
              "error.common.bitmap_range_size_exceeds",
              rangeSize,
              Math.min(getSize(), other.getSize())));
    }

    return implementation.contentEqualsInRange(other.implementation, rangeSize);
  }

  @Override
  public BitMap clone() {
    return new BitMap(implementation.copy());
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
    if (srcPos + length > src.getSize()) {
      throw new IndexOutOfBoundsException(
          Messages.format(
              "error.common.bitmap_out_of_src_range", (srcPos + length - 1), src.getSize()));
    } else if (destPos + length > dest.getSize()) {
      throw new IndexOutOfBoundsException(
          Messages.format(
              "error.common.bitmap_out_of_dest_range", (destPos + length - 1), dest.getSize()));
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
    return Arrays.copyOf(getByteArray(), getSizeOfBytes(size));
  }

  public void append(BitMap another, int position, int length) {
    for (int i = 0; i < length; i++) {
      if (another.isMarked(i)) {
        mark(position + i);
      } else {
        unmark(position + i);
      }
    }
  }

  public void extend(int newSize) {
    implementation = implementation.extend(newSize);
  }

  long getRetainedSizeInBytes() {
    return implementation.getRetainedSizeInBytes();
  }

  BitMapImpl getImplementation() {
    return implementation;
  }

  private static BitMapImpl createImplementation(int size) {
    return size >= 0 && size <= Long.SIZE ? new BitMapLongImpl(size) : new BitMapArrayImpl(size);
  }

  /** Initialize a BitMap whose implementation is selected according to the given size. */
  public static BitMap createBitMapDynamically(int size) {
    return new BitMap(createImplementation(size));
  }
}
