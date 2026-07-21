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

class BitMapLongImpl extends BitMapImpl {

  private static final long ALL_BITS_MARKED = -1L;
  // Object header + BitMapImpl.size (int) + bits (long) + paddingByte (byte).
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.alignObjectSize(
          (long) RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
              + Integer.BYTES
              + Long.BYTES
              + Byte.BYTES);

  private long bits;
  // BitMap serialization always has one extra byte, which does not fit in bits at size 64.
  private byte paddingByte;

  BitMapLongImpl(int size) {
    super(size);
  }

  BitMapLongImpl(int size, byte[] bytes) {
    super(size);
    int byteCount = Math.min(bytes.length, Long.BYTES);
    for (int i = 0; i < byteCount; i++) {
      bits |= (bytes[i] & 0xFFL) << (i * Byte.SIZE);
    }
    if (getByteArrayLength() > Long.BYTES && bytes.length > Long.BYTES) {
      paddingByte = bytes[Long.BYTES];
    }
  }

  @Override
  byte[] getByteArray() {
    byte[] bytes = new byte[BitMap.getSizeOfBytes(size)];
    int byteCount = Math.min(bytes.length, Long.BYTES);
    for (int i = 0; i < byteCount; i++) {
      bytes[i] = (byte) (bits >>> (i * Byte.SIZE));
    }
    if (bytes.length > Long.BYTES) {
      bytes[Long.BYTES] = paddingByte;
    }
    return bytes;
  }

  @Override
  int getByteArrayLength() {
    return BitMap.getSizeOfBytes(size);
  }

  @Override
  byte getByte(int index) {
    return index < Long.BYTES ? (byte) (bits >>> (index * Byte.SIZE)) : paddingByte;
  }

  @Override
  boolean isMarked(int position) {
    return (bits & (1L << position)) != 0;
  }

  @Override
  void markAll() {
    bits = ALL_BITS_MARKED;
    if (getByteArrayLength() > Long.BYTES) {
      paddingByte = (byte) 0xFF;
    }
  }

  @Override
  void mark(int position) {
    bits |= 1L << position;
  }

  @Override
  void markRange(int startPosition, int length) {
    if (length <= 0) {
      return;
    }
    checkRange(startPosition, length);
    bits |= lowerBitsMask(length) << startPosition;
  }

  @Override
  void reset() {
    bits = 0L;
    paddingByte = 0;
  }

  @Override
  void unmark(int position) {
    bits &= ~(1L << position);
  }

  @Override
  void unmarkRange(int startPosition, int length) {
    if (length <= 0) {
      return;
    }
    checkRange(startPosition, length);
    bits &= ~(lowerBitsMask(length) << startPosition);
  }

  @Override
  void merge(BitMapImpl src, int srcStart, int destStart, int len) {
    if (len <= 0) {
      return;
    }
    if (srcStart < 0 || destStart < 0 || srcStart + len > src.size || destStart + len > size) {
      throw new IndexOutOfBoundsException();
    }
    bits |= src.extractBits(srcStart, len) << destStart;
  }

  @Override
  long extractBits(int offset, int length) {
    return (bits >>> offset) & lowerBitsMask(length);
  }

  @Override
  boolean isAllUnmarked() {
    return (bits & lowerBitsMask(size)) == 0L;
  }

  @Override
  boolean isAllUnmarked(int rangeSize) {
    return (bits & lowerBitsMask(rangeSize)) == 0L;
  }

  @Override
  boolean isAllMarked() {
    long mask = lowerBitsMask(size);
    return (bits & mask) == mask;
  }

  @Override
  boolean contentEquals(BitMapImpl other) {
    if (!(other instanceof BitMapLongImpl)) {
      return super.contentEquals(other);
    }
    int serializedBitSize = Math.min(getByteArrayLength() * Byte.SIZE, Long.SIZE);
    long mask = lowerBitsMask(serializedBitSize);
    BitMapLongImpl otherLongImpl = (BitMapLongImpl) other;
    return (bits & mask) == (otherLongImpl.bits & mask)
        && (getByteArrayLength() <= Long.BYTES || paddingByte == otherLongImpl.paddingByte);
  }

  @Override
  boolean contentEqualsInRange(BitMapImpl other, int rangeSize) {
    if (!(other instanceof BitMapLongImpl)) {
      return super.contentEqualsInRange(other, rangeSize);
    }
    long mask = lowerBitsMask(rangeSize);
    return (bits & mask) == (((BitMapLongImpl) other).bits & mask);
  }

  @Override
  int contentHashCode() {
    int result = 1;
    long value = bits;
    int byteArrayLength = getByteArrayLength();
    int longByteCount = Math.min(byteArrayLength, Long.BYTES);
    for (int i = 0; i < longByteCount; i++) {
      result = 31 * result + (byte) value;
      value >>>= Byte.SIZE;
    }
    for (int i = Long.BYTES; i < byteArrayLength; i++) {
      result = 31 * result + paddingByte;
    }
    return result;
  }

  @Override
  BitMapImpl copy() {
    BitMapLongImpl copy = new BitMapLongImpl(size);
    copy.bits = bits;
    copy.paddingByte = paddingByte;
    return copy;
  }

  @Override
  BitMapImpl extend(int newSize) {
    if (size >= newSize) {
      return this;
    }
    if (newSize <= Long.SIZE) {
      size = newSize;
      return this;
    }
    return new BitMapArrayImpl(newSize, getExtendedByteArray(newSize));
  }

  @Override
  long getRetainedSizeInBytes() {
    return INSTANCE_SIZE;
  }

  static long lowerBitsMask(int length) {
    return length == Long.SIZE ? -1L : (1L << length) - 1;
  }

  private byte[] getExtendedByteArray(int newSize) {
    byte[] bytes = new byte[BitMap.getSizeOfBytes(newSize)];
    for (int i = 0; i < Long.BYTES; i++) {
      bytes[i] = (byte) (bits >>> (i * Byte.SIZE));
    }
    bytes[Long.BYTES] = paddingByte;
    return bytes;
  }

  private void checkRange(int startPosition, int length) {
    if (startPosition < 0 || startPosition + length > size) {
      throw new IndexOutOfBoundsException(
          Messages.format(
              "error.common.bitmap_start_length_out_of_range", startPosition, length, size));
    }
  }
}
