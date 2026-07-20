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

class BitMapArrayImpl extends BitMapImpl {

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

  private byte[] bits;

  BitMapArrayImpl(int size) {
    super(size);
    bits = new byte[BitMap.getSizeOfBytes(size)];
  }

  BitMapArrayImpl(int size, byte[] bits) {
    super(size);
    this.bits = bits;
  }

  @Override
  byte[] getByteArray() {
    return bits;
  }

  @Override
  boolean isMarked(int position) {
    return (bits[position / Byte.SIZE] & BIT_UTIL[position % Byte.SIZE]) != 0;
  }

  @Override
  void markAll() {
    Arrays.fill(bits, (byte) 0XFF);
  }

  @Override
  void mark(int position) {
    bits[position / Byte.SIZE] |= BIT_UTIL[position % Byte.SIZE];
  }

  @Override
  void markRange(int startPosition, int length) {
    if (length <= 0) {
      return;
    }

    checkRange(startPosition, length);

    int bitEnd = startPosition + length - 1;
    int byte0 = startPosition >>> 3;
    int byte1 = bitEnd >>> 3;

    if (byte0 == byte1) {
      bits[byte0] |= (byte) (((1 << length) - 1) << (startPosition & 7));
      return;
    }

    bits[byte0++] |= (byte) (0xFF << (startPosition & 7));

    while (byte0 < byte1) {
      bits[byte0++] = (byte) 0xFF;
    }

    bits[byte1] |= (byte) (0xFF >>> (7 - (bitEnd & 7)));
  }

  @Override
  void reset() {
    Arrays.fill(bits, (byte) 0);
  }

  @Override
  void unmark(int position) {
    bits[position / Byte.SIZE] &= UNMARK_BIT_UTIL[position % Byte.SIZE];
  }

  @Override
  void unmarkRange(int startPosition, int length) {
    if (length <= 0) {
      return;
    }

    checkRange(startPosition, length);

    int bitEnd = startPosition + length - 1;
    int byte0 = startPosition >>> 3;
    int byte1 = bitEnd >>> 3;

    if (byte0 == byte1) {
      bits[byte0] &= (byte) ~(((1 << length) - 1) << (startPosition & 7));
      return;
    }

    bits[byte0++] &= (byte) ~(0xFF << (startPosition & 7));

    while (byte0 < byte1) {
      bits[byte0++] = 0;
    }

    bits[byte1] &= (byte) (0xFF << ((bitEnd & 7) + 1));
  }

  @Override
  void merge(BitMapImpl src, int srcStart, int destStart, int len) {
    if (len <= 0) {
      return;
    }
    if (srcStart < 0 || destStart < 0 || srcStart + len > src.size || destStart + len > size) {
      throw new IndexOutOfBoundsException();
    }

    int done = 0;
    int dstBit = destStart & 7;
    while (done < len) {
      int batchSize = Math.min(len - done, Long.SIZE);
      long extractedBits = src.extractBits(srcStart + done, batchSize);
      int destStartByte = (destStart + done) >>> 3;
      bits[destStartByte++] |= (byte) ((extractedBits << dstBit) & 255L);
      extractedBits >>>= Byte.SIZE - dstBit;
      while (extractedBits > 0L) {
        bits[destStartByte++] |= (byte) (extractedBits & 255L);
        extractedBits >>>= Byte.SIZE;
      }
      done += batchSize;
    }
  }

  @Override
  long extractBits(int offset, int length) {
    int start = offset >>> 3;
    int extractedSize = Byte.SIZE - (offset & 7);
    long value = (bits[start++] & 0xFFL) >>> (offset & 7);
    while (extractedSize < length) {
      value |= (bits[start++] & 0xFFL) << extractedSize;
      extractedSize += Byte.SIZE;
    }

    return value & BitMapLongImpl.lowerBitsMask(length);
  }

  @Override
  boolean isAllUnmarked() {
    int index;
    for (index = 0; index < size / Byte.SIZE; index++) {
      if (bits[index] != (byte) 0) {
        return false;
      }
    }
    for (index = 0; index < size % Byte.SIZE; index++) {
      if ((bits[size / Byte.SIZE] & BIT_UTIL[index]) != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  boolean isAllUnmarked(int rangeSize) {
    int index;
    for (index = 0; index < rangeSize / Byte.SIZE; index++) {
      if (bits[index] != (byte) 0) {
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

  @Override
  boolean isAllMarked() {
    int index;
    for (index = 0; index < size / Byte.SIZE; index++) {
      if (bits[index] != (byte) 0XFF) {
        return false;
      }
    }
    for (index = 0; index < size % Byte.SIZE; index++) {
      if ((bits[size / Byte.SIZE] & BIT_UTIL[index]) == 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  BitMapImpl copy() {
    return new BitMapArrayImpl(size, Arrays.copyOf(bits, bits.length));
  }

  @Override
  BitMapImpl extend(int newSize) {
    if (size < newSize) {
      bits = Arrays.copyOf(bits, BitMap.getSizeOfBytes(newSize));
      size = newSize;
    }
    return this;
  }

  @Override
  long getRetainedSizeInBytes() {
    return RamUsageEstimator.shallowSizeOfInstance(BitMapArrayImpl.class)
        + RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + bits.length);
  }

  private void checkRange(int startPosition, int length) {
    if (startPosition < 0 || startPosition + length > size) {
      throw new IndexOutOfBoundsException(
          Messages.format(
              "error.common.bitmap_start_length_out_of_range", startPosition, length, size));
    }
  }
}
