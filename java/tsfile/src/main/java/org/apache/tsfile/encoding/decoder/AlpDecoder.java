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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.i18n.Messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Reads the C++ TsFile ALP v1 value stream, one block at a time. */
public final class AlpDecoder extends Decoder {

  private static final int VERSION = 1;
  private static final int PAGE_HEADER_BYTES = 16;
  private static final int BLOCK_HEADER_BYTES = 28;
  private static final int MAX_BLOCK_VALUES = 1024;
  private static final int BODY_ALIGNMENT = 32;
  private static final int BODY_PADDING = 16;
  private static final int SCHEME_ALP = 0;
  private static final int SCHEME_PLAIN = 1;

  private static final long[] FACTORS = {
    1L,
    10L,
    100L,
    1000L,
    10000L,
    100000L,
    1000000L,
    10000000L,
    100000000L,
    1000000000L,
    10000000000L,
    100000000000L,
    1000000000000L,
    10000000000000L,
    100000000000000L,
    1000000000000000L,
    10000000000000000L,
    100000000000000000L,
    1000000000000000000L
  };

  // Match the C++ constants and calculate in the column's own precision.
  private static final float[] FLOAT_FRACTIONS = {
    1f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f
  };

  private static final double[] DOUBLE_FRACTIONS = {
    1d, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14,
    1e-15, 1e-16, 1e-17, 1e-18
  };

  private final boolean floatType;
  private final int valueBytes;
  private final int maxExponent;
  private ByteBuffer page;
  private int blocksRemaining;
  private long valuesRemaining;
  private int blockValues;
  private int index;
  private int scheme;
  private int factor;
  private int exponent;
  private long base;
  private int width;
  private int bitmapOffset;
  private int exceptionOffset;
  private int bodyOffset;

  public AlpDecoder(TSDataType dataType) {
    super(TSEncoding.ALP);
    if (dataType != TSDataType.FLOAT && dataType != TSDataType.DOUBLE) {
      throw new TsFileDecodingException(
          Messages.format("error.encoding.alp_type_not_supported", dataType));
    }
    floatType = dataType == TSDataType.FLOAT;
    valueBytes = floatType ? Float.BYTES : Double.BYTES;
    maxExponent = (floatType ? FLOAT_FRACTIONS.length : DOUBLE_FRACTIONS.length) - 1;
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    if (index < blockValues) {
      return true;
    }
    if (page == null) {
      if (!buffer.hasRemaining()) {
        return false;
      }
      readPageHeader(buffer);
    }
    if (blocksRemaining == 0) {
      return false;
    }
    readBlock(buffer);
    return true;
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    return floatType ? Float.intBitsToFloat((int) readValueBits(buffer)) : super.readFloat(buffer);
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    return floatType ? super.readDouble(buffer) : Double.longBitsToDouble(readValueBits(buffer));
  }

  @Override
  public void reset() {
    page = null;
    index = 0;
    blockValues = 0;
  }

  private void readPageHeader(ByteBuffer buffer) {
    // Do not change the caller's byte order; TsFile's outer headers use their own order.
    ByteBuffer input = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    if (input.remaining() < PAGE_HEADER_BYTES
        || Byte.toUnsignedInt(input.get()) != VERSION
        || Byte.toUnsignedInt(input.get()) != (floatType ? 0 : 1)
        || input.getShort() != 0) {
      throw malformed();
    }
    valuesRemaining = Integer.toUnsignedLong(input.getInt());
    blocksRemaining = input.getInt();
    if (input.getInt() != 0
        || blocksRemaining < 0
        || blocksRemaining > input.remaining() / BLOCK_HEADER_BYTES
        || valuesRemaining < blocksRemaining
        || valuesRemaining > (long) blocksRemaining * MAX_BLOCK_VALUES
        || (blocksRemaining == 0 && input.hasRemaining())) {
      throw malformed();
    }
    page = input;
    buffer.position(input.position());
  }

  private void readBlock(ByteBuffer buffer) {
    if (page.remaining() < BLOCK_HEADER_BYTES) {
      throw malformed();
    }
    scheme = Byte.toUnsignedInt(page.get());
    factor = Byte.toUnsignedInt(page.get());
    exponent = Byte.toUnsignedInt(page.get());
    page.get(); // Flags are advisory; the exception count determines the bitmap layout.
    int count = page.getInt();
    int exceptions = page.getInt();
    int bodyBytes = page.getInt();
    base = page.getLong();
    width = Byte.toUnsignedInt(page.get());
    page.position(page.position() + 3); // Block reserved bytes.
    if (count <= 0
        || count > MAX_BLOCK_VALUES
        || exceptions < 0
        || exceptions > count
        || width > valueBytes * Byte.SIZE
        || factor > exponent
        || exponent > maxExponent) {
      throw malformed();
    }

    bitmapOffset = page.position();
    int bitmapBytes = 0;
    int exceptionBytes = 0;
    int expectedBodyBytes;
    if (scheme == SCHEME_PLAIN) {
      expectedBodyBytes = count * valueBytes;
    } else if (scheme == SCHEME_ALP) {
      bitmapBytes = exceptions == 0 ? 0 : (count + 7) / 8;
      exceptionBytes = exceptions * valueBytes;
      int packedBytes = (count * width + 7) / 8;
      expectedBodyBytes =
          width == 0 ? 0 : (packedBytes + BODY_PADDING + BODY_ALIGNMENT - 1) & -BODY_ALIGNMENT;
    } else {
      throw malformed(); // Includes the reserved ALP_RD scheme.
    }

    if (bodyBytes != expectedBodyBytes
        || bitmapBytes + exceptionBytes + bodyBytes > page.remaining()) {
      throw malformed();
    }
    if (bitmapBytes > 0) {
      int marked = 0;
      for (int i = 0; i < bitmapBytes; i++) {
        int validBits = Math.min(8, count - i * 8);
        marked += Integer.bitCount(page.get(bitmapOffset + i) & ((1 << validBits) - 1));
      }
      if (marked != exceptions) {
        throw malformed();
      }
    }

    exceptionOffset = bitmapOffset + bitmapBytes;
    bodyOffset = exceptionOffset + exceptionBytes;
    page.position(bodyOffset + bodyBytes);
    valuesRemaining -= count;
    blocksRemaining--;
    if (valuesRemaining < blocksRemaining
        || valuesRemaining > (long) blocksRemaining * MAX_BLOCK_VALUES
        || (blocksRemaining == 0 && page.hasRemaining())) {
      throw malformed();
    }
    buffer.position(page.position());
    index = 0;
    blockValues = count;
  }

  private long readValueBits(ByteBuffer buffer) {
    if (!hasNext(buffer)) {
      throw new TsFileDecodingException(Messages.get("error.encoding.alp_no_more_values"));
    }
    int i = index++;
    if (scheme == SCHEME_PLAIN) {
      return rawBits(bodyOffset + i * valueBytes);
    }
    if (exceptionOffset > bitmapOffset
        && (page.get(bitmapOffset + (i >>> 3)) & (1 << (i & 7))) != 0) {
      long bits = rawBits(exceptionOffset);
      exceptionOffset += valueBytes;
      return bits;
    }

    // Addition wraps modulo 2^32 (FLOAT) or 2^64 (DOUBLE), then the bits are read as signed.
    long encoded = base + unpackDelta(i);
    return floatType
        ? Float.floatToRawIntBits(
            (float) (int) encoded * (float) FACTORS[factor] * FLOAT_FRACTIONS[exponent])
        : Double.doubleToRawLongBits(
            (double) encoded * (double) FACTORS[factor] * DOUBLE_FRACTIONS[exponent]);
  }

  private long rawBits(int offset) {
    return floatType ? page.getInt(offset) : page.getLong(offset);
  }

  private long unpackDelta(int i) {
    if (width == 0) {
      return 0;
    }
    int bit = i * width;
    int offset = bodyOffset + (bit >>> 3);
    if (width == Long.SIZE) {
      return page.getLong(offset);
    }
    int shift = bit & 7;
    long value = page.getLong(offset) >>> shift;
    if (width + shift > Long.SIZE) {
      value |= (long) Byte.toUnsignedInt(page.get(offset + Long.BYTES)) << (Long.SIZE - shift);
    }
    return value & ((1L << width) - 1);
  }

  private static TsFileDecodingException malformed() {
    return new TsFileDecodingException(Messages.get("error.encoding.alp_malformed"));
  }
}
