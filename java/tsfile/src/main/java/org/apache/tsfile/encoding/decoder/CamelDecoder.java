/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.common.bitStream.BitInputStream;
import org.apache.tsfile.common.bitStream.ByteBufferBackedInputStream;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class CamelDecoder extends Decoder {
  // === Constants for decoding ===
  private static final int BITS_FOR_SIGN = 1;
  private static final int BITS_FOR_TYPE = 1;
  private static final int BITS_FOR_FIRST_VALUE = 64;
  private static final int BITS_FOR_LEADING_ZEROS = 6;
  private static final int BITS_FOR_SIGNIFICANT_BITS = 6;
  private static final int BITS_FOR_DECIMAL_COUNT = 4;
  private static final int DOUBLE_TOTAL_BITS = 64;
  private static final int DOUBLE_MANTISSA_BITS = 52;
  private static final int DECIMAL_MAX_COUNT = 15;

  // === Camel state ===
  private long previousValue = 0;
  private boolean isFirst = true;
  private long storedVal = 0;

  private double scale;

  // === Precomputed tables ===
  public static final long[] powers = new long[DECIMAL_MAX_COUNT];
  public static final long[] threshold = new long[DECIMAL_MAX_COUNT];

  static {
    for (int l = 1; l <= DECIMAL_MAX_COUNT; l++) {
      int idx = l - 1;
      powers[idx] = (long) Math.pow(10, l);
      long divisor = 1L << l;
      threshold[idx] = powers[idx] / divisor;
    }
  }

  private BitInputStream in;
  private final GorillaDecoder gorillaDecoder;

  public CamelDecoder(InputStream inputStream, long totalBits) {
    super(TSEncoding.CAMEL);
    // Initialize bit-level reader and nested Gorilla decoder
    this.in = new BitInputStream(inputStream, totalBits);
    this.gorillaDecoder = new GorillaDecoder();
  }

  public CamelDecoder() {
    super(TSEncoding.CAMEL);
    this.gorillaDecoder = new GorillaDecoder();
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    if (cacheIndex < cacheSize) {
      return true;
    }
    if (in != null && in.availableBits() > 0) {
      return true;
    }
    return buffer.hasRemaining();
  }

  @Override
  public void reset() {
    this.in = null;
    this.isFirst = true;
    this.previousValue = 0L;
    this.storedVal = 0L;
    this.gorillaDecoder.leadingZeros = Integer.MAX_VALUE;
    this.gorillaDecoder.trailingZeros = 0;
  }

  // Cache for batch decoding
  private double[] valueCache = new double[0];
  private int cacheIndex = 0;
  private int cacheSize = 0;

  // === Added reusable buffer for getValues ===
  private double[] valuesBuffer = new double[16];

  @Override
  public double readDouble(ByteBuffer buffer) {
    try {
      if (cacheIndex >= cacheSize) {
        if (in == null || in.availableBits() == 0) {
          if (!buffer.hasRemaining()) {
            throw new TsFileDecodingException("No more data to decode");
          }
          // read next chunk
          ByteBuffer slice = buffer.slice();
          ByteBufferBackedInputStream bais = new ByteBufferBackedInputStream(slice);
          int blockBits = ReadWriteForEncodingUtils.readVarInt(bais);
          this.in = new BitInputStream(bais, blockBits);
          // reset state
          this.isFirst = true;
          this.storedVal = 0L;
          this.previousValue = 0L;
          this.gorillaDecoder.leadingZeros = Integer.MAX_VALUE;
          this.gorillaDecoder.trailingZeros = 0;
          // decode current block
          double[] newValues = getValues();
          if (newValues.length == 0) {
            throw new TsFileDecodingException("Unexpected empty block");
          }
          valueCache = newValues;
          cacheSize = newValues.length;
          cacheIndex = 0;
          int consumed = bais.getConsumed();
          buffer.position(buffer.position() + consumed);
        }
      }
      return valueCache[cacheIndex++];
    } catch (IOException e) {
      throw new TsFileDecodingException(e.getMessage());
    }
  }

  /** Nested class to handle fallback encoding (Gorilla) for double values. */
  public class GorillaDecoder {
    private int leadingZeros = Integer.MAX_VALUE;
    private int trailingZeros = 0;

    /** Decode next value using Gorilla algorithm. */
    public double decode(BitInputStream in) throws IOException {
      if (isFirst) {
        previousValue = in.readLong(BITS_FOR_FIRST_VALUE);
        isFirst = false;
        return Double.longBitsToDouble(previousValue);
      }

      boolean controlBit = in.readBit();
      if (!controlBit) {
        return Double.longBitsToDouble(previousValue);
      }

      boolean reuseBlock = !in.readBit();
      long xor;
      if (reuseBlock) {
        int sigBits = DOUBLE_TOTAL_BITS - leadingZeros - trailingZeros;
        if (sigBits == 0) {
          return Double.longBitsToDouble(previousValue);
        }
        xor = in.readLong(sigBits) << trailingZeros;
      } else {
        leadingZeros = in.readInt(BITS_FOR_LEADING_ZEROS);
        int sigBits = in.readInt(BITS_FOR_SIGNIFICANT_BITS) + 1;
        trailingZeros = DOUBLE_TOTAL_BITS - leadingZeros - sigBits;
        xor = in.readLong(sigBits) << trailingZeros;
      }

      previousValue ^= xor;
      return Double.longBitsToDouble(previousValue);
    }
  }

  /** Retrieve nested GorillaDecoder. */
  public GorillaDecoder getGorillaDecoder() {
    return gorillaDecoder;
  }

  /** Read all values until the stream is exhausted, reusing valuesBuffer. */
  public double[] getValues() throws IOException {
    int count = 0;
    while (in.availableBits() > 0) {
      double val = next();
      if (count == valuesBuffer.length) {
        valuesBuffer = Arrays.copyOf(valuesBuffer, valuesBuffer.length * 2);
      }
      valuesBuffer[count++] = val;
    }
    return Arrays.copyOf(valuesBuffer, count);
  }

  /** Decode next available value, return null if no more bits. */
  private double next() throws IOException {
    double result;
    if (isFirst) {
      isFirst = false;
      long firstBits = in.readLong(BITS_FOR_FIRST_VALUE);
      result = Double.longBitsToDouble(firstBits);
      storedVal = (long) result;
    } else {
      result = nextValue();
    }
    previousValue = Double.doubleToLongBits(result);
    return result;
  }

  /** Decode according to Camel vs Gorilla path. */
  private double nextValue() throws IOException {
    // Read sign bit
    int signBit = in.readInt(BITS_FOR_SIGN);
    double sign = signBit == 1 ? -1.0 : 1.0;
    // Read encoding type bit
    int typeBit = in.readInt(BITS_FOR_TYPE);
    boolean useCamel = typeBit == 1;

    if (useCamel) {
      long intPart = readLong();
      // decimal = decPart / scale
      double decPart = readDecimal();
      double value =
          (intPart >= 0
              ? (intPart * scale + decPart) / scale
              : -(intPart * scale + decPart) / scale);
      return sign * value;
    } else {
      return sign * gorillaDecoder.decode(in);
    }
  }

  /** Read variable-length integer diff and update storedVal. */
  private long readLong() throws IOException {
    long diff = BitInputStream.readVarLong(in);
    storedVal += diff;
    return storedVal;
  }

  /** Read and reconstruct decimal component. */
  private double readDecimal() throws IOException {
    int count = in.readInt(BITS_FOR_DECIMAL_COUNT) + 1;
    boolean hasXor = in.readBit();
    long xor = 0;
    if (hasXor) {
      long bits = in.readLong(count);
      xor = bits << (DOUBLE_MANTISSA_BITS - count);
    }
    long mVal = BitInputStream.readVarLong(in);
    double frac;
    if (hasXor) {
      double base = (double) mVal / powers[count - 1] + 1;
      long merged = xor ^ Double.doubleToLongBits(base);
      frac = Double.longBitsToDouble(merged) - 1;
    } else {
      frac = (double) mVal / powers[count - 1];
    }
    // Round to original scale
    scale = Math.pow(10, count);
    return Math.round(frac * scale);
  }
}
