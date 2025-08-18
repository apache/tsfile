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

package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.common.bitStream.BitOutputStream;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CamelEncoder extends Encoder {
  private final GorillaEncoder gorillaEncoder;

  // === Constants for encoding ===
  private static final int BITS_FOR_SIGN = 1;
  private static final int BITS_FOR_TYPE = 1;
  private static final int BITS_FOR_FIRST_VALUE = 64;
  private static final int BITS_FOR_LEADING_ZEROS = 6;
  private static final int BITS_FOR_SIGNIFICANT_BITS = 6;
  private static final int BITS_FOR_DECIMAL_COUNT = 4;
  private static final int DOUBLE_TOTAL_BITS = 64;
  private static final int DOUBLE_MANTISSA_BITS = 52;
  private static final int DECIMAL_MAX_COUNT = 10;

  // === Camel state ===
  private long storedVal = 0;
  private boolean isFirst = true;
  long previousValue = 0;
  private boolean hasPending = false; // guard for empty or duplicate flush

  // === Precomputed tables ===
  public static final long[] powers = new long[DECIMAL_MAX_COUNT];
  public static final long[] threshold = new long[DECIMAL_MAX_COUNT];

  private final BitOutputStream out;
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

  static {
    for (int l = 1; l <= DECIMAL_MAX_COUNT; l++) {
      int idx = l - 1;
      powers[idx] = (long) Math.pow(10, l);
      long divisor = 1L << l;
      threshold[idx] = powers[idx] / divisor;
    }
  }

  public CamelEncoder() {
    super(TSEncoding.CAMEL);
    out = new BitOutputStream(baos);
    gorillaEncoder = new GorillaEncoder();
  }

  /**
   * Encode a double value and buffer bits for later flush. Marks that there is pending data to
   * flush.
   *
   * @param value the double value to encode
   * @param out unused here, uses internal buffer
   */
  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    try {
      this.addValue(value);
      hasPending = true;
    } catch (IOException ignored) {
    }
  }

  /**
   * Flush buffered encoded values to the provided stream. Writes a header indicating the number of
   * bits written, followed by the buffered bit data. Resets internal buffers and state afterward.
   * Consecutive calls without new data are no-ops.
   *
   * @param out the destination ByteArrayOutputStream to write flushed data
   * @throws IOException if an I/O error occurs during flush
   */
  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    if (!hasPending) {
      return;
    }
    int writtenBits = close();
    ReadWriteForEncodingUtils.writeVarInt(writtenBits, out);
    this.baos.writeTo(out);
    this.baos.reset();
    this.out.reset(this.baos);
    resetState();
    hasPending = false;
  }

  /**
   * Reset encoder state to initial conditions for a new block. Clears Camel and nested Gorilla
   * state, and resets pending flag.
   */
  private void resetState() {
    this.isFirst = true;
    this.storedVal = 0L;
    this.previousValue = 0L;
    this.hasPending = false;
    // reset Gorilla state
    this.gorillaEncoder.leadingZeros = Integer.MAX_VALUE;
    this.gorillaEncoder.trailingZeros = 0;
  }

  @Override
  public int getOneItemMaxSize() {
    return 8;
  }

  @Override
  public long getMaxByteSize() {
    // bitstream buffer | bytes buffer | storedVal | previousValue
    return 1 + this.baos.size() + 8 + 8;
  }

  public class GorillaEncoder {
    private int leadingZeros = Integer.MAX_VALUE;
    private int trailingZeros = 0;

    public void encode(double value, BitOutputStream out) throws IOException {
      long curr = Double.doubleToLongBits(value);
      if (isFirst) {
        out.writeLong(curr, BITS_FOR_FIRST_VALUE);
        previousValue = curr;
        isFirst = false;
        return;
      }

      long xor = curr ^ previousValue;
      if (xor == 0) {
        out.writeBit(false); // Control bit: same as previous
      } else {
        out.writeBit(true); // Control bit: value changed
        int leading = Long.numberOfLeadingZeros(xor);
        int trailing = Long.numberOfTrailingZeros(xor);
        if (leading >= leadingZeros && trailing >= trailingZeros) {
          out.writeBit(false); // Reuse previous block info
          int significantBits = DOUBLE_TOTAL_BITS - leadingZeros - trailingZeros;
          out.writeLong(xor >>> trailingZeros, significantBits);
        } else {
          out.writeBit(true); // Write new block info
          out.writeInt(leading, BITS_FOR_LEADING_ZEROS);
          int significantBits = DOUBLE_TOTAL_BITS - leading - trailing;
          out.writeInt(significantBits - 1, BITS_FOR_SIGNIFICANT_BITS);
          out.writeLong(xor >>> trailing, significantBits);
          leadingZeros = leading;
          trailingZeros = trailing;
        }
      }

      previousValue = curr;
    }

    public void close(BitOutputStream out) throws IOException {
      out.close();
    }
  }

  public void addValue(double value) throws IOException {
    if (isFirst) {
      writeFirst(Double.doubleToRawLongBits(value));
    } else {
      compressValue(value);
    }
    previousValue = Double.doubleToLongBits(value);
  }

  private void writeFirst(long value) throws IOException {
    isFirst = false;
    storedVal = (long) Double.longBitsToDouble(value);
    out.writeLong(value, BITS_FOR_FIRST_VALUE);
  }

  public int close() throws IOException {
    out.close();
    return out.getBitsWritten();
  }

  private void compressValue(double value) throws IOException {
    int signBit = (int) ((Double.doubleToLongBits(value) >>> (DOUBLE_TOTAL_BITS - 1)) & 1);
    out.writeInt(signBit, BITS_FOR_SIGN);

    value = Math.abs(value);
    if (value > Long.MAX_VALUE
        || value == 0
        || Math.abs(Math.floor(Math.log10(value))) > DECIMAL_MAX_COUNT) {
      out.writeInt(CamelInnerEncodingType.GORILLA.getCode(), BITS_FOR_TYPE);
      gorillaEncoder.encode(value, out);
      return;
    }

    long integerPart = (long) value;
    int numDigits = 1;
    long absInt = Math.abs(integerPart);
    while (absInt >= 10) {
      absInt /= 10;
      numDigits++;
    }

    double factor = 1;
    int decimalCount = 0;
    while (Math.abs(value * factor - Math.round(value * factor)) > 0) {
      factor *= 10.0;
      decimalCount++;
      if (numDigits + decimalCount > DECIMAL_MAX_COUNT) {
        break;
      }
    }

    decimalCount = Math.max(1, decimalCount);
    long decimalValue;

    if (decimalCount + numDigits <= DECIMAL_MAX_COUNT) {
      long pow = powers[decimalCount - 1];
      decimalValue = Math.round(value * pow) % pow;

      out.writeInt(CamelInnerEncodingType.CAMEL.getCode(), BITS_FOR_TYPE);
      compressIntegerValue(integerPart);
      compressDecimalValue(decimalValue, decimalCount);
    } else {
      out.writeInt(CamelInnerEncodingType.GORILLA.getCode(), BITS_FOR_TYPE);
      gorillaEncoder.encode(value, out);
    }
  }

  private void compressIntegerValue(long value) throws IOException {
    long diff = value - storedVal;
    storedVal = value;
    BitOutputStream.writeVarLong(diff, out);
  }

  private void compressDecimalValue(long decimalValue, int decimalCount) throws IOException {
    out.writeInt(decimalCount - 1, BITS_FOR_DECIMAL_COUNT);
    long thresh = threshold[decimalCount - 1];
    int m = (int) decimalValue;

    if (decimalValue >= thresh) {
      out.writeBit(true);
      m = (int) (decimalValue % thresh);

      long xor =
          Double.doubleToLongBits((double) decimalValue / powers[decimalCount - 1] + 1)
              ^ Double.doubleToLongBits((double) m / powers[decimalCount - 1] + 1);

      out.writeLong(xor >>> (DOUBLE_MANTISSA_BITS - decimalCount), decimalCount);
    } else {
      out.writeBit(false);
    }

    BitOutputStream.writeVarLong(m, out);
  }

  public int getWrittenBits() {
    return out.getBitsWritten();
  }

  public ByteArrayOutputStream getByteArrayOutputStream() {
    return this.baos;
  }

  public GorillaEncoder getGorillaEncoder() {
    return gorillaEncoder;
  }

  public enum CamelInnerEncodingType {
    GORILLA(0),
    CAMEL(1);

    private final int code;

    CamelInnerEncodingType(int code) {
      this.code = code;
    }

    public int getCode() {
      return code;
    }
  }
}
