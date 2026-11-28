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

import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;

/**
 * This class includes code modified from Michael Burman's gorilla-tsc project.
 *
 * <p>Copyright: 2016-2018 Michael Burman and/or other contributors
 *
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public abstract class GorillaEncoderV2 extends Encoder {

  protected boolean firstValueWasWritten = false;
  protected int storedLeadingZeros = Integer.MAX_VALUE;
  protected int storedTrailingZeros = 0;

  private long bitBuffer;
  private int bitsUsed;

  /** Reusable 8-byte buffer so a full 64-bit group is written with one call. */
  private final byte[] writeBuffer = new byte[Long.BYTES];

  protected GorillaEncoderV2() {
    super(TSEncoding.GORILLA);
  }

  @Override
  public final long getMaxByteSize() {
    return 0;
  }

  @Override
  public int getPendingByteCount() {
    return bitsUsed >>> 3;
  }

  protected void reset() {
    firstValueWasWritten = false;
    storedLeadingZeros = Integer.MAX_VALUE;
    storedTrailingZeros = 0;

    bitBuffer = 0;
    bitsUsed = 0;
  }

  /** Stores a 0 and increases the count of bits by 1. */
  protected void skipBit(ByteArrayOutputStream out) {
    appendBit(0, out);
  }

  /** Stores a 1 and increases the count of bits by 1. */
  protected void writeBit(ByteArrayOutputStream out) {
    appendBit(1, out);
  }

  private void appendBit(int bit, ByteArrayOutputStream out) {
    bitBuffer = (bitBuffer << 1) | bit;
    if (++bitsUsed == Long.SIZE) {
      writeFullLong(out);
    }
  }

  /**
   * Writes the given long value using the defined amount of least significant bits.
   *
   * @param value The value to write
   * @param bits How many bits are stored to the stream
   */
  protected void writeBits(long value, int bits, ByteArrayOutputStream out) {
    while (bits > 0) {
      int take = Math.min(bits, Long.SIZE - bitsUsed);
      if (take == Long.SIZE) {
        // bitsUsed is 0 here; taking 64 bits must not use a shift (Java shifts by 64 as 0),
        // otherwise a stale bitBuffer would leak into the stream.
        bitBuffer = value;
      } else {
        bitBuffer = (bitBuffer << take) | ((value >>> (bits - take)) & ((1L << take) - 1L));
      }
      bitsUsed += take;
      bits -= take;
      if (bitsUsed == Long.SIZE) {
        writeFullLong(out);
      }
    }
  }

  private void writeFullLong(ByteArrayOutputStream out) {
    byte[] buffer = writeBuffer;
    buffer[0] = (byte) (bitBuffer >>> 56);
    buffer[1] = (byte) (bitBuffer >>> 48);
    buffer[2] = (byte) (bitBuffer >>> 40);
    buffer[3] = (byte) (bitBuffer >>> 32);
    buffer[4] = (byte) (bitBuffer >>> 24);
    buffer[5] = (byte) (bitBuffer >>> 16);
    buffer[6] = (byte) (bitBuffer >>> 8);
    buffer[7] = (byte) bitBuffer;
    out.write(buffer, 0, Long.BYTES);
    bitBuffer = 0;
    bitsUsed = 0;
  }

  /**
   * Flushes the bits accumulated so far, padding the last byte with zeros. This reproduces the
   * previous byte-at-a-time output exactly, including the extra zero byte written when the stream
   * ends on a byte boundary (the decoder expects it).
   */
  protected void flushRemainingBits(ByteArrayOutputStream out) {
    if (bitsUsed == 0) {
      out.write(0);
      bitBuffer = 0;
      return;
    }
    int fullBytes = bitsUsed >>> 3;
    int remainder = bitsUsed & 7;
    long aligned = bitBuffer << (Long.SIZE - bitsUsed);
    byte[] buffer = writeBuffer;
    for (int i = 0; i < fullBytes; i++) {
      buffer[i] = (byte) (aligned >>> (56 - 8 * i));
    }
    if (remainder == 0) {
      out.write(buffer, 0, fullBytes);
      // The empty ending byte is necessary when decoding.
      out.write(0);
    } else {
      buffer[fullBytes] = (byte) (aligned >>> (56 - 8 * fullBytes));
      out.write(buffer, 0, fullBytes + 1);
    }
    bitBuffer = 0;
    bitsUsed = 0;
  }
}
