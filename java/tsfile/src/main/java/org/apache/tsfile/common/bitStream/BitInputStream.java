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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/** A stream for reading individual bits or groups of bits from an InputStream. */
public class BitInputStream extends BitStream {

  protected InputStream in;
  protected int buffer;
  protected int bufferBitCount;
  protected final long totalBits; // Total valid bits
  protected long bitsRead = 0; // Number of bits read so far

  protected int markedBuffer = 0;
  protected int markedBufferBitCount = 0;
  protected long markedBitsRead = 0;

  /**
   * Constructs a BitInputStream with a given InputStream and total number of valid bits.
   *
   * @param in the underlying InputStream
   * @param totalBits the total number of valid bits to read
   */
  public BitInputStream(InputStream in, long totalBits) {
    this.in = in;
    this.totalBits = totalBits;
    this.bufferBitCount = 0;
  }

  /**
   * Reads an integer value using the specified number of bits. If fewer bits are available, only
   * the available bits are returned.
   *
   * @param numBits the number of bits to read (≤ 32)
   * @return an integer whose lower bits contain the read value
   * @throws EOFException if no data is available to read
   * @throws IOException if an I/O error occurs
   */
  public int readInt(int numBits) throws IOException {
    if (availableBits() <= 0) {
      throw new EOFException();
    }

    bitsRead += numBits;
    int result = 0;
    boolean hasReadData = false;

    while (numBits > 0) {
      if (bufferBitCount == 0) {
        buffer = in.read();
        if (buffer < 0) {
          if (!hasReadData) {
            throw new EOFException();
          }
          return result;
        }
        bufferBitCount = BITS_PER_BYTE;
      }

      if (bufferBitCount > numBits) {
        result = ((buffer >> (bufferBitCount - numBits)) & MASKS[numBits]) | result;
        bufferBitCount -= numBits;
        numBits = 0;
      } else {
        result = ((buffer & MASKS[bufferBitCount]) << (numBits - bufferBitCount)) | result;
        numBits -= bufferBitCount;
        bufferBitCount = 0;
      }

      hasReadData = true;
    }

    return result;
  }

  /**
   * Reads a long value using the specified number of bits.
   *
   * @param numBits the number of bits to read (0 to 64)
   * @return a long value containing the read bits
   * @throws EOFException if no data is available to read
   * @throws IOException if an I/O error occurs
   */
  public long readLong(int numBits) throws IOException {
    if (availableBits() <= 0) {
      throw new EOFException();
    }
    bitsRead += numBits;
    if (numBits > 64 || numBits < 0) {
      throw new IllegalArgumentException("numBits must be between 0 and 64");
    }

    long result = 0;
    boolean hasReadData = false;

    while (numBits > 0) {
      if (bufferBitCount == 0) {
        buffer = in.read();
        if (buffer < 0) {
          if (!hasReadData) {
            throw new EOFException();
          }
          return result;
        }
        bufferBitCount = BITS_PER_BYTE;
      }

      if (bufferBitCount > numBits) {
        int shift = bufferBitCount - numBits;
        result = (result << numBits) | ((buffer >> shift) & MASKS[numBits]);
        bufferBitCount -= numBits;
        buffer &= MASKS[bufferBitCount];
        numBits = 0;
      } else {
        result = (result << bufferBitCount) | (buffer & MASKS[bufferBitCount]);
        numBits -= bufferBitCount;
        bufferBitCount = 0;
      }

      hasReadData = true;
    }

    return result;
  }

  public static int readVarInt(BitInputStream in) throws IOException {
    int result = 0;
    int shift = 0;

    while (true) {
      int chunk = in.readInt(7);
      boolean hasNext = in.readBit();
      result |= chunk << shift;
      if (!hasNext) break;
      shift += 7;
      if (shift >= 32) throw new IOException("VarInt too long");
    }

    return (result >>> 1) ^ -(result & 1);
  }

  public static long readVarLong(BitInputStream in) throws IOException {
    long result = 0;
    int shift = 0;

    while (true) {
      long chunk = in.readInt(7);
      boolean hasNext = in.readBit();

      result |= (chunk) << shift;
      shift += 7;

      if (!hasNext) {
        break;
      }

      if (shift >= 64) {
        throw new IOException("VarLong too long: overflow");
      }
    }

    // ZigZag 解码
    return (result >>> 1) ^ -(result & 1);
  }

  /**
   * Reads a single bit from the stream.
   *
   * @return true if the bit is 1, false if it is 0
   * @throws EOFException if no bits are available
   * @throws IOException if an I/O error occurs
   */
  public boolean readBit() throws IOException {
    if (availableBits() <= 0) {
      throw new EOFException();
    }
    bitsRead += 1;
    if (bufferBitCount == 0) {
      buffer = in.read();
      if (buffer < 0) {
        throw new EOFException();
      }
      bufferBitCount = BITS_PER_BYTE;
    }

    boolean bit = ((buffer >> (bufferBitCount - 1)) & 1) != 0;
    bufferBitCount--;
    return bit;
  }

  /**
   * Returns whether this stream supports mark/reset.
   *
   * @return true if mark/reset is supported
   */
  public boolean markSupported() {
    return in.markSupported();
  }

  /**
   * Marks the current position in the stream.
   *
   * @param readLimit the maximum number of bits that can be read before the mark becomes invalid
   */
  public void mark(int readLimit) {
    in.mark((readLimit + BITS_PER_BYTE - 1) / BITS_PER_BYTE);
    markedBuffer = buffer;
    markedBufferBitCount = bufferBitCount;
    markedBitsRead = bitsRead;
  }

  /**
   * Resets the stream to the most recent marked position.
   *
   * @throws IOException if mark was not called or has been invalidated
   */
  public void reset() throws IOException {
    in.reset();
    buffer = markedBuffer;
    bufferBitCount = markedBufferBitCount;
    bitsRead = markedBitsRead;
  }

  /**
   * Returns the number of bits still available to read.
   *
   * @return the number of remaining available bits
   * @throws IOException if an I/O error occurs
   */
  public int availableBits() throws IOException {
    return (int)
        Math.min(((long) in.available() * BITS_PER_BYTE) + bufferBitCount, totalBits - bitsRead);
  }

  /**
   * Closes the underlying InputStream.
   *
   * @throws IOException if an I/O error occurs
   */
  public void close() throws IOException {
    in.close();
  }
}
