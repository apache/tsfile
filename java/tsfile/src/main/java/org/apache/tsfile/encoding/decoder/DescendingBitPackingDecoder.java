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

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class DescendingBitPackingDecoder extends Decoder {
  private boolean isSigned;
  private int numberRemainingInCurrentBlock = 0, totalInCurrentBlock = 0;
  private long[] currentBlockValues = null;

  private int bitsToBytes(int bits) {
    return (bits + 7) / 8;
  }

  private static int getValueWidth(long value) {
    return 64 - Long.numberOfLeadingZeros(value);
  }

  private static long zigzagDecode(long value) {
    return (value >>> 1) ^ -(value & 1);
  }

  public DescendingBitPackingDecoder(boolean isSigned) {
    super(TSEncoding.DESCENDING_BIT_PACKING);
    this.isSigned = isSigned;
  }

  public DescendingBitPackingDecoder() {
    this(true);
  }

  private void loadNextBlock(ByteBuffer buffer) {
    byte[] currentBuffer = null;

    int n = ReadWriteIOUtils.readInt(buffer);
    if (n > 0) {
      this.currentBlockValues = new long[n];
      this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = n;
      int m = ReadWriteIOUtils.readInt(buffer);
      if (m > 0) {
        int indexBitWidth = getValueWidth(n - 1);
        int encodingLength = bitsToBytes(indexBitWidth * m);
        currentBuffer = new byte[encodingLength];
        buffer.get(currentBuffer);
        long[] sortedIndicesArray = new long[m];
        for (int i = 0; i < m; i++) {
          sortedIndicesArray[i] = BytesUtils.bytesToLong(currentBuffer, indexBitWidth * i, indexBitWidth);
        }
        currentBuffer = null;

        encodingLength = ReadWriteIOUtils.readInt(buffer);
        currentBuffer = new byte[encodingLength];
        int offset = 0;
        int previousValueWidth = ReadWriteIOUtils.readInt(buffer);
        buffer.get(currentBuffer);
        long tmp;
        for (int i = 0; i < m; i++) {
          tmp = BytesUtils.bytesToLong(currentBuffer, offset, previousValueWidth);
          this.currentBlockValues[Math.toIntExact(sortedIndicesArray[i])] = tmp;
          offset += previousValueWidth;
          previousValueWidth = getValueWidth(tmp);
        }
      }
    } else {
      this.currentBlockValues = new long[0];
      this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = 0;
    }
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    if (numberRemainingInCurrentBlock == 0) {
      loadNextBlock(buffer);
    }
    numberRemainingInCurrentBlock--;
    long value = currentBlockValues[totalInCurrentBlock - numberRemainingInCurrentBlock - 1];
    return isSigned ? zigzagDecode(value) : value;
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    if (numberRemainingInCurrentBlock > 0) {
      return true;
    }
    return buffer.hasRemaining();
  }

  @Override
  public void reset() {
    this.currentBlockValues = null;
    this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = 0;
  }

  public static class IntDescendingBitPackingDecoder extends DescendingBitPackingDecoder {
    public IntDescendingBitPackingDecoder() {
      super();
    }

    @Override
    public int readInt(ByteBuffer buffer) {
      return Math.toIntExact(super.readLong(buffer));
    }
  }
}
