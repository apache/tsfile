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

public class SeparateStorageDecoder extends Decoder {
  private boolean isSigned;
  private int numberRemainingInCurrentBlock = 0, totalInCurrentBlock = 0;
  private long[] currentBlockValues = null;

  public SeparateStorageDecoder(boolean isSigned) {
    super(TSEncoding.SEPARATE_STORAGE);
    this.isSigned = isSigned;
  }

  public SeparateStorageDecoder() {
    this(true);
  }

  private void loadNextBlock(ByteBuffer buffer) {
    byte[] currentBuffer = null;

    int n = ReadWriteIOUtils.readInt(buffer);
    if (n > 0) {
      int optimalWidth = ReadWriteIOUtils.readInt(buffer);

      Long[] highBits = new Long[n];
      Long[] lowBits = new Long[n];

      DescendingBitPackingDecoder highBitsDecoder = new DescendingBitPackingDecoder(false);
      for (int i = 0; i < n; i++) {
        highBits[i] = highBitsDecoder.readLong(buffer);
      }

      if (optimalWidth > 0) {
        int encodingLength = DescendingBitPackingDecoder.bitsToBytes(optimalWidth * n);
        currentBuffer = new byte[encodingLength];
        buffer.get(currentBuffer);
        for (int i = 0; i < n; i++) {
          lowBits[i] = BytesUtils.bytesToLong(currentBuffer, optimalWidth * i, optimalWidth);
        }
      }

      this.currentBlockValues = new long[n];
      this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = n;
      for (int i = 0; i < n; i++)
        this.currentBlockValues[i] = (highBits[i] << optimalWidth) | lowBits[i];
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
    return isSigned ? DescendingBitPackingDecoder.zigzagDecode(value) : value;
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

  public static class IntSeparateStorageDecoder extends SeparateStorageDecoder {
    public IntSeparateStorageDecoder() {
      super();
    }

    @Override
    public int readInt(ByteBuffer buffer) {
      return Math.toIntExact(super.readLong(buffer));
    }
  }
}
