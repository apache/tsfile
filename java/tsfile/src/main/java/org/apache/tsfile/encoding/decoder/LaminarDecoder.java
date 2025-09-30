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

public class LaminarDecoder extends Decoder {
  private int numberRemainingInCurrentBlock = 0, totalInCurrentBlock = 0;
  private long[] currentBlockValues = null;

  public LaminarDecoder() {
    super(TSEncoding.LAMINAR);
  }

  private long[] loadDecodeArray(ByteBuffer buffer) {
    int n = ReadWriteIOUtils.readInt(buffer);
    long[] values = new long[n];

    if (n > 0) {
      int[] laminarBitWidths = new int[n];
      int currentLaminarBitWidth = ReadWriteIOUtils.readInt(buffer);
      laminarBitWidths[0] = currentLaminarBitWidth;

      IntRleDecoder rleDecoder = new IntRleDecoder();
      for (int i = 1; i < n; i++) {
        while (rleDecoder.readInt(buffer) == 1) {
          currentLaminarBitWidth--;
        }
        laminarBitWidths[i] = currentLaminarBitWidth;
      }

      int totalBits = 0;
      for (int width : laminarBitWidths) totalBits += width;
      int encodingLength = DescendingBitPackingDecoder.bitsToBytes(totalBits);
      byte[] currentBuffer = new byte[encodingLength];
      buffer.get(currentBuffer);
      int offset = 0;
      for (int i = 0; i < n; i++) {
        if (laminarBitWidths[i] > 0) {
          values[i] = BytesUtils.bytesToLong(currentBuffer, offset, laminarBitWidths[i]);
          offset += laminarBitWidths[i];
        }
      }
    }
    return values;
  }

  private void loadNextBlock(ByteBuffer buffer) {
    byte[] currentBuffer = null;
    int n = ReadWriteIOUtils.readInt(buffer);

    if (n > 0) {
      this.currentBlockValues = new long[n];
      this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = n;

      int p = ReadWriteIOUtils.readInt(buffer);

      long[] denseValues = loadDecodeArray(buffer);
      for (int i = 0; i < p; i++) this.currentBlockValues[i] = denseValues[i];

      long[] sparseValues = loadDecodeArray(buffer);
      int indexBitWidth = DescendingBitPackingDecoder.getValueWidth(n - 1);
      int encodingLength =
          DescendingBitPackingDecoder.bitsToBytes(indexBitWidth * sparseValues.length);
      currentBuffer = new byte[encodingLength];
      buffer.get(currentBuffer);
      for (int i = 0; i < sparseValues.length; i++) {
        int currentIndex =
            Math.toIntExact(
                BytesUtils.bytesToLong(currentBuffer, indexBitWidth * i, indexBitWidth));
        this.currentBlockValues[currentIndex + p] = sparseValues[i];
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
    return DescendingBitPackingDecoder.zigzagDecode(value);
  }

  @Override
  public void reset() {
    this.currentBlockValues = null;
    this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = 0;
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    if (numberRemainingInCurrentBlock > 0) {
      return true;
    }
    return buffer.hasRemaining();
  }

  public static class IntLaminarDecoder extends LaminarDecoder {

    public IntLaminarDecoder() {
      super();
    }

    @Override
    public int readInt(ByteBuffer buffer) {
      return Math.toIntExact(super.readLong(buffer));
    }
  }
}
