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

import java.nio.ByteBuffer;

public class LaminarDecoder extends GorillaDecoderV2 {
  private int numberRemainingInCurrentBlock = 0, totalInCurrentBlock = 0;
  private long[] currentBlockValues = null;

  public LaminarDecoder() {
    this.setType(TSEncoding.LAMINAR);
    this.hasNext = true;
  }

  private long[] loadDecodeArray(ByteBuffer buffer) {
    int n = Math.toIntExact(readLong(32, buffer));
    long[] values = new long[n];

    if (n > 0) {
      int[] laminarBitWidths = new int[n];
      int currentLaminarBitWidth = Math.toIntExact(readLong(32, buffer));
      laminarBitWidths[0] = currentLaminarBitWidth;
      for (int i = 1; i < n; i++) {
        while (readBit(buffer)) {
          currentLaminarBitWidth--;
        }
        laminarBitWidths[i] = currentLaminarBitWidth;
      }

      for (int i = 0; i < n; i++) {
        if (laminarBitWidths[i] > 0) values[i] = readLong(laminarBitWidths[i], buffer);
      }
    }
    return values;
  }

  private void loadNextBlock(ByteBuffer buffer) {
    int n = Math.toIntExact(readLong(32, buffer));

    if (n > 0) {
      this.currentBlockValues = new long[n];
      this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = n;

      int p = Math.toIntExact(readLong(32, buffer));

      long[] denseValues = loadDecodeArray(buffer);
      for (int i = 0; i < p; i++) this.currentBlockValues[i] = denseValues[i];

      int indexBitWidth = DescendingBitPackingDecoder.getValueWidth(n - 1);
      long[] sparseValues = loadDecodeArray(buffer);
      for (int i = 0; i < sparseValues.length; i++) {
        int currentIndex = Math.toIntExact(readLong(indexBitWidth, buffer));
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
    if (numberRemainingInCurrentBlock == 0 && buffer.remaining() == 0) {
      hasNext = false;
    }
    long value = currentBlockValues[totalInCurrentBlock - numberRemainingInCurrentBlock - 1];
    return DescendingBitPackingDecoder.zigzagDecode(value);
  }

  @Override
  public void reset() {
    super.reset();

    this.currentBlockValues = null;
    this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = 0;
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
