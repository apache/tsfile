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

import org.apache.tsfile.encoding.encoder.FleaEncoder;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FleaDecoder extends Decoder {
  private int numberRemainingInCurrentBlock = 0, totalInCurrentBlock = 0;
  private long[] currentBlockValues = null;

  private static final boolean DEBUG_TIMING = Boolean.getBoolean("flea.debug.timing");
  private static long totalLaminarRealNs = 0, totalLaminarImagNs = 0;
  private static long totalIfftNs = 0, totalResidualNs = 0, totalCombineNs = 0;
  private static int blockCount = 0;

  public static void resetTiming() {
    totalLaminarRealNs = totalLaminarImagNs = totalIfftNs = totalResidualNs = totalCombineNs = 0;
    blockCount = 0;
  }

  public static void printTiming() {
    if (DEBUG_TIMING && blockCount > 0) {
      double total = totalLaminarRealNs + totalLaminarImagNs + totalIfftNs + totalResidualNs + totalCombineNs;
      System.err.printf("[FLEA Decode Timing] blocks=%d\n", blockCount);
      System.err.printf(
          "  LaminarReal: %8.3f ms (%5.1f%%)\n",
          totalLaminarRealNs / 1e6, 100.0 * totalLaminarRealNs / total);
      System.err.printf(
          "  LaminarImag: %8.3f ms (%5.1f%%)\n",
          totalLaminarImagNs / 1e6, 100.0 * totalLaminarImagNs / total);
      System.err.printf(
          "  IFFT:        %8.3f ms (%5.1f%%)\n", totalIfftNs / 1e6, 100.0 * totalIfftNs / total);
      System.err.printf(
          "  Residual:    %8.3f ms (%5.1f%%)\n",
          totalResidualNs / 1e6, 100.0 * totalResidualNs / total);
      System.err.printf(
          "  Combine:     %8.3f ms (%5.1f%%)\n",
          totalCombineNs / 1e6, 100.0 * totalCombineNs / total);
      System.err.printf("  Total:       %8.3f ms\n", total / 1e6);
    }
  }

  public FleaDecoder() {
    super(TSEncoding.FLEA);
  }

  private void loadNextBlock(ByteBuffer buffer) {
    int n = ReadWriteIOUtils.readInt(buffer);

    if (n > 0) {
      int paddingAmount = buffer.get() & 0xFF;
      int paddedN = n + paddingAmount;
      int freqLen = paddedN / 2 + 1;

      int beta = ReadWriteIOUtils.readInt(buffer);

      long startNs, endNs;

      startNs = DEBUG_TIMING ? System.nanoTime() : 0;
      LaminarDecoder laminarDecoder = new LaminarDecoder();
      long[] quantizedReal = new long[freqLen], quantizedImag = new long[freqLen];
      for (int i = 0; i < freqLen; i++) {
        quantizedReal[i] = laminarDecoder.readLong(buffer);
      }
      if (DEBUG_TIMING) {
        endNs = System.nanoTime();
        totalLaminarRealNs += (endNs - startNs);
      }

      startNs = DEBUG_TIMING ? System.nanoTime() : 0;
      laminarDecoder = new LaminarDecoder();
      for (int i = 0; i < freqLen; i++) {
        quantizedImag[i] = laminarDecoder.readLong(buffer);
      }
      if (DEBUG_TIMING) {
        endNs = System.nanoTime();
        totalLaminarImagNs += (endNs - startNs);
      }

      startNs = DEBUG_TIMING ? System.nanoTime() : 0;
      double[][] dequantized = new double[2][freqLen];
      for (int i = 0; i < freqLen; i++) {
        dequantized[0][i] = FleaEncoder.dequantize(quantizedReal[i], beta);
        dequantized[1][i] = FleaEncoder.dequantize(quantizedImag[i], beta);
      }
      double[] reconstructedFull = FleaEncoder.inverseRealFFT(dequantized, paddedN);
      if (DEBUG_TIMING) {
        endNs = System.nanoTime();
        totalIfftNs += (endNs - startNs);
      }

      startNs = DEBUG_TIMING ? System.nanoTime() : 0;
      long[] residuals = new long[n];
      SeparateStorageDecoder separateStorageDecoder = new SeparateStorageDecoder();
      for (int i = 0; i < n; i++) {
        residuals[i] = separateStorageDecoder.readLong(buffer);
      }
      if (DEBUG_TIMING) {
        endNs = System.nanoTime();
        totalResidualNs += (endNs - startNs);
      }

      startNs = DEBUG_TIMING ? System.nanoTime() : 0;
      this.currentBlockValues = new long[n];
      this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = n;
      for (int i = 0; i < n; i++) {
        this.currentBlockValues[i] = residuals[i] + Math.round(reconstructedFull[i]);
      }
      if (DEBUG_TIMING) {
        endNs = System.nanoTime();
        totalCombineNs += (endNs - startNs);
        blockCount++;
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
    return value;
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

  public static class IntFleaDecoder extends FleaDecoder {

    public IntFleaDecoder() {
      super();
    }

    @Override
    public int readInt(ByteBuffer buffer) {
      return Math.toIntExact(super.readLong(buffer));
    }
  }
}
