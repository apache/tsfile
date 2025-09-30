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

  public FleaDecoder() {
    super(TSEncoding.FLEA);
  }

  private void loadNextBlock(ByteBuffer buffer) {
    int n = ReadWriteIOUtils.readInt(buffer);

    if (n > 0) {
      int beta = ReadWriteIOUtils.readInt(buffer);

      LaminarDecoder laminarDecoder = new LaminarDecoder();
      long[] quantizedReal = new long[n / 2 + 1], quantizedImag = new long[n / 2 + 1];
      for (int i = 0; i <= n / 2; i++) {
        quantizedReal[i] = laminarDecoder.readLong(buffer);
      }
      laminarDecoder = new LaminarDecoder();
      for (int i = 0; i <= n / 2; i++) {
        quantizedImag[i] = laminarDecoder.readLong(buffer);
      }

      double[][] dequantized = new double[2][n / 2 + 1];
      for (int i = 0; i <= n / 2; i++) {
        dequantized[0][i] = FleaEncoder.dequantize(quantizedReal[i], beta);
        dequantized[1][i] = FleaEncoder.dequantize(quantizedImag[i], beta);
      }
      double[] reconstructed = FleaEncoder.inverseRealFFT(dequantized, n);

      long[] residuals = new long[n];
      SeparateStorageDecoder separateStorageDecoder = new SeparateStorageDecoder();
      for (int i = 0; i < n; i++) {
        residuals[i] = separateStorageDecoder.readLong(buffer);
      }

      this.currentBlockValues = new long[n];
      this.numberRemainingInCurrentBlock = this.totalInCurrentBlock = n;
      for (int i = 0; i < n; i++) {
        this.currentBlockValues[i] = residuals[i] + Math.round(reconstructed[i]);
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
