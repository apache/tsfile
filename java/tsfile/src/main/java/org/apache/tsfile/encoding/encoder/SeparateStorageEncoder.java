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
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.math3.complex.Complex;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SeparateStorageEncoder extends Encoder {
  private boolean isSigned;
  private List<Long> buffer = new ArrayList<>();
  private byte[] encodingBlockBuffer = null;

  public SeparateStorageEncoder(boolean isSigned) {
    super(TSEncoding.SEPARATE_STORAGE);
    this.isSigned = isSigned;
  }

  public SeparateStorageEncoder() {
    this(true);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    if (isSigned) {
      value = DescendingBitPackingEncoder.zigzagEncode(value);
    }
    buffer.add(value);
  }

  private static int getOptimalBitWidth(long[] widthCount) {
    long widthCountSum = 0, widthCountWeightedSum = 0;
    for (int i = 0; i <= 64; i++) {
      widthCountSum += widthCount[i];
      widthCountWeightedSum += widthCount[i] * i;
    }
    int indexWidth = DescendingBitPackingEncoder.getValueWidth(widthCountSum - 1);

    long currentWidthCountSum = widthCountSum, currentWidthCountWeightedSum = widthCountWeightedSum;
    int optimalWidth = -1;
    long optimalBitLength = -1;
    for (int i = 0; i <= 64; i++) {
      currentWidthCountSum -= widthCount[i];
      currentWidthCountWeightedSum -= widthCount[i] * i;
      long bitLength =
          widthCountSum * i
              + currentWidthCountSum * indexWidth
              + (currentWidthCountWeightedSum - currentWidthCountSum * i);
      if (optimalBitLength == -1 || bitLength < optimalBitLength) {
        optimalBitLength = bitLength;
        optimalWidth = i;
      }
    }

    return optimalWidth;
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    int n = buffer.size();
    ReadWriteIOUtils.write(n, out);

    if (n > 0) {
      long[] widthCount = new long[65];
      for (long value : buffer) widthCount[DescendingBitPackingEncoder.getValueWidth(value)]++;
      int optimalWidth = getOptimalBitWidth(widthCount);
      ReadWriteIOUtils.write(optimalWidth, out);

      Long[] highBits = new Long[n];
      Long[] lowBits = new Long[n];
      for (int i = 0; i < n; i++) {
        long value = buffer.get(i);
        highBits[i] = value >>> optimalWidth;
        lowBits[i] = value & ((1L << optimalWidth) - 1);
      }
      DescendingBitPackingEncoder highBitsEncoder = new DescendingBitPackingEncoder(false);
      for (long value : highBits) highBitsEncoder.encode(value, out);
      highBitsEncoder.flush(out);

      if (optimalWidth > 0) {
        int encodingLength = DescendingBitPackingEncoder.bitsToBytes(optimalWidth * n);
        this.encodingBlockBuffer = new byte[encodingLength];
        for (int i = 0; i < n; i++) {
          BytesUtils.longToBytes(lowBits[i], encodingBlockBuffer, optimalWidth * i, optimalWidth);
        }
        out.write(this.encodingBlockBuffer, 0, encodingLength);
        this.encodingBlockBuffer = null;
      }
    }
    this.buffer.clear();
  }

  public static class IntSeparateStorageEncoder extends SeparateStorageEncoder {
    public final Complex symbolicUse = Complex.I;

    public IntSeparateStorageEncoder() {
      super();
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      super.encode(Long.valueOf(value), out);
    }
  }
}
