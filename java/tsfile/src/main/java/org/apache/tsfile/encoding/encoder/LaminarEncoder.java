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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LaminarEncoder extends GorillaEncoderV2 {
  private List<Long> buffer = new ArrayList<>();

  public LaminarEncoder() {
    this.setType(TSEncoding.LAMINAR);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    value = DescendingBitPackingEncoder.zigzagEncode(value);
    buffer.add(value);
  }

  private int[] getLaminarBitWidths(long[] values) {
    int n = values.length;
    int[] laminarBitWidths = new int[n];
    for (int i = n - 1; i >= 0; i--) {
      laminarBitWidths[i] = DescendingBitPackingEncoder.getValueWidth(values[i]);
      if (i < n - 1) {
        laminarBitWidths[i] = Math.max(laminarBitWidths[i], laminarBitWidths[i + 1]);
      }
    }
    return laminarBitWidths;
  }

  private int partition(long[] values) {
    int n = values.length;

    int[] laminarBitWidths = getLaminarBitWidths(values);
    int indexBitWidth = DescendingBitPackingEncoder.getValueWidth(n - 1);

    long currentLength = 0;
    for (int i = 0; i < n; i++) {
      if (values[i] != 0) {
        currentLength += 1 + laminarBitWidths[i] + indexBitWidth;
      }
    }

    int bestP = 0;
    long bestLength = currentLength;
    for (int i = 0; i < n; i++) {
      if (values[i] != 0) {
        currentLength -= 1 + laminarBitWidths[i] + indexBitWidth;
      }
      currentLength += 1 + laminarBitWidths[i];
      if (currentLength < bestLength) {
        bestLength = currentLength;
        bestP = i + 1;
      }
    }

    return bestP;
  }

  private void flushEncodeArray(long[] values, ByteArrayOutputStream out) {
    int n = values.length;
    writeBits(n, 32, out);

    if (n > 0) {
      int[] laminarBitWidths = getLaminarBitWidths(values);
      writeBits(laminarBitWidths[0], 32, out);
      for (int i = 1; i < n; i++) {
        if (laminarBitWidths[i] < laminarBitWidths[i - 1])
          for (int j = laminarBitWidths[i - 1]; j > laminarBitWidths[i]; j--) writeBit(out);
        skipBit(out);
      }
      for (int i = 0; i < n; i++) {
        if (laminarBitWidths[i] > 0) writeBits(values[i], laminarBitWidths[i], out);
      }
    }
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    int n = this.buffer.size();
    writeBits(n, 32, out);

    if (n > 0) {
      long[] values = new long[n];
      for (int i = 0; i < n; i++) {
        values[i] = buffer.get(i);
      }
      int p = partition(values);
      writeBits(p, 32, out);
      flushEncodeArray(java.util.Arrays.copyOfRange(values, 0, p), out);

      List<Integer> sparseIndices = new ArrayList<>();
      List<Long> sparseValues = new ArrayList<>();
      for (int i = p; i < n; i++) {
        if (values[i] != 0) {
          sparseIndices.add(i - p);
          sparseValues.add(values[i]);
        }
      }
      int[] sparseIndicesArray = new int[sparseIndices.size()];
      long[] sparseValuesArray = new long[sparseValues.size()];
      for (int i = 0; i < sparseIndices.size(); i++) {
        sparseIndicesArray[i] = sparseIndices.get(i);
        sparseValuesArray[i] = sparseValues.get(i);
      }

      flushEncodeArray(sparseValuesArray, out);
      int indexBitWidth = DescendingBitPackingEncoder.getValueWidth(n - 1);
      for (int index : sparseIndicesArray) {
        writeBits(index, indexBitWidth, out);
      }
    }

    bitsLeft = 0;
    flipByte(out);
    this.buffer.clear();
  }

  public static class IntegerLaminarEncoder extends LaminarEncoder {

    public IntegerLaminarEncoder() {
      super();
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      super.encode(Long.valueOf(value), out);
    }
  }
}
