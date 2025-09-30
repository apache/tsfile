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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LaminarEncoder extends Encoder {
  private List<Long> buffer = new ArrayList<>();
  private byte[] encodingBlockBuffer = null;

  public LaminarEncoder() {
    super(TSEncoding.LAMINAR);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    value = DescendingBitPackingEncoder.zigzagEncode(value);
    buffer.add(value);
  }

  protected static int[] getLaminarBitWidths(long[] values) {
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

  private static int partition(long[] values) {
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

  private void flushEncodeArray(long[] values, ByteArrayOutputStream out) throws IOException {
    int n = values.length;
    ReadWriteIOUtils.write(n, out);

    if (n > 0) {
      int[] laminarBitWidths = getLaminarBitWidths(values);
      ReadWriteIOUtils.write(laminarBitWidths[0], out);

      IntRleEncoder rleEncoder = new IntRleEncoder();
      for (int i = 1; i < n; i++) {
        if (laminarBitWidths[i] < laminarBitWidths[i - 1])
          for (int j = laminarBitWidths[i - 1]; j > laminarBitWidths[i]; j--)
            rleEncoder.encode(1, out);
        rleEncoder.encode(0, out);
      }
      rleEncoder.flush(out);

      int totalBits = 0;
      for (int width : laminarBitWidths) totalBits += width;
      int encodingLength = DescendingBitPackingEncoder.bitsToBytes(totalBits);
      this.encodingBlockBuffer = new byte[encodingLength];
      int offset = 0;
      for (int i = 0; i < n; i++) {
        BytesUtils.longToBytes(values[i], encodingBlockBuffer, offset, laminarBitWidths[i]);
        offset += laminarBitWidths[i];
      }
      out.write(this.encodingBlockBuffer, 0, encodingLength);
      this.encodingBlockBuffer = null;
    }
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    int n = this.buffer.size();
    ReadWriteIOUtils.write(n, out);

    if (n > 0) {
      long[] values = new long[n];
      for (int i = 0; i < n; i++) {
        values[i] = buffer.get(i);
      }
      int p = partition(values);
      ReadWriteIOUtils.write(p, out);
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
      int encodingLength =
          DescendingBitPackingEncoder.bitsToBytes(indexBitWidth * sparseValuesArray.length);
      this.encodingBlockBuffer = new byte[encodingLength];
      for (int i = 0; i < sparseValuesArray.length; i++) {
        BytesUtils.intToBytes(
            sparseIndicesArray[i], encodingBlockBuffer, indexBitWidth * i, indexBitWidth);
      }
      out.write(this.encodingBlockBuffer, 0, encodingLength);
      this.encodingBlockBuffer = null;
    }

    this.buffer.clear();
  }

  @Override
  public final long getMaxByteSize() {
    return 0;
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
