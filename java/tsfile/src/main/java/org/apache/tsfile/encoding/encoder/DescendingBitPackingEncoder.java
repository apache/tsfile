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

import org.nd4j.linalg.api.buffer.DataType;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DescendingBitPackingEncoder extends Encoder {
  private boolean isSigned;
  private List<Long> buffer = new ArrayList<>();
  private byte[] encodingBlockBuffer = null;

  private static int bitsToBytes(int bits) {
    return (bits + 7) / 8;
  }

  private static int getValueWidth(long value) {
    return 64 - Long.numberOfLeadingZeros(value);
  }

  private static long zigzagEncode(long value) {
    return (value << 1) ^ (value >> 63);
  }

  public DescendingBitPackingEncoder(boolean isSigned) {
    super(TSEncoding.DESCENDING_BIT_PACKING);
    this.isSigned = isSigned;
  }

  public DescendingBitPackingEncoder() {
    this(true);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    if (isSigned) {
      value = zigzagEncode(value);
    }
    buffer.add(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    INDArray array = Nd4j.createFromArray(this.buffer.stream().mapToLong(i -> i).toArray());
    int n = Math.toIntExact(array.length());
    ReadWriteIOUtils.write(n, out);

    if (n > 0) {
      int m = array.neq(0).castTo(DataType.INT32).sumNumber().intValue();
      ReadWriteIOUtils.write(m, out);

      if (m > 0) {
        INDArray[] sortResult = Nd4j.sortWithIndices(array, -1, false);
        INDArray sortedIndices = sortResult[0], sortedValues = sortResult[1];
        long[] sortedValuesArray = sortedValues.toLongVector(),
            sortedIndicesArray = sortedIndices.toLongVector();

        int firstNegativeIndex = n;
        for (int i = n - 1; i >= 0; i--) {
          if (sortedValuesArray[i] < 0) firstNegativeIndex = i;
        }
        long[] tmpValuesArray = new long[n], tmpIndicesArray = new long[n];
        for (int i = firstNegativeIndex; i < n; i++) {
          tmpValuesArray[i - firstNegativeIndex] = sortedValuesArray[i];
          tmpIndicesArray[i - firstNegativeIndex] = sortedIndicesArray[i];
        }
        for (int i = 0; i < firstNegativeIndex; i++) {
          tmpValuesArray[i + n - firstNegativeIndex] = sortedValuesArray[i];
          tmpIndicesArray[i + n - firstNegativeIndex] = sortedIndicesArray[i];
        }
        sortedValuesArray = tmpValuesArray;
        sortedIndicesArray = tmpIndicesArray;

        int indexBitWidth = getValueWidth(n - 1);
        int encodingLength = bitsToBytes(indexBitWidth * m);
        this.encodingBlockBuffer = new byte[encodingLength];
        for (int i = 0; i < m; i++) {
          BytesUtils.intToBytes(
              Math.toIntExact(sortedIndicesArray[i]),
              this.encodingBlockBuffer,
              indexBitWidth * i,
              indexBitWidth);
        }
        out.write(this.encodingBlockBuffer, 0, encodingLength);
        this.encodingBlockBuffer = null;

        int valueWidthSum = 0;
        for (int i = 0; i < m; i++) valueWidthSum += getValueWidth(sortedValuesArray[i]);
        encodingLength = bitsToBytes(valueWidthSum + getValueWidth(sortedValuesArray[0]));
        ReadWriteIOUtils.write(encodingLength, out);

        this.encodingBlockBuffer = new byte[encodingLength];
        int offset = 0;
        int previousValueWidth = getValueWidth(sortedValuesArray[0]);
        ReadWriteIOUtils.write(previousValueWidth, out);
        BytesUtils.longToBytes(
            sortedValuesArray[0], this.encodingBlockBuffer, offset, previousValueWidth);
        offset += previousValueWidth;
        for (int i = 1; i < m; i++) {
          BytesUtils.longToBytes(
              sortedValuesArray[i], this.encodingBlockBuffer, offset, previousValueWidth);
          offset += previousValueWidth;
          previousValueWidth = getValueWidth(sortedValuesArray[i]);
        }
        out.write(this.encodingBlockBuffer, 0, encodingLength);
        this.encodingBlockBuffer = null;
      }
    }

    this.buffer.clear();
  }

  public static class IntDescendingBitPackingEncoder extends DescendingBitPackingEncoder {
    public IntDescendingBitPackingEncoder() {
      super();
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      super.encode(Long.valueOf(value), out);
    }
  }
}
