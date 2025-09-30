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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import org.jtransforms.fft.DoubleFFT_1D;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FleaEncoder extends Encoder {
  private List<Long> buffer = new ArrayList<>();
  private int minBeta, maxBeta;

  public FleaEncoder() {
    super(TSEncoding.FLEA);
    this.minBeta = 11;
    this.maxBeta = 20;
  }

  private long[] laminarEstimateLength(long[] values) {
    int n = values.length;
    long[] result = new long[this.maxBeta - this.minBeta + 1];

    if (n > 0) {
      int groupSize =
          1 + DescendingBitPackingEncoder.getValueWidth(TSFileConfig.RLE_MAX_REPEATED_NUM);
      int[] laminarBitWidths = LaminarEncoder.getLaminarBitWidths(values);
      List<Integer> repeatValues = new ArrayList<>(), repeatCounts = new ArrayList<>();
      int currentValue = -1, currentCount = 0;
      for (int width : laminarBitWidths) {
        if (width == currentValue) {
          currentCount++;
        } else {
          if (currentValue != -1) {
            repeatValues.add(currentValue);
            repeatCounts.add(currentCount);
          }
          currentValue = width;
          currentCount = 1;
        }
      }
      if (currentValue != -1) {
        repeatValues.add(currentValue);
        repeatCounts.add(currentCount);
      }

      for (int i = 0; i < repeatValues.size(); i++) {
        int width = repeatValues.get(i), count = repeatCounts.get(i);
        for (int beta = this.minBeta; beta <= this.maxBeta; beta++) {
          if (width > beta) {
            int groupCount =
                (1
                    + (count + TSFileConfig.RLE_MAX_REPEATED_NUM - 1)
                        / TSFileConfig.RLE_MAX_REPEATED_NUM);
            result[beta - this.minBeta] += groupCount * groupSize;
          }
        }
      }
    }
    return result;
  }

  private void laminarAddSparseMode(
      long[] resultD2, int realBitWidth, int groupBitWidth, int indexBitWidth, boolean subtract) {
    int d = subtract ? -1 : 1;
    if (minBeta < realBitWidth) {
      resultD2[0] += d * (1 + groupBitWidth - minBeta + indexBitWidth);
      if (minBeta + 1 <= maxBeta) {
        resultD2[1] += d * (-1 - (1 + groupBitWidth - minBeta + indexBitWidth));
      }
      if (realBitWidth <= maxBeta) {
        resultD2[realBitWidth - minBeta] +=
            d * (1 - (1 + groupBitWidth - (realBitWidth - 1) + indexBitWidth));
      }
      if (realBitWidth + 1 <= maxBeta) {
        resultD2[realBitWidth + 1 - minBeta] +=
            d * (1 + groupBitWidth - (realBitWidth - 1) + indexBitWidth);
      }
    }
  }

  private void laminarAddDenseMode(
      long[] resultD2, int realBitWidth, int groupBitWidth, boolean subtract) {
    int d = subtract ? -1 : 1;
    if (minBeta < realBitWidth) {
      resultD2[0] += d * (1 + groupBitWidth - minBeta);
      if (minBeta + 1 <= maxBeta) {
        resultD2[1] += d * (-1 - (1 + groupBitWidth - minBeta));
      }
      if (groupBitWidth <= maxBeta) {
        resultD2[groupBitWidth - minBeta] += d * (-1);
      }
      if (groupBitWidth + 1 <= maxBeta) {
        resultD2[groupBitWidth + 1 - minBeta] += d * 2;
      }
    }
  }

  private long[] laminarCalculateResult(long[] resultD2) {
    long[] resultD1 = new long[this.maxBeta - this.minBeta + 1];
    long[] result = new long[this.maxBeta - this.minBeta + 1];
    resultD1[0] = resultD2[0];
    result[0] = resultD1[0];
    for (int beta = this.minBeta + 1; beta <= this.maxBeta; beta++) {
      resultD1[beta - this.minBeta] =
          resultD1[beta - this.minBeta - 1] + resultD2[beta - this.minBeta];
      result[beta - this.minBeta] = result[beta - this.minBeta - 1] + resultD1[beta - this.minBeta];
    }
    return result;
  }

  private long[] laminarEstimateValue(long[] values) {
    int n = values.length;

    long[] resultD2 = new long[this.maxBeta - this.minBeta + 1];
    int[] bitWidths = new int[n];
    for (int i = 0; i < n; i++) {
      bitWidths[i] = DescendingBitPackingEncoder.getValueWidth(values[i]);
    }
    int[] laminarBitWidths = LaminarEncoder.getLaminarBitWidths(values);
    int indexBitWidth = DescendingBitPackingEncoder.getValueWidth(n - 1);

    for (int i = 0; i < n; i++) {
      laminarAddSparseMode(resultD2, bitWidths[i], laminarBitWidths[i], indexBitWidth, false);
    }

    long[] result = new long[this.maxBeta - this.minBeta + 1];
    for (int beta = this.minBeta; beta <= this.maxBeta; beta++) {
      result[beta - this.minBeta] = Long.MAX_VALUE;
    }
    int k = Math.min(n, 2 * (maxBeta - minBeta + 1));
    for (int i = 0; i < n; i++) {
      if (i % k == 0) {
        long[] currentResult = laminarCalculateResult(resultD2);
        for (int beta = this.minBeta; beta <= this.maxBeta; beta++) {
          result[beta - this.minBeta] =
              Math.min(result[beta - this.minBeta], currentResult[beta - this.minBeta]);
        }
      }
      laminarAddSparseMode(resultD2, bitWidths[i], laminarBitWidths[i], indexBitWidth, true);
      laminarAddDenseMode(resultD2, bitWidths[i], laminarBitWidths[i], false);
    }
    long[] currentResult = laminarCalculateResult(resultD2);
    for (int beta = this.minBeta; beta <= this.maxBeta; beta++) {
      result[beta - this.minBeta] =
          Math.min(result[beta - this.minBeta], currentResult[beta - this.minBeta]);
    }
    return result;
  }

  private long[] estimateFrequency(long[] values) {
    long[] result1 = laminarEstimateLength(values);
    long[] result2 = laminarEstimateValue(values);
    long[] result = new long[this.maxBeta - this.minBeta + 1];
    for (int beta = this.minBeta; beta <= this.maxBeta; beta++) {
      result[beta - this.minBeta] = result1[beta - this.minBeta] + result2[beta - this.minBeta];
    }
    return result;
  }

  private long[] estimateResidual(int n, long[] frequencyReal, long[] frequencyImag) {
    double[] squareSumDiff = new double[maxBeta - minBeta + 1];
    long[] partialCountDiff = new long[maxBeta - minBeta + 1];

    for (int i = 0; i < n; i++) {
      long real = i < frequencyReal.length ? frequencyReal[i] : frequencyReal[n - i];
      int realBitLength = DescendingBitPackingEncoder.getValueWidth(Math.abs(real));
      if (realBitLength - 1 >= minBeta) {
        partialCountDiff[0] += 1;
        if (realBitLength <= maxBeta) {
          partialCountDiff[realBitLength - minBeta] -= 1;
        }
      }
      if (realBitLength >= minBeta && realBitLength <= maxBeta) {
        squareSumDiff[realBitLength - minBeta] += ((double) real * real);
      }

      long imag = i < frequencyImag.length ? frequencyImag[i] : -frequencyImag[n - i];
      int imagBitLength = DescendingBitPackingEncoder.getValueWidth(Math.abs(imag));
      if (imagBitLength - 1 >= minBeta) {
        partialCountDiff[0] += 1;
        if (imagBitLength <= maxBeta) {
          partialCountDiff[imagBitLength - minBeta] -= 1;
        }
      }
      if (imagBitLength >= minBeta && imagBitLength <= maxBeta) {
        squareSumDiff[imagBitLength - minBeta] += ((double) imag * imag);
      }
    }

    double[] squareSum = new double[maxBeta - minBeta + 1];
    squareSum[0] = squareSumDiff[0];
    for (int beta = minBeta + 1; beta <= maxBeta; beta++) {
      squareSum[beta - minBeta] = squareSum[beta - minBeta - 1] + squareSumDiff[beta - minBeta];
    }
    long[] partialCount = new long[maxBeta - minBeta + 1];
    partialCount[0] = partialCountDiff[0];
    for (int beta = minBeta + 1; beta <= maxBeta; beta++) {
      partialCount[beta - minBeta] =
          partialCount[beta - minBeta - 1] + partialCountDiff[beta - minBeta];
    }

    long[] result = new long[maxBeta - minBeta + 1];
    for (int beta = minBeta; beta <= maxBeta; beta++) {
      double squareSumBeta =
          squareSum[beta - minBeta]
              + partialCount[beta - minBeta] * (1L << beta) * (1L << beta) / 3;
      long optimalBitWidth =
          Math.round(Math.ceil(Math.log(Math.sqrt(squareSumBeta) / n + 1) / Math.log(2)));
      result[beta - minBeta] = (optimalBitWidth + 2) * n;
    }

    return result;
  }

  private int getOptimalBeta(int n, long[] frequencyReal, long[] frequencyImag) {
    long[] frequency = estimateFrequency(frequencyReal);
    long[] frequency2 = estimateFrequency(frequencyImag);
    for (int beta = minBeta; beta <= maxBeta; beta++) {
      frequency[beta - minBeta] += frequency2[beta - minBeta];
    }
    long[] residual = estimateResidual(n, frequencyReal, frequencyImag);
    for (int beta = minBeta; beta <= maxBeta; beta++) {
      frequency[beta - minBeta] += residual[beta - minBeta];
    }
    int optimalBeta = minBeta;
    for (int beta = minBeta + 1; beta <= maxBeta; beta++) {
      if (frequency[beta - minBeta] < frequency[optimalBeta - minBeta]) {
        optimalBeta = beta;
      }
    }
    return optimalBeta;
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    buffer.add(value);
  }

  public static double[][] realFFT(double[] data) {
    double[] dataComplex = new double[data.length * 2];
    for (int i = 0; i < data.length; i++) {
      dataComplex[2 * i] = data[i];
      dataComplex[2 * i + 1] = 0;
    }
    DoubleFFT_1D fft = new DoubleFFT_1D(data.length);
    fft.complexForward(dataComplex);
    double[][] result = new double[2][data.length / 2 + 1];
    for (int i = 0; i <= data.length / 2; i++) {
      result[0][i] = dataComplex[2 * i];
      result[1][i] = dataComplex[2 * i + 1];
    }
    return result; // result[0] is real part, result[1] is imaginary part
  }

  public static double[] inverseRealFFT(double[][] data, int n) {
    double[] dataComplex = new double[n * 2];
    for (int i = 0; i <= n / 2; i++) {
      dataComplex[2 * i] = data[0][i];
      dataComplex[2 * i + 1] = data[1][i];
    }
    for (int i = n / 2 + 1; i < n; i++) {
      dataComplex[2 * i] = data[0][n - i];
      dataComplex[2 * i + 1] = -data[1][n - i];
    }
    DoubleFFT_1D fft = new DoubleFFT_1D(n);
    fft.complexInverse(dataComplex, true);
    double[] result = new double[n];
    for (int i = 0; i < n; i++) {
      result[i] = dataComplex[2 * i];
    }
    return result;
  }

  public static long quantize(double value, int beta) {
    return Math.round(value / (1 << beta));
  }

  public static double dequantize(long value, int beta) {
    return (double) value * (1 << beta);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    int n = this.buffer.size();
    ReadWriteIOUtils.write(n, out);
    if (n > 0) {
      double[] data = new double[n];
      for (int i = 0; i < n; i++) {
        data[i] = buffer.get(i);
      }
      double[][] fftResult = realFFT(data);
      double[] frequencyReal = fftResult[0], frequencyImag = fftResult[1];

      long[] frequencyRealLong = new long[frequencyReal.length];
      long[] frequencyImagLong = new long[frequencyImag.length];
      for (int i = 0; i < frequencyReal.length; i++) {
        frequencyRealLong[i] = Math.round(frequencyReal[i]);
      }
      for (int i = 0; i < frequencyImag.length; i++) {
        frequencyImagLong[i] = Math.round(frequencyImag[i]);
      }
      int beta = getOptimalBeta(n, frequencyRealLong, frequencyImagLong);
      ReadWriteIOUtils.write(beta, out);

      long[] quantizedReal = new long[frequencyReal.length],
          quantizedImag = new long[frequencyImag.length];
      for (int i = 0; i < frequencyReal.length; i++) {
        quantizedReal[i] = quantize(frequencyReal[i], beta);
        quantizedImag[i] = quantize(frequencyImag[i], beta);
      }
      LaminarEncoder laminarEncoder = new LaminarEncoder();
      for (long v : quantizedReal) {
        laminarEncoder.encode(v, out);
      }
      laminarEncoder.flush(out);
      laminarEncoder = new LaminarEncoder();
      for (long v : quantizedImag) {
        laminarEncoder.encode(v, out);
      }
      laminarEncoder.flush(out);

      double[][] dequantized = new double[2][frequencyReal.length];
      for (int i = 0; i < frequencyReal.length; i++) {
        dequantized[0][i] = dequantize(quantizedReal[i], beta);
        dequantized[1][i] = dequantize(quantizedImag[i], beta);
      }
      double[] reconstructed = inverseRealFFT(dequantized, n);
      long[] residuals = new long[n];
      for (int i = 0; i < n; i++) {
        residuals[i] = buffer.get(i) - Math.round(reconstructed[i]);
      }
      SeparateStorageEncoder separateStorageEncoder = new SeparateStorageEncoder();
      for (long v : residuals) {
        separateStorageEncoder.encode(v, out);
      }
      separateStorageEncoder.flush(out);
    }
    this.buffer.clear();
  }

  @Override
  public final long getMaxByteSize() {
    return 0;
  }

  public static class IntFleaEncoder extends FleaEncoder {

    public IntFleaEncoder() {
      super();
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      super.encode(Long.valueOf(value), out);
    }
  }
}
