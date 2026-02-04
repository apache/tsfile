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

  private static final double MAX_PADDING_RATIO = 0.10;

  private static final int MIN_LENGTH_FOR_PADDING = 100;

  private static final boolean USE_FFT_PADDING = Boolean.parseBoolean(System.getProperty("flea.fft.padding", "true"));

  public FleaEncoder() {
    super(TSEncoding.FLEA);
    this.minBeta = 11;
    this.maxBeta = 20;
  }

  private static boolean isFFTFriendly(int n) {
    if (n <= 0)
      return false;
    while (n % 2 == 0)
      n /= 2;
    while (n % 3 == 0)
      n /= 3;
    while (n % 5 == 0)
      n /= 5;
    while (n % 7 == 0)
      n /= 7;
    return n == 1;
  }

  private static int nextFFTFriendly(int n) {
    if (n <= 1)
      return 1;
    if (isFFTFriendly(n))
      return n;

    int best = n * 2;

    for (int a = 1; a <= best; a *= 2) {
      for (int b = a; b <= best; b *= 3) {
        for (int c = b; c <= best; c *= 5) {
          for (int d = c; d <= best; d *= 7) {
            if (d >= n && d < best) {
              best = d;
            }
          }
        }
      }
    }
    return best;
  }

  private static int getFFTLength(int n) {
    if (!USE_FFT_PADDING)
      return n;
    if (n < MIN_LENGTH_FOR_PADDING)
      return n;

    if (isFFTFriendly(n)) {
      return n;
    }

    int padded = nextFFTFriendly(n);
    double ratio = (double) padded / n;

    if (ratio <= 1 + MAX_PADDING_RATIO) {
      return padded;
    }

    return n;
  }

  @SuppressWarnings("unused")
  private static int largestPrimeFactor(int n) {
    if (n <= 1)
      return n;
    int largest = 1;
    for (int d = 2; d * d <= n; d++) {
      while (n % d == 0) {
        largest = d;
        n /= d;
      }
    }
    if (n > 1)
      largest = n;
    return largest;
  }

  private static int[] getLaminarBitWidthsAbs(long[] values) {
    int n = values.length;
    int[] laminarBitWidths = new int[n];
    for (int i = n - 1; i >= 0; i--) {
      laminarBitWidths[i] = DescendingBitPackingEncoder.getValueWidth(Math.abs(values[i]));
      if (i < n - 1) {
        laminarBitWidths[i] = Math.max(laminarBitWidths[i], laminarBitWidths[i + 1]);
      }
    }
    return laminarBitWidths;
  }

  private long[] laminarEstimateLength(int[] laminarBitWidths) {
    int n = laminarBitWidths.length;
    long[] result = new long[this.maxBeta - this.minBeta + 1];

    if (n > 0) {
      int groupSize = 1 + DescendingBitPackingEncoder.getValueWidth(TSFileConfig.RLE_MAX_REPEATED_NUM);

      int i = 0;
      while (i < n) {
        int width = laminarBitWidths[i];
        int count = 1;
        while (i + count < n && laminarBitWidths[i + count] == width) {
          count++;
        }
        for (int beta = this.minBeta; beta < width && beta <= this.maxBeta; beta++) {
          int groupCount = 1
              + (count + TSFileConfig.RLE_MAX_REPEATED_NUM - 1)
                  / TSFileConfig.RLE_MAX_REPEATED_NUM;
          result[beta - this.minBeta] += groupCount * groupSize;
        }
        i += count;
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
        resultD2[realBitWidth - minBeta] += d * (1 - (1 + groupBitWidth - (realBitWidth - 1) + indexBitWidth));
      }
      if (realBitWidth + 1 <= maxBeta) {
        resultD2[realBitWidth + 1 - minBeta] += d * (1 + groupBitWidth - (realBitWidth - 1) + indexBitWidth);
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

  private void laminarCalculateResultInPlace(long[] resultD2, long[] resultD1, long[] result) {
    resultD1[0] = resultD2[0];
    result[0] = resultD1[0];
    for (int beta = this.minBeta + 1; beta <= this.maxBeta; beta++) {
      int idx = beta - this.minBeta;
      resultD1[idx] = resultD1[idx - 1] + resultD2[idx];
      result[idx] = result[idx - 1] + resultD1[idx];
    }
  }

  private long[] laminarEstimateValue(int n, int[] bitWidths, int[] laminarBitWidths) {
    int betaRange = this.maxBeta - this.minBeta + 1;
    long[] resultD2 = new long[betaRange];
    int indexBitWidth = DescendingBitPackingEncoder.getValueWidth(n - 1);

    for (int i = 0; i < n; i++) {
      laminarAddSparseMode(resultD2, bitWidths[i], laminarBitWidths[i], indexBitWidth, false);
    }

    long[] result = new long[betaRange];
    for (int beta = this.minBeta; beta <= this.maxBeta; beta++) {
      result[beta - this.minBeta] = Long.MAX_VALUE;
    }

    long[] tempResultD1 = new long[betaRange];
    long[] tempResult = new long[betaRange];

    int k = Math.min(n, 2 * betaRange);
    for (int i = 0; i < n; i++) {
      if (i % k == 0) {
        laminarCalculateResultInPlace(resultD2, tempResultD1, tempResult);
        for (int beta = this.minBeta; beta <= this.maxBeta; beta++) {
          int idx = beta - this.minBeta;
          result[idx] = Math.min(result[idx], tempResult[idx]);
        }
        java.util.Arrays.fill(tempResultD1, 0);
      }
      laminarAddSparseMode(resultD2, bitWidths[i], laminarBitWidths[i], indexBitWidth, true);
      laminarAddDenseMode(resultD2, bitWidths[i], laminarBitWidths[i], false);
    }
    laminarCalculateResultInPlace(resultD2, tempResultD1, tempResult);
    for (int beta = this.minBeta; beta <= this.maxBeta; beta++) {
      int idx = beta - this.minBeta;
      result[idx] = Math.min(result[idx], tempResult[idx]);
    }
    return result;
  }

  private long[] estimateFrequency(long[] values) {
    int n = values.length;
    int[] bitWidths = new int[n];
    for (int i = 0; i < n; i++) {
      bitWidths[i] = DescendingBitPackingEncoder.getValueWidth(Math.abs(values[i]));
    }
    int[] laminarBitWidths = new int[n];
    if (n > 0) {
      laminarBitWidths[n - 1] = bitWidths[n - 1];
      for (int i = n - 2; i >= 0; i--) {
        laminarBitWidths[i] = Math.max(bitWidths[i], laminarBitWidths[i + 1]);
      }
    }

    long[] result1 = laminarEstimateLength(laminarBitWidths);
    long[] result2 = laminarEstimateValue(n, bitWidths, laminarBitWidths);
    if (DEBUG) {
      StringBuilder sb1 = new StringBuilder("[");
      StringBuilder sb2 = new StringBuilder("[");
      for (int i = 0; i < result1.length; i++) {
        if (i > 0) {
          sb1.append(", ");
          sb2.append(", ");
        }
        sb1.append(result1[i]);
        sb2.append(result2[i]);
      }
      sb1.append("]");
      sb2.append("]");
      System.out.println(
          "[FLEA EST] n=" + values.length + " est_length=" + sb1 + " est_value=" + sb2);
    }
    long[] result = new long[this.maxBeta - this.minBeta + 1];
    for (int beta = this.minBeta; beta <= this.maxBeta; beta++) {
      result[beta - this.minBeta] = result1[beta - this.minBeta] + result2[beta - this.minBeta];
    }
    return result;
  }

  private long[] estimateResidual(int n, long[] frequencyReal, long[] frequencyImag) {
    double[] squareSumDiff = new double[maxBeta - minBeta + 1];
    long[] partialCountDiff = new long[maxBeta - minBeta + 1];
    int freqLen = frequencyReal.length;

    for (int i = 0; i < freqLen; i++) {
      long real = frequencyReal[i];
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

      long imag = frequencyImag[i];
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

    for (int i = freqLen; i < n; i++) {
      int mirrorIdx = n - i;
      long real = frequencyReal[mirrorIdx];
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

      long imag = -frequencyImag[mirrorIdx];
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
      partialCount[beta - minBeta] = partialCount[beta - minBeta - 1] + partialCountDiff[beta - minBeta];
    }

    long[] result = new long[maxBeta - minBeta + 1];
    for (int beta = minBeta; beta <= maxBeta; beta++) {
      double squareSumBeta = squareSum[beta - minBeta]
          + partialCount[beta - minBeta] * (1L << beta) * (1L << beta) / 3;
      long optimalBitWidth = Math.round(Math.ceil(Math.log(Math.sqrt(squareSumBeta) / n + 1) / Math.log(2)));
      result[beta - minBeta] = (optimalBitWidth + 2) * n;
    }

    return result;
  }

  private int getOptimalBeta(int n, long[] frequencyReal, long[] frequencyImag) {
    long[] freqEstReal = estimateFrequency(frequencyReal);
    long[] freqEstImag = estimateFrequency(frequencyImag);
    long[] frequency = new long[maxBeta - minBeta + 1];
    for (int beta = minBeta; beta <= maxBeta; beta++) {
      frequency[beta - minBeta] = freqEstReal[beta - minBeta] + freqEstImag[beta - minBeta];
    }
    long[] residual = estimateResidual(n, frequencyReal, frequencyImag);

    if (DEBUG) {
      for (int beta = minBeta; beta <= maxBeta; beta++) {
        long total = frequency[beta - minBeta] + residual[beta - minBeta];
        System.out.println(
            "[FLEA BETA] beta="
                + beta
                + ": freq_real="
                + freqEstReal[beta - minBeta]
                + ", freq_imag="
                + freqEstImag[beta - minBeta]
                + ", residual="
                + residual[beta - minBeta]
                + ", total="
                + total);
      }
    }

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

  private static final ThreadLocal<DoubleFFT_1D> cachedFFT = new ThreadLocal<>();
  private static final ThreadLocal<Integer> cachedFFTSize = ThreadLocal.withInitial(() -> 0);

  private static final ThreadLocal<double[]> workArrayFFT = new ThreadLocal<>();
  private static final ThreadLocal<Integer> workArrayFFTSize = ThreadLocal.withInitial(() -> 0);

  private static DoubleFFT_1D getFFT(int n) {
    if (cachedFFT.get() == null || cachedFFTSize.get() != n) {
      cachedFFT.set(new DoubleFFT_1D(n));
      cachedFFTSize.set(n);
    }
    return cachedFFT.get();
  }

  private static double[] getWorkArray(int size) {
    if (workArrayFFT.get() == null || workArrayFFTSize.get() < size) {
      workArrayFFT.set(new double[size]);
      workArrayFFTSize.set(size);
    }
    return workArrayFFT.get();
  }

  public static double[][] realFFT(double[] data) {
    int n = data.length;
    double[] workArray = getWorkArray(n * 2);

    for (int i = 0; i < n; i++) {
      workArray[2 * i] = data[i];
      workArray[2 * i + 1] = 0;
    }

    DoubleFFT_1D fft = getFFT(n);
    fft.complexForward(workArray);

    double[][] result = new double[2][n / 2 + 1];
    for (int i = 0; i <= n / 2; i++) {
      result[0][i] = workArray[2 * i];
      result[1][i] = workArray[2 * i + 1];
    }
    return result;
  }

  public static double[] inverseRealFFT(double[][] data, int n) {
    double[] workArray = getWorkArray(n * 2);

    for (int i = 0; i <= n / 2; i++) {
      workArray[2 * i] = data[0][i];
      workArray[2 * i + 1] = data[1][i];
    }
    for (int i = n / 2 + 1; i < n; i++) {
      workArray[2 * i] = data[0][n - i];
      workArray[2 * i + 1] = -data[1][n - i];
    }

    DoubleFFT_1D fft = getFFT(n);
    fft.complexInverse(workArray, true);

    double[] result = new double[n];
    for (int i = 0; i < n; i++) {
      result[i] = workArray[2 * i];
    }
    return result;
  }

  public static long quantize(double value, int beta) {
    return Math.round(value / (1 << beta));
  }

  public static double dequantize(long value, int beta) {
    return (double) value * (1 << beta);
  }

  private static boolean DEBUG = Boolean.getBoolean("flea.debug");

  private LaminarEncoder cachedLaminarEncoder = null;
  private SeparateStorageEncoder cachedSeparateStorageEncoder = null;

  private LaminarEncoder getLaminarEncoder() {
    if (cachedLaminarEncoder == null) {
      cachedLaminarEncoder = new LaminarEncoder();
    }
    return cachedLaminarEncoder;
  }

  private SeparateStorageEncoder getSeparateStorageEncoder() {
    if (cachedSeparateStorageEncoder == null) {
      cachedSeparateStorageEncoder = new SeparateStorageEncoder();
    }
    return cachedSeparateStorageEncoder;
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    int n = this.buffer.size();
    ReadWriteIOUtils.write(n, out);
    if (n > 0) {
      int paddedN = getFFTLength(n);
      int freqLen = paddedN / 2 + 1;

      int paddingAmount = paddedN - n;
      if (paddingAmount > 255) {
        paddedN = n;
        paddingAmount = 0;
        freqLen = n / 2 + 1;
      }
      out.write(paddingAmount);

      double[] data = new double[paddedN];
      long lastValue = buffer.get(n - 1);
      for (int i = 0; i < n; i++) {
        data[i] = buffer.get(i);
      }
      for (int i = n; i < paddedN; i++) {
        data[i] = lastValue;
      }
      double[][] fftResult = realFFT(data);
      double[] frequencyReal = fftResult[0], frequencyImag = fftResult[1];

      long[] quantizedReal = new long[freqLen];
      long[] quantizedImag = new long[freqLen];

      for (int i = 0; i < freqLen; i++) {
        quantizedReal[i] = Math.round(frequencyReal[i]);
        quantizedImag[i] = Math.round(frequencyImag[i]);
      }
      int beta = getOptimalBeta(paddedN, quantizedReal, quantizedImag);
      if (DEBUG) {
        System.err.println(
            "[FLEA DEBUG] n="
                + n
                + ", paddedN="
                + paddedN
                + ", freq_len="
                + freqLen
                + ", beta="
                + beta);
      }
      ReadWriteIOUtils.write(beta, out);

      for (int i = 0; i < freqLen; i++) {
        quantizedReal[i] = quantize(frequencyReal[i], beta);
        quantizedImag[i] = quantize(frequencyImag[i], beta);
      }

      if (DEBUG) {
        long realNonZero = java.util.Arrays.stream(quantizedReal).filter(x -> x != 0).count();
        long imagNonZero = java.util.Arrays.stream(quantizedImag).filter(x -> x != 0).count();
        System.err.println("[FLEA DEBUG] quantized_real non-zero: " + realNonZero + "/" + freqLen);
        System.err.println("[FLEA DEBUG] quantized_imag non-zero: " + imagNonZero + "/" + freqLen);
      }

      int sizeBeforeLaminarReal = out.size();
      LaminarEncoder laminarEncoder = getLaminarEncoder();
      for (long v : quantizedReal) {
        laminarEncoder.encode(v, out);
      }
      laminarEncoder.flush(out);
      int sizeAfterLaminarReal = out.size();

      for (long v : quantizedImag) {
        laminarEncoder.encode(v, out);
      }
      laminarEncoder.flush(out);
      int sizeAfterLaminarImag = out.size();

      if (DEBUG) {
        System.err.println(
            "[FLEA DEBUG] laminar_real bytes: " + (sizeAfterLaminarReal - sizeBeforeLaminarReal));
        System.err.println(
            "[FLEA DEBUG] laminar_imag bytes: " + (sizeAfterLaminarImag - sizeAfterLaminarReal));
      }

      for (int i = 0; i < freqLen; i++) {
        fftResult[0][i] = dequantize(quantizedReal[i], beta);
        fftResult[1][i] = dequantize(quantizedImag[i], beta);
      }
      double[] reconstructedFull = inverseRealFFT(fftResult, paddedN);

      long[] residuals = (quantizedReal.length >= n) ? quantizedReal : new long[n];
      for (int i = 0; i < n; i++) {
        residuals[i] = buffer.get(i) - Math.round(reconstructedFull[i]);
      }

      if (DEBUG) {
        long residualNonZero = 0;
        long residualMin = Long.MAX_VALUE, residualMax = Long.MIN_VALUE;
        for (int i = 0; i < n; i++) {
          if (residuals[i] != 0)
            residualNonZero++;
          if (residuals[i] < residualMin)
            residualMin = residuals[i];
          if (residuals[i] > residualMax)
            residualMax = residuals[i];
        }
        System.err.println(
            "[FLEA DEBUG] residual range: [" + residualMin + ", " + residualMax + "]");
        System.err.println("[FLEA DEBUG] residual non-zero: " + residualNonZero + "/" + n);
      }

      int sizeBeforeResidual = out.size();
      SeparateStorageEncoder separateStorageEncoder = getSeparateStorageEncoder();
      for (int i = 0; i < n; i++) {
        separateStorageEncoder.encode(residuals[i], out);
      }
      separateStorageEncoder.flush(out);
      int sizeAfterResidual = out.size();

      if (DEBUG) {
        System.err.println(
            "[FLEA DEBUG] residual bytes: " + (sizeAfterResidual - sizeBeforeResidual));
        System.err.println("[FLEA DEBUG] total bytes: " + sizeAfterResidual);
      }
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
