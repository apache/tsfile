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
import org.apache.tsfile.utils.ReadWriteIOUtils;

import org.jtransforms.fft.DoubleFFT_1D;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FleaEncoder extends Encoder {
  private List<Long> buffer = new ArrayList<>();

  public FleaEncoder() {
    super(TSEncoding.FLEA);
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

      int beta = 15; // TODO: choose beta adaptively
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
