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

import org.apache.tsfile.common.bitStream.BitInputStream;
import org.apache.tsfile.common.bitStream.BitOutputStream;
import org.apache.tsfile.encoding.encoder.CamelEncoder;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CamelDecoderTest {

  @Test
  public void testRandomizedCompressDecompress() throws Exception {
    Random random = new Random();
    int sampleSize = 10_000;
    double[] original = new double[sampleSize];

    // Generate random test data (excluding NaN and ±Infinity)
    for (int i = 0; i < sampleSize; i++) {
      double v;
      do {
        long bits = random.nextLong();
        v = Double.longBitsToDouble(bits);
      } while (Double.isNaN(v) || Double.isInfinite(v));
      original[i] = v;
    }

    compressDecompressAndAssert(original, 0);
  }

  private void compressDecompressAndAssert(double[] original, double tolerance) throws Exception {
    CamelEncoder encoder = new CamelEncoder();
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    for (double v : original) {
      encoder.encode(v, bout);
    }
    encoder.flush(bout);
    // Decode and verify
    CamelDecoder decoder = new CamelDecoder();
    ByteBuffer buffer = ByteBuffer.wrap(bout.toByteArray());

    int i = 0;
    while (decoder.hasNext(buffer)) {
      double actual = decoder.readDouble(buffer);
      double expected = original[i];
      if (Double.isNaN(expected)) {
        assertTrue("Expected NaN at index " + i, Double.isNaN(actual));
      } else {
        assertEquals("Mismatch at index " + i, expected, actual, tolerance);
      }
      i++;
    }
    assertEquals(original.length, i);
  }

  @Test
  public void testSpecialFloatingValues() throws Exception {
    double[] original =
        new double[] {
          Double.NaN,
          Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY,
          +0.0,
          -0.0,
          Double.MIN_VALUE,
          -Double.MIN_VALUE,
          Double.MIN_NORMAL,
          -Double.MIN_NORMAL,
          Double.MAX_VALUE,
          -Double.MAX_VALUE
        };
    compressDecompressAndAssert(original, 0.0);
  }

  @Test
  public void testMonotonicSequence() throws Exception {
    double[] increasing = new double[500];
    double[] decreasing = new double[500];
    for (int i = 0; i < 500; i++) {
      increasing[i] = 100.0 + i * 0.0001;
      decreasing[i] = 100.0 - i * 0.0001;
    }
    compressDecompressAndAssert(increasing, 0);
    compressDecompressAndAssert(decreasing, 0);
  }

  @Test
  public void testPrecisionEdgeCases() throws Exception {
    double[] original = {
      9007199254740991.0, // 2^53 - 1
      9007199254740992.0, // 2^53
      9007199254740993.0,
      1.0000000000000001, // Precision loss (equals 1.0)
      1.0000000000000002,
      12345,
      21332213
    };
    compressDecompressAndAssert(original, 0.0);
  }

  @Test
  public void testAlternatingSignsAndDecimals() throws Exception {
    double[] original = new double[2];
    for (int i = 0; i < 2; i++) {
      double base = i * 0.123456 % 1000;
      original[i] = (i % 2 == 0) ? base : -base;
    }
    compressDecompressAndAssert(original, 0.0);
  }

  @Test
  public void testMinimalDeltaSequence() throws Exception {
    double[] original = new double[64];
    double base = 100.0;
    for (int i = 0; i < original.length; i++) {
      original[i] = base + i * Math.ulp(base);
    }
    compressDecompressAndAssert(original, 0.0);
  }

  @Test
  public void testRepeatedValues() throws Exception {
    double repeated = 123.456789;
    double[] original = new double[1000];
    Arrays.fill(original, repeated);
    compressDecompressAndAssert(original, 0.0);
  }

  private void testGorillaValues(double[] values) throws Exception {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    BitOutputStream out = new BitOutputStream(bout);

    CamelEncoder.GorillaEncoder encoder = new CamelEncoder().getGorillaEncoder();
    for (double v : values) {
      encoder.encode(v, out);
    }
    encoder.close(out);

    byte[] encoded = bout.toByteArray();
    BitInputStream in = new BitInputStream(new ByteArrayInputStream(encoded), out.getBitsWritten());
    InputStream inputStream = new ByteArrayInputStream(encoded);
    CamelDecoder.GorillaDecoder decoder =
        new CamelDecoder(inputStream, out.getBitsWritten()).getGorillaDecoder();

    int idx = 0;
    for (double expected : values) {
      double actual = decoder.decode(in);
      Assert.assertEquals("Mismatch decoding: ", expected, actual, 0.0);
    }
  }

  @Test
  public void testGorillaAllZeros() throws Exception {
    double[] values = new double[100];
    Arrays.fill(values, 0.0);
    testGorillaValues(values);
  }

  @Test
  public void testGorillaConstantValue() throws Exception {
    double[] values = new double[200];
    Arrays.fill(values, 123456.789);
    testGorillaValues(values);
  }

  @Test
  public void testGorillaMinMaxValues() throws Exception {
    double[] values = {
      Double.MIN_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MIN_VALUE, 0.0, -0.0
    };
    testGorillaValues(values);
  }

  @Test
  public void testGorillaMixedSigns() throws Exception {
    double[] values = {-1.1, 2.2, -3.3, 4.4, -5.5, 6.6, -7.7};
    testGorillaValues(values);
  }

  @Test
  public void testGorillaHighPrecisionValues() throws Exception {
    double[] values = {0.1, 0.2, 0.3, 0.1 + 0.2, 0.4 - 0.1};
    testGorillaValues(values);
  }

  @Test
  public void testGorillaXorEdgeTrigger() throws Exception {
    double[] values = {
      1.00000001,
      1.00000002,
      1.00000003,
      1.00000001, // back to earlier value
      1.00000009
    };
    testGorillaValues(values);
  }

  @Test
  public void testLargeSeries() throws Exception {
    double[] values = new double[1000];
    for (int i = 0; i < values.length; i++) {
      values[i] = Math.sin(i / 10.0);
    }
    testGorillaValues(values);
  }

  private static final int[] FLUSH_SIZES = {32, 64, 128, 256, 512, 1000};
  private static final int TOTAL_VALUES = 1_000_000;

  @Test
  public void testBatchFlushForVariousBlockSizes() throws IOException {
    Random random = new Random(42);
    for (int blockSize : FLUSH_SIZES) {
      // Prepare encoder and output buffer
      CamelEncoder encoder = new CamelEncoder();
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      double[] original = new double[TOTAL_VALUES];

      // Generate random data and flush every blockSize values
      for (int i = 0; i < TOTAL_VALUES; i++) {
        double v;
        do {
          long bits = random.nextLong();
          v = Double.longBitsToDouble(bits);
        } while (Double.isNaN(v) || Double.isInfinite(v));
        original[i] = v;
        encoder.encode(v, bout);
        if ((i + 1) % blockSize == 0) {
          encoder.flush(bout);
        }
      }
      // Final flush to cover trailing values
      encoder.flush(bout);

      // Decode and verify
      CamelDecoder decoder = new CamelDecoder();
      ByteBuffer buffer = ByteBuffer.wrap(bout.toByteArray());
      for (int i = 0; i < TOTAL_VALUES; i++) {
        Assert.assertTrue(
            "Decoder should have next for blockSize=" + blockSize, decoder.hasNext(buffer));
        double decoded = decoder.readDouble(buffer);

        Assert.assertEquals(
            "Mismatch at index " + i + " for blockSize=" + blockSize, original[i], decoded, 0);
      }
      Assert.assertFalse(
          "Decoder should be exhausted after reading all values for blockSize=" + blockSize,
          decoder.hasNext(buffer));
    }
  }
}
