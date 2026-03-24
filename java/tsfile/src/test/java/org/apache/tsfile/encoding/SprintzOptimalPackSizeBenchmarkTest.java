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

package org.apache.tsfile.encoding;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.LongSprintzDecoder;
import org.apache.tsfile.encoding.encoder.LongSprintzEncoder;
import org.apache.tsfile.encoding.optimal.SprintzOptimalPackSize;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Benchmark test for comparing Sprintz encoding with default block size (8) vs optimal block size.
 * Demonstrates how to measure storage space and read/write time before and after optimization.
 */
public class SprintzOptimalPackSizeBenchmarkTest {

  private static final int SAMPLE_SIZE = 10240;
  private static final int WARMUP_ITERATIONS = 3;
  private static final int MEASURE_ITERATIONS = 10;

  @Before
  public void setUp() {
    TSFileDescriptor.getInstance().getConfig().setSprintzBlockSize(8);
    TSFileDescriptor.getInstance().getConfig().setSprintzUseOptimalPackSize(false);
  }

  /**
   * Generate sample residuals (Sprintz transform format) from raw values for optimal pack size
   * computation. Matches LongSprintzEncoder's delta predict + transform: pred = value - prev;
   * residual = pred <= 0 ? -2*pred : 2*pred-1.
   */
  private long[] getResidualsForOptimal(long[] rawValues) {
    if (rawValues.length < 2) {
      return rawValues;
    }
    long[] residuals = new long[rawValues.length - 1];
    long prev = rawValues[0];
    for (int i = 1; i < rawValues.length; i++) {
      long pred = rawValues[i] - prev;
      residuals[i - 1] = pred <= 0 ? -2 * pred : 2 * pred - 1;
      prev = rawValues[i];
    }
    return residuals;
  }

  @Test
  public void testSprintzOptimalPackSizeBenchmark() throws IOException {
    long[] testData = generateTestData(SAMPLE_SIZE);

    System.out.println("=== Sprintz Long Encoding Benchmark (Optimization Comparison) ===\n");
    System.out.println("Sample size: " + SAMPLE_SIZE + " values\n");

    // 1. Default block size (8)
    TSFileDescriptor.getInstance().getConfig().setSprintzBlockSize(8);
    BenchmarkResult defaultResult = runBenchmark(testData);
    System.out.println("--- Default (Block size = 8) ---");
    printResult(defaultResult);

    // 2. Find optimal block size from sample
    long[] residuals = getResidualsForOptimal(testData);
    int optimalBlockSize = SprintzOptimalPackSize.findOptimalPackSize(residuals);
    optimalBlockSize = Math.max(1, Math.min(32, optimalBlockSize));
    System.out.println("\nOptimal block size from SprintzOptimalPackSize: " + optimalBlockSize);

    // 3. Optimal block size
    TSFileDescriptor.getInstance().getConfig().setSprintzBlockSize(optimalBlockSize);
    BenchmarkResult optimalResult = runBenchmark(testData);
    System.out.println("\n--- Optimal (Block size = " + optimalBlockSize + ") ---");
    printResult(optimalResult);

    // 4. Comparison
    System.out.println("\n--- Comparison ---");
    double sizeRatio =
        (double) optimalResult.compressedSize / (double) defaultResult.compressedSize;
    double encodeRatio =
        (double) optimalResult.encodeTimeNs / (double) defaultResult.encodeTimeNs;
    double decodeRatio =
        (double) optimalResult.decodeTimeNs / (double) defaultResult.decodeTimeNs;
    System.out.printf(
        "Compressed size: %.2f%% of default (smaller is better)%n", sizeRatio * 100);
    System.out.printf("Encode time: %.2f%% of default%n", encodeRatio * 100);
    System.out.printf("Decode time: %.2f%% of default%n", decodeRatio * 100);

    // 5. Auto optimal mode (each block finds its own optimal pack size)
    TSFileDescriptor.getInstance().getConfig().setSprintzUseOptimalPackSize(true);
    BenchmarkResult autoOptimalResult = runBenchmark(testData);
    System.out.println("\n--- Auto Optimal (per-block optimal pack size) ---");
    printResult(autoOptimalResult);

    // Restore default
    TSFileDescriptor.getInstance().getConfig().setSprintzBlockSize(8);
    TSFileDescriptor.getInstance().getConfig().setSprintzUseOptimalPackSize(false);
  }

  @Test
  public void testOptimalModeEncodeDecode() throws IOException {
    TSFileDescriptor.getInstance().getConfig().setSprintzUseOptimalPackSize(true);
    try {
      LongSprintzEncoder encoder = new LongSprintzEncoder();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      long[] data = generateTestData(128);
      for (long v : data) {
        encoder.encode(v, baos);
      }
      encoder.flush(baos);

      ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
      LongSprintzDecoder decoder = new LongSprintzDecoder();
      for (long expected : data) {
        assertTrue("Expected more data", decoder.hasNext(buffer));
        long actual = decoder.readLong(buffer);
        assertEquals("Value mismatch", expected, actual);
      }
      assertFalse("Should have no more data", decoder.hasNext(buffer));
    } finally {
      TSFileDescriptor.getInstance().getConfig().setSprintzUseOptimalPackSize(false);
    }
  }

  private long[] generateTestData(int size) {
    Random rand = new Random(42);
    long[] data = new long[size];
    long base = 1000000L;
    for (int i = 0; i < size; i++) {
      data[i] = base + (long) (rand.nextGaussian() * 1000);
    }
    return data;
  }

  private BenchmarkResult runBenchmark(long[] data) throws IOException {
    long totalEncodeNs = 0;
    long totalDecodeNs = 0;
    int compressedSize = 0;

    for (int iter = 0; iter < WARMUP_ITERATIONS + MEASURE_ITERATIONS; iter++) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      LongSprintzEncoder encoder = new LongSprintzEncoder();

      long encodeStart = System.nanoTime();
      for (long v : data) {
        encoder.encode(v, baos);
      }
      encoder.flush(baos);
      long encodeEnd = System.nanoTime();

      byte[] encoded = baos.toByteArray();
      compressedSize = encoded.length;

      ByteBuffer buffer = ByteBuffer.wrap(encoded);
      LongSprintzDecoder decoder = new LongSprintzDecoder();
      List<Long> decoded = new ArrayList<>();

      long decodeStart = System.nanoTime();
      while (decoder.hasNext(buffer)) {
        decoded.add(decoder.readLong(buffer));
      }
      long decodeEnd = System.nanoTime();

      if (iter >= WARMUP_ITERATIONS) {
        totalEncodeNs += (encodeEnd - encodeStart);
        totalDecodeNs += (decodeEnd - decodeStart);
      }
    }

    return new BenchmarkResult(
        compressedSize,
        totalEncodeNs / MEASURE_ITERATIONS,
        totalDecodeNs / MEASURE_ITERATIONS);
  }

  private void printResult(BenchmarkResult r) {
    double compressionRatio = (double) r.compressedSize / (SAMPLE_SIZE * 8.0);
    double encodeThroughput = (SAMPLE_SIZE * 8.0) / (r.encodeTimeNs / 1e9) / (1024 * 1024);
    double decodeThroughput = (SAMPLE_SIZE * 8.0) / (r.decodeTimeNs / 1e9) / (1024 * 1024);
    System.out.printf("  Compressed size: %d bytes (ratio: %.4f)%n", r.compressedSize, compressionRatio);
    System.out.printf("  Encode time: %.3f ms (throughput: %.2f MB/s)%n", r.encodeTimeNs / 1e6, encodeThroughput);
    System.out.printf("  Decode time: %.3f ms (throughput: %.2f MB/s)%n", r.decodeTimeNs / 1e6, decodeThroughput);
  }

  private static class BenchmarkResult {
    final int compressedSize;
    final long encodeTimeNs;
    final long decodeTimeNs;

    BenchmarkResult(int compressedSize, long encodeTimeNs, long decodeTimeNs) {
      this.compressedSize = compressedSize;
      this.encodeTimeNs = encodeTimeNs;
      this.decodeTimeNs = decodeTimeNs;
    }
  }
}
