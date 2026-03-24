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

package org.apache.tsfile.encoding.optimal;

/**
 * Utility for finding optimal pack size for Sprintz bit-packing encoding using RMQ (Range Maximum
 * Query) sparse table. Minimizes total storage cost: sum(pack_size * max_bitwidth_in_pack) +
 * num_packs * BITS_PER_BLOCK_OVERHEAD. Each block in the encoder writes packSize(1 byte) +
 * bitWidth(1 byte) + preValue(8 bytes) = 80 bits overhead, so we use 80 in the cost model.
 */
public class SprintzOptimalPackSize {

  private SprintzOptimalPackSize() {}

  /**
   * Find optimal pack size for long values (e.g., Sprintz residuals) using RMQ-based cost
   * minimization.
   *
   * @param values array of non-negative long values (e.g., after Sprintz predict transform)
   * @return optimal pack size in range [1, n]
   */
  public static int findOptimalPackSize(long[] values) {
    int n = values.length;
    if (n <= 0) {
      return 1;
    }
    if (n < 8) {
      return Math.max(1, n);
    }

    // Build sparse table for RMQ on bit widths
    int[] bitWidths = new int[n];
    long globalMax = 0;
    for (int i = 0; i < n; i++) {
      long value = values[i];
      if (value > globalMax) {
        globalMax = value;
      }
      bitWidths[i] = 64 - Long.numberOfLeadingZeros(Math.max(1, value));
    }

    // Per-block overhead in encoder: 1 byte packSize + 1 byte bitWidth + 8 bytes preValue = 80 bits
    final int bitsPerBlockOverhead = 80;

    int logN = 32 - Integer.numberOfLeadingZeros(n);
    int[][] st = new int[logN][n];

    for (int i = 0; i < n; i++) {
      st[0][i] = bitWidths[i];
    }

    for (int k = 1; k < logN; k++) {
      int step = 1 << (k - 1);
      for (int i = 0; i + (1 << k) <= n; i++) {
        st[k][i] = Math.max(st[k - 1][i], st[k - 1][i + step]);
      }
    }

    int[] log2 = new int[n + 1];
    for (int i = 2; i <= n; i++) {
      log2[i] = log2[i / 2] + 1;
    }

    int bestPackSize = 1;
    long bestCost = Long.MAX_VALUE;
    int maxPackSize = Math.min(32, n); // encoder caps pack size at 32

    for (int p = 1; p <= maxPackSize; p++) {
      int m = (n + p - 1) / p;
      long cost = 0;

      for (int i = 0; i < m - 1; i++) {
        int start = i * p;
        int end = start + p - 1;
        int k = log2[p];
        int maxBitWidth =
            Math.max(st[k][start], st[k][end - (1 << k) + 1]);
        cost += (long) p * maxBitWidth;
      }

      if (m > 0) {
        int lastStart = (m - 1) * p;
        int lastEnd = n - 1;
        int r = n - lastStart;

        if (r > 0) {
          int k = log2[r];
          int lastMaxBitWidth =
              Math.max(st[k][lastStart], st[k][lastEnd - (1 << k) + 1]);
          cost += (long) r * lastMaxBitWidth;
        }
      }

      cost += (long) m * bitsPerBlockOverhead;

      if (cost < bestCost) {
        bestCost = cost;
        bestPackSize = p;
      }
    }

    return bestPackSize;
  }
}
