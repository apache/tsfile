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
 * Utility for finding optimal pack size for Sprintz bit-packing encoding. Minimizes total storage
 * cost: sum(pack_size * max_bitwidth_in_pack) + num_packs * BITS_PER_BLOCK_OVERHEAD. Each block in
 * the encoder writes packSize(1 byte) + bitWidth(1 byte) + preValue(8 bytes) = 80 bits overhead,
 * so we use 80 in the cost model.
 *
 * <p>Implementation: for each candidate pack size p in [1, min(32, n)], scan segments in O(n) with
 * no auxiliary heap allocation (avoids sparse-table alloc/GC on every chunk). Overall O(32 * n),
 * which is fast for typical chunk sizes and much cheaper than building an RMQ table per call.
 */
public class SprintzOptimalPackSize {

  private SprintzOptimalPackSize() {}

  /**
   * Find optimal pack size for long values (e.g., Sprintz residuals) using cost minimization.
   *
   * @param values array of non-negative long values (e.g., after Sprintz predict transform)
   * @return optimal pack size in range [1, n]
   */
  public static int findOptimalPackSize(long[] values) {
    return findOptimalPackSize(values, values.length, null);
  }

  /**
   * Same as {@link #findOptimalPackSize(long[])} but uses only {@code values[0..n)} and optionally
   * reuses {@code bwScratch} (length &gt;= n) to store per-element bit widths, avoiding extra
   * allocation and redundant {@code numberOfLeadingZeros} work across pack-size candidates.
   */
  public static int findOptimalPackSize(long[] values, int n, int[] bwScratch) {
    if (n <= 0) {
      return 1;
    }
    if (n < 8) {
      return Math.max(1, n);
    }

    int[] bw = bwScratch != null && bwScratch.length >= n ? bwScratch : new int[n];
    for (int i = 0; i < n; i++) {
      long value = values[i];
      bw[i] = 64 - Long.numberOfLeadingZeros(Math.max(1L, value));
    }

    // Per-block overhead in encoder: 1 byte packSize + 1 byte bitWidth + 8 bytes preValue = 80 bits
    final int bitsPerBlockOverhead = 80;

    int bestPackSize = 1;
    long bestCost = Long.MAX_VALUE;
    int maxPackSize = Math.min(32, n);

    for (int p = 1; p <= maxPackSize; p++) {
      int m = (n + p - 1) / p;
      long cost = 0;

      for (int i = 0; i < m; i++) {
        int start = i * p;
        int end = Math.min(start + p, n);
        int maxBw = 1;
        for (int j = start; j < end; j++) {
          int b = bw[j];
          if (b > maxBw) {
            maxBw = b;
          }
        }
        cost += (long) (end - start) * maxBw;
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
