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
package org.apache.tsfile.utils;

import java.util.Arrays;

public class Murmur128Hash {

  private static final int BLOCK_BYTES = 16;
  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;

  private Murmur128Hash() {
    // util class
  }

  /**
   * get hashcode of value by seed
   *
   * @param value value
   * @param seed seed
   * @return hashcode of value
   */
  public static int hash(String value, int seed) {
    byte[] bytes = value.getBytes();
    return (int) innerHash(bytes, bytes.length, bytes.length, seed, seed);
  }

  /**
   * get hashcode of two values by seed
   *
   * @param value1 the first value
   * @param value2 the second value
   * @param seed seed
   * @return hashcode of value
   */
  public static int hash(String value1, long value2, int seed) {
    int length = value1.length() + Long.BYTES;
    return (int)
        innerHash(
            BytesUtils.concatByteArray(value1.getBytes(), BytesUtils.longToBytes(value2)),
            length,
            length,
            seed,
            seed);
  }

  /** Methods to perform murmur 128 hash. */
  private static long getBlock(byte[] key, int blockOffset) {
    return ((long) key[blockOffset] & 0xff)
        + (((long) key[blockOffset + 1] & 0xff) << 8)
        + (((long) key[blockOffset + 2] & 0xff) << 16)
        + (((long) key[blockOffset + 3] & 0xff) << 24)
        + (((long) key[blockOffset + 4] & 0xff) << 32)
        + (((long) key[blockOffset + 5] & 0xff) << 40)
        + (((long) key[blockOffset + 6] & 0xff) << 48)
        + (((long) key[blockOffset + 7] & 0xff) << 56);
  }

  private static long rotl64(long v, int n) {
    return ((v << n) | (v >>> (64 - n)));
  }

  private static long fmix(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;
    return k;
  }

  // A complete 16-byte block updates h1 first. The new h1 feeds the update of h2.
  private static long mixH1(long h1, long h2, long k1) {
    h1 ^= rotl64(k1 * C1, 31) * C2;
    return (rotl64(h1, 27) + h2) * 5 + 0x52dce729;
  }

  private static long mixH2(long h1, long h2, long k2) {
    h2 ^= rotl64(k2 * C2, 33) * C1;
    return (rotl64(h2, 31) + h1) * 5 + 0x38495ab5;
  }

  /** Hashes multiple suffixes sharing the same prefix, with one state per Bloom filter seed. */
  static final class PrefixHasher {
    private final long[] h1;
    private final long[] h2;
    private final int prefixBlockBytes;
    private final int prefixTailBytes;
    private byte[] buffer;
    private int length;

    PrefixHasher(byte[] prefix, int[] seeds, int seedCount) {
      h1 = new long[seedCount];
      h2 = new long[seedCount];
      for (int i = 0; i < seedCount; i++) {
        h1[i] = seeds[i];
        h2[i] = seeds[i];
      }
      prefixTailBytes = prefix.length % BLOCK_BYTES;
      prefixBlockBytes = prefix.length - prefixTailBytes;
      for (int offset = 0; offset < prefixBlockBytes; offset += BLOCK_BYTES) {
        long k1 = getBlock(prefix, offset);
        long k2 = getBlock(prefix, offset + Long.BYTES);
        for (int i = 0; i < seedCount; i++) {
          h1[i] = mixH1(h1[i], h2[i], k1);
          h2[i] = mixH2(h1[i], h2[i], k2);
        }
      }
      // A partial prefix block must be completed with bytes from the current measurement.
      buffer = Arrays.copyOfRange(prefix, prefixBlockBytes, prefix.length);
    }

    void setSuffix(byte[] suffix) {
      length = prefixTailBytes + suffix.length;
      if (buffer.length < length) {
        buffer = Arrays.copyOf(buffer, length);
      }
      System.arraycopy(suffix, 0, buffer, prefixTailBytes, suffix.length);
    }

    int hash(int seedIndex) {
      return (int)
          innerHash(buffer, length, prefixBlockBytes + length, h1[seedIndex], h2[seedIndex]);
    }
  }

  private static long innerHash(byte[] key, int length, int totalLength, long h1, long h2) {
    int blockBytes = length - length % BLOCK_BYTES;
    int offset = 0;
    for (; offset < blockBytes; offset += BLOCK_BYTES) {
      h1 = mixH1(h1, h2, getBlock(key, offset));
      h2 = mixH2(h1, h2, getBlock(key, offset + Long.BYTES));
    }
    long k1 = 0;
    long k2 = 0;
    switch (length & 15) {
      case 15:
        k2 ^= ((long) key[offset + 14]) << 48;
      // fallthrough
      case 14:
        k2 ^= ((long) key[offset + 13]) << 40;
      // fallthrough
      case 13:
        k2 ^= ((long) key[offset + 12]) << 32;
      // fallthrough
      case 12:
        k2 ^= ((long) key[offset + 11]) << 24;
      // fallthrough
      case 11:
        k2 ^= ((long) key[offset + 10]) << 16;
      // fallthrough
      case 10:
        k2 ^= ((long) key[offset + 9]) << 8;
      // fallthrough
      case 9:
        k2 ^= key[offset + 8];
        k2 *= C2;
        k2 = rotl64(k2, 33);
        k2 *= C1;
        h2 ^= k2;
      // fallthrough
      case 8:
        k1 ^= ((long) key[offset + 7]) << 56;
      // fallthrough
      case 7:
        k1 ^= ((long) key[offset + 6]) << 48;
      // fallthrough
      case 6:
        k1 ^= ((long) key[offset + 5]) << 40;
      // fallthrough
      case 5:
        k1 ^= ((long) key[offset + 4]) << 32;
      // fallthrough
      case 4:
        k1 ^= ((long) key[offset + 3]) << 24;
      // fallthrough
      case 3:
        k1 ^= ((long) key[offset + 2]) << 16;
      // fallthrough
      case 2:
        k1 ^= ((long) key[offset + 1]) << 8;
      // fallthrough
      case 1:
        k1 ^= (key[offset]);
        k1 *= C1;
        k1 = rotl64(k1, 31);
        k1 *= C2;
        h1 ^= k1;
        break;
      default: // 0
        // do nothing

    }
    // ----------
    // finalization
    h1 ^= totalLength;
    h2 ^= totalLength;
    h1 += h2;
    h2 += h1;
    h1 = fmix(h1);
    h2 = fmix(h2);
    h1 += h2;
    h2 += h1;
    return h1 + h2;
  }
}
