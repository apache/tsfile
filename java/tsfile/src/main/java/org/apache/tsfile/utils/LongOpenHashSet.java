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

/**
 * A type-specific open-addressing hash set for {@code long} values.
 *
 * <p>Copied and trimmed from fastutil 8.5.8 (Apache License 2.0, http://fastutil.di.unimi.it/):
 *
 * <ul>
 *   <li>{@code it.unimi.dsi.fastutil.longs.LongOpenHashSet}
 *   <li>{@code it.unimi.dsi.fastutil.HashCommon} (inlined helpers: {@code mix}, {@code arraySize},
 *       {@code maxFill}, {@code nextPowerOfTwo})
 * </ul>
 *
 * <p>Only {@code add}/{@code contains}/{@code remove}/size ops are kept. Key {@code 0L} uses a
 * dedicated null-key slot, matching fastutil.
 *
 * <p>Copyright (C) 2002-2022 Sebastiano Vigna
 */
public class LongOpenHashSet {

  /** Copied from fastutil {@code Hash#DEFAULT_INITIAL_SIZE}. */
  private static final int DEFAULT_INITIAL_SIZE = 16;

  /** Copied from fastutil {@code Hash#DEFAULT_LOAD_FACTOR}. */
  private static final float DEFAULT_LOAD_FACTOR = .75f;

  /** Copied from fastutil {@code HashCommon#LONG_PHI}. */
  private static final long LONG_PHI = 0x9E3779B97F4A7C15L;

  /** The array of keys. Copied from fastutil {@code LongOpenHashSet#key}. */
  private long[] key;

  /**
   * The mask for wrapping a position counter. Copied from fastutil {@code LongOpenHashSet#mask}.
   */
  private int mask;

  /**
   * Whether this set contains the null key ({@code 0L}). Copied from fastutil {@code
   * LongOpenHashSet#containsNull}.
   */
  private boolean containsNull;

  /**
   * The current table size. Note that an additional element is allocated for storing the null key.
   * Copied from fastutil {@code LongOpenHashSet#n}.
   */
  private int n;

  /** Threshold after which we rehash. Copied from fastutil {@code LongOpenHashSet#maxFill}. */
  private int maxFill;

  /**
   * We never resize below this threshold, which is the construction-time {@code n}. Copied from
   * fastutil {@code LongOpenHashSet#minN}.
   */
  private final int minN;

  /**
   * Number of entries in the set (including the null key, if present). Copied from fastutil {@code
   * LongOpenHashSet#size}.
   */
  private int size;

  /** The acceptable load factor. Copied from fastutil {@code LongOpenHashSet#f}. */
  private final float f;

  /** Copied from fastutil {@code LongOpenHashSet(int, float)}. */
  public LongOpenHashSet(final int expected, final float f) {
    if (f <= 0 || f >= 1) {
      throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than 1");
    }
    if (expected < 0) {
      throw new IllegalArgumentException("The expected number of elements must be nonnegative");
    }
    this.f = f;
    minN = n = arraySize(expected, f);
    mask = n - 1;
    maxFill = maxFill(n, f);
    key = new long[n + 1];
  }

  /** Copied from fastutil {@code LongOpenHashSet(int)}. */
  public LongOpenHashSet(final int expected) {
    this(expected, DEFAULT_LOAD_FACTOR);
  }

  /** Copied from fastutil {@code LongOpenHashSet()}. */
  public LongOpenHashSet() {
    this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
  }

  /** Copied from fastutil {@code LongOpenHashSet#add(long)}. */
  public boolean add(final long k) {
    int pos;
    if (k == 0L) {
      if (containsNull) {
        return false;
      }
      containsNull = true;
    } else {
      long curr;
      final long[] key = this.key;
      if (!((curr = key[pos = (int) mix(k) & mask]) == 0L)) {
        if (curr == k) {
          return false;
        }
        while (!((curr = key[pos = (pos + 1) & mask]) == 0L)) {
          if (curr == k) {
            return false;
          }
        }
      }
      key[pos] = k;
    }
    if (size++ >= maxFill) {
      rehash(arraySize(size + 1, f));
    }
    return true;
  }

  /** Copied from fastutil {@code LongOpenHashSet#contains(long)}. */
  public boolean contains(final long k) {
    if (k == 0L) {
      return containsNull;
    }
    long curr;
    final long[] key = this.key;
    int pos;
    if ((curr = key[pos = (int) mix(k) & mask]) == 0L) {
      return false;
    }
    if (k == curr) {
      return true;
    }
    while (true) {
      if ((curr = key[pos = (pos + 1) & mask]) == 0L) {
        return false;
      }
      if (k == curr) {
        return true;
      }
    }
  }

  /** Copied from fastutil {@code LongOpenHashSet#remove(long)}. */
  public boolean remove(final long k) {
    if (k == 0L) {
      if (containsNull) {
        return removeNullEntry();
      }
      return false;
    }
    long curr;
    final long[] key = this.key;
    int pos;
    if ((curr = key[pos = (int) mix(k) & mask]) == 0L) {
      return false;
    }
    if (k == curr) {
      return removeEntry(pos);
    }
    while (true) {
      if ((curr = key[pos = (pos + 1) & mask]) == 0L) {
        return false;
      }
      if (k == curr) {
        return removeEntry(pos);
      }
    }
  }

  /** Copied from fastutil {@code LongOpenHashSet#clear()}. */
  public void clear() {
    if (size == 0) {
      return;
    }
    size = 0;
    containsNull = false;
    Arrays.fill(key, 0L);
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  /** Copied from fastutil {@code LongOpenHashSet#realSize()}. */
  private int realSize() {
    return containsNull ? size - 1 : size;
  }

  /** Copied from fastutil {@code LongOpenHashSet#removeEntry(int)}. */
  private boolean removeEntry(final int pos) {
    size--;
    shiftKeys(pos);
    if (n > minN && size < maxFill / 4 && n > DEFAULT_INITIAL_SIZE) {
      rehash(n / 2);
    }
    return true;
  }

  /** Copied from fastutil {@code LongOpenHashSet#removeNullEntry()}. */
  private boolean removeNullEntry() {
    containsNull = false;
    key[n] = 0L;
    size--;
    if (n > minN && size < maxFill / 4 && n > DEFAULT_INITIAL_SIZE) {
      rehash(n / 2);
    }
    return true;
  }

  /**
   * Shifts left entries with the specified hash code, starting at the specified position, and
   * empties the resulting free entry.
   *
   * <p>Copied from fastutil {@code LongOpenHashSet#shiftKeys(int)}.
   */
  private void shiftKeys(int pos) {
    int last;
    int slot;
    long curr;
    final long[] key = this.key;
    for (; ; ) {
      pos = ((last = pos) + 1) & mask;
      for (; ; ) {
        if ((curr = key[pos]) == 0L) {
          key[last] = 0L;
          return;
        }
        slot = (int) mix(curr) & mask;
        if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
          break;
        }
        pos = (pos + 1) & mask;
      }
      key[last] = curr;
    }
  }

  /** Copied from fastutil {@code LongOpenHashSet#rehash(int)}. */
  private void rehash(final int newN) {
    final long[] key = this.key;
    final int mask = newN - 1;
    final long[] newKey = new long[newN + 1];
    int i = n;
    int pos;
    for (int j = realSize(); j-- != 0; ) {
      while (key[--i] == 0L) {
        // skip empty slots
      }
      if (!(newKey[pos = (int) mix(key[i]) & mask] == 0L)) {
        while (!(newKey[pos = (pos + 1) & mask] == 0L)) {
          // probe
        }
      }
      newKey[pos] = key[i];
    }
    n = newN;
    this.mask = mask;
    maxFill = maxFill(n, f);
    this.key = newKey;
  }

  /** Copied from fastutil {@code HashCommon#mix(long)}. */
  private static long mix(final long x) {
    long h = x * LONG_PHI;
    h ^= h >>> 32;
    return h ^ (h >>> 16);
  }

  /** Copied from fastutil {@code HashCommon#arraySize(int, float)}. */
  private static int arraySize(final int expected, final float f) {
    final long s = Math.max(2, nextPowerOfTwo((long) Math.ceil(expected / f)));
    if (s > (1 << 30)) {
      throw new IllegalArgumentException(
          "Too large (" + expected + " expected elements with load factor " + f + ")");
    }
    return (int) s;
  }

  /** Copied from fastutil {@code HashCommon#maxFill(int, float)}. */
  private static int maxFill(final int n, final float f) {
    return Math.min((int) Math.ceil(n * f), n - 1);
  }

  /** Copied from fastutil {@code HashCommon#nextPowerOfTwo(long)}. */
  private static long nextPowerOfTwo(long x) {
    if (x == 0) {
      return 1;
    }
    x--;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    return (x | x >> 32) + 1;
  }
}
