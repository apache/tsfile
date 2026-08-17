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

import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Correctness and smoke-performance tests for the fastutil-derived {@link LongHeapPriorityQueue}
 * and {@link LongOpenHashSet}.
 *
 * <p>Coverage split (no overlap by design):
 *
 * <ul>
 *   <li>{@link #testLongHeapPriorityQueueOrder()} — heap only
 *   <li>{@link #testLongOpenHashSetBasicOps()} — set API / {@code 0L} / ctor validation
 *   <li>{@link #testLongOpenHashSetMatchesHashSet()} — random parity vs {@link HashSet}
 *   <li>{@link #testLongOpenHashSetHeavyChurn()} — sustained add/remove (rehash / {@code
 *       shiftKeys})
 *   <li>{@link #testHeapAndSetDedupPatternMatchesPriorityQueue()} — combined dedup-merge pattern
 *   <li>{@link #testPrimitiveStructuresPerformance()} — micro-benchmark only
 * </ul>
 */
public class LongOpenHashSetAndHeapPriorityQueueTest {

  private static final String RUN_PERFORMANCE_TEST_PROPERTY = "tsfile.runPerformanceTests";

  /**
   * Heap-only: empty-queue errors, natural order (incl. duplicates / extremes), {@code clear} and
   * growth — step-by-step parity with {@link PriorityQueue}{@code <Long>}.
   */
  @Test
  public void testLongHeapPriorityQueueOrder() {
    LongHeapPriorityQueue empty = new LongHeapPriorityQueue();
    assertTrue(empty.isEmpty());
    assertEquals(0, empty.size());
    try {
      empty.firstLong();
      fail("expected NoSuchElementException on firstLong()");
    } catch (NoSuchElementException expected) {
      // ok
    }
    try {
      empty.dequeueLong();
      fail("expected NoSuchElementException on dequeueLong()");
    } catch (NoSuchElementException expected) {
      // ok
    }

    LongHeapPriorityQueue heap = new LongHeapPriorityQueue();
    PriorityQueue<Long> boxed = new PriorityQueue<>();
    long[] values =
        new long[] {5L, 1L, 3L, 2L, 4L, 1L, 1L, Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1L};
    for (long v : values) {
      heap.enqueue(v);
      boxed.add(v);
      assertEquals(boxed.size(), heap.size());
      assertEquals(boxed.peek().longValue(), heap.firstLong());
    }
    while (!boxed.isEmpty()) {
      assertEquals(boxed.poll().longValue(), heap.dequeueLong());
      assertEquals(boxed.size(), heap.size());
      if (!boxed.isEmpty()) {
        assertEquals(boxed.peek().longValue(), heap.firstLong());
      }
    }

    // clear + grow from zero-capacity
    heap.enqueue(9L);
    heap.clear();
    assertTrue(heap.isEmpty());
    boxed.clear();
    for (int i = 0; i < 64; i++) {
      long v = 1000L - i;
      heap.enqueue(v);
      boxed.add(v);
    }
    List<Long> fromPrimitive = new ArrayList<>();
    List<Long> fromBoxed = new ArrayList<>();
    while (!heap.isEmpty()) {
      fromPrimitive.add(heap.dequeueLong());
      fromBoxed.add(boxed.poll());
    }
    assertEquals(fromBoxed, fromPrimitive);
  }

  /**
   * Set-only deterministic API smoke: empty ops, duplicate add, null-key {@code 0L}, extremes,
   * {@code clear}, and illegal constructor arguments. Broader random parity lives in {@link
   * #testLongOpenHashSetMatchesHashSet()}.
   */
  @Test
  public void testLongOpenHashSetBasicOps() {
    LongOpenHashSet set = new LongOpenHashSet(8);
    Set<Long> boxed = new HashSet<>();

    assertTrue(set.isEmpty());
    assertFalse(set.contains(1L));
    assertEquals(boxed.remove(1L), set.remove(1L));

    assertEquals(boxed.add(10L), set.add(10L));
    assertEquals(boxed.add(10L), set.add(10L));
    assertTrue(set.contains(10L));
    assertFalse(set.contains(11L));

    // fastutil null-key slot
    assertEquals(boxed.add(0L), set.add(0L));
    assertEquals(boxed.remove(0L), set.remove(0L));
    assertEquals(boxed.add(0L), set.add(0L));
    assertEquals(boxed.add(0L), set.add(0L));

    for (long edge : new long[] {Long.MIN_VALUE, Long.MAX_VALUE, -1L}) {
      assertEquals(boxed.add(edge), set.add(edge));
      assertEquals(boxed.contains(edge), set.contains(edge));
    }
    assertEquals(boxed.size(), set.size());

    set.clear();
    boxed.clear();
    assertTrue(set.isEmpty());
    assertFalse(set.contains(0L));
    assertEquals(boxed.add(0L), set.add(0L));
    assertEquals(1, set.size());

    try {
      new LongOpenHashSet(-1);
      fail("expected IllegalArgumentException for negative expected size");
    } catch (IllegalArgumentException expected) {
      // ok
    }
    try {
      new LongOpenHashSet(8, 0f);
      fail("expected IllegalArgumentException for invalid load factor");
    } catch (IllegalArgumentException expected) {
      // ok
    }
    try {
      new LongOpenHashSet(8, 1f);
      fail("expected IllegalArgumentException for invalid load factor");
    } catch (IllegalArgumentException expected) {
      // ok
    }
  }

  /**
   * Set-only randomized parity with {@link HashSet}{@code <Long>}: mixed add/contains/remove,
   * boundary keys, and a mid-run {@code clear}. Does not stress sliding-window rehash (see {@link
   * #testLongOpenHashSetHeavyChurn()}).
   */
  @Test
  public void testLongOpenHashSetMatchesHashSet() {
    LongOpenHashSet primitive = new LongOpenHashSet(16);
    Set<Long> boxed = new HashSet<>();
    Random random = new Random(42);
    long[] edges =
        new long[] {0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE};

    for (int i = 0; i < 20_000; i++) {
      long value;
      int mode = i % 10;
      if (mode == 0) {
        value = edges[random.nextInt(edges.length)];
      } else {
        value = random.nextLong();
      }

      int op = i % 5;
      if (op <= 1) {
        assertEquals(boxed.add(value), primitive.add(value));
      } else if (op <= 3) {
        assertEquals(boxed.contains(value), primitive.contains(value));
      } else {
        assertEquals(boxed.remove(value), primitive.remove(value));
      }
      assertEquals(boxed.size(), primitive.size());

      if (i == 10_000) {
        primitive.clear();
        boxed.clear();
        assertEquals(0, primitive.size());
      }
    }

    for (Long v : boxed) {
      assertTrue(primitive.contains(v));
    }
  }

  /**
   * Set-only sliding-window churn (fixed live size) to exercise {@code shiftKeys} / shrink-rehash,
   * including periodic {@code 0L}. Compared against {@link HashSet} for each op.
   */
  @Test
  public void testLongOpenHashSetHeavyChurn() {
    LongOpenHashSet set = new LongOpenHashSet(8);
    Set<Long> boxed = new HashSet<>();
    final int window = 16;
    for (int i = 0; i < 100_000; i++) {
      long addVal = (i % 32 == 0) ? 0L : i;
      assertEquals(boxed.add(addVal), set.add(addVal));
      if (i >= window) {
        long removeVal = ((i - window) % 32 == 0) ? 0L : (i - window);
        assertEquals(boxed.remove(removeVal), set.remove(removeVal));
      }
      assertEquals(boxed.size(), set.size());
    }
    for (Long v : new ArrayList<>(boxed)) {
      assertTrue(set.remove(v));
    }
    assertTrue(set.isEmpty());
  }

  /**
   * Combined heap+set dedup pattern used by {@code DataSetWithoutTimeGenerator#timeHeapPut/Get},
   * mirrored against {@link PriorityQueue}+{@link HashSet}. Covers aligned duplicate puts and a few
   * representative time bases (0 / normal / near {@link Long#MAX_VALUE}).
   */
  @Test
  public void testHeapAndSetDedupPatternMatchesPriorityQueue() {
    final int series = 64;
    final int rows = 30_000;
    long[] bases = new long[] {0L, 1_700_000_000_000L, Long.MAX_VALUE - series - 10};
    for (long baseTime : bases) {
      runDedupMergeParity(series, rows, baseTime);
    }
  }

  /**
   * Micro-benchmark only (not a correctness test): boxed vs primitive under the same merge pattern.
   * Soft guard against catastrophic regressions. Gated like other TsFile perf tests via {@code
   * -Dtsfile.runPerformanceTests=true}.
   */
  @Test
  public void testPrimitiveStructuresPerformance() {
    Assume.assumeTrue(
        "Set -Dtsfile.runPerformanceTests=true to run the performance test",
        Boolean.getBoolean(RUN_PERFORMANCE_TEST_PROPERTY));

    final int series = 50;
    final int rows = 100_000;
    final long baseTime = 1_700_000_000_000L;

    runBoxed(series, 10_000, baseTime);
    runPrimitive(series, 10_000, baseTime);

    long boxedNs = runBoxed(series, rows, baseTime);
    long primitiveNs = runPrimitive(series, rows, baseTime);

    System.out.printf(
        "DataSetWithoutTimeGenerator heap/set microbench: boxed=%d ms, primitive=%d ms, speedup=%.2fx%n",
        boxedNs / 1_000_000L, primitiveNs / 1_000_000L, (double) boxedNs / (double) primitiveNs);

    assertTrue(
        "primitive path unexpectedly much slower than boxed: boxed="
            + boxedNs
            + "ns primitive="
            + primitiveNs
            + "ns",
        primitiveNs < boxedNs * 2);
  }

  private static void runDedupMergeParity(int series, int rows, long baseTime) {
    LongHeapPriorityQueue primitiveHeap = new LongHeapPriorityQueue(series);
    LongOpenHashSet primitiveSet = new LongOpenHashSet(series);
    PriorityQueue<Long> boxedHeap = new PriorityQueue<>();
    Set<Long> boxedSet = new HashSet<>();

    Random random = new Random(7 ^ baseTime);
    long[] heads = new long[series];
    for (int i = 0; i < series; i++) {
      heads[i] = baseTime + i;
      put(primitiveHeap, primitiveSet, heads[i]);
      putBoxed(boxedHeap, boxedSet, heads[i]);
    }

    for (int row = 0; row < rows; row++) {
      assertEquals(boxedHeap.peek().longValue(), primitiveHeap.firstLong());
      long min = poll(primitiveHeap, primitiveSet);
      assertEquals(min, pollBoxed(boxedHeap, boxedSet));
      assertEquals(boxedSet.size(), primitiveSet.size());

      for (int s = 0; s < series; s++) {
        if (heads[s] == min) {
          // step==0 produces duplicate put (dedup path)
          heads[s] += (row % 5 == 0) ? 0 : 1 + random.nextInt(3);
          put(primitiveHeap, primitiveSet, heads[s]);
          putBoxed(boxedHeap, boxedSet, heads[s]);
        }
      }
      assertEquals(boxedHeap.size(), primitiveHeap.size());
    }
  }

  private static long runBoxed(int series, int rows, long baseTime) {
    PriorityQueue<Long> heap = new PriorityQueue<>(series);
    Set<Long> set = new HashSet<>(series * 2);
    long[] heads = new long[series];
    for (int i = 0; i < series; i++) {
      heads[i] = baseTime + i;
      putBoxed(heap, set, heads[i]);
    }

    long start = System.nanoTime();
    for (int row = 0; row < rows; row++) {
      long min = pollBoxed(heap, set);
      for (int s = 0; s < series; s++) {
        if (heads[s] == min) {
          heads[s] += 1;
          putBoxed(heap, set, heads[s]);
        }
      }
    }
    return System.nanoTime() - start;
  }

  private static long runPrimitive(int series, int rows, long baseTime) {
    LongHeapPriorityQueue heap = new LongHeapPriorityQueue(series);
    LongOpenHashSet set = new LongOpenHashSet(series);
    long[] heads = new long[series];
    for (int i = 0; i < series; i++) {
      heads[i] = baseTime + i;
      put(heap, set, heads[i]);
    }

    long start = System.nanoTime();
    for (int row = 0; row < rows; row++) {
      long min = poll(heap, set);
      for (int s = 0; s < series; s++) {
        if (heads[s] == min) {
          heads[s] += 1;
          put(heap, set, heads[s]);
        }
      }
    }
    return System.nanoTime() - start;
  }

  private static void put(LongHeapPriorityQueue heap, LongOpenHashSet set, long time) {
    if (!set.contains(time)) {
      set.add(time);
      heap.enqueue(time);
    }
  }

  private static long poll(LongHeapPriorityQueue heap, LongOpenHashSet set) {
    long t = heap.dequeueLong();
    set.remove(t);
    return t;
  }

  private static void putBoxed(PriorityQueue<Long> heap, Set<Long> set, long time) {
    if (!set.contains(time)) {
      set.add(time);
      heap.add(time);
    }
  }

  private static long pollBoxed(PriorityQueue<Long> heap, Set<Long> set) {
    Long t = heap.poll();
    set.remove(t);
    return t;
  }
}
