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
import java.util.NoSuchElementException;

/**
 * A type-specific heap-based priority queue for {@code long} values (natural order).
 *
 * <p>Copied and trimmed from fastutil 8.5.8 (Apache License 2.0, http://fastutil.di.unimi.it/):
 *
 * <ul>
 *   <li>{@code it.unimi.dsi.fastutil.longs.LongHeapPriorityQueue}
 *   <li>{@code it.unimi.dsi.fastutil.longs.LongHeaps} ({@code upHeap}/{@code downHeap}
 *       natural-order branches inlined below)
 * </ul>
 *
 * <p>Comparator / serialization / collection constructors were dropped. Array growth uses {@link
 * Arrays#copyOf} instead of fastutil {@code LongArrays#grow}.
 *
 * <p>Copyright (C) 2003-2022 Paolo Boldi and Sebastiano Vigna
 */
public class LongHeapPriorityQueue {

  private static final long[] EMPTY = new long[0];

  /** The heap array. Copied from fastutil {@code LongHeapPriorityQueue#heap}. */
  private long[] heap = EMPTY;

  /**
   * The number of elements in this queue. Copied from fastutil {@code LongHeapPriorityQueue#size}.
   */
  private int size;

  /** Copied from fastutil {@code LongHeapPriorityQueue(int)} (natural order only). */
  public LongHeapPriorityQueue(int capacity) {
    if (capacity > 0) {
      this.heap = new long[capacity];
    }
  }

  /** Copied from fastutil {@code LongHeapPriorityQueue()}. */
  public LongHeapPriorityQueue() {
    this(0);
  }

  /** Copied from fastutil {@code LongHeapPriorityQueue#enqueue(long)}. */
  public void enqueue(long x) {
    if (size == heap.length) {
      heap = grow(heap, size + 1);
    }
    heap[size++] = x;
    upHeap(heap, size, size - 1);
  }

  /** Copied from fastutil {@code LongHeapPriorityQueue#dequeueLong()}. */
  public long dequeueLong() {
    if (size == 0) {
      throw new NoSuchElementException();
    }
    final long result = heap[0];
    heap[0] = heap[--size];
    if (size != 0) {
      downHeap(heap, size, 0);
    }
    return result;
  }

  /** Copied from fastutil {@code LongHeapPriorityQueue#firstLong()}. */
  public long firstLong() {
    if (size == 0) {
      throw new NoSuchElementException();
    }
    return heap[0];
  }

  /** Copied from fastutil {@code LongHeapPriorityQueue#size()}. */
  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  /** Copied from fastutil {@code LongHeapPriorityQueue#clear()}. */
  public void clear() {
    size = 0;
  }

  /** Local substitute for fastutil {@code LongArrays#grow(long[], int)}. */
  private static long[] grow(final long[] array, final int length) {
    int newLength = (int) Math.min(Math.max(2L * array.length, length), Integer.MAX_VALUE - 8);
    if (newLength < length) {
      newLength = length;
    }
    return Arrays.copyOf(array, newLength);
  }

  /**
   * Copied from fastutil {@code LongHeaps#downHeap(long[], int, int, LongComparator)} natural-order
   * branch ({@code c == null}).
   */
  private static int downHeap(final long[] heap, final int size, int i) {
    final long e = heap[i];
    int child;
    while ((child = (i << 1) + 1) < size) {
      long t = heap[child];
      final int right = child + 1;
      if (right < size && heap[right] < t) {
        t = heap[child = right];
      }
      if (e <= t) {
        break;
      }
      heap[i] = t;
      i = child;
    }
    heap[i] = e;
    return i;
  }

  /**
   * Copied from fastutil {@code LongHeaps#upHeap(long[], int, int, LongComparator)} natural-order
   * branch ({@code c == null}).
   */
  private static int upHeap(final long[] heap, final int size, int i) {
    final long e = heap[i];
    while (i != 0) {
      final int parent = (i - 1) >>> 1;
      final long t = heap[parent];
      if (t <= e) {
        break;
      }
      heap[i] = t;
      i = parent;
    }
    heap[i] = e;
    return i;
  }
}
