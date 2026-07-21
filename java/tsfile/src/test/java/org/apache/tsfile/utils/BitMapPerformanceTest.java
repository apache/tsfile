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
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BitMapPerformanceTest {

  private static final int BIT_MAP_SIZE = Long.SIZE;
  private static final int WARMUP_ROUNDS = 3;
  private static final int MEASUREMENT_ROUNDS = 7;
  private static final int CHEAP_OPERATION_COUNT = 2_000_000;
  private static final int ALLOCATION_OPERATION_COUNT = 200_000;
  private static final int COMPOSITE_OPERATION_COUNT = 50_000;
  private static final int BULK_INSTANCE_COUNT = 1_000_000;
  private static final String RUN_PERFORMANCE_TEST_PROPERTY = "tsfile.runPerformanceTests";

  private static volatile long blackHole;
  private static volatile Object referenceBlackHole;

  @Test
  public void testLength64Implementations() {
    Assume.assumeTrue(
        "Set -Dtsfile.runPerformanceTests=true to run the performance test",
        Boolean.getBoolean(RUN_PERFORMANCE_TEST_PROPERTY));

    List<BenchmarkResult> results = new ArrayList<>();
    results.add(
        benchmark(
            "isMarked",
            CHEAP_OPERATION_COUNT,
            readMarkedWorkload(arrayBitMap()),
            readMarkedWorkload(longBitMap())));
    results.add(
        benchmark(
            "isAllMarked/isAllUnmarked",
            CHEAP_OPERATION_COUNT,
            readStateWorkload(BitMapPerformanceTest::arrayBitMap),
            readStateWorkload(BitMapPerformanceTest::longBitMap)));
    results.add(
        benchmark(
            "mark/unmark (one write)",
            CHEAP_OPERATION_COUNT,
            pointWriteWorkload(BitMapPerformanceTest::arrayBitMap),
            pointWriteWorkload(BitMapPerformanceTest::longBitMap)));
    results.add(
        benchmark(
            "markRange/unmarkRange (one write)",
            CHEAP_OPERATION_COUNT,
            rangeWriteWorkload(BitMapPerformanceTest::arrayBitMap),
            rangeWriteWorkload(BitMapPerformanceTest::longBitMap)));
    results.add(
        benchmark(
            "markAll/reset (one write)",
            CHEAP_OPERATION_COUNT,
            bulkWriteWorkload(BitMapPerformanceTest::arrayBitMap),
            bulkWriteWorkload(BitMapPerformanceTest::longBitMap)));
    results.add(
        benchmark(
            "getByteArray",
            ALLOCATION_OPERATION_COUNT,
            getByteArrayWorkload(arrayBitMap()),
            getByteArrayWorkload(longBitMap())));
    results.add(
        benchmark(
            "getTruncatedByteArray",
            ALLOCATION_OPERATION_COUNT,
            getTruncatedByteArrayWorkload(arrayBitMap()),
            getTruncatedByteArrayWorkload(longBitMap())));
    results.add(
        benchmark(
            "constructFromBytes",
            ALLOCATION_OPERATION_COUNT,
            constructFromBytesWorkload(BitMapArrayImpl::new),
            constructFromBytesWorkload(BitMapLongImpl::new)));
    results.add(
        benchmark(
            "clone",
            ALLOCATION_OPERATION_COUNT,
            cloneWorkload(arrayBitMap()),
            cloneWorkload(longBitMap())));
    results.add(
        benchmark(
            "merge",
            CHEAP_OPERATION_COUNT,
            mergeWorkload(BitMapPerformanceTest::arrayBitMap),
            mergeWorkload(BitMapPerformanceTest::longBitMap)));
    results.add(
        benchmark(
            "append",
            COMPOSITE_OPERATION_COUNT,
            appendWorkload(BitMapPerformanceTest::arrayBitMap),
            appendWorkload(BitMapPerformanceTest::longBitMap)));
    results.add(
        benchmark(
            "getRegion",
            COMPOSITE_OPERATION_COUNT,
            regionWorkload(arrayBitMap()),
            regionWorkload(longBitMap())));
    results.add(
        benchmark(
            "equals/equalsInRange",
            ALLOCATION_OPERATION_COUNT,
            equalityWorkload(BitMapPerformanceTest::arrayBitMap),
            equalityWorkload(BitMapPerformanceTest::longBitMap)));
    results.add(
        benchmark(
            "hashCode",
            CHEAP_OPERATION_COUNT,
            hashCodeWorkload(BitMapPerformanceTest::arrayBitMap),
            hashCodeWorkload(BitMapPerformanceTest::longBitMap)));
    results.add(
        benchmark(
            "toString",
            COMPOSITE_OPERATION_COUNT,
            toStringWorkload(arrayBitMap()),
            toStringWorkload(longBitMap())));

    printResults(results);
    printMemoryUsage();
  }

  private static BenchmarkResult benchmark(
      String name, int operationCount, Workload arrayWorkload, Workload longWorkload) {
    for (int round = 0; round < WARMUP_ROUNDS; round++) {
      run(arrayWorkload, operationCount);
      run(longWorkload, operationCount);
    }

    long[] arrayNanos = new long[MEASUREMENT_ROUNDS];
    long[] longNanos = new long[MEASUREMENT_ROUNDS];
    for (int round = 0; round < MEASUREMENT_ROUNDS; round++) {
      TimedRun arrayRun;
      TimedRun longRun;
      if ((round & 1) == 0) {
        arrayRun = run(arrayWorkload, operationCount);
        longRun = run(longWorkload, operationCount);
      } else {
        longRun = run(longWorkload, operationCount);
        arrayRun = run(arrayWorkload, operationCount);
      }
      assertEquals(arrayRun.checksum, longRun.checksum);
      arrayNanos[round] = arrayRun.elapsedNanos;
      longNanos[round] = longRun.elapsedNanos;
    }
    return new BenchmarkResult(
        name,
        median(arrayNanos) / (double) operationCount,
        median(longNanos) / (double) operationCount);
  }

  private static TimedRun run(Workload workload, int operationCount) {
    long startTime = System.nanoTime();
    long checksum = workload.run(operationCount);
    long elapsedNanos = System.nanoTime() - startTime;
    blackHole = checksum;
    return new TimedRun(elapsedNanos, checksum);
  }

  private static long median(long[] values) {
    long[] sortedValues = Arrays.copyOf(values, values.length);
    Arrays.sort(sortedValues);
    return sortedValues[sortedValues.length / 2];
  }

  private static Workload readMarkedWorkload(BitMap bitMap) {
    markAlternatingBits(bitMap);
    return operationCount -> {
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        checksum += bitMap.isMarked(i & 63) ? 1 : 0;
      }
      return checksum;
    };
  }

  private static Workload readStateWorkload(BitMapFactory factory) {
    BitMap[] bitMaps = new BitMap[BIT_MAP_SIZE];
    for (int i = 0; i < bitMaps.length; i++) {
      bitMaps[i] = factory.create();
      if (i % 3 == 0) {
        bitMaps[i].markAll();
      } else if (i % 3 == 1) {
        markAlternatingBits(bitMaps[i]);
      }
    }
    return operationCount -> {
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        BitMap bitMap = bitMaps[i & 63];
        checksum += bitMap.isAllMarked() ? 1 : 0;
        checksum += bitMap.isAllUnmarked() ? 2 : 0;
        checksum += bitMap.isAllUnmarked(BIT_MAP_SIZE) ? 4 : 0;
      }
      return checksum + BIT_MAP_SIZE;
    };
  }

  private static Workload pointWriteWorkload(BitMapFactory factory) {
    return operationCount -> {
      BitMap bitMap = factory.create();
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        int position = i & 63;
        if ((i & 64) == 0) {
          bitMap.mark(position);
        } else {
          bitMap.unmark(position);
        }
        if ((i & 255) == 0) {
          checksum += bitMap.isMarked(position) ? 1 : 0;
        }
      }
      referenceBlackHole = bitMap;
      return checksum;
    };
  }

  private static Workload rangeWriteWorkload(BitMapFactory factory) {
    return operationCount -> {
      BitMap bitMap = factory.create();
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        int start = (i * 7) & 63;
        int length = Math.min(8, BIT_MAP_SIZE - start);
        if ((i & 64) == 0) {
          bitMap.markRange(start, length);
        } else {
          bitMap.unmarkRange(start, length);
        }
        if ((i & 255) == 0) {
          checksum += bitMap.isMarked(start) ? 1 : 0;
        }
      }
      referenceBlackHole = bitMap;
      return checksum;
    };
  }

  private static Workload bulkWriteWorkload(BitMapFactory factory) {
    return operationCount -> {
      BitMap bitMap = factory.create();
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        if ((i & 1) == 0) {
          bitMap.markAll();
        } else {
          bitMap.reset();
        }
        if ((i & 255) == 0) {
          checksum += bitMap.isAllMarked() ? 1 : 0;
        }
      }
      referenceBlackHole = bitMap;
      return checksum;
    };
  }

  private static Workload getByteArrayWorkload(BitMap bitMap) {
    markAlternatingBits(bitMap);
    return operationCount -> {
      byte[][] sink = new byte[1024][];
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        byte[] bytes = bitMap.getByteArray();
        sink[i & 1023] = bytes;
        checksum += bytes[i & 7] & 0xFFL;
        checksum += bytes.length;
      }
      referenceBlackHole = sink[operationCount & 1023];
      return checksum;
    };
  }

  private static Workload getTruncatedByteArrayWorkload(BitMap bitMap) {
    markAlternatingBits(bitMap);
    return operationCount -> {
      byte[][] sink = new byte[1024][];
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        byte[] bytes = bitMap.getTruncatedByteArray(BIT_MAP_SIZE);
        sink[i & 1023] = bytes;
        checksum += bytes[i & 7] & 0xFFL;
        checksum += bytes.length;
      }
      referenceBlackHole = sink[operationCount & 1023];
      return checksum;
    };
  }

  private static Workload constructFromBytesWorkload(BitMapImplFactory factory) {
    byte[] bytes = serializedAlternatingBits();
    return operationCount -> {
      BitMap[] sink = new BitMap[1024];
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        BitMap bitMap = new BitMap(factory.create(BIT_MAP_SIZE, bytes));
        sink[i & 1023] = bitMap;
        checksum += bitMap.isMarked(i & 63) ? 1 : 0;
      }
      referenceBlackHole = sink[operationCount & 1023];
      return checksum;
    };
  }

  private static Workload cloneWorkload(BitMap bitMap) {
    markAlternatingBits(bitMap);
    return operationCount -> {
      BitMap[] sink = new BitMap[1024];
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        BitMap clone = bitMap.clone();
        sink[i & 1023] = clone;
        checksum += clone.isMarked(i & 63) ? 1 : 0;
      }
      referenceBlackHole = sink[operationCount & 1023];
      return checksum;
    };
  }

  private static Workload mergeWorkload(BitMapFactory factory) {
    BitMap source = factory.create();
    markAlternatingBits(source);
    return operationCount -> {
      BitMap destination = factory.create();
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        destination.merge(source, 0, 0, BIT_MAP_SIZE);
        checksum += destination.isMarked(i & 63) ? 1 : 0;
      }
      return checksum;
    };
  }

  private static Workload appendWorkload(BitMapFactory factory) {
    BitMap source = factory.create();
    markAlternatingBits(source);
    return operationCount -> {
      BitMap destination = factory.create();
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        destination.append(source, 0, BIT_MAP_SIZE);
        checksum += destination.isMarked(i & 63) ? 1 : 0;
      }
      return checksum;
    };
  }

  private static Workload regionWorkload(BitMap bitMap) {
    markAlternatingBits(bitMap);
    return operationCount -> {
      BitMap[] sink = new BitMap[1024];
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        BitMap region = bitMap.getRegion(0, BIT_MAP_SIZE);
        sink[i & 1023] = region;
        checksum += region.isMarked(i & 63) ? 1 : 0;
      }
      referenceBlackHole = sink[operationCount & 1023];
      return checksum;
    };
  }

  private static Workload equalityWorkload(BitMapFactory factory) {
    BitMap[] left = new BitMap[BIT_MAP_SIZE];
    BitMap[] right = new BitMap[BIT_MAP_SIZE];
    for (int i = 0; i < BIT_MAP_SIZE; i++) {
      left[i] = factory.create();
      right[i] = factory.create();
      left[i].mark(i);
      right[i].mark(i);
    }
    return operationCount -> {
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        int index = i & 63;
        checksum += left[index].equals(right[index]) ? 1 : 0;
        checksum += left[index].equalsInRange(right[index], BIT_MAP_SIZE) ? 2 : 0;
      }
      return checksum;
    };
  }

  private static Workload hashCodeWorkload(BitMapFactory factory) {
    BitMap[] bitMaps = new BitMap[BIT_MAP_SIZE];
    for (int i = 0; i < bitMaps.length; i++) {
      bitMaps[i] = factory.create();
      bitMaps[i].mark(i);
    }
    return operationCount -> {
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        checksum += bitMaps[i & 63].hashCode();
      }
      return checksum;
    };
  }

  private static Workload toStringWorkload(BitMap bitMap) {
    markAlternatingBits(bitMap);
    return operationCount -> {
      String[] sink = new String[1024];
      long checksum = 0;
      for (int i = 0; i < operationCount; i++) {
        String value = bitMap.toString();
        sink[i & 1023] = value;
        checksum += value.charAt(i & 63);
        checksum += value.length();
      }
      referenceBlackHole = sink[operationCount & 1023];
      return checksum;
    };
  }

  private static BitMap arrayBitMap() {
    return new BitMap(new BitMapArrayImpl(BIT_MAP_SIZE));
  }

  private static BitMap longBitMap() {
    return new BitMap(new BitMapLongImpl(BIT_MAP_SIZE));
  }

  private static void markAlternatingBits(BitMap bitMap) {
    for (int i = 0; i < BIT_MAP_SIZE; i += 2) {
      bitMap.mark(i);
    }
  }

  private static byte[] serializedAlternatingBits() {
    BitMap bitMap = arrayBitMap();
    markAlternatingBits(bitMap);
    return Arrays.copyOf(bitMap.getByteArray(), bitMap.getByteArray().length);
  }

  private static void printResults(List<BenchmarkResult> results) {
    System.out.println("BitMap length=64 performance (median ns/op)");
    System.out.printf("%-34s %14s %14s %14s%n", "operation", "ArrayImpl", "LongImpl", "Long/Array");
    for (BenchmarkResult result : results) {
      System.out.printf(
          "%-34s %14.3f %14.3f %13.3fx%n",
          result.name,
          result.arrayNanosPerOperation,
          result.longNanosPerOperation,
          result.longNanosPerOperation / result.arrayNanosPerOperation);
    }
  }

  private static void printMemoryUsage() {
    BitMap arrayBitMap = arrayBitMap();
    BitMap longBitMap = longBitMap();
    long wrapperBytes = RamUsageEstimator.shallowSizeOfInstance(BitMap.class);
    long arrayBytes = wrapperBytes + arrayBitMap.getRetainedSizeInBytes();
    long longBytes = wrapperBytes + longBitMap.getRetainedSizeInBytes();
    assertTrue(longBytes < arrayBytes);

    long arrayContainerBytes =
        RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
                + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * BULK_INSTANCE_COUNT);
    long arrayBulkBytes = arrayContainerBytes + arrayBytes * BULK_INSTANCE_COUNT;
    long longBulkBytes = arrayContainerBytes + longBytes * BULK_INSTANCE_COUNT;

    System.out.println("BitMap length=64 retained memory estimate");
    System.out.printf(
        "ArrayImpl: %,d bytes/object, %,d bytes for %,d objects%n",
        arrayBytes, arrayBulkBytes, BULK_INSTANCE_COUNT);
    System.out.printf(
        "LongImpl : %,d bytes/object, %,d bytes for %,d objects%n",
        longBytes, longBulkBytes, BULK_INSTANCE_COUNT);
    System.out.printf(
        "Reduction: %.2f%% per object%n", (arrayBytes - longBytes) * 100.0 / arrayBytes);
  }

  @FunctionalInterface
  private interface Workload {
    long run(int operationCount);
  }

  @FunctionalInterface
  private interface BitMapFactory {
    BitMap create();
  }

  @FunctionalInterface
  private interface BitMapImplFactory {
    BitMapImpl create(int size, byte[] bytes);
  }

  private static class TimedRun {

    private final long elapsedNanos;
    private final long checksum;

    private TimedRun(long elapsedNanos, long checksum) {
      this.elapsedNanos = elapsedNanos;
      this.checksum = checksum;
    }
  }

  private static class BenchmarkResult {

    private final String name;
    private final double arrayNanosPerOperation;
    private final double longNanosPerOperation;

    private BenchmarkResult(
        String name, double arrayNanosPerOperation, double longNanosPerOperation) {
      this.name = name;
      this.arrayNanosPerOperation = arrayNanosPerOperation;
      this.longNanosPerOperation = longNanosPerOperation;
    }
  }
}
