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

package org.apache.tsfile.read.query.dataset;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.tsfile.utils.LongHeapPriorityQueue;
import org.apache.tsfile.utils.LongOpenHashSet;

import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

/**
 * Manual benchmark for the merge hot path in {@link DataSetWithoutTimeGenerator}.
 *
 * <p>The workload size can be changed with system properties prefixed by {@code
 * tsfile.datasetWithoutTimeGenerator.}.
 */
public class DataSetWithoutTimeGeneratorPerformanceTest {

  private static final String RUN_PERFORMANCE_TEST_PROPERTY = "tsfile.runPerformanceTests";
  private static final String PROPERTY_PREFIX = "tsfile.datasetWithoutTimeGenerator.";
  private static final int SERIES_COUNT = positiveIntProperty("seriesCount", 64);
  private static final int ALIGNED_POINTS = positiveIntProperty("alignedPoints", 10_000);
  private static final int STAGGERED_POINTS = positiveIntProperty("staggeredPoints", 500);
  private static final int WARMUP_ROUNDS = positiveIntProperty("warmupRounds", 3);
  private static final int MEASUREMENT_ROUNDS = positiveIntProperty("measurementRounds", 7);
  private static final int BATCH_SIZE = positiveIntProperty("batchSize", 128);

  private static final List<Path> PATHS = createPaths();
  private static final List<TSDataType> DATA_TYPES =
      Collections.nCopies(SERIES_COUNT, TSDataType.INT64);

  @Test
  public void testMergePerformanceAgainstHeapAndSetImplementation() throws IOException {
    Assume.assumeTrue(
        "Set -Dtsfile.runPerformanceTests=true to run the performance test",
        Boolean.getBoolean(RUN_PERFORMANCE_TEST_PROPERTY));

    System.out.printf(
        Locale.ROOT,
        "DataSetWithoutTimeGenerator benchmark: series=%,d, batchSize=%,d, "
            + "warmups=%d, measurements=%d%n",
        SERIES_COUNT,
        BATCH_SIZE,
        WARMUP_ROUNDS,
        MEASUREMENT_ROUNDS);

    BenchmarkResult aligned = benchmark(createScenario("aligned", ALIGNED_POINTS, false));
    BenchmarkResult staggered = benchmark(createScenario("staggered", STAGGERED_POINTS, true));

    printResult(aligned);
    printResult(staggered);

    // This is a manual diagnostic benchmark. Exact timings are JVM, CPU, and workload dependent,
    // so the test deliberately does not turn a particular speedup into a pass/fail condition.
  }

  private static BenchmarkResult benchmark(Scenario scenario) throws IOException {
    for (int round = 0; round < WARMUP_ROUNDS; round++) {
      RunResult optimized = runOptimized(scenario);
      RunResult heapSet = runHeapSet(scenario);
      assertSameResult(optimized, heapSet);
    }

    long[] optimizedNanos = new long[MEASUREMENT_ROUNDS];
    long[] heapSetNanos = new long[MEASUREMENT_ROUNDS];
    long expectedChecksum = Long.MIN_VALUE;
    int expectedRows = -1;
    for (int round = 0; round < MEASUREMENT_ROUNDS; round++) {
      RunResult optimized;
      RunResult heapSet;
      if ((round & 1) == 0) {
        optimized = runOptimized(scenario);
        heapSet = runHeapSet(scenario);
      } else {
        heapSet = runHeapSet(scenario);
        optimized = runOptimized(scenario);
      }
      assertSameResult(optimized, heapSet);
      if (round == 0) {
        expectedChecksum = optimized.checksum;
        expectedRows = optimized.rows;
      } else {
        assertEquals(expectedChecksum, optimized.checksum);
        assertEquals(expectedRows, optimized.rows);
      }
      optimizedNanos[round] = optimized.elapsedNanos;
      heapSetNanos[round] = heapSet.elapsedNanos;
    }

    return new BenchmarkResult(
        scenario.name,
        expectedRows,
        median(optimizedNanos) / (double) expectedRows,
        median(heapSetNanos) / (double) expectedRows);
  }

  private static RunResult runOptimized(Scenario scenario) throws IOException {
    return consume(
        new OptimizedMergeDataSet(
            new DataSetWithoutTimeGenerator(PATHS, DATA_TYPES, createReaders(scenario))),
        scenario);
  }

  private static RunResult runHeapSet(Scenario scenario) throws IOException {
    LegacyMergeDataSet dataSet = new LegacyMergeDataSet(createReaders(scenario));
    return consume(dataSet, scenario);
  }

  private static RunResult consume(DataSet dataSet, Scenario scenario) throws IOException {
    long checksum = 0;
    int rows = 0;
    long start = System.nanoTime();
    while (dataSet.hasNextWithoutConstraint()) {
      RowRecord row = dataSet.nextWithoutConstraint();
      checksum ^= row.getTimestamp();
      for (Field field : row.getFields()) {
        if (field != null) {
          checksum ^= field.getLongV();
        }
      }
      rows++;
    }
    long elapsedNanos = System.nanoTime() - start;
    assertEquals(scenario.expectedRows(), rows);
    return new RunResult(elapsedNanos, rows, checksum);
  }

  private static void assertSameResult(RunResult optimized, RunResult heapSet) {
    assertEquals(heapSet.rows, optimized.rows);
    assertEquals(heapSet.checksum, optimized.checksum);
  }

  private static List<AbstractFileSeriesReader> createReaders(Scenario scenario) {
    List<AbstractFileSeriesReader> readers = new ArrayList<>(SERIES_COUNT);
    for (List<BatchData> batches : scenario.batchesBySeries) {
      for (BatchData batch : batches) {
        batch.resetBatchData();
      }
      readers.add(new FakeSeriesReader(batches));
    }
    return readers;
  }

  private static Scenario createScenario(String name, int points, boolean staggered) {
    List<List<BatchData>> batchesBySeries = new ArrayList<>(SERIES_COUNT);
    for (int series = 0; series < SERIES_COUNT; series++) {
      List<BatchData> batches = new ArrayList<>();
      for (int batchStart = 0; batchStart < points; batchStart += BATCH_SIZE) {
        BatchData batch = new BatchData(TSDataType.INT64);
        int batchEnd = Math.min(batchStart + BATCH_SIZE, points);
        for (int point = batchStart; point < batchEnd; point++) {
          long timestamp = staggered ? (long) point * SERIES_COUNT + series : point;
          long value = ((long) series << 32) ^ point;
          batch.putLong(timestamp, value);
        }
        batches.add(batch);
      }
      batchesBySeries.add(batches);
    }
    return new Scenario(name, points, staggered, batchesBySeries);
  }

  private static List<Path> createPaths() {
    List<Path> paths = new ArrayList<>(SERIES_COUNT);
    for (int series = 0; series < SERIES_COUNT; series++) {
      paths.add(new Path("root.performance", "s" + series, true));
    }
    return Collections.unmodifiableList(paths);
  }

  private static long median(long[] values) {
    long[] sorted = Arrays.copyOf(values, values.length);
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  private static int positiveIntProperty(String suffix, int defaultValue) {
    String name = PROPERTY_PREFIX + suffix;
    int value = Integer.getInteger(name, defaultValue);
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive: " + value);
    }
    return value;
  }

  private static void printResult(BenchmarkResult result) {
    System.out.printf(
        Locale.ROOT,
        "DataSetWithoutTimeGenerator %s merge: rows=%,d, optimized=%.1f ns/row, "
            + "heap/set=%.1f ns/row, speedup=%.2fx%n",
        result.name,
        result.rows,
        result.optimizedNanosPerRow,
        result.heapSetNanosPerRow,
        result.heapSetNanosPerRow / result.optimizedNanosPerRow);
  }

  private interface DataSet {
    boolean hasNextWithoutConstraint() throws IOException;

    RowRecord nextWithoutConstraint() throws IOException;
  }

  private static final class OptimizedMergeDataSet implements DataSet {

    private final DataSetWithoutTimeGenerator delegate;

    private OptimizedMergeDataSet(DataSetWithoutTimeGenerator delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNextWithoutConstraint() throws IOException {
      return delegate.hasNextWithoutConstraint();
    }

    @Override
    public RowRecord nextWithoutConstraint() throws IOException {
      return delegate.nextWithoutConstraint();
    }
  }

  private static final class LegacyMergeDataSet implements DataSet {

    private final List<AbstractFileSeriesReader> readers;
    private final List<BatchData> batchDataList;
    private final List<Boolean> hasDataRemaining;
    private final LongHeapPriorityQueue timeHeap;
    private final LongOpenHashSet timeSet;

    private LegacyMergeDataSet(List<AbstractFileSeriesReader> readers) throws IOException {
      this.readers = readers;
      batchDataList = new ArrayList<>(readers.size());
      hasDataRemaining = new ArrayList<>(readers.size());
      timeHeap = new LongHeapPriorityQueue(Math.max(readers.size(), 1));
      timeSet = new LongOpenHashSet(Math.max(readers.size(), 1));
      for (AbstractFileSeriesReader reader : readers) {
        if (reader.hasNextBatch()) {
          batchDataList.add(reader.nextBatch());
          hasDataRemaining.add(true);
        } else {
          batchDataList.add(new BatchData());
          hasDataRemaining.add(false);
        }
      }
      for (BatchData data : batchDataList) {
        if (data.hasCurrent()) {
          timeHeapPut(data.currentTime());
        }
      }
    }

    @Override
    public boolean hasNextWithoutConstraint() {
      return !timeHeap.isEmpty();
    }

    @Override
    public RowRecord nextWithoutConstraint() throws IOException {
      long minTime = timeHeapGet();
      RowRecord rowRecord = new RowRecord(minTime);
      for (int i = 0; i < readers.size(); i++) {
        if (!hasDataRemaining.get(i)) {
          rowRecord.addField(null);
          continue;
        }

        BatchData data = batchDataList.get(i);
        if (data.hasCurrent() && data.currentTime() == minTime) {
          Field field = toField(data);
          data.next();
          if (!data.hasCurrent()) {
            AbstractFileSeriesReader reader = readers.get(i);
            if (reader.hasNextBatch()) {
              data = reader.nextBatch();
              if (data.hasCurrent()) {
                batchDataList.set(i, data);
                timeHeapPut(data.currentTime());
              } else {
                hasDataRemaining.set(i, false);
              }
            } else {
              hasDataRemaining.set(i, false);
            }
          } else {
            timeHeapPut(data.currentTime());
          }
          rowRecord.addField(field);
        } else {
          rowRecord.addField(null);
        }
      }
      return rowRecord;
    }

    private void timeHeapPut(long time) {
      if (timeSet.add(time)) {
        timeHeap.enqueue(time);
      }
    }

    private long timeHeapGet() {
      long time = timeHeap.dequeueLong();
      timeSet.remove(time);
      return time;
    }
  }

  private static Field toField(BatchData data) {
    TSDataType type = data.getDataType();
    Field field = new Field(type);
    switch (type) {
      case INT64:
        field.setLongV(data.getLong());
        break;
      default:
        throw new IllegalStateException("Unexpected benchmark data type: " + type);
    }
    return field;
  }

  private static final class FakeSeriesReader extends AbstractFileSeriesReader {

    private final List<BatchData> batches;
    private int index;

    private FakeSeriesReader(List<BatchData> batches) {
      super(null, Collections.emptyList(), null);
      this.batches = batches;
    }

    @Override
    public boolean hasNextBatch() {
      return index < batches.size();
    }

    @Override
    public BatchData nextBatch() {
      return batches.get(index++);
    }

    @Override
    protected void initChunkReader(IChunkMetadata chunkMetaData) {
      // unused in fake reader
    }

    @Override
    protected boolean chunkCanSkip(IChunkMetadata chunkMetaData) {
      return false;
    }

    @Override
    public void close() {
      // no-op
    }
  }

  private static final class Scenario {

    private final String name;
    private final int points;
    private final boolean staggered;
    private final List<List<BatchData>> batchesBySeries;

    private Scenario(
        String name, int points, boolean staggered, List<List<BatchData>> batchesBySeries) {
      this.name = name;
      this.points = points;
      this.staggered = staggered;
      this.batchesBySeries = batchesBySeries;
    }

    private int expectedRows() {
      return staggered ? points * SERIES_COUNT : points;
    }
  }

  private static final class RunResult {

    private final long elapsedNanos;
    private final int rows;
    private final long checksum;

    private RunResult(long elapsedNanos, int rows, long checksum) {
      this.elapsedNanos = elapsedNanos;
      this.rows = rows;
      this.checksum = checksum;
    }
  }

  private static final class BenchmarkResult {

    private final String name;
    private final int rows;
    private final double optimizedNanosPerRow;
    private final double heapSetNanosPerRow;

    private BenchmarkResult(
        String name, int rows, double optimizedNanosPerRow, double heapSetNanosPerRow) {
      this.name = name;
      this.rows = rows;
      this.optimizedNanosPerRow = optimizedNanosPerRow;
      this.heapSetNanosPerRow = heapSetNanosPerRow;
    }

    @Override
    public String toString() {
      return String.format(
          Locale.ROOT,
          "%s optimized=%.1f ns/row heap/set=%.1f ns/row",
          name,
          optimizedNanosPerRow,
          heapSetNanosPerRow);
    }
  }
}
