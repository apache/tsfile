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

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DataSetWithoutTimeGeneratorTest {

  @Test
  public void testMultiWayMergeWithSparseTimestamps() throws IOException {
    BatchData series0 = batchOf(TSDataType.INT64, new long[] {1, 3, 5}, new long[] {10, 30, 50});
    BatchData series1 = batchOf(TSDataType.INT64, new long[] {2, 3, 4}, new long[] {20, 31, 40});

    List<Path> paths =
        Arrays.asList(new Path("root.d1", "s0", true), new Path("root.d1", "s1", true));
    List<TSDataType> types = Arrays.asList(TSDataType.INT64, TSDataType.INT64);
    List<AbstractFileSeriesReader> readers =
        Arrays.asList(new FakeSeriesReader(series0), new FakeSeriesReader(series1));

    DataSetWithoutTimeGenerator dataSet = new DataSetWithoutTimeGenerator(paths, types, readers);

    assertRow(dataSet, 1, 10L, null);
    assertRow(dataSet, 2, null, 20L);
    assertRow(dataSet, 3, 30L, 31L);
    assertRow(dataSet, 4, null, 40L);
    assertRow(dataSet, 5, 50L, null);
    assertFalse(dataSet.hasNext());
  }

  @Test
  public void testMultiWayMergeAcrossBatches() throws IOException {
    BatchData batch1 = batchOf(TSDataType.INT32, new long[] {1, 2}, new int[] {1, 2});
    BatchData batch2 = batchOf(TSDataType.INT32, new long[] {3}, new int[] {3});

    List<Path> paths = Collections.singletonList(new Path("root.d1", "s0", true));
    List<TSDataType> types = Collections.singletonList(TSDataType.INT32);
    List<AbstractFileSeriesReader> readers =
        Collections.singletonList(new FakeSeriesReader(batch1, batch2));

    DataSetWithoutTimeGenerator dataSet = new DataSetWithoutTimeGenerator(paths, types, readers);
    List<Long> times = new ArrayList<>();
    while (dataSet.hasNext()) {
      times.add(dataSet.next().getTimestamp());
    }
    assertEquals(Arrays.asList(1L, 2L, 3L), times);
  }

  @Test
  public void testMergeAcrossBatchesAndExtremeTimestamps() throws IOException {
    AbstractFileSeriesReader firstReader =
        new FakeSeriesReader(
            batchOf(TSDataType.INT64, new long[] {Long.MIN_VALUE, -1}, new long[] {10, 11}),
            batchOf(TSDataType.INT64, new long[] {5, Long.MAX_VALUE}, new long[] {12, 13}));
    AbstractFileSeriesReader secondReader =
        new FakeSeriesReader(
            batchOf(TSDataType.INT64, new long[] {-1, 0}, new long[] {20, 21}),
            batchOf(TSDataType.INT64, new long[] {6, Long.MAX_VALUE}, new long[] {22, 23}));
    AbstractFileSeriesReader emptyReader = new FakeSeriesReader();

    DataSetWithoutTimeGenerator dataSet =
        new DataSetWithoutTimeGenerator(
            Arrays.asList(
                new Path("root.d1", "s0", true),
                new Path("root.d1", "s1", true),
                new Path("root.d1", "s2", true)),
            Arrays.asList(TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
            Arrays.asList(firstReader, secondReader, emptyReader));

    assertRow(dataSet, Long.MIN_VALUE, 10L, null, null);
    assertRow(dataSet, -1, 11L, 20L, null);
    assertRow(dataSet, 0, null, 21L, null);
    assertRow(dataSet, 5, 12L, null, null);
    assertRow(dataSet, 6, null, 22L, null);
    assertRow(dataSet, Long.MAX_VALUE, 13L, 23L, null);
    assertFalse(dataSet.hasNext());
  }

  private static void assertRow(
      DataSetWithoutTimeGenerator dataSet, long timestamp, Long... expectedValues)
      throws IOException {
    assertTrue(dataSet.hasNext());
    RowRecord row = dataSet.next();
    assertEquals(timestamp, row.getTimestamp());
    assertEquals(expectedValues.length, row.getFields().size());
    for (int i = 0; i < expectedValues.length; i++) {
      Field field = row.getFields().get(i);
      if (expectedValues[i] == null) {
        assertNull(field);
      } else {
        assertNotNull(field);
        assertEquals(expectedValues[i].longValue(), field.getLongV());
      }
    }
  }

  private static BatchData batchOf(TSDataType type, long[] times, long[] values) {
    BatchData batchData = new BatchData(type);
    for (int i = 0; i < times.length; i++) {
      batchData.putLong(times[i], values[i]);
    }
    return batchData;
  }

  private static BatchData batchOf(TSDataType type, long[] times, int[] values) {
    BatchData batchData = new BatchData(type);
    for (int i = 0; i < times.length; i++) {
      batchData.putInt(times[i], values[i]);
    }
    return batchData;
  }

  private static class FakeSeriesReader extends AbstractFileSeriesReader {

    private final List<BatchData> batches;
    private int index;

    FakeSeriesReader(BatchData... batches) {
      super(null, Collections.emptyList(), null);
      this.batches = Arrays.asList(batches);
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
}
