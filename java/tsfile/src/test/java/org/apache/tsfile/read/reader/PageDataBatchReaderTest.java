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
package org.apache.tsfile.read.reader;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.encoding.decoder.PlainDecoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.BatchDataFactory;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.utils.TypeServices;
import org.apache.tsfile.utils.TypeServices.PageDataReadStatus;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PageDataBatchReaderTest {
  private static final TSDataType[] TYPES = {
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.DATE,
    TSDataType.INT64,
    TSDataType.TIMESTAMP,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TEXT,
    TSDataType.STRING,
    TSDataType.BLOB,
    TSDataType.OBJECT
  };

  @Test
  public void columnsMatchScalarReaders() throws Exception {
    for (TSDataType type : TYPES) {
      for (int size : new int[] {0, 1, 7, 8, 9, 65}) {
        PageDataTestSupport f = new PageDataTestSupport(type, size, true);
        for (int mode = 0; mode < 4; mode++) {
          for (boolean binaryDate : new boolean[] {false, true}) {
            if (binaryDate && type != TSDataType.DATE) {
              continue;
            }
            ColumnBuilder expected = builder(type, size, binaryDate);
            ColumnBuilder actual = builder(type, size, binaryDate);
            ByteBuffer input = ByteBuffer.wrap(f.values);
            PlainDecoder decoder = new PlainDecoder();
            var scalar =
                TypeServices.READ_PAGE_VALUE_TO_COLUMNBUILDER_SERVICE.call(
                    Type.fromTsDataType(type));
            boolean[] keep = f.keep.clone();
            if (mode == 3) {
              Arrays.fill(keep, false);
            }
            int start = mode == 2 ? size / 3 : 0;
            for (int i = 0; i < size; i++) {
              boolean selected = mode == 2 ? i >= start : keep[i];
              if ((f.bitmap[i / 8] & (0x80 >>> (i % 8))) == 0) {
                if (selected) {
                  expected.appendNull();
                }
              } else {
                scalar.read(decoder, input, expected, selected, mode == 0 && f.deleted[i]);
              }
            }
            ValuePageReader reader = f.alignedReader(false);
            if (mode == 0) {
              reader.writeColumnBuilderWithNextBatch(size, actual, keep, f.deleted);
            } else if (mode == 2) {
              reader.writeColumnBuilderWithNextBatch(start, size, actual);
            } else {
              reader.writeColumnBuilderWithNextBatch(size, actual, keep);
            }
            assertColumn(expected.build(), actual.build());
          }
        }
      }
    }
  }

  @Test
  public void batchesAndPrimitiveArraysMatchScalarReaders() throws Exception {
    for (TSDataType type : TYPES) {
      PageDataTestSupport f = new PageDataTestSupport(type, 65, true);
      for (boolean aligned : new boolean[] {false, true}) {
        for (boolean ascending : new boolean[] {false, true}) {
          for (Filter filter : new Filter[] {null, TimeFilterApi.gt(10)}) {
            BatchData expected = BatchDataFactory.createBatchData(type, ascending, false);
            ByteBuffer input = ByteBuffer.wrap(aligned ? f.values : f.allValues);
            PlainDecoder decoder = new PlainDecoder();
            var scalar =
                TypeServices.READ_PAGE_VALUE_TO_BATCHDATA_SERVICE.call(Type.fromTsDataType(type));
            for (int i = 0; i < f.size; i++) {
              if (!aligned || (f.bitmap[i / 8] & (0x80 >>> (i % 8))) != 0) {
                scalar.read(
                    decoder, input, filter, expected, i, filter == null, t -> f.deleted[(int) t]);
              }
            }
            expected.flip();
            BatchData actual =
                aligned
                    ? f.alignedReader(true).nextBatch(f.times, ascending, filter)
                    : f.pageReader(filter, true).getAllSatisfiedPageData(ascending);
            while (expected.hasCurrent()) {
              assertTrue(actual.hasCurrent());
              assertEquals(expected.currentTime(), actual.currentTime());
              assertEquals(expected.currentValue(), actual.currentValue());
              expected.next();
              actual.next();
            }
            assertFalse(actual.hasCurrent());
          }
        }
      }
      TsPrimitiveType[] actual = f.alignedReader(true).nextValueBatch(f.times);
      ValuePageReader scalarReader = f.alignedReader(true);
      for (int i = 0; i < f.size; i++) {
        TsPrimitiveType expected = scalarReader.nextValue(i, i);
        if (expected == null) {
          assertNull(actual[i]);
        } else {
          assertEquals(expected.getDataType(), actual[i].getDataType());
          assertEquals(expected.getValue(), actual[i].getValue());
        }
      }
    }
  }

  @Test
  public void blockPaginationPreservesFilterCountsAndDecoderPositions() throws Exception {
    for (TSDataType type : TYPES) {
      PageDataTestSupport f = new PageDataTestSupport(type, 65, false);
      for (Filter filter : new Filter[] {null, TimeFilterApi.gt(10), TimeFilterApi.gt(100)}) {
        for (long limit : new long[] {0, 1, 8, 100}) {
          for (long offset : new long[] {0, 5, 100}) {
            var scalar =
                TypeServices.READ_PAGE_VALUE_TO_TSBLOCK_SERVICE.call(Type.fromTsDataType(type));
            var batch = TypeServices.READ_PAGE_BATCH_SERVICE.call(Type.fromTsDataType(type));
            ByteBuffer expectedTimes = ByteBuffer.wrap(f.timeBytes);
            ByteBuffer expectedValues = ByteBuffer.wrap(f.allValues);
            ByteBuffer actualTimes = ByteBuffer.wrap(f.timeBytes);
            ByteBuffer actualValues = ByteBuffer.wrap(f.allValues);
            TsBlockBuilder expected = new TsBlockBuilder(Collections.singletonList(type));
            TsBlockBuilder actual = new TsBlockBuilder(Collections.singletonList(type));
            PaginationController p1 = new PaginationController(limit, offset);
            PaginationController p2 = new PaginationController(limit, offset);
            PlainDecoder decoder = new PlainDecoder();
            long filtered = 0;
            while (expectedTimes.hasRemaining()) {
              long timestamp = decoder.readLong(expectedTimes);
              PageDataReadStatus status =
                  scalar.read(
                      decoder,
                      expectedValues,
                      filter,
                      expected,
                      timestamp,
                      filter == null,
                      t -> t >= 20 && t <= 30,
                      p1);
              if (status == PageDataReadStatus.FILTERED) {
                filtered++;
              } else if (status == PageDataReadStatus.STOP) {
                break;
              }
            }
            long actualFiltered =
                batch.readBlock(
                    new PlainDecoder(),
                    actualTimes,
                    new PlainDecoder(),
                    actualValues,
                    filter,
                    actual,
                    filter == null,
                    t -> t >= 20 && t <= 30,
                    p2);
            assertEquals(filtered, actualFiltered);
            assertEquals(expectedTimes.position(), actualTimes.position());
            assertEquals(expectedValues.position(), actualValues.position());
            assertEquals(p1.getCurLimit(), p2.getCurLimit());
            assertEquals(p1.getCurOffset(), p2.getCurOffset());
            TsBlock expectedBlock = expected.build();
            TsBlock actualBlock = actual.build();
            assertColumn(expectedBlock.getTimeColumn(), actualBlock.getTimeColumn());
            assertColumn(expectedBlock.getColumn(0), actualBlock.getColumn(0));
          }
        }
      }
    }
  }

  private static ColumnBuilder builder(TSDataType type, int size, boolean binaryDate) {
    return binaryDate
        ? new BinaryColumnBuilder(null, size)
        : Type.fromTsDataType(type).createColumnBuilder(size);
  }

  @Test
  public void discardedRowsConsumeValuesAndEmptyPagesAppendNulls() throws Exception {
    for (TSDataType type : TYPES) {
      PageDataTestSupport f = new PageDataTestSupport(type, 65, true);
      ValuePageReader reader = f.alignedReader(false);
      ValuePageReader scalar = f.alignedReader(false);
      ColumnBuilder output = builder(type, 65, false);
      reader.writeColumnBuilderWithNextBatch(32, output, new boolean[32]);
      assertEquals(0, output.build().getPositionCount());
      for (int i = 0; i < 32; i++) {
        scalar.nextValue(i, i);
      }
      for (int i = 32; i < 65; i++) {
        TsPrimitiveType expected = scalar.nextValue(i, i);
        TsPrimitiveType actual = reader.nextValue(i, i);
        assertEquals(
            expected == null ? null : expected.getValue(),
            actual == null ? null : actual.getValue());
      }
      ValuePageReader empty =
          new ValuePageReader(f.header, (ByteBuffer) null, type, new PlainDecoder());
      ColumnBuilder nulls = builder(type, 5, false);
      empty.writeColumnBuilderWithNextBatch(3, nulls, new boolean[] {true, false, true});
      empty.writeColumnBuilderWithNextBatch(1, 4, nulls);
      Column result = nulls.build();
      assertEquals(5, result.getPositionCount());
      for (int i = 0; i < 5; i++) {
        assertTrue(result.isNull(i));
      }
      assertEquals(0, empty.nextValueBatch(new long[0]).length);
    }
  }

  private static void assertColumn(Column expected, Column actual) {
    assertEquals(expected.getPositionCount(), actual.getPositionCount());
    for (int i = 0; i < expected.getPositionCount(); i++) {
      assertEquals(expected.isNull(i), actual.isNull(i));
      if (!expected.isNull(i)) {
        assertEquals(expected.getObject(i), actual.getObject(i));
      }
    }
  }
}
