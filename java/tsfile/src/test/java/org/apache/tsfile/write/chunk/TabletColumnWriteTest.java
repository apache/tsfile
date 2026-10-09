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
package org.apache.tsfile.write.chunk;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.TypeServices;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TabletColumnWriteTest {
  @Test
  public void testEncodedPagesMatchScalarForAllTypes() throws Exception {
    int oldLimit = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    try {
      // Force multiple pages and exercise both bitmap boundaries and a nonzero source offset.
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(7);
      for (TSDataType dataType : TSDataType.values()) {
        if (dataType == TSDataType.UNKNOWN || dataType == TSDataType.VECTOR) {
          continue;
        }
        Type type = Type.fromTsDataType(dataType);
        for (boolean sparse : new boolean[] {false, true}) {
          Object values = type.createArray(35);
          long[] times = new long[35];
          BitMap nulls = sparse ? new BitMap(35) : null;
          for (int row = 0; row < 35; row++) {
            times[row] = row - 20;
            type.addValue(row, value(dataType, row), values);
            if (sparse && row % 3 == 0) {
              nulls.mark(row);
            }
          }
          compare(type, dataType, times, values, nulls);
          if (dataType == TSDataType.DATE) {
            int[] intDates = new int[35];
            for (int row = 0; row < intDates.length; row++) {
              intDates[row] = 20240101 + row % 28;
            }
            compare(type, dataType, times, intDates, nulls);
          }
        }
      }
    } finally {
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(oldLimit);
    }
  }

  private void compare(Type type, TSDataType dataType, long[] times, Object values, BitMap nulls)
      throws Exception {
    MeasurementSchema schema =
        new MeasurementSchema("s", dataType, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
    compareWriters(type, dataType, times, values, nulls, schema);
    if (dataType == TSDataType.INT32
        || dataType == TSDataType.INT64
        || dataType == TSDataType.FLOAT
        || dataType == TSDataType.DOUBLE) {
      schema.setProps(Map.of("loss", "sdt", "compdev", "0.1", "compmaxtime", "5"));
      compareWriters(type, dataType, times, values, nulls, schema);
    }
  }

  private void compareWriters(
      Type type,
      TSDataType dataType,
      long[] times,
      Object values,
      BitMap nulls,
      MeasurementSchema schema)
      throws Exception {
    ChunkWriterImpl scalar = new ChunkWriterImpl(schema);
    ChunkWriterImpl batch = new ChunkWriterImpl(schema);
    int count = 0;
    for (int row = 2; row < 34; row++) {
      if (nulls == null || !nulls.isMarked(row)) {
        type.write(scalar, times[row], values, row);
        count++;
      }
    }
    TabletWriteContext context =
        new TabletWriteContext(IDeviceID.Factory.DEFAULT_FACTORY.create("root.d"), "s", null);
    TypeServices.WRITE_TABLET_COLUMN_SERVICE
        .call(type)
        .write(batch, times, values, nulls, 2, 34, context);
    scalar.sealCurrentPage();
    batch.sealCurrentPage();
    assertEquals(dataType.toString(), scalar.getByteBuffer(), batch.getByteBuffer());
    assertEquals(scalar.getStatistics(), batch.getStatistics());
    assertEquals(scalar.getNumOfPages(), batch.getNumOfPages());
    assertEquals(count, context.getPointCount());
  }

  @Test
  public void testOutOfOrderKeepsWrittenPrefixAndSkipsNulls() throws Exception {
    NonAlignedChunkGroupWriterImpl writer =
        new NonAlignedChunkGroupWriterImpl(IDeviceID.Factory.DEFAULT_FACTORY.create("root.d"));
    writer.tryToAddSeriesWriter(new MeasurementSchema("s", TSDataType.INT32));
    Tablet tablet =
        new Tablet(
            "root.d", Collections.singletonList(new MeasurementSchema("s", TSDataType.INT32)), 5);
    long[] times = {Long.MIN_VALUE, 10, 3, 9, 20};
    for (int row = 0; row < times.length; row++) {
      tablet.addTimestamp(row, times[row]);
      if (row != 2) {
        tablet.addValue("s", row, row);
      }
    }
    assertThrows(WriteProcessException.class, () -> writer.write(tablet, 0, 5));
    assertEquals(Long.valueOf(10), writer.getLastTimeMap().get("s"));
    assertEquals(1, writer.write(tablet, 4, 5));
    assertEquals(Long.valueOf(20), writer.getLastTimeMap().get("s"));
    assertEquals(0, writer.write(tablet, 2, 3));
    assertEquals(Long.valueOf(20), writer.getLastTimeMap().get("s"));
    tablet.getValues()[0] = null;
    assertEquals(0, writer.write(tablet, 2, 3));
    assertEquals(0, writer.write(tablet, 0, 0));
  }

  private Object value(TSDataType type, int row) {
    return switch (type) {
      case BOOLEAN -> row % 2 == 0;
      case INT32 -> row;
      case DATE -> LocalDate.of(2024, 1, 1 + row % 28);
      case INT64, TIMESTAMP -> (long) row;
      case FLOAT -> row * 1.5f;
      case DOUBLE -> row * 1.5;
      case TEXT, STRING, BLOB, OBJECT -> new Binary("v" + row, StandardCharsets.UTF_8);
      case VECTOR, UNKNOWN -> throw new AssertionError(type);
    };
  }
}
