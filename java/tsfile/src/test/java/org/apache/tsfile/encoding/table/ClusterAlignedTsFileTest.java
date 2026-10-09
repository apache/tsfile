/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.encoding.table;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.PlainDecoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@RunWith(Parameterized.class)
public class ClusterAlignedTsFileTest {
  @Parameterized.Parameters(name = "{0}")
  public static Object[] encodings() {
    return new Object[] {TSEncoding.ACLUSTER, TSEncoding.KCLUSTER};
  }

  private final TSEncoding encoding;
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public ClusterAlignedTsFileTest(TSEncoding encoding) {
    this.encoding = encoding;
  }

  private List<IMeasurementSchema> schemas() {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("a", TSDataType.DOUBLE, encoding, CompressionType.LZ4));
    schemas.add(new MeasurementSchema("b", TSDataType.INT64, encoding, CompressionType.LZ4));
    schemas.add(new MeasurementSchema("c", TSDataType.FLOAT, encoding, CompressionType.LZ4));
    schemas.add(new MeasurementSchema("empty", TSDataType.INT32, encoding, CompressionType.LZ4));
    schemas.add(new MeasurementSchema("plain", TSDataType.INT32, TSEncoding.PLAIN));
    return schemas;
  }

  private static double a(int r) {
    return r % 31 == 0 ? -0.0 : ((r * 17) % 37) / 10.0;
  }

  private static long b(int r) {
    return r % 11 == 0 ? Long.MAX_VALUE - r : (r * 13) % 41;
  }

  private static float c(int r) {
    return (float) (((r * 3) % 19) / 10.0);
  }

  @Test
  public void alignedFilePreservesTimestampValuePairsAndProjection() throws Exception {
    File file = new File(temp.getRoot(), "aligned.tsfile");
    int old = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(127);
    try {
      try (TsFileWriter writer = new TsFileWriter(file)) {
        writer.registerAlignedTimeseries(new Path("root.native.d"), schemas());
        for (int r = 0; r < 641; r++) {
          TSRecord row = new TSRecord("root.native.d", 1000 + r);
          row.addPoint("a", a(r)).addPoint("b", b(r)).addPoint("plain", r);
          if (r % 7 != 0) row.addPoint("c", c(r));
          writer.writeRecord(row);
          if (r == 300) writer.flush();
        }
      }
      verify(file, 641);
    } finally {
      TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(old);
    }
  }

  @Test
  public void columnWiseMemtableWriterPath() throws Exception {
    File file = new File(temp.getRoot(), "column-wise.tsfile");
    try (TsFileIOWriter fileWriter = new TsFileIOWriter(file)) {
      fileWriter.startChunkGroup(IDeviceID.Factory.DEFAULT_FACTORY.create("root.native.d"));
      AlignedChunkWriterImpl writer = new AlignedChunkWriterImpl(schemas());
      for (int start = 0; start < 641; start += 127) {
        int end = Math.min(641, start + 127);
        for (int r = start; r < end; r++) writer.writeByColumn(1000 + r, a(r), false);
        writer.nextColumn();
        for (int r = start; r < end; r++) writer.writeByColumn(1000 + r, b(r), false);
        writer.nextColumn();
        for (int r = start; r < end; r++) writer.writeByColumn(1000 + r, c(r), r % 7 == 0);
        writer.nextColumn();
        for (int r = start; r < end; r++) writer.writeByColumn(1000 + r, 0, true);
        writer.nextColumn();
        for (int r = start; r < end; r++) writer.writeByColumn(1000 + r, r, false);
        writer.nextColumn();
        long[] times = new long[end - start];
        for (int r = 0; r < times.length; r++) times[r] = 1000 + start + r;
        writer.write(times, times.length, 0);
        writer.sealCurrentPage();
      }
      writer.writeToFileWriter(fileWriter);
      fileWriter.endChunkGroup();
      fileWriter.endFile();
    }
    verify(file, 641);
  }

  private void verify(File file, int count) throws Exception {
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getAbsolutePath());
        TsFileReader reader = new TsFileReader(sequence)) {
      List<Path> fields =
          Arrays.asList(
              new Path("root.native.d", "a", true),
              new Path("root.native.d", "b", true),
              new Path("root.native.d", "c", true),
              new Path("root.native.d", "plain", true));
      QueryDataSet data = reader.query(QueryExpression.create(fields, null));
      int r = 0;
      while (data.hasNext()) {
        RowRecord row = data.next();
        assertEquals(1000 + r, row.getTimestamp());
        assertEquals(
            Double.doubleToRawLongBits(a(r)),
            Double.doubleToRawLongBits(row.getFields().get(0).getDoubleV()));
        assertEquals(b(r), row.getFields().get(1).getLongV());
        if (r % 7 == 0) assertNull(row.getFields().get(2));
        else
          assertEquals(
              Float.floatToRawIntBits(c(r)),
              Float.floatToRawIntBits(row.getFields().get(2).getFloatV()));
        assertEquals(r, row.getFields().get(3).getIntV());
        r++;
      }
      assertEquals(count, r);
      QueryDataSet projected =
          reader.query(QueryExpression.create(Collections.singletonList(fields.get(1)), null));
      r = 0;
      while (projected.hasNext()) {
        RowRecord row = projected.next();
        assertEquals(1000 + r, row.getTimestamp());
        assertEquals(b(r++), row.getFields().get(0).getLongV());
      }
      assertEquals(count, r);
    }
  }

  @Test
  public void scalarSqlPageKeepsActualTimestampKeys() throws Exception {
    File file = new File(temp.getRoot(), "scalar-native.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      writer.registerTimeseries(
          "root.scalar.d", new MeasurementSchema("v", TSDataType.FLOAT, encoding));
      for (int r = 0; r < 701; r++) {
        writer.writeRecord(new TSRecord("root.scalar.d", 1000 + r).addPoint("v", c(r)));
        if (r == 300) writer.flush();
      }
    }
    try (TsFileSequenceReader sequence = new TsFileSequenceReader(file.getAbsolutePath());
        TsFileReader reader = new TsFileReader(sequence)) {
      QueryDataSet rows =
          reader.query(
              QueryExpression.create(
                  Collections.singletonList(new Path("root.scalar.d", "v", true)), null));
      int r = 0;
      while (rows.hasNext()) {
        RowRecord row = rows.next();
        assertEquals(1000 + r, row.getTimestamp());
        assertEquals(
            Float.floatToRawIntBits(c(r++)),
            Float.floatToRawIntBits(row.getFields().get(0).getFloatV()));
      }
      assertEquals(701, r);
    }
  }

  @Test
  public void jointSelectionAndCorruptNativePage() throws Exception {
    int n = 301;
    String[] names = {"x", "y", "nullable"};
    TSDataType[] types = {TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.INT64};
    ClusterColumnBuffer[] columns = {
      new ClusterColumnBuffer(), new ClusterColumnBuffer(), new ClusterColumnBuffer()
    };
    long[] times = new long[n];
    for (int r = 0; r < n; r++) {
      times[r] = 1000 + r;
      double x = (r * 23 % 37) / 10.0;
      columns[0].append(Double.doubleToRawLongBits(x), false);
      columns[1].append(Double.doubleToRawLongBits(x * 2), false);
      columns[2].append(Long.MAX_VALUE - r, r % 7 == 0);
    }
    ClusterTableOptions options =
        encoding == TSEncoding.ACLUSTER
            ? ClusterTableOptions.aCluster()
            : ClusterTableOptions.kCluster();
    byte[][] pages =
        ClusterNativePageCodec.encode(
            times, names, types, columns, options, new ClusterNativePageCodec.Selection());
    for (int col = 0; col < 3; col++) {
      ClusterNativePageCodec.Decoded decoded =
          ClusterNativePageCodec.decode(ByteBuffer.wrap(pages[col]), types[col]);
      assertArrayEquals(times, decoded.times);
      PlainDecoder decoder = new PlainDecoder();
      for (int r = 0; r < n; r++) {
        boolean missing = columns[col].isNull(r);
        assertEquals(!missing, (decoded.bitmap[r / 8] & (0x80 >>> (r % 8))) != 0);
        if (!missing)
          assertEquals(
              columns[col].bits(r),
              col == 2
                  ? decoder.readLong(decoded.values)
                  : Double.doubleToRawLongBits(decoder.readDouble(decoded.values)));
      }
      assertFalse(decoded.values.hasRemaining());
    }
    byte[] bad = pages[0].clone();
    bad[bad.length - 1] ^= 1;
    assertThrows(
        IOException.class,
        () -> ClusterNativePageCodec.decode(ByteBuffer.wrap(bad), TSDataType.DOUBLE));
    assertThrows(
        IOException.class,
        () -> ClusterNativePageCodec.decode(ByteBuffer.wrap(pages[0]), TSDataType.INT64));
    times[1] = times[0];
    assertThrows(
        IOException.class,
        () ->
            ClusterNativePageCodec.encode(
                times, names, types, columns, options, new ClusterNativePageCodec.Selection()));
  }
}
