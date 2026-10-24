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

import org.apache.tsfile.encoding.decoder.PlainDecoder;
import org.apache.tsfile.encoding.encoder.PlainEncoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/** Deterministic encoded pages shared by regression tests and the standalone JMH benchmark. */
public class PageDataTestSupport {
  public final TSDataType type;
  public final int size;
  public final byte[] bitmap;
  public final long[] times;
  public final boolean[] keep;
  public final boolean[] deleted;
  public final byte[] values;
  public final byte[] allValues;
  public final byte[] timeBytes;
  public final byte[] aligned;
  public final byte[] nonAligned;
  public final PageHeader header;

  public PageDataTestSupport(TSDataType type, int size, boolean sparse) throws IOException {
    this.type = type;
    this.size = size;
    bitmap = new byte[(size + 7) / 8];
    times = new long[size];
    keep = new boolean[size];
    deleted = new boolean[size];
    ByteArrayOutputStream valueOut = new ByteArrayOutputStream();
    ByteArrayOutputStream allValueOut = new ByteArrayOutputStream();
    ByteArrayOutputStream timeOut = new ByteArrayOutputStream();
    PlainEncoder encoder = new PlainEncoder(type, 128);
    PlainEncoder timeEncoder = new PlainEncoder(TSDataType.INT64, 0);
    for (int i = 0; i < size; i++) {
      times[i] = i;
      timeEncoder.encode((long) i, timeOut);
      keep[i] = !sparse || i % 3 != 0;
      deleted[i] = sparse && i >= size / 3 && i < size / 2;
      encode(encoder, allValueOut, type, i);
      if (!sparse || i % 5 != 0) {
        bitmap[i / 8] |= (byte) (0x80 >>> (i % 8));
        encode(encoder, valueOut, type, i);
      }
    }
    values = valueOut.toByteArray();
    allValues = allValueOut.toByteArray();
    timeBytes = timeOut.toByteArray();
    aligned =
        ByteBuffer.allocate(4 + bitmap.length + values.length)
            .putInt(size)
            .put(bitmap)
            .put(values)
            .array();
    ByteArrayOutputStream page = new ByteArrayOutputStream();
    ReadWriteForEncodingUtils.writeUnsignedVarInt(timeBytes.length, page);
    page.write(timeBytes);
    page.write(allValues);
    nonAligned = page.toByteArray();
    Statistics<?> statistics = Statistics.getStatsByType(type);
    statistics.setCount(size);
    statistics.setStartTime(0);
    statistics.setEndTime(size - 1L);
    statistics.setEmpty(false);
    header = new PageHeader(nonAligned.length, nonAligned.length, statistics);
  }

  public ValuePageReader alignedReader(boolean withDeletion) {
    ValuePageReader reader =
        new ValuePageReader(header, ByteBuffer.wrap(aligned), type, new PlainDecoder());
    if (withDeletion) {
      reader.setDeleteIntervalList(
          Collections.singletonList(new TimeRange(size / 3, size / 2 - 1)));
    }
    return reader;
  }

  public PageReader pageReader(Filter filter, boolean withDeletion) {
    PageReader reader =
        new PageReader(
            header,
            ByteBuffer.wrap(nonAligned),
            type,
            new PlainDecoder(),
            new PlainDecoder(),
            filter);
    if (withDeletion) {
      reader.setDeleteIntervalList(
          Collections.singletonList(new TimeRange(size / 3, size / 2 - 1)));
    }
    return reader;
  }

  private static void encode(
      PlainEncoder encoder, ByteArrayOutputStream out, TSDataType type, int i) {
    switch (type) {
      case BOOLEAN -> encoder.encode(i % 2 == 0, out);
      case INT32 -> encoder.encode(i * 13 - 50, out);
      case DATE -> encoder.encode(20240101 + i % 28, out);
      case INT64, TIMESTAMP -> encoder.encode(i * 1000003L, out);
      case FLOAT -> encoder.encode(i * 0.25f, out);
      case DOUBLE -> encoder.encode(i * 0.125, out);
      case TEXT, STRING, BLOB, OBJECT ->
          encoder.encode(new Binary("value-" + i, StandardCharsets.UTF_8), out);
      case VECTOR, UNKNOWN -> throw new AssertionError(type);
    }
  }
}
