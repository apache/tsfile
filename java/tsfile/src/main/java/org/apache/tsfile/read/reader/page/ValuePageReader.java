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

package org.apache.tsfile.read.reader.page;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.BatchDataFactory;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.utils.TypeServices;
import org.apache.tsfile.utils.TypeServices.PageDataBatchReader;
import org.apache.tsfile.utils.TypeServices.PageDataTsPrimitiveValueReader;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.function.LongPredicate;

public class ValuePageReader {

  private static final int MASK = 0x80;

  private final PageHeader pageHeader;

  private final TSDataType dataType;

  /** decoder for value column */
  private final Decoder valueDecoder;

  // Reuse the type-specific reader across per-row nextValue calls.
  private PageDataTsPrimitiveValueReader valueReader;

  // Reuse readers for batch and column-builder APIs across repeated page reads.
  private PageDataBatchReader batchReader;

  // Reuse the bound predicate across page reads instead of creating a method reference per call.
  private final LongPredicate deletePredicate = this::isDeleted;

  private byte[] bitmap;

  private int size;

  /** value column in memory */
  protected ByteBuffer valueBuffer;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  private LazyLoadPageData lazyLoadPageData;

  public ValuePageReader(
      PageHeader pageHeader, ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.pageHeader = pageHeader;
    if (pageData != null) {
      splitDataToBitmapAndValue(pageData);
    }
    this.valueBuffer = pageData;
  }

  public ValuePageReader(
      PageHeader pageHeader,
      LazyLoadPageData lazyLoadPageData,
      TSDataType dataType,
      Decoder valueDecoder) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.pageHeader = pageHeader;
    this.lazyLoadPageData = lazyLoadPageData;
  }

  private void splitDataToBitmapAndValue(ByteBuffer pageData) {
    if (!pageData.hasRemaining()) { // Empty Page
      return;
    }
    this.size = ReadWriteIOUtils.readInt(pageData);
    this.bitmap = new byte[(size + 7) / 8];
    pageData.get(bitmap);
    this.valueBuffer = pageData.slice();
  }

  /** Call this method before accessing data. */
  private void uncompressDataIfNecessary() throws IOException {
    if (lazyLoadPageData != null && valueBuffer == null) {
      ByteBuffer pageData = lazyLoadPageData.uncompressPageData(pageHeader);
      splitDataToBitmapAndValue(pageData);
      this.valueBuffer = pageData;
      lazyLoadPageData = null;
    }
  }

  /**
   * return a BatchData with the corresponding timeBatch, the BatchData's dataType is same as this
   * sub sensor
   */
  public BatchData nextBatch(long[] timeBatch, boolean ascending, Filter filter)
      throws IOException {
    uncompressDataIfNecessary();
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    getBatchReader()
        .readAlignedBatch(
            timeBatch, bitmap, valueDecoder, valueBuffer, filter, pageData, deletePredicate);
    return pageData.flip();
  }

  public TsPrimitiveType nextValue(long timestamp, int timeIndex) throws IOException {
    uncompressDataIfNecessary();
    if (valueBuffer == null || ((bitmap[timeIndex / 8] & 0xFF) & (MASK >>> (timeIndex % 8))) == 0) {
      return null;
    }
    if (valueReader == null) {
      valueReader =
          TypeServices.READ_PAGE_VALUE_TO_TSPRIMITIVETYPE_SERVICE.call(
              Type.fromTsDataType(dataType));
    }
    return valueReader.read(valueDecoder, valueBuffer, timestamp, deletePredicate);
  }

  /**
   * return the value array of the corresponding time, if this sub sensor don't have a value in a
   * time, just fill it with null
   */
  public TsPrimitiveType[] nextValueBatch(long[] timeBatch) throws IOException {
    uncompressDataIfNecessary();
    TsPrimitiveType[] valueBatch = new TsPrimitiveType[size];
    if (valueBuffer == null) {
      return valueBatch;
    }
    getBatchReader()
        .readValues(timeBatch, bitmap, valueDecoder, valueBuffer, valueBatch, deletePredicate);
    return valueBatch;
  }

  public void writeColumnBuilderWithNextBatch(
      int readEndIndex, ColumnBuilder columnBuilder, boolean[] keepCurrentRow, boolean[] isDeleted)
      throws IOException {
    uncompressDataIfNecessary();
    if (valueBuffer == null) {
      for (int i = 0; i < readEndIndex; i++) {
        if (keepCurrentRow[i]) {
          columnBuilder.appendNull();
        }
      }
      return;
    }
    getBatchReader()
        .readColumn(
            readEndIndex,
            bitmap,
            valueDecoder,
            valueBuffer,
            columnBuilder,
            keepCurrentRow,
            isDeleted);
  }

  public void writeColumnBuilderWithNextBatch(
      int readEndIndex, ColumnBuilder columnBuilder, boolean[] keepCurrentRow) throws IOException {
    uncompressDataIfNecessary();
    if (valueBuffer == null) {
      for (int i = 0; i < readEndIndex; i++) {
        if (keepCurrentRow[i]) {
          columnBuilder.appendNull();
        }
      }
      return;
    }
    getBatchReader()
        .readColumn(
            readEndIndex, bitmap, valueDecoder, valueBuffer, columnBuilder, keepCurrentRow, null);
  }

  public void writeColumnBuilderWithNextBatch(
      int readStartIndex, int readEndIndex, ColumnBuilder columnBuilder) throws IOException {
    uncompressDataIfNecessary();
    if (valueBuffer == null) {
      columnBuilder.appendNull(readEndIndex - readStartIndex);
      return;
    }
    getBatchReader()
        .readColumn(readStartIndex, readEndIndex, bitmap, valueDecoder, valueBuffer, columnBuilder);
  }

  private PageDataBatchReader getBatchReader() {
    if (batchReader == null) {
      batchReader = TypeServices.READ_PAGE_BATCH_SERVICE.call(Type.fromTsDataType(dataType));
    }
    return batchReader;
  }

  public Statistics<? extends Serializable> getStatistics() {
    return pageHeader.getStatistics();
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  public boolean isModified() {
    return pageHeader.isModified();
  }

  public void setModified(boolean modified) {
    pageHeader.setModified(modified);
  }

  public boolean isDeleted(long timestamp) {
    while (deleteIntervalList != null && deleteCursor < deleteIntervalList.size()) {
      if (deleteIntervalList.get(deleteCursor).contains(timestamp)) {
        return true;
      } else if (deleteIntervalList.get(deleteCursor).getMax() < timestamp) {
        deleteCursor++;
      } else {
        return false;
      }
    }
    return false;
  }

  public void fillIsDeleted(long[] timestamp, boolean[] isDeleted) {
    for (int i = 0, n = timestamp.length; i < n; i++) {
      isDeleted[i] = isDeleted(timestamp[i]);
    }
  }

  public void fillIsDeleted(long[] timestamp, boolean[] isDeleted, boolean[] keepCurrentRow) {
    for (int i = 0, n = timestamp.length; i < n; i++) {
      if (keepCurrentRow[i]) {
        isDeleted[i] = isDeleted(timestamp[i]);
      }
    }
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public byte[] getBitmap() throws IOException {
    uncompressDataIfNecessary();
    return Arrays.copyOf(bitmap, bitmap.length);
  }

  public int getSize() {
    return size;
  }
}
