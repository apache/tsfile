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

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.BatchDataFactory;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.TypeServices;
import org.apache.tsfile.utils.TypeServices.PageDataBlockValueReader;
import org.apache.tsfile.utils.TypeServices.PageDataReadStatus;
import org.apache.tsfile.utils.TypeServices.PageDataValueReader;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

import static org.apache.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;
import static org.apache.tsfile.utils.Preconditions.checkArgument;

public class PageReader implements IPageReader {

  private final PageHeader pageHeader;

  private final TSDataType dataType;

  /** decoder for value column */
  private final Decoder valueDecoder;

  /** decoder for time column */
  private final Decoder timeDecoder;

  /** time column in memory */
  private ByteBuffer timeBuffer;

  /** value column in memory */
  private ByteBuffer valueBuffer;

  private Filter recordFilter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  // used for lazy decoding

  private LazyLoadPageData lazyLoadPageData;

  public PageReader(
      ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder, Decoder timeDecoder) {
    this(null, pageData, dataType, valueDecoder, timeDecoder, null);
  }

  public PageReader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder) {
    this(pageHeader, pageData, dataType, valueDecoder, timeDecoder, null);
  }

  public PageReader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter recordFilter) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.recordFilter = recordFilter;
    this.pageHeader = pageHeader;
    splitDataToTimeStampAndValue(pageData);
  }

  public PageReader(
      PageHeader pageHeader,
      LazyLoadPageData lazyLoadPageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter recordFilter) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.recordFilter = recordFilter;
    this.pageHeader = pageHeader;
    this.lazyLoadPageData = lazyLoadPageData;
  }

  /**
   * split pageContent into two stream: time and value
   *
   * @param pageData uncompressed bytes size of time column, time column, value column
   */
  private void splitDataToTimeStampAndValue(ByteBuffer pageData) {
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  /** Call this method before accessing data. */
  private void uncompressDataIfNecessary() throws IOException {
    if (lazyLoadPageData != null && (timeBuffer == null || valueBuffer == null)) {
      splitDataToTimeStampAndValue(lazyLoadPageData.uncompressPageData(pageHeader));
      lazyLoadPageData = null;
    }
  }

  /**
   * @return the returned BatchData may be empty, but never be null
   */
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    uncompressDataIfNecessary();
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    boolean allSatisfy = recordFilter == null || recordFilter.allSatisfy(this);
    PageDataValueReader valueReader =
        TypeServices.READ_PAGE_VALUE_TO_BATCHDATA_SERVICE.call(Type.fromTsDataType(dataType));
    LongPredicate isDeleted = this::isDeleted;
    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);
      valueReader.read(
          valueDecoder, valueBuffer, recordFilter, pageData, timestamp, allSatisfy, isDeleted);
    }
    return pageData.flip();
  }

  @Override
  public TsBlock getAllSatisfiedData() throws IOException {
    return getAllSatisfiedData(null);
  }

  @Override
  public TsBlock getAllSatisfiedData(LongConsumer filterRowsRecorder) throws IOException {
    uncompressDataIfNecessary();
    TsBlockBuilder builder;
    int initialExpectedEntries = pageHeader.getStatistics().getCount();
    if (paginationController.hasSetLimit()) {
      initialExpectedEntries =
          (int) Math.min(initialExpectedEntries, paginationController.getCurLimit());
    }
    builder = new TsBlockBuilder(initialExpectedEntries, Collections.singletonList(dataType));

    long allFilteredRows = 0;
    boolean allSatisfy = recordFilter == null || recordFilter.allSatisfy(this);
    PageDataBlockValueReader valueReader =
        TypeServices.READ_PAGE_VALUE_TO_TSBLOCK_SERVICE.call(Type.fromTsDataType(dataType));
    LongPredicate isDeleted = this::isDeleted;
    while (timeDecoder.hasNext(timeBuffer)) {
      long timestamp = timeDecoder.readLong(timeBuffer);
      PageDataReadStatus status =
          valueReader.read(
              valueDecoder,
              valueBuffer,
              recordFilter,
              builder,
              timestamp,
              allSatisfy,
              isDeleted,
              paginationController);
      if (status == PageDataReadStatus.FILTERED) {
        allFilteredRows++;
      } else if (status == PageDataReadStatus.STOP) {
        break;
      }
    }
    if (filterRowsRecorder != null && allFilteredRows > 0) {
      filterRowsRecorder.accept(allFilteredRows);
    }
    return builder.build();
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return pageHeader.getStatistics();
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return getStatistics();
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    checkArgument(
        measurementIndex == 0,
        "Non-aligned page only has one measurement, but measurementIndex is " + measurementIndex);
    return Optional.ofNullable(getStatistics());
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    return false;
  }

  @Override
  public void addRecordFilter(Filter filter) {
    this.recordFilter = FilterFactory.and(recordFilter, filter);
  }

  @Override
  public void setLimitOffset(PaginationController paginationController) {
    this.paginationController = paginationController;
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  @Override
  public boolean isModified() {
    return pageHeader.isModified();
  }

  @Override
  public void setModified(boolean modified) {
    pageHeader.setModified(modified);
  }

  @Override
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {
    // do nothing
  }

  protected boolean isDeleted(long timestamp) {
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
}
