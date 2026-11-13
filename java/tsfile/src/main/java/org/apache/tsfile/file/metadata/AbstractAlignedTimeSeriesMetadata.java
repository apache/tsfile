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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.controller.IChunkMetadataLoader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public abstract class AbstractAlignedTimeSeriesMetadata implements ITimeSeriesMetadata {

  // TimeSeriesMetadata for time column
  protected final TimeseriesMetadata timeseriesMetadata;
  // TimeSeriesMetadata for all subSensors in the vector
  protected final List<TimeseriesMetadata> valueTimeseriesMetadataList;

  protected IChunkMetadataLoader chunkMetadataLoader;

  AbstractAlignedTimeSeriesMetadata(
      TimeseriesMetadata timeseriesMetadata, List<TimeseriesMetadata> valueTimeseriesMetadataList) {
    this.timeseriesMetadata = timeseriesMetadata;
    this.valueTimeseriesMetadataList = valueTimeseriesMetadataList;
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return timeseriesMetadata.getStatistics();
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    TimeseriesMetadata metadata = valueTimeseriesMetadataList.get(measurementIndex);
    return Optional.ofNullable(metadata == null ? null : metadata.getStatistics());
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    long rowCount = getTimeStatistics().getCount();
    Optional<Statistics<? extends Serializable>> statistics =
        getMeasurementStatistics(measurementIndex);
    return statistics.map(stat -> stat.hasNullValue(rowCount)).orElse(true);
  }

  @Override
  public int getMeasurementCount() {
    return valueTimeseriesMetadataList.size();
  }

  @Override
  public boolean isModified() {
    return timeseriesMetadata.isModified();
  }

  @Override
  public void setModified(boolean modified) {
    timeseriesMetadata.setModified(modified);
    for (TimeseriesMetadata subSensor : valueTimeseriesMetadataList) {
      if (subSensor != null) {
        subSensor.setModified(modified);
      }
    }
  }

  @Override
  public boolean isDataTypeModifiedAndCannotUseStatistics() {
    return timeseriesMetadata.isDataTypeModifiedAndCannotUseStatistics();
  }

  @Override
  public void setDataTypeModifiedAndCannotUseStatistics(
      boolean dataTypeModifiedAndCannotUseStatistics) {
    timeseriesMetadata.setDataTypeModifiedAndCannotUseStatistics(
        dataTypeModifiedAndCannotUseStatistics);
  }

  @Override
  public boolean isSeq() {
    return timeseriesMetadata.isSeq();
  }

  @Override
  public void setSeq(boolean seq) {
    timeseriesMetadata.setSeq(seq);
    for (TimeseriesMetadata subSensor : valueTimeseriesMetadataList) {
      if (subSensor != null) {
        subSensor.setSeq(seq);
      }
    }
  }

  /**
   * If the chunkMetadataLoader is MemChunkMetadataLoader, the VectorChunkMetadata is already
   * assembled while constructing the in-memory TsFileResource, so we just return the assembled
   * VectorChunkMetadata list.
   *
   * <p>Otherwise, we need to assemble the ChunkMetadata of time column and the ChunkMetadata of all
   * the subSensors to generate the VectorChunkMetadata
   */
  @Override
  public List<IChunkMetadata> loadChunkMetadataList() {
    return chunkMetadataLoader.loadChunkMetadataList(this);
  }

  public List<AbstractAlignedChunkMetadata> getCopiedChunkMetadataList() {
    List<IChunkMetadata> timeChunkMetadata = timeseriesMetadata.getCopiedChunkMetadataList();
    List<List<IChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
    for (TimeseriesMetadata metadata : valueTimeseriesMetadataList) {
      valueChunkMetadataList.add(metadata == null ? null : metadata.getCopiedChunkMetadataList());
    }

    return getAlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
  }

  public List<AbstractAlignedChunkMetadata> getChunkMetadataList() {
    List<IChunkMetadata> timeChunkMetadata = timeseriesMetadata.getChunkMetadataList();
    List<List<IChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
    for (TimeseriesMetadata metadata : valueTimeseriesMetadataList) {
      valueChunkMetadataList.add(metadata == null ? null : metadata.getChunkMetadataList());
    }

    return getAlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
  }

  /** Notice: if all the value chunks is empty chunk, then return empty list. */
  private List<AbstractAlignedChunkMetadata> getAlignedChunkMetadata(
      List<IChunkMetadata> timeChunkMetadata, List<List<IChunkMetadata>> valueChunkMetadataList) {
    List<AbstractAlignedChunkMetadata> res = new ArrayList<>();
    if (valueTimeseriesMetadataList.isEmpty()) {
      for (IChunkMetadata timeChunk : timeChunkMetadata) {
        res.add(new AlignedChunkMetadata(timeChunk, Collections.emptyList()));
      }
      return res;
    }

    // A value column written only in some chunk groups (a column added or renamed later) has fewer
    // chunks than the time column, so the value chunks cannot be aligned positionally. Chunks are
    // laid out in file order (the time chunk of a group is written first, then its value chunks),
    // so a value chunk belongs to the time chunk whose file offset immediately precedes it and is
    // followed by the next time chunk's offset. This forward cursor keeps the alignment correct for
    // both uniform and uneven column histories.
    final int columnCount = valueChunkMetadataList.size();
    final int[] cursors = new int[columnCount];
    for (int i = 0; i < timeChunkMetadata.size(); i++) {
      final IChunkMetadata timeChunk = timeChunkMetadata.get(i);
      final long groupStartOffset = timeChunk.getOffsetOfChunkHeader();
      final long groupEndOffset =
          i + 1 < timeChunkMetadata.size()
              ? timeChunkMetadata.get(i + 1).getOffsetOfChunkHeader()
              : Long.MAX_VALUE;
      final List<IChunkMetadata> chunkMetadataList = new ArrayList<>(columnCount);
      boolean exits = false;
      for (int c = 0; c < columnCount; c++) {
        final List<IChunkMetadata> chunks = valueChunkMetadataList.get(c);
        IChunkMetadata value = null;
        if (chunks != null) {
          while (cursors[c] < chunks.size()
              && chunks.get(cursors[c]).getOffsetOfChunkHeader() < groupStartOffset) {
            cursors[c]++;
          }
          if (cursors[c] < chunks.size()
              && chunks.get(cursors[c]).getOffsetOfChunkHeader() < groupEndOffset) {
            value = chunks.get(cursors[c]);
          }
        }
        if (value != null && value.getStatistics().getCount() != 0) {
          exits = true;
        } else {
          value = null;
        }
        chunkMetadataList.add(value);
      }
      constructAlignedChunkMetadata(res, timeChunk, chunkMetadataList, exits);
    }
    return res;
  }

  abstract void constructAlignedChunkMetadata(
      List<AbstractAlignedChunkMetadata> res,
      IChunkMetadata timeChunkMetadata,
      List<IChunkMetadata> chunkMetadataList,
      boolean exits);

  abstract AbstractAlignedChunkMetadata constructOnlyTimeChunkMetadata(
      IChunkMetadata timeChunkMetadata);

  @Override
  public void setChunkMetadataLoader(IChunkMetadataLoader chunkMetadataLoader) {
    this.chunkMetadataLoader = chunkMetadataLoader;
  }

  @Override
  public boolean typeMatch(List<TSDataType> dataTypes) {
    if (dataTypes.isEmpty()) {
      return true;
    }
    if (valueTimeseriesMetadataList != null) {
      for (int i = 0, size = dataTypes.size(); i < size; i++) {
        TimeseriesMetadata valueTimeSeriesMetadata = valueTimeseriesMetadataList.get(i);
        if (valueTimeSeriesMetadata != null
            && !valueTimeSeriesMetadata.typeMatch(dataTypes.get(i))) {
          valueTimeseriesMetadataList.set(i, null);
        }
      }
    }
    return true;
  }

  public List<TimeseriesMetadata> getValueTimeseriesMetadataList() {
    return valueTimeseriesMetadataList;
  }

  public TimeseriesMetadata getTimeseriesMetadata() {
    return timeseriesMetadata;
  }
}
