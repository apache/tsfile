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

package org.apache.tsfile.write.v4;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkGroupWriterImpl;
import org.apache.tsfile.write.chunk.IChunkGroupWriter;
import org.apache.tsfile.write.chunk.TableChunkGroupWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Convert TsBlock (table model) into TsFile format. Core responsibilities: 1. Split TsBlock by
 * device (based on tag columns) 2. Optionally generate a new time column per device 3. Dispatch
 * rows to corresponding ChunkGroupWriter
 */
public class TableTsBlock2TsFileWriter extends DeviceTableModelWriter {

  private final boolean generateNewTimeColumn;
  private final int timeColumnIndexInTsBlock;
  private final int[] tagColumnIndexInTsBlock;
  private final int[] fieldColumnIndexInTsBlock;
  private final IMeasurementSchema[] fieldColumnSchemas;

  private final String tableName;
  private final Map<IDeviceID, Long> deviceRowCountMap;

  private int rowCount = 0;

  public TableTsBlock2TsFileWriter(
      File file,
      TableSchema tableSchema,
      long memoryThreshold,
      boolean generateNewTimeColumn,
      int timeColumnIndexInTsBlock,
      int[] tagColumnIndexesInTsBlock,
      int[] fieldColumnIndexesInTsBlock,
      IMeasurementSchema[] fieldColumnSchemas)
      throws IOException {
    super(file, tableSchema, memoryThreshold);
    this.tableName = tableSchema.getTableName();
    this.generateNewTimeColumn = generateNewTimeColumn;
    this.timeColumnIndexInTsBlock = timeColumnIndexInTsBlock;
    this.tagColumnIndexInTsBlock = tagColumnIndexesInTsBlock;
    this.fieldColumnIndexInTsBlock = fieldColumnIndexesInTsBlock;
    this.deviceRowCountMap = generateNewTimeColumn ? new HashMap<>() : null;
    this.fieldColumnSchemas = fieldColumnSchemas;
  }

  public void write(TsBlock tsBlock) throws IOException, WriteProcessException {
    if (tsBlock == null || tsBlock.isEmpty()) {
      return;
    }
    // Split TsBlock into device partitions and prepare time column
    Pair<Column, List<Pair<IDeviceID, Integer>>> timeColumnAndDeviceIdEndIndexPairs =
        splitTsBlockByDeviceAndGetTimeColumn(tsBlock);
    Column timeColumn = timeColumnAndDeviceIdEndIndexPairs.left;
    // Extract value columns according to schema mapping
    Column[] valueColumns = new Column[fieldColumnIndexInTsBlock.length];
    for (int i = 0; i < valueColumns.length; i++) {
      valueColumns[i] = tsBlock.getColumn(fieldColumnIndexInTsBlock[i]);
    }
    List<Pair<IDeviceID, Integer>> deviceIdEndIndexPairs = timeColumnAndDeviceIdEndIndexPairs.right;
    int startIndex = 0;

    // Iterate each device segment and write data into its ChunkGroup
    for (Pair<IDeviceID, Integer> pair : deviceIdEndIndexPairs) {
      TableTsBlockChunkGroupWriterImpl chunkGroupWriter =
          (TableTsBlockChunkGroupWriterImpl) tryToInitialGroupWriter(pair.left, true, true);
      int writeCount = chunkGroupWriter.write(timeColumn, valueColumns, startIndex, pair.right);
      rowCount += writeCount;
      recordCount += writeCount;
      startIndex = pair.right;
    }

    this.checkMemorySizeAndMayFlushChunks();
  }

  /**
   * Split TsBlock by device boundary. If generateNewTimeColumn is true, generate a monotonically
   * increasing time column per device using deviceRowCountMap.
   *
   * @return Pair of (time column, device -> end index list)
   */
  private Pair<Column, List<Pair<IDeviceID, Integer>>> splitTsBlockByDeviceAndGetTimeColumn(
      TsBlock tsBlock) {
    long[] timestamps = null;
    if (generateNewTimeColumn) {
      timestamps = new long[tsBlock.getPositionCount()];
    }
    List<Pair<IDeviceID, Integer>> deviceSplitResult = new ArrayList<>();
    IDeviceID lastDeviceID = null;
    long lastDeviceCount = 0;

    // Iterate rows and detect device boundary changes
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      IDeviceID currDeviceID = getDeviceId(tsBlock, i);
      // Device changed, flush previous segment
      if (!currDeviceID.equals(lastDeviceID)) {
        if (lastDeviceID != null) {
          deviceSplitResult.add(new Pair(lastDeviceID, i));
          if (generateNewTimeColumn) {
            deviceRowCountMap.put(lastDeviceID, lastDeviceCount);
          }
        }
        lastDeviceID = currDeviceID;
        if (generateNewTimeColumn) {
          lastDeviceCount = deviceRowCountMap.getOrDefault(lastDeviceID, 0L);
        }
      }
      // Generate synthetic time if required
      if (generateNewTimeColumn) {
        timestamps[i] = lastDeviceCount++;
      }
    }

    deviceSplitResult.add(new Pair(lastDeviceID, tsBlock.getPositionCount()));
    if (generateNewTimeColumn) {
      deviceRowCountMap.put(lastDeviceID, lastDeviceCount);
      return new Pair<>(new TimeColumn(timestamps.length, timestamps), deviceSplitResult);
    } else {
      return new Pair<>(tsBlock.getColumn(timeColumnIndexInTsBlock), deviceSplitResult);
    }
  }

  private IDeviceID getDeviceId(TsBlock tsBlock, int rowIdx) {
    String[] segments = new String[tagColumnIndexInTsBlock.length + 1];
    segments[0] = tableName;
    for (int i = 0; i < tagColumnIndexInTsBlock.length; i++) {
      Column tagColumn = tsBlock.getColumn(tagColumnIndexInTsBlock[i]);
      if (tagColumn.isNull(rowIdx)) {
        segments[i + 1] = null;
      } else {
        segments[i + 1] = tagColumn.getBinary(rowIdx).getStringValue(TSFileConfig.STRING_CHARSET);
      }
    }
    return new StringArrayDeviceID(segments);
  }

  @Override
  protected IChunkGroupWriter tryToInitialGroupWriter(
      IDeviceID deviceId, boolean isAligned, boolean isTableModel) throws IOException {
    IChunkGroupWriter groupWriter = groupWriters.get(deviceId);
    if (groupWriter == null) {
      groupWriter = new TableTsBlockChunkGroupWriterImpl(deviceId);
      ((AlignedChunkGroupWriterImpl) groupWriter)
          .setLastTime(alignedDeviceLastTimeMap.get(deviceId));
      groupWriters.put(deviceId, groupWriter);
    }
    return groupWriter;
  }

  public int getDeviceCount() {
    return alignedDeviceLastTimeMap.size();
  }

  public int getRowCount() {
    return rowCount;
  }

  /**
   * ChunkGroup writer for a single device. Responsible for writing time column and multiple value
   * columns.
   */
  private class TableTsBlockChunkGroupWriterImpl extends TableChunkGroupWriterImpl {
    private final ValueChunkWriter[] valueChunkWriters;

    public TableTsBlockChunkGroupWriterImpl(IDeviceID deviceId) throws IOException {
      super(deviceId);
      // Initialize ValueChunkWriter for each measurement
      this.valueChunkWriters = new ValueChunkWriter[fieldColumnSchemas.length];
      for (int i = 0; i < fieldColumnSchemas.length; i++) {
        valueChunkWriters[i] = tryToAddSeriesWriterInternal(fieldColumnSchemas[i]);
      }
    }

    /**
     * Write a range of rows into chunk group.
     *
     * @param startRowIndex inclusive
     * @param endRowIndex exclusive
     */
    public int write(Column timeColumn, Column[] valueColumns, int startRowIndex, int endRowIndex)
        throws WriteProcessException {
      int pointCount = 0;
      for (int rowIndex = startRowIndex; rowIndex < endRowIndex; rowIndex++) {
        if (timeColumn.isNull(rowIndex)) {
          throw new WriteProcessException("All values in time column should not be null");
        }
        long time = timeColumn.getLong(rowIndex);
        checkIsHistoryData(time);
        for (int valueColumnIndex = 0; valueColumnIndex < valueColumns.length; valueColumnIndex++) {
          Column valueColumn = valueColumns[valueColumnIndex];
          ValueChunkWriter valueChunkWriter = valueChunkWriters[valueColumnIndex];
          boolean isNull = valueColumn.isNull(rowIndex);
          switch (valueChunkWriter.getDataType()) {
            case BOOLEAN:
              valueChunkWriter.write(
                  time, isNull ? false : valueColumn.getBoolean(rowIndex), isNull);
              break;
            case INT32:
            case DATE:
              valueChunkWriter.write(time, isNull ? 0 : valueColumn.getInt(rowIndex), isNull);
              break;
            case INT64:
            case TIMESTAMP:
              valueChunkWriter.write(time, isNull ? 0 : valueColumn.getLong(rowIndex), isNull);
              break;
            case FLOAT:
              valueChunkWriter.write(time, isNull ? 0 : valueColumn.getFloat(rowIndex), isNull);
              break;
            case DOUBLE:
              valueChunkWriter.write(time, isNull ? 0 : valueColumn.getDouble(rowIndex), isNull);
              break;
            case TEXT:
            case BLOB:
            case STRING:
              valueChunkWriter.write(time, isNull ? null : valueColumn.getBinary(rowIndex), isNull);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format(
                      "Data type %s is not supported.", valueChunkWriter.getDataType().getType()));
          }
        }
        timeChunkWriter.write(time);
        lastTime = time;
        isInitLastTime = true;
        if (checkPageSizeAndMayOpenANewPage()) {
          writePageToPageBuffer();
        }
        pointCount++;
      }
      return pointCount;
    }
  }
}
