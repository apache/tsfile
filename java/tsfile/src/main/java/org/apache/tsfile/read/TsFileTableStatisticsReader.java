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

package org.apache.tsfile.read;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.file.metadata.statistics.TableStatistics;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TsFileTableStatisticsReader implements ITsFileTableStatisticsReader {

  private static final String treeModelStartStr =
      TsFileConstant.PATH_ROOT + TsFileConstant.PATH_SEPARATOR;
  private final TsFileSequenceReader reader;
  private final List<Long> tableStatisticOffsets;
  private final List<Long> tableStatisticSizes;
  private final boolean hasTableAndTreeData;

  public TsFileTableStatisticsReader(TsFileSequenceReader reader, long tableStatisticsBlockOffset)
      throws IOException {
    this.reader = reader;
    int tableNum = reader.readFileMetadata().getTableSchemaNum();
    this.hasTableAndTreeData =
        tableNum != reader.readFileMetadata().getTableMetadataIndexNodeMap().size();
    this.tableStatisticOffsets = new ArrayList<>(tableNum);
    this.tableStatisticSizes = new ArrayList<>(tableNum);
    ByteBuffer offsetListBuffer =
        reader.readData(tableStatisticsBlockOffset, tableNum * Long.BYTES * 2);
    for (int i = 0; i < tableNum; i++) {
      this.tableStatisticOffsets.add(ReadWriteIOUtils.readLong(offsetListBuffer));
    }
    for (int i = 0; i < tableNum; i++) {
      this.tableStatisticSizes.add(ReadWriteIOUtils.readLong(offsetListBuffer));
    }
  }

  public TableStatistics getTableStatistics(String tableName) throws IOException {
    Optional<Integer> tableIndex = findTableIndex(tableName);
    if (!tableIndex.isPresent()) {
      return null;
    }
    int index = tableIndex.get();
    return readTableStatistics(
        tableStatisticOffsets.get(index), tableStatisticSizes.get(index), null);
  }

  public TableStatistics getTableFieldColumnStatistics(String tableName, String... fieldNames)
      throws IOException {
    Optional<Integer> tableIndex = findTableIndex(tableName);
    if (!tableIndex.isPresent()) {
      return null;
    }
    int index = tableIndex.get();
    Set<String> queriedColumns = new HashSet<>(fieldNames.length + 1);
    queriedColumns.add(TsFileConstant.TIME_COLUMN_ID);
    queriedColumns.addAll(Arrays.asList(fieldNames));
    return readTableStatistics(
        tableStatisticOffsets.get(index), tableStatisticSizes.get(index), queriedColumns);
  }

  public Map<String, TableStatistics> getAllTableStatistics() throws IOException {
    Map<String, TableStatistics> tableStatisticsMap = new LinkedHashMap<>();
    int i = 0;
    for (String tableName : reader.tsFileMetaData.getTableMetadataIndexNodeMap().keySet()) {
      if (hasTableAndTreeData && tableName.startsWith(treeModelStartStr)) {
        continue;
      }
      long offset = tableStatisticOffsets.get(i);
      long size = tableStatisticSizes.get(i++);
      tableStatisticsMap.put(tableName, readTableStatistics(offset, size, null));
    }
    return tableStatisticsMap;
  }

  private Optional<Integer> findTableIndex(String tableName) {
    int index = 0;
    boolean found = false;
    for (String key : reader.tsFileMetaData.getTableMetadataIndexNodeMap().keySet()) {
      if (hasTableAndTreeData && key.startsWith(treeModelStartStr)) {
        continue;
      }
      if (key.equals(tableName)) {
        found = true;
        break;
      }
      index++;
    }
    if (!found) {
      return Optional.empty();
    }
    return Optional.of(index);
  }

  private TableStatistics readTableStatistics(
      long tableStatisticsOffset, long length, Set<String> queriedColumns) throws IOException {
    if (length > Integer.MAX_VALUE) {
      synchronized (reader) {
        TsFileInput tsFileInput = reader.getTsFileInput();
        tsFileInput.position(tableStatisticsOffset + Long.BYTES);
        InputStream inputStream = tsFileInput.wrapAsInputStream();
        return TableStatistics.deserialize(inputStream, queriedColumns);
      }
    } else {
      ByteBuffer contentBuffer =
          reader.readData(tableStatisticsOffset, tableStatisticsOffset + length);
      return TableStatistics.deserialize(contentBuffer, queriedColumns);
    }
  }
}
