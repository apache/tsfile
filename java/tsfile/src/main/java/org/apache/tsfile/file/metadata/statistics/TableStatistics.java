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

package org.apache.tsfile.file.metadata.statistics;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TableStatistics {
  private final Map<String, Statistics<? extends Serializable>> fieldColumnStatisticsMap =
      new TreeMap<>();

  public void updateStatistics(
      String fieldColumnName, Statistics<? extends Serializable> statistics) {
    fieldColumnStatisticsMap
        .computeIfAbsent(fieldColumnName, k -> Statistics.getStatsByType(statistics.getType()))
        .mergeStatistics(statistics);
  }

  public int columnCount() {
    return fieldColumnStatisticsMap.size();
  }

  public TimeStatistics getTimeStatistics() {
    return (TimeStatistics) fieldColumnStatisticsMap.get(TsFileConstant.TIME_COLUMN_ID);
  }

  public Statistics<? extends Serializable> getStatistics(String fieldName) {
    return fieldColumnStatisticsMap.get(fieldName);
  }

  public static TableStatistics deserialize(InputStream inputStream, Set<String> queriedColumns)
      throws IOException {
    TableStatistics tableStatistics = new TableStatistics();
    List<String> columnNameList = ReadWriteIOUtils.readStringList(inputStream);
    List<TSDataType> dataTypeList = new ArrayList<>(columnNameList.size());
    List<Integer> statisticsSizeList = new ArrayList<>(columnNameList.size());
    for (int i = 0; i < columnNameList.size(); i++) {
      dataTypeList.add(ReadWriteIOUtils.readDataType(inputStream));
    }
    for (int i = 0; i < columnNameList.size(); i++) {
      statisticsSizeList.add(ReadWriteForEncodingUtils.readVarInt(inputStream));
    }

    for (int i = 0; i < columnNameList.size(); i++) {
      String columnName = columnNameList.get(i);
      if (queriedColumns != null) {
        if (tableStatistics.columnCount() >= queriedColumns.size()) {
          break;
        }
        if (!queriedColumns.contains(columnName)) {
          inputStream.skip(statisticsSizeList.get(i));
          continue;
        }
      }
      Statistics<? extends Serializable> columnStatistics =
          Statistics.deserialize(inputStream, dataTypeList.get(i));
      tableStatistics.updateStatistics(columnName, columnStatistics);
    }
    return tableStatistics;
  }

  public static TableStatistics deserialize(ByteBuffer byteBuffer, Set<String> queriedColumns)
      throws IOException {
    TableStatistics tableStatistics = new TableStatistics();
    List<String> columnNameList = ReadWriteIOUtils.readStringList(byteBuffer);
    List<TSDataType> dataTypeList = new ArrayList<>(columnNameList.size());
    List<Integer> statisticsSizeList = new ArrayList<>(columnNameList.size());
    for (int i = 0; i < columnNameList.size(); i++) {
      dataTypeList.add(ReadWriteIOUtils.readDataType(byteBuffer));
    }
    for (int i = 0; i < columnNameList.size(); i++) {
      statisticsSizeList.add(ReadWriteForEncodingUtils.readVarInt(byteBuffer));
    }

    for (int i = 0; i < columnNameList.size(); i++) {
      String columnName = columnNameList.get(i);
      if (queriedColumns != null) {
        if (tableStatistics.columnCount() >= queriedColumns.size()) {
          break;
        }
        if (!queriedColumns.contains(columnName)) {
          byteBuffer.position(byteBuffer.position() + statisticsSizeList.get(i));
          continue;
        }
      }
      Statistics<? extends Serializable> columnStatistics =
          Statistics.deserialize(byteBuffer, dataTypeList.get(i));
      tableStatistics.updateStatistics(columnName, columnStatistics);
    }
    return tableStatistics;
  }

  public void serializeTo(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(fieldColumnStatisticsMap.size(), outputStream);
    for (String fieldName : fieldColumnStatisticsMap.keySet()) {
      ReadWriteIOUtils.write(fieldName, outputStream);
    }
    for (Statistics<? extends Serializable> statistics : fieldColumnStatisticsMap.values()) {
      ReadWriteIOUtils.write(statistics.getType(), outputStream);
    }
    for (Statistics<? extends Serializable> statistics : fieldColumnStatisticsMap.values()) {
      ReadWriteForEncodingUtils.writeVarInt(statistics.getSerializedSize(), outputStream);
    }
    for (Statistics<? extends Serializable> statistics : fieldColumnStatisticsMap.values()) {
      statistics.serialize(outputStream);
    }
  }
}
