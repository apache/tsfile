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

package org.apache.tsfile.read.query.dataset;

import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class ResultSetMetadataImpl implements ResultSetMetadata {

  private List<String> columnNameList;
  private List<TSDataType> dataTypeList;

  public ResultSetMetadataImpl(List<String> columnNameList, List<TSDataType> dataTypeList) {
    int capacity = columnNameList.size() + 1;
    this.columnNameList = new ArrayList<>(capacity);
    this.dataTypeList = new ArrayList<>(capacity);
    // add time column
    this.columnNameList.add("Time");
    this.dataTypeList.add(TSDataType.INT64);
    // add other columns
    this.columnNameList.addAll(columnNameList);
    this.dataTypeList.addAll(dataTypeList);
  }

  // columnIndex starting from 1
  public String getColumnName(int columnIndex) {
    return columnNameList.get(columnIndex - 1);
  }

  // columnIndex starting from 1
  public TSDataType getColumnType(int columnIndex) {
    return dataTypeList.get(columnIndex - 1);
  }

  @Override
  public String toString() {
    return "ResultSetMetadataImpl{"
        + "columnNameList="
        + columnNameList
        + ", dataTypeList="
        + dataTypeList
        + '}';
  }
}
