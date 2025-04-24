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
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.block.TsBlockReader;
import org.apache.tsfile.utils.DateUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

public class TableResultSet extends AbstractResultSet {
  private static final Logger LOG = LoggerFactory.getLogger(TableResultSet.class);

  private final TsBlockReader tsBlockReader;
  private TsBlock currentTsBlock;
  private int currentTsBlockIndex;

  public TableResultSet(
      TsBlockReader tsBlockReader, List<String> columnNameList, List<TSDataType> dataTypeList) {
    super(columnNameList, dataTypeList);
    this.tsBlockReader = tsBlockReader;
  }

  @Override
  public boolean next() throws IOException {
    while ((currentTsBlock == null || currentTsBlockIndex >= currentTsBlock.getPositionCount() - 1)
        && tsBlockReader.hasNext()) {
      currentTsBlock = tsBlockReader.next();
      currentTsBlockIndex = -1;
    }
    currentTsBlockIndex++;
    return currentTsBlock != null && currentTsBlockIndex < currentTsBlock.getPositionCount();
  }

  @Override
  public int getInt(int columnIndex) {
    // -2 because the columnIndex starts from 1 and the first column is fixed as the time column
    return currentTsBlock.getValueColumns()[columnIndex - 2].getInt(currentTsBlockIndex);
  }

  @Override
  public long getLong(int columnIndex) {
    return columnIndex == 1
        ? currentTsBlock.getTimeByIndex(currentTsBlockIndex)
        // -2 because the columnIndex starts from 1 and the first column is fixed as the time column
        : currentTsBlock.getValueColumns()[columnIndex - 2].getLong(currentTsBlockIndex);
  }

  @Override
  public double getDouble(int columnIndex) {
    // -2 because the columnIndex starts from 1 and the first column is fixed as the time column
    return currentTsBlock.getValueColumns()[columnIndex - 2].getDouble(currentTsBlockIndex);
  }

  @Override
  public float getFloat(int columnIndex) {
    // -2 because the columnIndex starts from 1 and the first column is fixed as the time column
    return currentTsBlock.getValueColumns()[columnIndex - 2].getFloat(currentTsBlockIndex);
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    // -2 because the columnIndex starts from 1 and the first column is fixed as the time column
    return currentTsBlock.getValueColumns()[columnIndex - 2].getBoolean(currentTsBlockIndex);
  }

  @Override
  public LocalDate getDate(int columnIndex) {
    // -2 because the columnIndex starts from 1 and the first column is fixed as the time column
    return DateUtils.parseIntToLocalDate(
        currentTsBlock.getValueColumns()[columnIndex - 2].getInt(currentTsBlockIndex));
  }

  @Override
  public byte[] getBinary(int columnIndex) {
    // -2 because the columnIndex starts from 1 and the first column is fixed as the time column
    return currentTsBlock
        .getValueColumns()[columnIndex - 2]
        .getBinary(currentTsBlockIndex)
        .getValues();
  }

  @Override
  public String getString(int columnIndex) {
    // -2 because the columnIndex starts from 1 and the first column is fixed as the time column
    return currentTsBlock
        .getValueColumns()[columnIndex - 2]
        .getBinary(currentTsBlockIndex)
        .toString();
  }

  @Override
  public boolean isNull(int columnIndex) {
    // -2 because the columnIndex starts from 1 and the first column is fixed as the time column
    return currentTsBlock.getValueColumns()[columnIndex - 2].isNull(currentTsBlockIndex);
  }

  @Override
  public void close() {
    if (tsBlockReader == null) {
      return;
    }
    try {
      tsBlockReader.close();
    } catch (Exception e) {
      LOG.error("Failed to close tsBlockReader");
    }
  }
}
