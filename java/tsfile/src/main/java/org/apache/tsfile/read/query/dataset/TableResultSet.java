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
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.block.TsBlockReader;
import org.apache.tsfile.write.record.TSRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class TableResultSet extends AbstractResultSet {

  private static final Logger LOG = LoggerFactory.getLogger(TableResultSet.class);

  private TsBlockReader tsBlockReader;
  private IPointReader tsBlockPointReader;
  private List<String> columnNameList;
  private List<TSDataType> dataTypeList;
  private String tableName;

  public TableResultSet(
      TsBlockReader tsBlockReader,
      List<String> columnNameList,
      List<TSDataType> dataTypeList,
      String tableName) {
    super(columnNameList, dataTypeList);
    this.tsBlockReader = tsBlockReader;
    this.columnNameList = columnNameList;
    this.dataTypeList = dataTypeList;
    this.tableName = tableName;
  }

  @Override
  public boolean next() throws IOException {
    while ((tsBlockPointReader == null || !tsBlockPointReader.hasNextTimeValuePair())
        && tsBlockReader.hasNext()) {
      TsBlock currentTsBlock = tsBlockReader.next();
      tsBlockPointReader = currentTsBlock.getTsBlockAlignedRowIterator();
    }
    if (tsBlockPointReader == null || !tsBlockPointReader.hasNextTimeValuePair()) {
      return false;
    }
    TimeValuePair currentTimeValuePair = tsBlockPointReader.nextTimeValuePair();
    currentRow = convertTimeValuePairToRowRecord(currentTimeValuePair);
    return true;
  }

  private RowRecord convertTimeValuePairToRowRecord(TimeValuePair timeValuePair) {
    RowRecord rowRecord = new RowRecord(timeValuePair.getValues().length);
    rowRecord.setTimestamp(timeValuePair.getTimestamp());
    for (int i = 0; i < timeValuePair.getValues().length; i++) {
      Object value = timeValuePair.getValues()[i];
      rowRecord.addField(Field.getField(value, dataTypeList.get(i)));
    }
    return rowRecord;
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

  @Override
  public Iterator<TSRecord> recordIterator() {
    return new RecordIterator();
  }

  private class RecordIterator implements Iterator<TSRecord> {

    private TSRecord cachedRecord = null;
    private boolean exhausted = false;

    @Override
    public boolean hasNext() {
      if (cachedRecord != null) {
        return true;
      }
      if (exhausted) {
        return false;
      }

      try {
        return cacheNextRecord();
      } catch (IOException e) {
        throw new NoSuchElementException(e.toString());
      }
    }

    private boolean cacheNextRecord() throws IOException {
      boolean next = TableResultSet.this.next();
      if (!next) {
        exhausted = true;
        return false;
      }

      cachedRecord = new TSRecord(tableName, getLong("Time"));
      for (int i = 0; i < columnNameList.size(); i++) {
        Field field = currentRow.getFields().get(i);
        if (field != null) {
          switch (field.getDataType()) {
            case INT32:
            case DATE:
              cachedRecord.addPoint(columnNameList.get(i), field.getIntV());
              break;
            case INT64:
            case TIMESTAMP:
              cachedRecord.addPoint(columnNameList.get(i), field.getLongV());
              break;
            case FLOAT:
              cachedRecord.addPoint(columnNameList.get(i), field.getFloatV());
              break;
            case DOUBLE:
              cachedRecord.addPoint(columnNameList.get(i), field.getDoubleV());
              break;
            case STRING:
            case TEXT:
              cachedRecord.addPoint(columnNameList.get(i), field.getStringValue());
              break;
            case BLOB:
              cachedRecord.addPoint(columnNameList.get(i), field.getBinaryV().getValues());
              break;
            case BOOLEAN:
              cachedRecord.addPoint(columnNameList.get(i), field.getBoolV());
              break;
            case VECTOR:
            case UNKNOWN:
            default:
              break;
          }
        } else {
          cachedRecord.dataPointList.add(null);
        }
      }
      return true;
    }

    @Override
    public TSRecord next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      TSRecord ret = cachedRecord;
      cachedRecord = null;
      return ret;
    }
  }
}
