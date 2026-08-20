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
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.util.List;

/** multi-way merging data set, no need to use TimeGenerator. */
public class DataSetWithoutTimeGenerator extends QueryDataSet {

  private final List<AbstractFileSeriesReader> readers;

  private BatchData[] batchDataList;

  private boolean[] hasDataRemaining;

  private long nextTime;

  private boolean hasNext;

  /**
   * constructor of DataSetWithoutTimeGenerator.
   *
   * @param paths paths in List structure
   * @param dataTypes TSDataTypes in List structure
   * @param readers readers in List(FileSeriesReaderByTimestamp) structure
   * @throws IOException IOException
   */
  public DataSetWithoutTimeGenerator(
      List<Path> paths, List<TSDataType> dataTypes, List<AbstractFileSeriesReader> readers)
      throws IOException {
    super(paths, dataTypes);
    this.readers = readers;
    init();
  }

  private void init() throws IOException {
    hasDataRemaining = new boolean[paths.size()];
    batchDataList = new BatchData[paths.size()];

    for (int i = 0; i < paths.size(); i++) {
      AbstractFileSeriesReader reader = readers.get(i);
      if (!reader.hasNextBatch()) {
        batchDataList[i] = new BatchData();
      } else {
        batchDataList[i] = reader.nextBatch();
        hasDataRemaining[i] = true;
      }
    }

    for (BatchData data : batchDataList) {
      if (data.hasCurrent()) {
        updateNextTime(data.currentTime());
      }
    }
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    return hasNext;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    long minTime = nextTime;
    hasNext = false;

    RowRecord rowRecord = new RowRecord(minTime);

    for (int i = 0; i < paths.size(); i++) {
      if (!hasDataRemaining[i]) {
        rowRecord.addField(null);
        continue;
      }

      BatchData data = batchDataList[i];

      if (data.hasCurrent() && data.currentTime() == minTime) {
        Field field = putValueToField(data);
        data.next();

        if (!data.hasCurrent()) {
          AbstractFileSeriesReader reader = readers.get(i);
          if (reader.hasNextBatch()) {
            data = reader.nextBatch();
            if (data.hasCurrent()) {
              batchDataList[i] = data;
            } else {
              hasDataRemaining[i] = false;
            }
          } else {
            hasDataRemaining[i] = false;
          }
        }
        rowRecord.addField(field);
      } else {
        rowRecord.addField(null);
      }

      if (hasDataRemaining[i] && data.hasCurrent()) {
        updateNextTime(data.currentTime());
      }
    }
    return rowRecord;
  }

  private void updateNextTime(long time) {
    if (!hasNext || time < nextTime) {
      nextTime = time;
      hasNext = true;
    }
  }

  private Field putValueToField(BatchData col) {
    TSDataType type = col.getDataType();
    Field field;
    if (type == TSDataType.VECTOR) {
      field = new Field((col.getVector())[0].getDataType());
    } else {
      field = new Field(col.getDataType());
    }
    switch (col.getDataType()) {
      case BOOLEAN:
        field.setBoolV(col.getBoolean());
        break;
      case INT32:
      case DATE:
        field.setIntV(col.getInt());
        break;
      case INT64:
      case TIMESTAMP:
        field.setLongV(col.getLong());
        break;
      case FLOAT:
        field.setFloatV(col.getFloat());
        break;
      case DOUBLE:
        field.setDoubleV(col.getDouble());
        break;
      case TEXT:
      case BLOB:
      case STRING:
      case OBJECT:
        field.setBinaryV(col.getBinary());
        break;
      case VECTOR:
        Field.setTsPrimitiveValue((col.getVector())[0], field);
        break;
      default:
        throw new UnSupportedDataTypeException(
            Messages.format("error.read.dataset_unsupported_type", col.getDataType()));
    }
    return field;
  }
}
