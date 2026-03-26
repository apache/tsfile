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

package org.apache.tsfile.write.chunk;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;

public class TableChunkGroupWriterImpl extends AlignedChunkGroupWriterImpl {

  public TableChunkGroupWriterImpl(IDeviceID deviceId) {
    super(deviceId);
    setConvertColumnNameToLowerCase(true);
  }

  public TableChunkGroupWriterImpl(IDeviceID deviceId, EncryptParameter encryptParam) {
    super(deviceId, encryptParam);
    setConvertColumnNameToLowerCase(true);
  }

  public int write(
      Column timeColumn,
      Column[] valueColumns,
      List<IMeasurementSchema> measurementSchemas,
      int startRowIndex,
      int endRowIndex)
      throws IOException {
    int pointCount = 0;
    ValueChunkWriter[] valueChunkWriters = new ValueChunkWriter[valueColumns.length];
    for (int i = 0; i < measurementSchemas.size(); i++) {
      valueChunkWriters[i] = tryToAddSeriesWriterInternal(measurementSchemas.get(i));
    }
    for (int rowIndex = startRowIndex; rowIndex < endRowIndex; rowIndex++) {
      long time = timeColumn.getLong(rowIndex);
      for (int valueColumnIndex = 0; valueColumnIndex < valueColumns.length; valueColumnIndex++) {
        Column valueColumn = valueColumns[valueColumnIndex];
        IMeasurementSchema measurementSchema = measurementSchemas.get(valueColumnIndex);
        ValueChunkWriter valueChunkWriter = valueChunkWriters[rowIndex];
        boolean isNull = valueColumn.isNull(rowIndex);
        switch (measurementSchema.getType()) {
          case BOOLEAN:
            valueChunkWriter.write(time, isNull ? false : valueColumn.getBoolean(rowIndex), isNull);
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
          case OBJECT:
            valueChunkWriter.write(time, isNull ? null : valueColumn.getBinary(rowIndex), isNull);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "Data type %s is not supported.",
                    measurementSchemas.get(valueColumnIndex).getType()));
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
