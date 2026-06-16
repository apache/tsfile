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

package org.apache.tsfile.spark;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TagFilterBuilder;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.v4.DeviceTableModelReader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileTablePartitionReader implements PartitionReader<InternalRow> {

  private final String file;
  private final TsFileTableReadContext context;
  private final Map<String, Integer> resultColumnIndex = new HashMap<>();
  private DeviceTableModelReader reader;
  private ResultSet resultSet;
  private InternalRow current;
  private boolean initialized;

  public TsFileTablePartitionReader(String file, TsFileTableReadContext context) {
    this.file = file;
    this.context = context;
  }

  @Override
  public boolean next() throws IOException {
    initialize();
    if (resultSet == null || !resultSet.next()) {
      current = null;
      return false;
    }
    current = buildRow();
    return true;
  }

  private void initialize() throws IOException {
    if (initialized) {
      return;
    }
    initialized = true;
    if (context.isEmptyTimeRange()) {
      return;
    }
    List<String> queryColumns = context.queryColumns();
    for (int i = 0; i < queryColumns.size(); i++) {
      resultColumnIndex.put(queryColumns.get(i), i + 2);
    }
    reader = new DeviceTableModelReader(new File(file));
    try {
      resultSet =
          reader.query(
              context.tableSchema().tableName(),
              queryColumns,
              context.startTime(),
              context.endTime(),
              tagFilter());
    } catch (NoMeasurementException | NoTableException | ReadProcessException e) {
      throw new IOException("Failed to query TsFile table data from " + file, e);
    }
  }

  private Filter tagFilter() {
    if (context.tagEqualities().isEmpty()) {
      return null;
    }
    TagFilterBuilder builder = new TagFilterBuilder(context.tableSchema().toTableSchema());
    Filter filter = null;
    for (Map.Entry<String, String> entry : context.tagEqualities().entrySet()) {
      Filter current = builder.eq(entry.getKey(), entry.getValue());
      filter = filter == null ? current : builder.and(filter, current);
    }
    return filter;
  }

  private InternalRow buildRow() {
    StructType readSchema = context.readSchema();
    Object[] values = new Object[readSchema.size()];
    StructField[] fields = readSchema.fields();
    String normalizedTime = TsFileTableOptions.normalizeName(context.options().timeColumn());
    for (int i = 0; i < fields.length; i++) {
      String normalizedName = TsFileTableOptions.normalizeName(fields[i].name());
      if (normalizedTime.equals(normalizedName)) {
        values[i] = convertTime(resultSet.getLong(1), fields[i].dataType());
      } else {
        TsFileTableSchema.ColumnInfo column = context.tableSchema().column(normalizedName);
        values[i] =
            convertColumn(column, resultColumnIndex.get(normalizedName), fields[i].dataType());
      }
    }
    return new GenericInternalRow(values);
  }

  private Object convertTime(long rawTime, DataType sparkType) {
    if (sparkType.sameType(DataTypes.TimestampType)) {
      return TsFileTableTypeConverter.rawToTimestampMicros(
          rawTime, context.options().timestampPrecision());
    }
    return rawTime;
  }

  private Object convertColumn(
      TsFileTableSchema.ColumnInfo column, int resultIndex, DataType sparkType) {
    if (resultSet.isNull(resultIndex)) {
      return null;
    }
    TSDataType type = column.type();
    switch (type) {
      case BOOLEAN:
        return resultSet.getBoolean(resultIndex);
      case INT32:
        return resultSet.getInt(resultIndex);
      case INT64:
        return resultSet.getLong(resultIndex);
      case FLOAT:
        return resultSet.getFloat(resultIndex);
      case DOUBLE:
        return resultSet.getDouble(resultIndex);
      case TEXT:
      case STRING:
        return UTF8String.fromString(resultSet.getString(resultIndex));
      case DATE:
        LocalDate date = resultSet.getDate(resultIndex);
        return TsFileTableTypeConverter.toSparkDate(date);
      case TIMESTAMP:
        long rawTimestamp = resultSet.getLong(resultIndex);
        if (sparkType.sameType(DataTypes.TimestampType)) {
          return TsFileTableTypeConverter.rawToTimestampMicros(
              rawTimestamp, context.options().timestampPrecision());
        }
        return rawTimestamp;
      case BLOB:
        return resultSet.getBinary(resultIndex);
      case VECTOR:
      case UNKNOWN:
      case OBJECT:
      default:
        throw new TsFileSparkException("Unsupported TsFile data type: " + type);
    }
  }

  @Override
  public InternalRow get() {
    return current;
  }

  @Override
  public void close() {
    if (resultSet != null) {
      resultSet.close();
    }
    if (reader != null) {
      reader.close();
    }
  }
}
