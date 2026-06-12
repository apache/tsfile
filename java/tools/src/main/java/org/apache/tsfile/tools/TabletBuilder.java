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
package org.apache.tsfile.tools;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TabletBuilder {

  private final ImportSchema importSchema;
  private final TimeConverter timeConverter;
  private final TableSchema tableSchema;
  private final Map<String, Object> tagDefaults;
  private final Map<String, Integer> sourceColumnIndex;
  private final int timeColumnSourceIndex;

  public TabletBuilder(ImportSchema importSchema, TimeConverter timeConverter) {
    this.importSchema = importSchema;
    this.timeConverter = timeConverter;
    this.tagDefaults = new HashMap<>();
    this.sourceColumnIndex = new HashMap<>();
    this.tableSchema = buildTableSchema();
    this.timeColumnSourceIndex = resolveTimeColumnIndex();
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public Tablet build(SourceBatch batch) {
    int rowCount = batch.getRowCount();
    int[] sortedIndices = sortByTimestamp(batch);

    Tablet tablet =
        new Tablet(
            tableSchema.getTableName(),
            IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
            IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
            tableSchema.getColumnTypes(),
            rowCount);

    for (int i = 0; i < rowCount; i++) {
      int row = sortedIndices[i];
      Object timeValue = batch.getValue(row, timeColumnSourceIndex);
      long timestamp = timeConverter.convert(timeValue, importSchema.getTimePrecision());
      tablet.addTimestamp(i, timestamp);

      for (int col = 0; col < tableSchema.getColumnSchemas().size(); col++) {
        IMeasurementSchema colSchema = tableSchema.getColumnSchemas().get(col);
        String colName = colSchema.getMeasurementName();

        if (tagDefaults.containsKey(colName)) {
          tablet.addValue(colName, i, tagDefaults.get(colName));
          continue;
        }

        Integer srcIdx = sourceColumnIndex.get(colName);
        if (srcIdx == null) {
          continue;
        }

        Object rawValue = batch.getValue(row, srcIdx);
        if (isNull(rawValue)) {
          continue;
        }

        boolean isMeasurement = tableSchema.getColumnTypes().get(col) == ColumnCategory.FIELD;
        Object converted =
            ValueConverter.convert(
                rawValue, colSchema.getType(), isMeasurement, importSchema.getTimePrecision());
        tablet.addValue(colName, i, converted);
      }
    }

    tablet.setRowSize(rowCount);
    return tablet;
  }

  private int[] sortByTimestamp(SourceBatch batch) {
    int rowCount = batch.getRowCount();
    Integer[] indices = new Integer[rowCount];
    for (int i = 0; i < rowCount; i++) {
      indices[i] = i;
    }
    java.util.Arrays.sort(
        indices,
        (a, b) -> {
          Object va = batch.getValue(a, timeColumnSourceIndex);
          Object vb = batch.getValue(b, timeColumnSourceIndex);
          long ta = timeConverter.convert(va, importSchema.getTimePrecision());
          long tb = timeConverter.convert(vb, importSchema.getTimePrecision());
          return Long.compare(ta, tb);
        });
    int[] result = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      result[i] = indices[i];
    }
    return result;
  }

  private TableSchema buildTableSchema() {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();

    for (ImportSchema.TagColumn tag : importSchema.getTagColumns()) {
      if (tag.hasDefault()) {
        tagDefaults.put(tag.getName().toLowerCase(), tag.getDefaultValue());
      }
      schemas.add(new MeasurementSchema(tag.getName(), TSDataType.STRING));
      categories.add(ColumnCategory.TAG);
    }

    for (ImportSchema.SourceColumn field : importSchema.fieldColumns()) {
      schemas.add(new MeasurementSchema(field.getName(), field.getDataType()));
      categories.add(ColumnCategory.FIELD);
    }

    return new TableSchema(importSchema.getTableName(), schemas, categories);
  }

  private int resolveTimeColumnIndex() {
    String[] colNames = findSourceColumnNames();
    String timeName = importSchema.getTimeColumnName();
    for (int i = 0; i < colNames.length; i++) {
      if (colNames[i] != null && colNames[i].equalsIgnoreCase(timeName)) {
        return i;
      }
    }
    throw new IllegalArgumentException(
        "Time column '" + timeName + "' not found in source columns");
  }

  private String[] findSourceColumnNames() {
    List<ImportSchema.SourceColumn> srcCols = importSchema.getSourceColumns();
    String[] names = new String[srcCols.size()];
    for (int i = 0; i < srcCols.size(); i++) {
      ImportSchema.SourceColumn col = srcCols.get(i);
      names[i] = col.isSkip() ? null : col.getName();
      if (!col.isSkip()) {
        sourceColumnIndex.put(col.getName().toLowerCase(), i);
      }
    }
    return names;
  }

  private boolean isNull(Object value) {
    if (value == null) {
      return true;
    }
    if (value instanceof String) {
      String s = (String) value;
      String nullFormat = importSchema.getNullFormat();
      if (nullFormat != null && nullFormat.equals(s)) {
        return true;
      }
      return s.isEmpty();
    }
    return false;
  }
}
