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
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds {@link Tablet} instances from supplement CSV batches that have no time column. Timestamps
 * are assigned as {@code timeOffset + rowIndex + 1}.
 */
public class SyntheticTabletBuilder {

  private final ImportSchema importSchema;
  private final TableSchema tableSchema;
  private final Map<String, Object> tagDefaults;
  private final Map<String, Integer> sourceColumnIndex;
  private final boolean validateUniformTags;
  private long timeOffset;

  public SyntheticTabletBuilder(ImportSchema importSchema, boolean validateUniformTags) {
    this.importSchema = importSchema;
    this.validateUniformTags = validateUniformTags;
    this.tagDefaults = new HashMap<>();
    this.sourceColumnIndex = new HashMap<>();
    this.tableSchema = buildTableSchema();
    buildSourceColumnIndex();
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public void setTimeOffset(long timeOffset) {
    this.timeOffset = timeOffset;
  }

  public long getTimeOffset() {
    return timeOffset;
  }

  public Tablet build(SourceBatch batch) {
    int rowCount = batch.getRowCount();
    long[] timestamps = new long[rowCount];
    for (int i = 0; i < rowCount; i++) {
      timestamps[i] = timeOffset + i + 1;
    }
    return build(batch, timestamps);
  }

  /**
   * Builds a tablet using explicit per-row timestamps (e.g. global supplement ids).
   *
   * @param timestamps length must equal {@code batch.getRowCount()}
   */
  public Tablet build(SourceBatch batch, long[] timestamps) {
    int rowCount = batch.getRowCount();
    if (timestamps.length != rowCount) {
      throw new IllegalArgumentException(
          Messages.format(
              "error.tools.hybrid_timestamps_length_mismatch",
              timestamps.length,
              rowCount));
    }
    Tablet tablet =
        new Tablet(
            tableSchema.getTableName(),
            IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
            IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
            tableSchema.getColumnTypes(),
            rowCount);

    for (int i = 0; i < rowCount; i++) {
      tablet.addTimestamp(i, timestamps[i]);
      fillRow(tablet, batch, i);
    }
    tablet.setRowSize(rowCount);

    if (validateUniformTags && rowCount > 0) {
      validateSingleDevice(tablet);
    }
    return tablet;
  }

  private void fillRow(Tablet tablet, SourceBatch batch, int outputRow) {
    for (int col = 0; col < tableSchema.getColumnSchemas().size(); col++) {
      IMeasurementSchema colSchema = tableSchema.getColumnSchemas().get(col);
      String colName = colSchema.getMeasurementName();

      if (tagDefaults.containsKey(colName)) {
        tablet.addValue(colName, outputRow, tagDefaults.get(colName));
        continue;
      }

      Integer srcIdx = sourceColumnIndex.get(colName);
      if (srcIdx == null) {
        continue;
      }

      Object rawValue = batch.getValue(outputRow, srcIdx);
      if (isNull(rawValue)) {
        continue;
      }

      boolean isMeasurement = tableSchema.getColumnTypes().get(col) == ColumnCategory.FIELD;
      Object converted = ValueConverter.convert(rawValue, colSchema.getType(), isMeasurement);
      tablet.addValue(colName, outputRow, converted);
    }
  }

  private void validateSingleDevice(Tablet tablet) {
    IDeviceID expected = tablet.getDeviceID(0);
    for (int i = 1; i < tablet.getRowSize(); i++) {
      if (!tablet.getDeviceID(i).equals(expected)) {
        throw new IllegalArgumentException(
            Messages.format(
                "error.tools.hybrid_uniform_tags_violation",
                0,
                i,
                expected,
                tablet.getDeviceID(i)));
      }
    }
  }

  private TableSchema buildTableSchema() {
    List<IMeasurementSchema> schemas = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();

    for (ImportSchema.TagColumn tag : importSchema.getTagColumns()) {
      if (tag.hasDefault()) {
        tagDefaults.put(tag.getName(), tag.getDefaultValue());
      }
      schemas.add(
          new MeasurementSchema(
              tag.getName(),
              TSDataType.TEXT,
              org.apache.tsfile.file.metadata.enums.TSEncoding.PLAIN,
              org.apache.tsfile.file.metadata.enums.CompressionType.UNCOMPRESSED));
      categories.add(ColumnCategory.TAG);
    }

    for (ImportSchema.SourceColumn field : importSchema.fieldColumns()) {
      schemas.add(
          new MeasurementSchema(
              field.getName(),
              field.getDataType(),
              org.apache.tsfile.file.metadata.enums.TSEncoding.PLAIN,
              org.apache.tsfile.file.metadata.enums.CompressionType.UNCOMPRESSED));
      categories.add(ColumnCategory.FIELD);
    }

    return new TableSchema(importSchema.getTableName(), schemas, categories);
  }

  private void buildSourceColumnIndex() {
    List<ImportSchema.SourceColumn> cols = ImportSchemaUtils.supplementSourceColumns(importSchema);
    for (int i = 0; i < cols.size(); i++) {
      sourceColumnIndex.put(cols.get(i).getName(), i);
    }
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
