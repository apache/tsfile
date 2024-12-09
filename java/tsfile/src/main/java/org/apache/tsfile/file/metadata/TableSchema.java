/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableSchema {

  // the tableName is not serialized since the TableSchema is always stored in a Map, from whose
  // key the tableName can be known
  protected String tableName;
  protected List<IMeasurementSchema> measurementSchemas;
  protected List<ColumnCategory> columnCategories;
  protected boolean updatable = false;

  // columnName -> pos in columnSchemas
  private Map<String, Integer> columnPosIndex;
  // columnName -> pos in all id columns
  private Map<String, Integer> idColumnOrder;

  public TableSchema(String tableName) {
    this.tableName = tableName.toLowerCase();
    this.measurementSchemas = new ArrayList<>();
    this.columnCategories = new ArrayList<>();
    this.updatable = true;
  }

  // for deserialize
  public TableSchema(
      List<IMeasurementSchema> columnSchemas, List<ColumnCategory> columnCategories) {
    this.measurementSchemas =
        columnSchemas.stream()
            .map(
                measurementSchema ->
                    new MeasurementSchema(
                        measurementSchema.getMeasurementName().toLowerCase(),
                        measurementSchema.getType(),
                        measurementSchema.getEncodingType(),
                        measurementSchema.getCompressor(),
                        measurementSchema.getProps()))
            .collect(Collectors.toList());
    this.columnCategories = columnCategories;
    this.updatable = false;
  }

  public TableSchema(
      String tableName,
      List<IMeasurementSchema> columnSchemas,
      List<ColumnCategory> columnCategories) {
    this.tableName = tableName.toLowerCase();
    this.measurementSchemas = new ArrayList<>(columnSchemas.size());
    this.columnPosIndex = new HashMap<>(columnSchemas.size());
    for (int i = 0; i < columnSchemas.size(); i++) {
      IMeasurementSchema columnSchema = columnSchemas.get(i);
      String measurementName = columnSchema.getMeasurementName().toLowerCase();
      this.measurementSchemas.add(
          new MeasurementSchema(
              measurementName,
              columnSchema.getType(),
              columnSchema.getEncodingType(),
              columnSchema.getCompressor(),
              columnSchema.getProps()));
      columnPosIndex.put(measurementName, i);
    }
    if (measurementSchemas.size() != columnPosIndex.size()) {
      throw new IllegalArgumentException(
          "Each column name in the table should be unique(case insensitive).");
    }
    this.columnCategories = columnCategories;
    this.updatable = false;
  }

  public TableSchema(
      String tableName,
      List<String> columnNameList,
      List<TSDataType> dataTypeList,
      List<ColumnCategory> categoryList) {
    this.tableName = tableName.toLowerCase();
    this.measurementSchemas = new ArrayList<>(columnNameList.size());
    this.columnPosIndex = new HashMap<>(columnNameList.size());
    for (int i = 0; i < columnNameList.size(); i++) {
      String columnName = columnNameList.get(i).toLowerCase();
      measurementSchemas.add(new MeasurementSchema(columnName, dataTypeList.get(i)));
      columnPosIndex.put(columnName, i);
    }
    if (columnNameList.size() != columnPosIndex.size()) {
      throw new IllegalArgumentException(
          "Each column name in the table should be unique(case insensitive).");
    }
    this.columnCategories = categoryList;
    this.updatable = false;
  }

  @TsFileApi
  public TableSchema(String tableName, List<ColumnSchema> columnSchemaList) {
    this.tableName = tableName.toLowerCase();
    this.measurementSchemas = new ArrayList<>(columnSchemaList.size());
    this.columnCategories = new ArrayList<>(columnSchemaList.size());
    this.columnPosIndex = new HashMap<>(columnSchemaList.size());
    for (int i = 0; i < columnSchemaList.size(); i++) {
      ColumnSchema columnSchema = columnSchemaList.get(i);
      String columnName = columnSchema.getColumnName().toLowerCase();
      this.measurementSchemas.add(new MeasurementSchema(columnName, columnSchema.getDataType()));
      this.columnCategories.add(columnSchema.getColumnCategory());
      this.columnPosIndex.put(columnName, i);
    }
    if (columnSchemaList.size() != columnPosIndex.size()) {
      throw new IllegalArgumentException(
          "Each column name in the table should be unique(case insensitive).");
    }
    this.updatable = false;
  }

  public Map<String, Integer> getColumnPosIndex() {
    if (columnPosIndex == null) {
      columnPosIndex = new HashMap<>();
    }
    return columnPosIndex;
  }

  // Only for deserialized TableSchema
  public Map<String, Integer> buildColumnPosIndex() {
    if (columnPosIndex == null) {
      columnPosIndex = new HashMap<>();
    }
    if (columnPosIndex.size() >= measurementSchemas.size()) {
      return columnPosIndex;
    }
    for (int i = 0; i < measurementSchemas.size(); i++) {
      IMeasurementSchema currentColumnSchema = measurementSchemas.get(i);
      columnPosIndex.putIfAbsent(currentColumnSchema.getMeasurementName(), i);
    }
    return columnPosIndex;
  }

  public Map<String, Integer> getIdColumnOrder() {
    if (idColumnOrder == null) {
      idColumnOrder = new HashMap<>();
    }
    return idColumnOrder;
  }

  /**
   * @return i if the given column is the i-th column, -1 if the column is not in the schema
   */
  public int findColumnIndex(String columnName) {
    final String lowerCaseColumnName = columnName.toLowerCase();
    return getColumnPosIndex()
        .computeIfAbsent(
            lowerCaseColumnName,
            colName -> {
              for (int i = 0; i < measurementSchemas.size(); i++) {
                if (measurementSchemas.get(i).getMeasurementName().equals(lowerCaseColumnName)) {
                  return i;
                }
              }
              return -1;
            });
  }

  /**
   * @return i if the given column is the i-th ID column, -1 if the column is not in the schema or
   *     not an ID column
   */
  public int findIdColumnOrder(String columnName) {
    final String lowerCaseColumnName = columnName.toLowerCase();
    return getIdColumnOrder()
        .computeIfAbsent(
            lowerCaseColumnName,
            colName -> {
              int columnOrder = 0;
              for (int i = 0; i < measurementSchemas.size(); i++) {
                if (measurementSchemas.get(i).getMeasurementName().equals(lowerCaseColumnName)
                    && columnCategories.get(i) == ColumnCategory.ID) {
                  return columnOrder;
                } else if (columnCategories.get(i) == ColumnCategory.ID) {
                  columnOrder++;
                }
              }
              return -1;
            });
  }

  public IMeasurementSchema findColumnSchema(String columnName) {
    final int columnIndex = findColumnIndex(columnName.toLowerCase());
    return columnIndex >= 0 ? measurementSchemas.get(columnIndex) : null;
  }

  public void update(ChunkGroupMetadata chunkGroupMetadata) {
    if (!updatable) {
      return;
    }

    for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
      int columnIndex = findColumnIndex(chunkMetadata.getMeasurementUid());
      // if the measurement is not found in the column list, add it
      if (columnIndex == -1) {
        measurementSchemas.add(chunkMetadata.toMeasurementSchema());
        columnCategories.add(ColumnCategory.MEASUREMENT);
        getColumnPosIndex().put(chunkMetadata.getMeasurementUid(), measurementSchemas.size() - 1);
      } else {
        final IMeasurementSchema originSchema = measurementSchemas.get(columnIndex);
        if (originSchema.getType() != chunkMetadata.getDataType()) {
          originSchema.setDataType(TSDataType.STRING);
        }
      }
    }
  }

  public List<IMeasurementSchema> getColumnSchemas() {
    return measurementSchemas;
  }

  public List<ColumnCategory> getColumnTypes() {
    return columnCategories;
  }

  public int serialize(OutputStream out) throws IOException {
    int cnt = 0;
    if (measurementSchemas != null) {
      cnt += ReadWriteForEncodingUtils.writeUnsignedVarInt(measurementSchemas.size(), out);
      for (int i = 0; i < measurementSchemas.size(); i++) {
        IMeasurementSchema columnSchema = measurementSchemas.get(i);
        ColumnCategory columnCategory = columnCategories.get(i);
        cnt += columnSchema.serializeTo(out);
        cnt += ReadWriteIOUtils.write(columnCategory.ordinal(), out);
      }
    } else {
      cnt += ReadWriteForEncodingUtils.writeUnsignedVarInt(0, out);
    }

    return cnt;
  }

  public int serializedSize() {
    try {
      return serialize(new ByteArrayOutputStream());
    } catch (IOException e) {
      return -1;
    }
  }

  public static TableSchema deserialize(ByteBuffer buffer, DeserializeConfig context) {
    final int columnCnt = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(columnCnt);
    List<ColumnCategory> columnCategories = new ArrayList<>();
    for (int i = 0; i < columnCnt; i++) {
      MeasurementSchema measurementSchema =
          context.measurementSchemaBufferDeserializer.deserialize(buffer, context);
      measurementSchemas.add(measurementSchema);
      columnCategories.add(ColumnCategory.values()[buffer.getInt()]);
    }
    return new TableSchema(measurementSchemas, columnCategories);
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName.toLowerCase();
  }

  @Override
  public String toString() {
    return "TableSchema{"
        + "tableName='"
        + tableName
        + '\''
        + ", columnSchemas="
        + measurementSchemas
        + ", columnTypes="
        + columnCategories
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableSchema)) {
      return false;
    }
    TableSchema that = (TableSchema) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(measurementSchemas, that.measurementSchemas)
        && Objects.equals(columnCategories, that.columnCategories);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, measurementSchemas, columnCategories);
  }
}
