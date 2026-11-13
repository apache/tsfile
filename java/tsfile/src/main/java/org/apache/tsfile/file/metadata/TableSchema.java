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

import org.apache.tsfile.annotations.TsFileApi;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.compatibility.DeserializeConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
  private int tagColumnCnt = -1;

  // Schema-evolution state. Serialized separately from the column list (see
  // TableSchemaEvolution) so that the on-disk column format stays backward compatible.

  // historical column name -> current column name (a rename chain is flattened: a->b then b->c is
  // stored as a->c and b->c)
  private Map<String, String> columnAliases;
  // names of dropped columns, retained as tombstones so a re-added column can reuse the name with
  // a different type while a rename cannot silently resurrect a dropped name
  private Set<String> tombstones;

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
      throw new IllegalArgumentException(Messages.get("error.file.table_schema_duplicate_column"));
    }
    this.columnCategories = columnCategories;
    this.updatable = false;
  }

  public TableSchema(
      String tableName,
      List<IMeasurementSchema> columnSchemas,
      List<ColumnCategory> columnCategories,
      Map<String, Integer> columnPosIndex) {
    this.tableName = tableName.toLowerCase();
    this.measurementSchemas = columnSchemas;
    this.columnPosIndex = columnPosIndex;
    if (measurementSchemas.size() != columnPosIndex.size()) {
      throw new IllegalArgumentException(Messages.get("error.file.table_schema_duplicate_column"));
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
      throw new IllegalArgumentException(Messages.get("error.file.table_schema_duplicate_column"));
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
      throw new IllegalArgumentException(Messages.get("error.file.table_schema_duplicate_column"));
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
    // historical aliases resolve to the index of their current column
    if (columnAliases != null) {
      for (Map.Entry<String, String> alias : columnAliases.entrySet()) {
        final Integer currentIndex = columnPosIndex.get(alias.getValue());
        if (currentIndex != null) {
          columnPosIndex.putIfAbsent(alias.getKey(), currentIndex);
        }
      }
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
   * @return i if the given column is the i-th column, -1 if the column is not in the schema. A
   *     historical alias of a renamed column resolves to the index of its current column.
   */
  public int findColumnIndex(String columnName) {
    final String lowerCaseColumnName = resolveColumnName(columnName);
    return getColumnPosIndex()
        .computeIfAbsent(
            lowerCaseColumnName,
            colName -> {
              for (int i = 0; i < measurementSchemas.size(); i++) {
                if (measurementSchemas.get(i).getMeasurementName().equals(colName)) {
                  return i;
                }
              }
              return -1;
            });
  }

  /**
   * Resolve a column name to its current name: a historical alias of a renamed column maps to the
   * current name, any other name is returned lowercased unchanged.
   */
  public String resolveColumnName(String columnName) {
    final String lowerCaseColumnName = columnName.toLowerCase();
    if (columnAliases != null) {
      final String currentName = columnAliases.get(lowerCaseColumnName);
      if (currentName != null) {
        return currentName;
      }
    }
    return lowerCaseColumnName;
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
                    && columnCategories.get(i) == ColumnCategory.TAG) {
                  return columnOrder;
                } else if (columnCategories.get(i) == ColumnCategory.TAG) {
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
      if (TsFileConstant.TIME_COLUMN_ID.equals(chunkMetadata.getMeasurementUid())) {
        continue;
      }
      int columnIndex = findColumnIndex(chunkMetadata.getMeasurementUid());
      // if the measurement is not found in the column list, add it
      if (columnIndex == -1) {
        measurementSchemas.add(chunkMetadata.toMeasurementSchema());
        columnCategories.add(ColumnCategory.FIELD);
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

  public int getTagColumnCnt() {
    if (tagColumnCnt != -1) {
      return tagColumnCnt;
    }
    tagColumnCnt = (int) columnCategories.stream().filter(c -> c == ColumnCategory.TAG).count();
    return tagColumnCnt;
  }

  public boolean isUpdatable() {
    return updatable;
  }

  public void setUpdatable(boolean updatable) {
    this.updatable = updatable;
  }

  /**
   * @return historical column name -> current column name (flattened rename chain). Never null.
   */
  public Map<String, String> getColumnAliases() {
    if (columnAliases == null) {
      columnAliases = new HashMap<>();
    }
    return columnAliases;
  }

  public void setColumnAliases(Map<String, String> columnAliases) {
    this.columnAliases = columnAliases;
  }

  /**
   * @return names of dropped columns. Never null.
   */
  public Set<String> getTombstones() {
    if (tombstones == null) {
      tombstones = new HashSet<>();
    }
    return tombstones;
  }

  public void setTombstones(Set<String> tombstones) {
    this.tombstones = tombstones;
  }

  /**
   * @return true if this table has undergone at least one add/drop/rename.
   */
  public boolean hasSchemaEvolution() {
    return (columnAliases != null && !columnAliases.isEmpty())
        || (tombstones != null && !tombstones.isEmpty());
  }

  /** Add a new FIELD column (or re-add a dropped column with a possibly different type). */
  public void addColumn(String columnName, TSDataType dataType) {
    addColumn(new MeasurementSchema(columnName.toLowerCase(), dataType));
  }

  /** Add a new FIELD column with explicit encoding and compression. */
  public void addColumn(
      String columnName, TSDataType dataType, TSEncoding encoding, CompressionType compression) {
    addColumn(new MeasurementSchema(columnName.toLowerCase(), dataType, encoding, compression));
  }

  private void addColumn(MeasurementSchema measurementSchema) {
    final String name = measurementSchema.getMeasurementName();
    if (getTombstones().contains(name)) {
      // re-add of a dropped column: the new type wins
      getTombstones().remove(name);
    } else {
      if (getColumnAliases().containsKey(name)) {
        throw new IllegalArgumentException(
            Messages.format("error.file.table_schema_column_name_used", name, tableName));
      }
      if (findColumnIndex(name) >= 0) {
        throw new IllegalArgumentException(
            Messages.format("error.file.table_schema_column_already_exists", name, tableName));
      }
    }
    measurementSchemas.add(measurementSchema);
    columnCategories.add(ColumnCategory.FIELD);
    getColumnPosIndex().put(name, measurementSchemas.size() - 1);
  }

  /** Drop a FIELD column. The data stays on disk; the column is hidden from results. */
  public void dropColumn(String columnName) {
    final String name = resolveColumnName(columnName);
    final int columnIndex = findColumnIndex(name);
    if (columnIndex < 0) {
      throw new IllegalArgumentException(
          Messages.format("error.file.table_schema_column_not_found", name, tableName));
    }
    if (columnCategories.get(columnIndex) != ColumnCategory.FIELD) {
      throw new IllegalArgumentException(
          Messages.format("error.file.table_schema_column_not_field", name));
    }
    measurementSchemas.remove(columnIndex);
    columnCategories.remove(columnIndex);
    getTombstones().add(name);
    // aliases that pointed at the dropped column are dropped with it, so a later re-add of the
    // name is a fresh column rather than resurrecting a historical alias
    if (columnAliases != null) {
      columnAliases.entrySet().removeIf(alias -> name.equals(alias.getValue()));
    }
    rebuildColumnPosIndex();
  }

  /** Rename a FIELD column. Historical data written under the old name reads back under the new. */
  public void renameColumn(String oldName, String newName) {
    final String currentName = resolveColumnName(oldName);
    final int columnIndex = findColumnIndex(currentName);
    if (columnIndex < 0) {
      throw new IllegalArgumentException(
          Messages.format("error.file.table_schema_column_not_found", currentName, tableName));
    }
    if (columnCategories.get(columnIndex) != ColumnCategory.FIELD) {
      throw new IllegalArgumentException(
          Messages.format("error.file.table_schema_column_not_field", currentName));
    }
    final String renamedName = newName.toLowerCase();
    if (renamedName.equals(currentName)) {
      return;
    }
    if (getTombstones().contains(renamedName)
        || getColumnAliases().containsKey(renamedName)
        || findColumnIndex(renamedName) >= 0) {
      throw new IllegalArgumentException(
          Messages.format("error.file.table_schema_column_name_used", renamedName, tableName));
    }
    ((MeasurementSchema) measurementSchemas.get(columnIndex)).setMeasurementName(renamedName);
    // the renamed name becomes an alias of the current name, and any alias that pointed at the old
    // current name is re-pointed at the new one (flattening the chain)
    getColumnAliases().put(currentName, renamedName);
    for (Map.Entry<String, String> alias : getColumnAliases().entrySet()) {
      if (currentName.equals(alias.getValue()) && !currentName.equals(alias.getKey())) {
        alias.setValue(renamedName);
      }
    }
    rebuildColumnPosIndex();
  }

  private void rebuildColumnPosIndex() {
    if (columnPosIndex == null) {
      columnPosIndex = new HashMap<>();
    } else {
      columnPosIndex.clear();
    }
    for (int i = 0; i < measurementSchemas.size(); i++) {
      columnPosIndex.put(measurementSchemas.get(i).getMeasurementName(), i);
    }
  }

  /**
   * Serialize only the schema-evolution state (aliases + tombstones), not the column list. Used by
   * {@link TableSchemaEvolution} to store history in a file property. Byte format is
   * language-neutral.
   */
  public int serializeEvolution(OutputStream out) throws IOException {
    int cnt = 0;
    final List<String> aliases = new ArrayList<>(getColumnAliases().keySet());
    Collections.sort(aliases);
    cnt += ReadWriteForEncodingUtils.writeUnsignedVarInt(aliases.size(), out);
    for (String historical : aliases) {
      cnt += ReadWriteIOUtils.writeVar(historical, out);
      cnt += ReadWriteIOUtils.writeVar(getColumnAliases().get(historical), out);
    }
    final List<String> dropped = new ArrayList<>(getTombstones());
    Collections.sort(dropped);
    cnt += ReadWriteForEncodingUtils.writeUnsignedVarInt(dropped.size(), out);
    for (String name : dropped) {
      cnt += ReadWriteIOUtils.writeVar(name, out);
    }
    return cnt;
  }

  /** Read the schema-evolution state serialized by {@link #serializeEvolution(OutputStream)}. */
  public static void deserializeEvolution(TableSchema schema, ByteBuffer buffer) {
    final int aliasCount = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    final Map<String, String> aliases = new HashMap<>(aliasCount);
    for (int i = 0; i < aliasCount; i++) {
      final String historical = ReadWriteIOUtils.readVarIntString(buffer);
      final String current = ReadWriteIOUtils.readVarIntString(buffer);
      aliases.put(historical, current);
    }
    final int tombstoneCount = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    final Set<String> tombstones = new HashSet<>(tombstoneCount);
    for (int i = 0; i < tombstoneCount; i++) {
      tombstones.add(ReadWriteIOUtils.readVarIntString(buffer));
    }
    schema.setColumnAliases(aliases);
    schema.setTombstones(tombstones);
  }
}
