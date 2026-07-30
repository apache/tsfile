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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsFileTableSchema implements Serializable {

  private final String tableName;
  private final String timeColumn;
  private final List<ColumnInfo> columns;
  private final StructType sparkSchema;
  private final Map<String, ColumnInfo> columnByName;

  public TsFileTableSchema(
      String tableName,
      String timeColumn,
      List<ColumnInfo> columns,
      TsFileTableOptions.TimestampAs timestampAs) {
    this.tableName = TsFileTableOptions.normalizeName(tableName);
    this.timeColumn = timeColumn;
    this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    this.columnByName = buildColumnMap(columns);
    this.sparkSchema = buildSparkSchema(timeColumn, columns, timestampAs);
  }

  private static Map<String, ColumnInfo> buildColumnMap(List<ColumnInfo> columns) {
    Map<String, ColumnInfo> map = new HashMap<>();
    for (ColumnInfo column : columns) {
      ColumnInfo previous = map.put(column.name(), column);
      if (previous != null) {
        throw new TsFileSparkException(
            Messages.format("error.spark.duplicate_tsfile_column", column.name()));
      }
    }
    return Collections.unmodifiableMap(map);
  }

  private static StructType buildSparkSchema(
      String timeColumn, List<ColumnInfo> columns, TsFileTableOptions.TimestampAs timestampAs) {
    List<StructField> fields = new ArrayList<>();
    fields.add(
        DataTypes.createStructField(
            timeColumn,
            timestampAs == TsFileTableOptions.TimestampAs.TIMESTAMP
                ? DataTypes.TimestampType
                : DataTypes.LongType,
            false,
            Metadata.empty()));
    Set<String> normalizedSparkNames = new HashSet<>();
    normalizedSparkNames.add(TsFileTableOptions.normalizeName(timeColumn));
    for (ColumnInfo column : columns) {
      if (!normalizedSparkNames.add(column.name())) {
        throw new TsFileSparkException(
            Messages.format("error.spark.duplicate_spark_schema_column", column.name()));
      }
      fields.add(
          DataTypes.createStructField(
              column.name(),
              TsFileTableTypeConverter.toSparkType(column.type(), timestampAs),
              column.category() == ColumnCategory.FIELD,
              Metadata.empty()));
    }
    return DataTypes.createStructType(fields);
  }

  public static TsFileTableSchema fromTableSchema(
      TableSchema tableSchema, String timeColumn, TsFileTableOptions.TimestampAs timestampAs) {
    List<IMeasurementSchema> measurementSchemas = tableSchema.getColumnSchemas();
    List<ColumnCategory> categories = tableSchema.getColumnTypes();
    List<ColumnInfo> columns = new ArrayList<>();
    for (int i = 0; i < measurementSchemas.size(); i++) {
      String columnName =
          TsFileTableOptions.normalizeName(measurementSchemas.get(i).getMeasurementName());
      TSDataType type = measurementSchemas.get(i).getType();
      ColumnCategory category = categories.get(i);
      validateColumn(tableSchema.getTableName(), columnName, type, category);
      columns.add(new ColumnInfo(columnName, type, category));
    }
    return new TsFileTableSchema(tableSchema.getTableName(), timeColumn, columns, timestampAs);
  }

  private static void validateColumn(
      String tableName, String columnName, TSDataType type, ColumnCategory category) {
    if (category == ColumnCategory.TAG) {
      if (type != TSDataType.STRING) {
        throw new TsFileSparkException(
            Messages.format(
                "error.spark.tag_column_must_string_tsfile", tableName, columnName, type));
      }
      return;
    }
    if (category != ColumnCategory.FIELD) {
      throw new TsFileSparkException(
          Messages.format(
              "error.spark.unsupported_column_category", category, tableName, columnName));
    }
    TsFileTableTypeConverter.toSparkType(type, TsFileTableOptions.TimestampAs.LONG);
  }

  public TableSchema toTableSchema() {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    for (ColumnInfo column : columns) {
      measurementSchemas.add(new MeasurementSchema(column.name(), column.type()));
      categories.add(column.category());
    }
    return new TableSchema(tableName, measurementSchemas, categories);
  }

  public String tableName() {
    return tableName;
  }

  public String timeColumn() {
    return timeColumn;
  }

  public List<ColumnInfo> columns() {
    return columns;
  }

  public StructType sparkSchema() {
    return sparkSchema;
  }

  public ColumnInfo column(String normalizedName) {
    return columnByName.get(normalizedName);
  }

  public ColumnInfo firstFieldColumn() {
    for (ColumnInfo column : columns) {
      if (column.category() == ColumnCategory.FIELD) {
        return column;
      }
    }
    return null;
  }

  public List<String> columnNames(List<String> normalizedNames) {
    List<String> names = new ArrayList<>(normalizedNames.size());
    for (String name : normalizedNames) {
      ColumnInfo column = column(name);
      if (column == null) {
        throw new TsFileSparkException(Messages.format("error.spark.unknown_tsfile_column", name));
      }
      names.add(column.name());
    }
    return names;
  }

  public static class ColumnInfo implements Serializable {
    private final String name;
    private final TSDataType type;
    private final ColumnCategory category;

    public ColumnInfo(String name, TSDataType type, ColumnCategory category) {
      this.name = TsFileTableOptions.normalizeName(name);
      this.type = type;
      this.category = category;
    }

    public String name() {
      return name;
    }

    public TSDataType type() {
      return type;
    }

    public ColumnCategory category() {
      return category;
    }
  }
}
