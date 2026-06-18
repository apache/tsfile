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
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsFileTableWriteContext implements Serializable {

  private final TsFileTableOptions options;
  private final String outputPath;
  private final int timeColumnIndex;
  private final DataType timeColumnType;
  private final List<WriteColumn> columns;
  private transient TableSchema tableSchema;

  private TsFileTableWriteContext(
      TsFileTableOptions options,
      String outputPath,
      int timeColumnIndex,
      DataType timeColumnType,
      List<WriteColumn> columns,
      TableSchema tableSchema) {
    this.options = options;
    this.outputPath = outputPath;
    this.timeColumnIndex = timeColumnIndex;
    this.timeColumnType = timeColumnType;
    this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    this.tableSchema = tableSchema;
  }

  public static TsFileTableWriteContext build(TsFileTableOptions options, StructType schema) {
    Map<String, Integer> indexByName = buildIndex(schema);
    String normalizedTime = TsFileTableOptions.normalizeName(options.timeColumn());
    Integer timeColumnIndex = indexByName.get(normalizedTime);
    if (timeColumnIndex == null) {
      throw new TsFileSparkException(
          Messages.format("error.spark.time_column_missing", options.timeColumn()));
    }
    DataType timeColumnType = schema.fields()[timeColumnIndex].dataType();
    if (!timeColumnType.sameType(DataTypes.LongType)
        && !timeColumnType.sameType(DataTypes.TimestampType)) {
      throw new TsFileSparkException(Messages.get("error.spark.time_column_type_invalid"));
    }

    List<String> normalizedTags = normalizeColumns(options.tagColumns());
    Set<String> tagSet = new LinkedHashSet<>(normalizedTags);
    if (tagSet.size() != normalizedTags.size()) {
      throw new TsFileSparkException(Messages.get("error.spark.duplicate_tag_columns"));
    }
    if (tagSet.contains(normalizedTime)) {
      throw new TsFileSparkException(Messages.get("error.spark.time_column_in_tag_columns"));
    }

    List<String> normalizedFields =
        options.fieldColumns().isEmpty()
            ? inferFieldColumns(schema, normalizedTime, tagSet)
            : normalizeColumns(options.fieldColumns());
    Set<String> fieldSet = new LinkedHashSet<>(normalizedFields);
    if (fieldSet.size() != normalizedFields.size()) {
      throw new TsFileSparkException(Messages.get("error.spark.duplicate_field_columns"));
    }
    if (fieldSet.contains(normalizedTime)) {
      throw new TsFileSparkException(Messages.get("error.spark.time_column_in_field_columns"));
    }
    for (String tag : tagSet) {
      if (fieldSet.contains(tag)) {
        throw new TsFileSparkException(Messages.format("error.spark.column_both_tag_field", tag));
      }
    }
    if (fieldSet.isEmpty()) {
      throw new TsFileSparkException(Messages.get("error.spark.field_required_write"));
    }

    List<WriteColumn> columns = new ArrayList<>();
    for (String tag : normalizedTags) {
      Integer index = indexByName.get(tag);
      if (index == null) {
        throw new TsFileSparkException(Messages.format("error.spark.tag_column_missing", tag));
      }
      DataType type = schema.fields()[index].dataType();
      if (!type.sameType(DataTypes.StringType)) {
        throw new TsFileSparkException(
            Messages.format("error.spark.tag_column_must_string_spark", tag));
      }
      columns.add(new WriteColumn(tag, index, type, TSDataType.STRING, ColumnCategory.TAG));
    }
    for (String field : normalizedFields) {
      Integer index = indexByName.get(field);
      if (index == null) {
        throw new TsFileSparkException(Messages.format("error.spark.field_column_missing", field));
      }
      DataType sparkType = schema.fields()[index].dataType();
      TSDataType tsType =
          TsFileTableTypeConverter.toTsFileFieldType(sparkType, options.timestampAs());
      columns.add(new WriteColumn(field, index, sparkType, tsType, ColumnCategory.FIELD));
    }

    return new TsFileTableWriteContext(
        options,
        outputPath(options.path()),
        timeColumnIndex,
        timeColumnType,
        columns,
        buildTableSchema(options, columns));
  }

  private static Map<String, Integer> buildIndex(StructType schema) {
    Map<String, Integer> indexByName = new HashMap<>();
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      String normalized = TsFileTableOptions.normalizeName(fields[i].name());
      Integer previous = indexByName.put(normalized, i);
      if (previous != null) {
        throw new TsFileSparkException(
            Messages.format("error.spark.duplicate_dataframe_column", normalized));
      }
    }
    return indexByName;
  }

  private static List<String> normalizeColumns(List<String> columns) {
    List<String> normalized = new ArrayList<>();
    for (String column : columns) {
      normalized.add(TsFileTableOptions.normalizeName(column));
    }
    return normalized;
  }

  private static List<String> inferFieldColumns(
      StructType schema, String normalizedTime, Set<String> tagSet) {
    List<String> fields = new ArrayList<>();
    for (StructField field : schema.fields()) {
      String normalized = TsFileTableOptions.normalizeName(field.name());
      if (!normalized.equals(normalizedTime) && !tagSet.contains(normalized)) {
        fields.add(normalized);
      }
    }
    return fields;
  }

  private static TableSchema buildTableSchema(
      TsFileTableOptions options, List<WriteColumn> columns) {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    List<ColumnCategory> categories = new ArrayList<>();
    CompressionType compression = TsFileTableTypeConverter.parseCompression(options.compression());
    for (WriteColumn column : columns) {
      TSEncoding encoding = null;
      if (column.category() == ColumnCategory.FIELD) {
        encoding = TsFileTableTypeConverter.parseEncoding(options.encoding(), column.tsType());
      }
      if (encoding == null && compression == null) {
        measurementSchemas.add(new MeasurementSchema(column.name(), column.tsType()));
      } else if (encoding == null) {
        measurementSchemas.add(
            new MeasurementSchema(
                column.name(),
                column.tsType(),
                org.apache.tsfile.common.conf.TSFileDescriptor.getInstance()
                    .getConfig()
                    .getValueEncoder(column.tsType()),
                compression));
      } else if (compression == null) {
        measurementSchemas.add(new MeasurementSchema(column.name(), column.tsType(), encoding));
      } else {
        measurementSchemas.add(
            new MeasurementSchema(column.name(), column.tsType(), encoding, compression));
      }
      categories.add(column.category());
    }
    return new TableSchema(options.table(), measurementSchemas, categories);
  }

  private static String outputPath(String value) {
    try {
      URI uri = URI.create(value);
      if (uri.getScheme() == null) {
        return Paths.get(value).toAbsolutePath().toString();
      }
      if ("file".equalsIgnoreCase(uri.getScheme())) {
        return Paths.get(uri).toString();
      }
    } catch (IllegalArgumentException e) {
      return Paths.get(value).toAbsolutePath().toString();
    }
    throw new TsFileSparkException(Messages.format("error.spark.local_output_paths_only", value));
  }

  public TsFileTableOptions options() {
    return options;
  }

  public Path outputPath() {
    return Paths.get(outputPath);
  }

  public int timeColumnIndex() {
    return timeColumnIndex;
  }

  public DataType timeColumnType() {
    return timeColumnType;
  }

  public List<WriteColumn> columns() {
    return columns;
  }

  public TableSchema tableSchema() {
    if (tableSchema == null) {
      tableSchema = buildTableSchema(options, columns);
    }
    return tableSchema;
  }

  public List<String> columnNames() {
    List<String> names = new ArrayList<>(columns.size());
    for (WriteColumn column : columns) {
      names.add(column.name());
    }
    return names;
  }

  public List<TSDataType> dataTypes() {
    List<TSDataType> dataTypes = new ArrayList<>(columns.size());
    for (WriteColumn column : columns) {
      dataTypes.add(column.tsType());
    }
    return dataTypes;
  }

  public List<ColumnCategory> categories() {
    List<ColumnCategory> categories = new ArrayList<>(columns.size());
    for (WriteColumn column : columns) {
      categories.add(column.category());
    }
    return categories;
  }

  public static class WriteColumn implements Serializable {
    private final String name;
    private final int inputIndex;
    private final DataType sparkType;
    private final TSDataType tsType;
    private final ColumnCategory category;

    public WriteColumn(
        String name,
        int inputIndex,
        DataType sparkType,
        TSDataType tsType,
        ColumnCategory category) {
      this.name = name;
      this.inputIndex = inputIndex;
      this.sparkType = sparkType;
      this.tsType = tsType;
      this.category = category;
    }

    public String name() {
      return name;
    }

    public int inputIndex() {
      return inputIndex;
    }

    public DataType sparkType() {
      return sparkType;
    }

    public TSDataType tsType() {
      return tsType;
    }

    public ColumnCategory category() {
      return category;
    }
  }
}
