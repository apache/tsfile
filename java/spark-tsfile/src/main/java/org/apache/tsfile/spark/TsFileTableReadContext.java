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
import org.apache.tsfile.i18n.Messages;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public class TsFileTableReadContext implements Serializable {

  private final TsFileTableOptions options;
  private final List<String> files;
  private final TsFileTableSchema tableSchema;
  private final Map<String, TsFileTableSchema> schemasByFile;
  private final StructType readSchema;
  private final List<String> queryColumns;
  private final String hiddenFieldColumn;
  private final long startTime;
  private final long endTime;
  private final Map<String, String> tagEqualities;

  public TsFileTableReadContext(
      TsFileTableOptions options,
      List<String> files,
      TsFileTableSchema tableSchema,
      Map<String, TsFileTableSchema> schemasByFile,
      StructType readSchema,
      long startTime,
      long endTime,
      Map<String, String> tagEqualities) {
    this.options = options;
    this.files = Collections.unmodifiableList(new ArrayList<>(files));
    this.tableSchema = tableSchema;
    this.schemasByFile = Collections.unmodifiableMap(new HashMap<>(schemasByFile));
    this.readSchema = readSchema;
    QueryColumns selected = selectQueryColumns(readSchema, tableSchema, options.timeColumn());
    this.queryColumns = selected.queryColumns;
    this.hiddenFieldColumn = selected.hiddenFieldColumn;
    this.startTime = startTime;
    this.endTime = endTime;
    this.tagEqualities = tagEqualities;
  }

  public TsFileTableReadContext(
      TsFileTableOptions options,
      List<String> files,
      TsFileTableSchema tableSchema,
      StructType readSchema,
      long startTime,
      long endTime,
      Map<String, String> tagEqualities) {
    this(
        options,
        files,
        tableSchema,
        schemaByFile(files, tableSchema),
        readSchema,
        startTime,
        endTime,
        tagEqualities);
  }

  private static Map<String, TsFileTableSchema> schemaByFile(
      List<String> files, TsFileTableSchema tableSchema) {
    Map<String, TsFileTableSchema> schemas = new HashMap<>();
    for (String file : files) {
      schemas.put(file, tableSchema);
    }
    return schemas;
  }

  private static QueryColumns selectQueryColumns(
      StructType readSchema, TsFileTableSchema tableSchema, String timeColumn) {
    LinkedHashSet<String> queryColumns = new LinkedHashSet<>();
    boolean hasFieldColumn = false;
    String normalizedTime = TsFileTableOptions.normalizeName(timeColumn);
    for (StructField field : readSchema.fields()) {
      String normalizedName = TsFileTableOptions.normalizeName(field.name());
      if (normalizedTime.equals(normalizedName)) {
        continue;
      }
      TsFileTableSchema.ColumnInfo column = tableSchema.column(normalizedName);
      if (column == null) {
        throw new TsFileSparkException(
            Messages.format("error.spark.unknown_projected_column", field.name()));
      }
      queryColumns.add(column.name());
      hasFieldColumn = hasFieldColumn || column.category() == ColumnCategory.FIELD;
    }
    String hiddenFieldColumn = null;
    if (!hasFieldColumn) {
      TsFileTableSchema.ColumnInfo hidden = tableSchema.firstFieldColumn();
      if (hidden == null) {
        throw new TsFileSparkException(
            Messages.get("error.spark.time_tag_projection_requires_field"));
      }
      hiddenFieldColumn = hidden.name();
      queryColumns.add(hidden.name());
    }
    return new QueryColumns(new ArrayList<>(queryColumns), hiddenFieldColumn);
  }

  public TsFileTableOptions options() {
    return options;
  }

  public List<String> files() {
    return files;
  }

  public TsFileTableSchema tableSchema() {
    return tableSchema;
  }

  public StructType readSchema() {
    return readSchema;
  }

  public List<String> queryColumns() {
    return queryColumns;
  }

  public List<String> queryColumns(String file) {
    TsFileTableSchema fileSchema = schemasByFile.get(file);
    if (fileSchema == null) {
      fileSchema = tableSchema;
    }
    List<String> fileQueryColumns = new ArrayList<>();
    boolean hasFieldColumn = false;
    for (String columnName : queryColumns) {
      TsFileTableSchema.ColumnInfo fileColumn = fileSchema.column(columnName);
      if (fileColumn == null) {
        continue;
      }
      fileQueryColumns.add(fileColumn.name());
      hasFieldColumn = hasFieldColumn || fileColumn.category() == ColumnCategory.FIELD;
    }
    if (!hasFieldColumn) {
      TsFileTableSchema.ColumnInfo hidden = fileSchema.firstFieldColumn();
      if (hidden == null) {
        throw new TsFileSparkException(
            Messages.get("error.spark.time_tag_projection_requires_field"));
      }
      if (!fileQueryColumns.contains(hidden.name())) {
        fileQueryColumns.add(hidden.name());
      }
    }
    return Collections.unmodifiableList(fileQueryColumns);
  }

  public String hiddenFieldColumn() {
    return hiddenFieldColumn;
  }

  public long startTime() {
    return startTime;
  }

  public long endTime() {
    return endTime;
  }

  public Map<String, String> tagEqualities() {
    return tagEqualities;
  }

  public boolean isEmptyTimeRange() {
    return startTime > endTime;
  }

  private static class QueryColumns {
    private final List<String> queryColumns;
    private final String hiddenFieldColumn;

    private QueryColumns(List<String> queryColumns, String hiddenFieldColumn) {
      this.queryColumns = Collections.unmodifiableList(queryColumns);
      this.hiddenFieldColumn = hiddenFieldColumn;
    }
  }
}
