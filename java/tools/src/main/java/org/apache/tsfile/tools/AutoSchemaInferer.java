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

import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class AutoSchemaInferer {

  static final int DEFAULT_SAMPLE_SIZE = 100;
  static final Set<String> DEFAULT_CSV_NULL_TOKENS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("", "\\N")));

  private static final Pattern INTEGER_PATTERN = Pattern.compile("-?\\d+");
  private static final Pattern DECIMAL_PATTERN = Pattern.compile("-?\\d+\\.\\d*|-?\\.\\d+");

  enum InferredType {
    UNKNOWN,
    BOOLEAN,
    INT64,
    DOUBLE,
    STRING
  }

  public static String detectTimeColumn(List<String> columnNames) {
    List<String> matches = new ArrayList<>();
    for (String name : columnNames) {
      if ("time".equals(name) || "TIME".equals(name)) {
        matches.add(name);
      }
    }
    if (matches.isEmpty()) {
      throw new IllegalArgumentException(
          "No time column found. Auto mode requires exactly one column named 'time' or 'TIME'.");
    }
    if (matches.size() > 1) {
      throw new IllegalArgumentException(
          "Ambiguous time column: found multiple columns matching 'time'/'TIME': " + matches);
    }
    return matches.get(0);
  }

  public static TSDataType[] inferColumnTypes(
      List<String> columnNames,
      List<Object[]> sampleRows,
      String timeColumnName,
      Set<String> nullTokens) {

    int numCols = columnNames.size();
    InferredType[] types = new InferredType[numCols];
    Arrays.fill(types, InferredType.UNKNOWN);

    for (Object[] row : sampleRows) {
      for (int i = 0; i < numCols && i < row.length; i++) {
        if (columnNames.get(i).equals(timeColumnName)) {
          continue;
        }
        Object val = row[i];
        if (val == null) {
          continue;
        }
        String str = val.toString();
        if (nullTokens.contains(str)) {
          continue;
        }
        if (str.trim().isEmpty()) {
          continue;
        }
        InferredType cellType = classifyCell(str);
        types[i] = promote(types[i], cellType);
      }
    }

    TSDataType[] result = new TSDataType[numCols];
    for (int i = 0; i < numCols; i++) {
      if (columnNames.get(i).equals(timeColumnName)) {
        result[i] = TSDataType.INT64;
      } else {
        result[i] = toTSDataType(types[i]);
      }
    }
    return result;
  }

  public static String deriveTableName(String fileName, String formatDefaultName) {
    String name = removeExtension(fileName);
    name = name.trim();
    name = name.replaceAll("[^a-zA-Z0-9_.]", "_");
    name = name.replaceAll("_+", "_");
    name = name.replaceAll("^_+|_+$", "");
    if (name.isEmpty()) {
      return formatDefaultName;
    }
    if (Character.isDigit(name.charAt(0))) {
      return "t_" + name;
    }
    return name;
  }

  public static ImportSchema buildAutoSchema(
      String tableName,
      String timeColumnName,
      List<String> columnNames,
      TSDataType[] columnTypes,
      String timePrecision) {

    ImportSchema schema = new ImportSchema();
    schema.setTableName(tableName);
    schema.setTimeColumnName(timeColumnName);
    schema.setTimePrecision(timePrecision != null ? timePrecision : "ms");
    schema.setTagColumns(new ArrayList<ImportSchema.TagColumn>());
    schema.setHasHeader(true);

    List<ImportSchema.SourceColumn> sourceColumns = new ArrayList<>();
    for (int i = 0; i < columnNames.size(); i++) {
      sourceColumns.add(new ImportSchema.SourceColumn(columnNames.get(i), columnTypes[i]));
    }
    schema.setSourceColumns(sourceColumns);

    return schema;
  }

  static InferredType classifyCell(String value) {
    String lower = value.toLowerCase();
    if ("true".equals(lower) || "false".equals(lower)) {
      return InferredType.BOOLEAN;
    }
    if (INTEGER_PATTERN.matcher(value).matches()) {
      return InferredType.INT64;
    }
    if (DECIMAL_PATTERN.matcher(value).matches()) {
      return InferredType.DOUBLE;
    }
    return InferredType.STRING;
  }

  static InferredType promote(InferredType current, InferredType incoming) {
    if (current == InferredType.UNKNOWN) {
      return incoming;
    }
    if (incoming == InferredType.UNKNOWN) {
      return current;
    }
    if (current == incoming) {
      return current;
    }
    if (current == InferredType.BOOLEAN || incoming == InferredType.BOOLEAN) {
      return InferredType.STRING;
    }
    if ((current == InferredType.INT64 && incoming == InferredType.DOUBLE)
        || (current == InferredType.DOUBLE && incoming == InferredType.INT64)) {
      return InferredType.DOUBLE;
    }
    return InferredType.STRING;
  }

  static TSDataType toTSDataType(InferredType type) {
    switch (type) {
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case INT64:
        return TSDataType.INT64;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case STRING:
        return TSDataType.STRING;
      case UNKNOWN:
        return TSDataType.STRING;
      default:
        return TSDataType.STRING;
    }
  }

  private static String removeExtension(String fileName) {
    String[] extensions = {".csv", ".parquet", ".arrow", ".ipc", ".feather"};
    String lower = fileName.toLowerCase();
    for (String ext : extensions) {
      if (lower.endsWith(ext)) {
        return fileName.substring(0, fileName.length() - ext.length());
      }
    }
    int dot = fileName.lastIndexOf('.');
    if (dot > 0) {
      return fileName.substring(0, dot);
    }
    return fileName;
  }
}
