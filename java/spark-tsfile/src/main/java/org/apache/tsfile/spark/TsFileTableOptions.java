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

import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TsFileTableOptions implements Serializable {

  private static final Set<String> KNOWN_OPTIONS =
      new HashSet<>(
          Arrays.asList(
              "path",
              "paths",
              "model",
              "table",
              "timecolumn",
              "tagcolumns",
              "fieldcolumns",
              "timestampas",
              "timestampprecision",
              "mergeschema",
              "pushdown",
              "compression",
              "encoding",
              "nulltagpolicy",
              "maxrowspertablet"));

  private final Map<String, String> rawOptions;
  private final String path;
  private final String table;
  private final String timeColumn;
  private final TimestampAs timestampAs;
  private final TimestampPrecision timestampPrecision;
  private final boolean mergeSchema;
  private final boolean pushdown;
  private final List<String> tagColumns;
  private final List<String> fieldColumns;
  private final String compression;
  private final String encoding;
  private final String nullTagPolicy;
  private final int maxRowsPerTablet;

  private TsFileTableOptions(
      Map<String, String> rawOptions,
      String path,
      String table,
      String timeColumn,
      TimestampAs timestampAs,
      TimestampPrecision timestampPrecision,
      boolean mergeSchema,
      boolean pushdown,
      List<String> tagColumns,
      List<String> fieldColumns,
      String compression,
      String encoding,
      String nullTagPolicy,
      int maxRowsPerTablet) {
    this.rawOptions = rawOptions;
    this.path = path;
    this.table = normalizeName(table);
    this.timeColumn = timeColumn;
    this.timestampAs = timestampAs;
    this.timestampPrecision = timestampPrecision;
    this.mergeSchema = mergeSchema;
    this.pushdown = pushdown;
    this.tagColumns = tagColumns;
    this.fieldColumns = fieldColumns;
    this.compression = compression;
    this.encoding = encoding;
    this.nullTagPolicy = nullTagPolicy;
    this.maxRowsPerTablet = maxRowsPerTablet;
  }

  public static TsFileTableOptions forRead(CaseInsensitiveStringMap options) {
    validateKnownOptions(options);
    validateModel(options);
    String path = path(options);
    String timeColumn = option(options, "timeColumn", "time");
    TimestampAs timestampAs = timestampAs(option(options, "timestampAs", "long"));
    TimestampPrecision timestampPrecision =
        timestampPrecision(option(options, "timestampPrecision", "ms"));
    boolean mergeSchema = options.getBoolean("mergeSchema", false);
    if (mergeSchema) {
      throw new TsFileSparkException("mergeSchema=true is not supported in this initial connector");
    }
    return new TsFileTableOptions(
        options.asCaseSensitiveMap(),
        path,
        blankToNull(options.get("table")),
        timeColumn,
        timestampAs,
        timestampPrecision,
        mergeSchema,
        options.getBoolean("pushdown", true),
        Collections.emptyList(),
        Collections.emptyList(),
        blankToNull(options.get("compression")),
        blankToNull(options.get("encoding")),
        option(options, "nullTagPolicy", "error"),
        positiveInt(options, "maxRowsPerTablet", 1024));
  }

  public static TsFileTableOptions forWrite(CaseInsensitiveStringMap options) {
    validateKnownOptions(options);
    validateModel(options);
    String path = path(options);
    String table = blankToNull(options.get("table"));
    if (table == null) {
      throw new TsFileSparkException("Writing TsFile table model requires option \"table\"");
    }
    List<String> tagColumns = parseColumns(options.get("tagColumns"));
    if (tagColumns.isEmpty()) {
      throw new TsFileSparkException("Writing TsFile table model requires option \"tagColumns\"");
    }
    String nullTagPolicy = option(options, "nullTagPolicy", "error").toLowerCase(Locale.ROOT);
    if (!"error".equals(nullTagPolicy)) {
      throw new TsFileSparkException("Only nullTagPolicy=error is supported");
    }
    return new TsFileTableOptions(
        options.asCaseSensitiveMap(),
        path,
        table,
        option(options, "timeColumn", "time"),
        timestampAs(option(options, "timestampAs", "long")),
        timestampPrecision(option(options, "timestampPrecision", "ms")),
        false,
        options.getBoolean("pushdown", true),
        tagColumns,
        parseColumns(options.get("fieldColumns")),
        blankToNull(options.get("compression")),
        blankToNull(options.get("encoding")),
        nullTagPolicy,
        positiveInt(options, "maxRowsPerTablet", 1024));
  }

  private static void validateKnownOptions(CaseInsensitiveStringMap options) {
    for (String key : options.keySet()) {
      String normalized = key.toLowerCase(Locale.ROOT);
      if (!KNOWN_OPTIONS.contains(normalized)) {
        throw new TsFileSparkException("Unsupported TsFile connector option: " + key);
      }
    }
  }

  private static void validateModel(CaseInsensitiveStringMap options) {
    String model = option(options, "model", "table").toLowerCase(Locale.ROOT);
    if (!"table".equals(model)) {
      throw new TsFileSparkException(
          "Only TsFile table model is supported, but option \"model\" was " + model);
    }
  }

  private static String path(CaseInsensitiveStringMap options) {
    if (blankToNull(options.get("paths")) != null) {
      throw new TsFileSparkException(
          "Multiple Spark load paths are not supported yet; pass a directory or glob path instead");
    }
    String path = blankToNull(options.get("path"));
    if (path == null) {
      throw new TsFileSparkException("TsFile connector requires a path");
    }
    return path;
  }

  private static String option(
      CaseInsensitiveStringMap options, String optionName, String defaultValue) {
    String value = blankToNull(options.get(optionName));
    return value == null ? defaultValue : value;
  }

  private static int positiveInt(
      CaseInsensitiveStringMap options, String optionName, int defaultValue) {
    int value = options.getInt(optionName, defaultValue);
    if (value <= 0) {
      throw new TsFileSparkException(optionName + " must be positive");
    }
    return value;
  }

  private static TimestampAs timestampAs(String value) {
    switch (value.toLowerCase(Locale.ROOT)) {
      case "long":
        return TimestampAs.LONG;
      case "timestamp":
        return TimestampAs.TIMESTAMP;
      default:
        throw new TsFileSparkException("timestampAs must be either long or timestamp");
    }
  }

  private static TimestampPrecision timestampPrecision(String value) {
    switch (value.toLowerCase(Locale.ROOT)) {
      case "ms":
        return TimestampPrecision.MS;
      case "us":
        return TimestampPrecision.US;
      case "ns":
        return TimestampPrecision.NS;
      default:
        throw new TsFileSparkException("timestampPrecision must be one of ms, us, or ns");
    }
  }

  private static List<String> parseColumns(String value) {
    if (blankToNull(value) == null) {
      return Collections.emptyList();
    }
    List<String> columns =
        Arrays.stream(value.split(","))
            .map(String::trim)
            .filter(column -> !column.isEmpty())
            .collect(Collectors.toCollection(ArrayList::new));
    if (columns.isEmpty()) {
      throw new TsFileSparkException("Column list option must not be empty");
    }
    return Collections.unmodifiableList(columns);
  }

  public static String normalizeName(String name) {
    return name == null ? null : name.toLowerCase(Locale.ROOT);
  }

  public static String blankToNull(String value) {
    return value == null || value.trim().isEmpty() ? null : value.trim();
  }

  public Map<String, String> rawOptions() {
    return rawOptions;
  }

  public String path() {
    return path;
  }

  public String table() {
    return table;
  }

  public String timeColumn() {
    return timeColumn;
  }

  public TimestampAs timestampAs() {
    return timestampAs;
  }

  public TimestampPrecision timestampPrecision() {
    return timestampPrecision;
  }

  public boolean mergeSchema() {
    return mergeSchema;
  }

  public boolean pushdown() {
    return pushdown;
  }

  public List<String> tagColumns() {
    return tagColumns;
  }

  public List<String> fieldColumns() {
    return fieldColumns;
  }

  public String compression() {
    return compression;
  }

  public String encoding() {
    return encoding;
  }

  public String nullTagPolicy() {
    return nullTagPolicy;
  }

  public int maxRowsPerTablet() {
    return maxRowsPerTablet;
  }

  public enum TimestampAs {
    LONG,
    TIMESTAMP
  }

  public enum TimestampPrecision {
    MS,
    US,
    NS
  }
}
