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
import org.apache.tsfile.i18n.Messages;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ImportSchemaParser {

  private enum Section {
    NONE,
    TAG_COLUMNS,
    SOURCE_COLUMNS
  }

  public static ImportSchema parse(String filePath) throws IOException {
    ImportSchema schema = new ImportSchema();
    List<ImportSchema.TagColumn> tagColumns = new ArrayList<>();
    List<ImportSchema.SourceColumn> sourceColumns = new ArrayList<>();

    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                Files.newInputStream(Paths.get(filePath)), StandardCharsets.UTF_8))) {
      String line;
      Section section = Section.NONE;

      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("//")) {
          continue;
        }

        if (line.startsWith("table_name=")) {
          schema.setTableName(extractValue(line));
          section = Section.NONE;
        } else if (line.startsWith("time_precision=")) {
          schema.setTimePrecision(extractValue(line));
          section = Section.NONE;
        } else if (line.startsWith("has_header=")) {
          String val = extractValue(line);
          if (!"true".equals(val) && !"false".equals(val)) {
            throw new IllegalArgumentException(
                Messages.get("error.tools.import_has_header_invalid"));
          }
          schema.setHasHeader(Boolean.parseBoolean(val));
          section = Section.NONE;
        } else if (line.startsWith("separator=")) {
          schema.setSeparator(extractValue(line));
          section = Section.NONE;
        } else if (line.startsWith("null_format=")) {
          schema.setNullFormat(extractValue(line));
          section = Section.NONE;
        } else if (line.startsWith("time_column=")) {
          schema.setTimeColumnName(extractValue(line));
          section = Section.NONE;
        } else if (line.equals("tag_columns") || line.equals("id_columns")) {
          section = Section.TAG_COLUMNS;
        } else if (line.equals("source_columns") || line.equals("csv_columns")) {
          section = Section.SOURCE_COLUMNS;
        } else if (section == Section.TAG_COLUMNS) {
          tagColumns.add(parseTagColumn(line));
        } else if (section == Section.SOURCE_COLUMNS) {
          sourceColumns.add(parseSourceColumn(line));
        }
      }
    }

    schema.setTagColumns(tagColumns);
    schema.setSourceColumns(sourceColumns);

    if ("tab".equals(schema.getSeparator())) {
      schema.setSeparator("\t");
    }

    validate(schema);
    return schema;
  }

  private static String extractValue(String line) {
    int index = line.indexOf('=');
    return line.substring(index + 1);
  }

  private static ImportSchema.TagColumn parseTagColumn(String line) {
    String[] parts = line.split(" ");
    if (parts.length == 3 && parts[1].trim().equalsIgnoreCase("DEFAULT")) {
      return new ImportSchema.TagColumn(parts[0].trim(), parts[2].trim());
    } else if (parts.length == 1) {
      return new ImportSchema.TagColumn(parts[0].trim());
    }
    throw new IllegalArgumentException(
        Messages.format("error.tools.import_tag_columns_invalid", line));
  }

  private static ImportSchema.SourceColumn parseSourceColumn(String line) {
    String[] parts = line.split(" ");
    String name = parts[0].trim();
    if (name.endsWith(",") || name.endsWith(";")) {
      name = name.substring(0, name.length() - 1);
    }

    if (parts.length == 2) {
      String dataType = parts[1].trim();
      if (dataType.endsWith(",") || dataType.endsWith(";")) {
        dataType = dataType.substring(0, dataType.length() - 1);
      }
      if (dataType.equalsIgnoreCase("SKIP")) {
        return ImportSchema.SourceColumn.skip(name);
      }
      return new ImportSchema.SourceColumn(name, resolveDataType(dataType));
    } else if (parts.length == 1) {
      if (name.equalsIgnoreCase("SKIP")) {
        return ImportSchema.SourceColumn.skip();
      }
      return new ImportSchema.SourceColumn(name, TSDataType.STRING);
    }
    throw new IllegalArgumentException(
        Messages.format("error.tools.import_source_columns_invalid", line));
  }

  private static TSDataType resolveDataType(String typeStr) {
    switch (typeStr.toUpperCase()) {
      case "TEXT":
        return TSDataType.TEXT;
      case "STRING":
        return TSDataType.STRING;
      case "INT32":
        return TSDataType.INT32;
      case "INT64":
        return TSDataType.INT64;
      case "FLOAT":
        return TSDataType.FLOAT;
      case "DOUBLE":
        return TSDataType.DOUBLE;
      case "BOOLEAN":
        return TSDataType.BOOLEAN;
      case "BLOB":
        return TSDataType.BLOB;
      case "DATE":
        return TSDataType.DATE;
      case "TIMESTAMP":
        return TSDataType.TIMESTAMP;
      default:
        throw new IllegalArgumentException(
            Messages.format("error.tools.import_unknown_data_type", typeStr));
    }
  }

  private static void validate(ImportSchema schema) {
    String tp = schema.getTimePrecision();
    if (!"ms".equals(tp) && !"us".equals(tp) && !"ns".equals(tp) && !"s".equals(tp)) {
      throw new IllegalArgumentException(
          Messages.get("error.tools.import_time_precision_unsupported"));
    }

    String sep = schema.getSeparator();
    if (!",".equals(sep) && !"\t".equals(sep) && !";".equals(sep)) {
      throw new IllegalArgumentException(Messages.get("error.tools.import_separator_invalid"));
    }

    if (schema.getTableName().isEmpty()) {
      throw new IllegalArgumentException(Messages.get("error.tools.import_table_name_required"));
    }

    if (schema.getTimeColumnName().isEmpty()) {
      throw new IllegalArgumentException(Messages.get("error.tools.import_time_column_required"));
    }

    if (schema.getSourceColumns().isEmpty()) {
      throw new IllegalArgumentException(
          Messages.get("error.tools.import_source_columns_required"));
    }

    boolean timeFound = false;
    for (ImportSchema.SourceColumn col : schema.getSourceColumns()) {
      if (!col.isSkip() && col.getName().equals(schema.getTimeColumnName())) {
        timeFound = true;
        break;
      }
    }
    if (!timeFound) {
      throw new IllegalArgumentException(
          Messages.format("error.tools.import_time_column_not_found", schema.getTimeColumnName()));
    }

    Set<String> sourceNames = new HashSet<>();
    for (ImportSchema.SourceColumn col : schema.getSourceColumns()) {
      if (!col.isSkip()) {
        sourceNames.add(col.getName());
      }
    }
    for (ImportSchema.TagColumn tag : schema.getTagColumns()) {
      if (!tag.isVirtual() && !sourceNames.contains(tag.getName())) {
        throw new IllegalArgumentException(
            Messages.format("error.tools.import_tag_column_not_found", tag.getName()));
      }
    }
  }
}
