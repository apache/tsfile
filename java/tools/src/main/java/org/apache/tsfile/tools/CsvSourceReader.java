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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class CsvSourceReader implements SourceReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(CsvSourceReader.class);
  private static final long DEFAULT_CHUNK_SIZE = 256L * 1024 * 1024;

  private final File sourceFile;
  private ImportSchema schema;
  private final long chunkSizeBytes;
  private final String separator;

  private BufferedReader reader;
  private String[] columnNames;
  private boolean headerConsumed;
  private boolean exhausted;

  private List<Object[]> bufferedSampleRows;
  private String overrideTableName;
  private String overrideTimePrecision;

  public CsvSourceReader(File sourceFile, ImportSchema schema) {
    this(sourceFile, schema, DEFAULT_CHUNK_SIZE);
  }

  public CsvSourceReader(File sourceFile, ImportSchema schema, long chunkSizeBytes) {
    this.sourceFile = sourceFile;
    this.schema = schema;
    this.chunkSizeBytes = chunkSizeBytes;
    this.separator = schema.getSeparator();
    this.headerConsumed = false;
    this.exhausted = false;
  }

  public CsvSourceReader(File sourceFile, String separator) {
    this(sourceFile, separator, DEFAULT_CHUNK_SIZE);
  }

  public CsvSourceReader(File sourceFile, String separator, long chunkSizeBytes) {
    this.sourceFile = sourceFile;
    this.schema = null;
    this.chunkSizeBytes = chunkSizeBytes;
    this.separator = separator != null ? separator : ",";
    this.headerConsumed = false;
    this.exhausted = false;
  }

  public void setOverrideTableName(String tableName) {
    this.overrideTableName = tableName;
  }

  public void setOverrideTimePrecision(String timePrecision) {
    this.overrideTimePrecision = timePrecision;
  }

  @Override
  public ImportSchema inferSchema() {
    if (schema != null) {
      throw new UnsupportedOperationException(
          Messages.get("error.tools.infer_schema_not_in_auto_mode_csv"));
    }

    try {
      ensureReaderOpen();

      String headerLine = reader.readLine();
      if (headerLine == null) {
        throw new IllegalArgumentException(
            Messages.format("error.tools.csv_file_empty", sourceFile.getAbsolutePath()));
      }
      columnNames = splitLine(headerLine);
      headerConsumed = true;

      List<String> colNameList = new ArrayList<>(columnNames.length);
      for (String name : columnNames) {
        colNameList.add(name);
      }

      bufferedSampleRows = new ArrayList<>();
      for (int i = 0; i < AutoSchemaInferer.DEFAULT_SAMPLE_SIZE; i++) {
        String line = reader.readLine();
        if (line == null) {
          exhausted = true;
          break;
        }
        bufferedSampleRows.add(parseLineAutoMode(line));
      }

      String timeColumn = AutoSchemaInferer.detectTimeColumn(colNameList);
      TSDataType[] types =
          AutoSchemaInferer.inferColumnTypes(
              colNameList,
              bufferedSampleRows,
              timeColumn,
              AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS);

      String tableName =
          overrideTableName != null
              ? overrideTableName
              : AutoSchemaInferer.deriveTableName(sourceFile.getName(), "csv_data");
      String timePrecision = overrideTimePrecision != null ? overrideTimePrecision : "ms";

      schema =
          AutoSchemaInferer.buildAutoSchema(
              tableName, timeColumn, colNameList, types, timePrecision);
      schema.setNullFormat("\\N");

      return schema;
    } catch (IOException e) {
      throw new RuntimeException(
          Messages.format("error.tools.infer_schema_failed", sourceFile.getAbsolutePath()), e);
    }
  }

  @Override
  public SourceBatch readBatch() {
    boolean hasBuffered = bufferedSampleRows != null && !bufferedSampleRows.isEmpty();
    if (exhausted && !hasBuffered) {
      return null;
    }

    try {
      ensureReaderOpen();

      if (schema.isHasHeader() && !headerConsumed) {
        String headerLine = reader.readLine();
        if (headerLine == null) {
          exhausted = true;
          return null;
        }
        columnNames = splitLine(headerLine);
        validateColumnCount();
        headerConsumed = true;
      } else if (!headerConsumed) {
        columnNames = buildColumnNamesFromSchema();
        headerConsumed = true;
      }

      List<Object[]> rows = new ArrayList<>();
      long currentSize = 0;

      if (hasBuffered) {
        rows.addAll(bufferedSampleRows);
        bufferedSampleRows = null;
      }

      if (!exhausted) {
        String line;
        while ((line = reader.readLine()) != null) {
          byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
          long lineSize = lineBytes.length;

          if (currentSize > 0 && currentSize + lineSize > chunkSizeBytes) {
            rows.add(parseLine(line));
            currentSize += lineSize;
            return buildBatch(rows);
          }

          rows.add(parseLine(line));
          currentSize += lineSize;
        }
        exhausted = true;
      }

      if (rows.isEmpty()) {
        return null;
      }
      return buildBatch(rows);

    } catch (IOException e) {
      LOGGER.error(Messages.format("log.tools.csv_read_error", sourceFile.getAbsolutePath()), e);
      exhausted = true;
      return null;
    }
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        LOGGER.error(Messages.get("log.tools.csv_close_reader_error"), e);
      }
      reader = null;
    }
  }

  private void ensureReaderOpen() throws IOException {
    if (reader == null) {
      reader =
          new BufferedReader(
              new InputStreamReader(
                  Files.newInputStream(sourceFile.toPath()), StandardCharsets.UTF_8));
    }
  }

  /**
   * RFC 4180-style tokenizer that handles quoted fields with embedded delimiters and escaped quotes
   * ({@code ""}). Multi-line quoted records are not supported — quoted values must not contain line
   * breaks, since the surrounding read loop is line-oriented.
   */
  String[] splitLine(String line) {
    if (line.indexOf('"') < 0) {
      return line.split(separator, -1);
    }
    List<String> tokens = new ArrayList<>();
    StringBuilder cur = new StringBuilder();
    boolean inQuotes = false;
    boolean fieldStart = true;
    int sepLen = separator.length();
    int i = 0;
    while (i < line.length()) {
      char c = line.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            cur.append('"');
            i += 2;
            continue;
          }
          inQuotes = false;
          i++;
          continue;
        }
        cur.append(c);
        i++;
      } else {
        if (fieldStart && c == '"') {
          inQuotes = true;
          fieldStart = false;
          i++;
          continue;
        }
        if (line.regionMatches(i, separator, 0, sepLen)) {
          tokens.add(cur.toString());
          cur.setLength(0);
          fieldStart = true;
          i += sepLen;
          continue;
        }
        cur.append(c);
        fieldStart = false;
        i++;
      }
    }
    tokens.add(cur.toString());
    return tokens.toArray(new String[0]);
  }

  private Object[] parseLine(String line) {
    String[] parts = splitLine(line);
    Object[] row = new Object[columnNames.length];
    for (int i = 0; i < row.length && i < parts.length; i++) {
      String val = parts[i];
      String nullFormat = schema.getNullFormat();
      if (val.isEmpty() || (nullFormat != null && nullFormat.equals(val))) {
        row[i] = null;
      } else {
        row[i] = val;
      }
    }
    return row;
  }

  private Object[] parseLineAutoMode(String line) {
    String[] parts = splitLine(line);
    Object[] row = new Object[columnNames.length];
    for (int i = 0; i < row.length && i < parts.length; i++) {
      String val = parts[i];
      if (AutoSchemaInferer.DEFAULT_CSV_NULL_TOKENS.contains(val)) {
        row[i] = null;
      } else {
        row[i] = val;
      }
    }
    return row;
  }

  private void validateColumnCount() {
    int expected = schema.getSourceColumns().size();
    if (columnNames.length != expected) {
      throw new IllegalArgumentException(
          Messages.format(
              "error.tools.csv_column_count_mismatch",
              expected,
              columnNames.length,
              sourceFile.getAbsolutePath()));
    }
  }

  private String[] buildColumnNamesFromSchema() {
    List<ImportSchema.SourceColumn> srcCols = schema.getSourceColumns();
    String[] names = new String[srcCols.size()];
    for (int i = 0; i < srcCols.size(); i++) {
      ImportSchema.SourceColumn col = srcCols.get(i);
      names[i] = col.isSkip() ? "_skip_" + i : col.getName();
    }
    return names;
  }

  private SourceBatch buildBatch(List<Object[]> rows) {
    List<String> nameList = new ArrayList<>(columnNames.length);
    for (String name : columnNames) {
      nameList.add(name);
    }
    return SourceBatch.fromRows(nameList, rows);
  }
}
