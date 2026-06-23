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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads supplement CSV files whose columns match {@link ImportSchemaUtils#supplementSourceColumns}
 * (no time column).
 */
public class SupplementCsvSourceReader implements SourceReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SupplementCsvSourceReader.class);
  private static final long DEFAULT_CHUNK_SIZE = 256L * 1024 * 1024;

  private final File sourceFile;
  private final ImportSchema schema;
  private final long chunkSizeBytes;
  private final String separator;
  private final List<ImportSchema.SourceColumn> supplementColumns;

  /** Maps CSV file column index to index in {@link #supplementColumns}. */
  private final int[] fileColumnToSupplementIndex;

  private BufferedReader reader;
  private boolean headerConsumed;
  private boolean exhausted;

  public SupplementCsvSourceReader(File sourceFile, ImportSchema schema) {
    this(sourceFile, schema, DEFAULT_CHUNK_SIZE);
  }

  public SupplementCsvSourceReader(File sourceFile, ImportSchema schema, long chunkSizeBytes) {
    this.sourceFile = sourceFile;
    this.schema = schema;
    this.chunkSizeBytes = chunkSizeBytes;
    this.separator = schema.getSeparator();
    this.supplementColumns = ImportSchemaUtils.supplementSourceColumns(schema);
    this.fileColumnToSupplementIndex = new int[supplementColumns.size()];
    for (int i = 0; i < fileColumnToSupplementIndex.length; i++) {
      fileColumnToSupplementIndex[i] = -1;
    }
    this.headerConsumed = false;
    this.exhausted = false;
  }

  @Override
  public ImportSchema inferSchema() {
    throw new UnsupportedOperationException(
        Messages.get("error.tools.hybrid_infer_schema_unsupported"));
  }

  @Override
  public SourceBatch readBatch() {
    if (exhausted) {
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
        parseHeader(splitLine(headerLine));
        headerConsumed = true;
      } else if (!headerConsumed) {
        throw new IllegalArgumentException(
            Messages.format(
                "error.tools.hybrid_supplement_header_required", sourceFile.getAbsolutePath()));
      }

      List<Object[]> rows = new ArrayList<>();
      long currentSize = 0;
      String line;
      while ((line = reader.readLine()) != null) {
        byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
        long lineSize = lineBytes.length;

        if (currentSize > 0 && currentSize + lineSize > chunkSizeBytes) {
          rows.add(parseLine(splitLine(line)));
          return buildBatch(rows);
        }

        rows.add(parseLine(splitLine(line)));
        currentSize += lineSize;
      }
      exhausted = true;

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

  private void parseHeader(String[] headerNames) {
    if (headerNames.length != supplementColumns.size()) {
      throw new IllegalArgumentException(
          Messages.format(
              "error.tools.csv_column_count_mismatch",
              supplementColumns.size(),
              headerNames.length,
              sourceFile.getAbsolutePath()));
    }
    for (int fileCol = 0; fileCol < headerNames.length; fileCol++) {
      String name = headerNames[fileCol].trim();
      int supplementIdx = -1;
      for (int j = 0; j < supplementColumns.size(); j++) {
        if (supplementColumns.get(j).getName().equals(name)) {
          supplementIdx = j;
          break;
        }
      }
      if (supplementIdx < 0) {
        throw new IllegalArgumentException(
            Messages.format("error.tools.hybrid_supplement_unexpected_column", name));
      }
      fileColumnToSupplementIndex[fileCol] = supplementIdx;
    }
    boolean[] seen = new boolean[supplementColumns.size()];
    for (int mapped : fileColumnToSupplementIndex) {
      if (mapped >= 0) {
        seen[mapped] = true;
      }
    }
    for (int j = 0; j < supplementColumns.size(); j++) {
      if (!seen[j]) {
        throw new IllegalArgumentException(
            Messages.format(
                "error.tools.hybrid_supplement_missing_column",
                supplementColumns.get(j).getName()));
      }
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

  private String[] splitLine(String line) {
    return line.split(separator, -1);
  }

  private Object[] parseLine(String[] parts) {
    if (parts.length != supplementColumns.size()) {
      throw new IllegalArgumentException(
          Messages.format(
              "error.tools.csv_column_count_mismatch",
              supplementColumns.size(),
              parts.length,
              sourceFile.getAbsolutePath()));
    }
    Object[] row = new Object[supplementColumns.size()];
    for (int fileCol = 0; fileCol < parts.length; fileCol++) {
      int supplementIdx = fileColumnToSupplementIndex[fileCol];
      String val = parts[fileCol];
      String nullFormat = schema.getNullFormat();
      if (val.isEmpty() || (nullFormat != null && nullFormat.equals(val))) {
        row[supplementIdx] = null;
      } else {
        row[supplementIdx] = val;
      }
    }
    return row;
  }

  private SourceBatch buildBatch(List<Object[]> rows) {
    String[] names = new String[supplementColumns.size()];
    for (int i = 0; i < supplementColumns.size(); i++) {
      names[i] = supplementColumns.get(i).getName();
    }
    Object[][] colData = new Object[names.length][rows.size()];
    for (int r = 0; r < rows.size(); r++) {
      Object[] row = rows.get(r);
      for (int c = 0; c < names.length; c++) {
        colData[c][r] = row[c];
      }
    }
    return new SourceBatch(names, colData, rows.size());
  }
}
