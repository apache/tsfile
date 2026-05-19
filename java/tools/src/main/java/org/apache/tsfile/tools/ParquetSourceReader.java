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

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetSourceReader implements SourceReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetSourceReader.class);

  private final File sourceFile;
  private ImportSchema schema;
  private ParquetFileReader parquetReader;
  private MessageType parquetSchema;
  private boolean exhausted;

  private String overrideTableName;
  private String overrideTimePrecision;

  public ParquetSourceReader(File sourceFile, ImportSchema schema) {
    this.sourceFile = sourceFile;
    this.schema = schema;
    this.exhausted = false;
  }

  public ParquetSourceReader(File sourceFile) {
    this.sourceFile = sourceFile;
    this.schema = null;
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
          Messages.get("error.tools.infer_schema_not_in_auto_mode"));
    }

    try {
      ensureReaderOpen();

      List<String> columnNames = new ArrayList<>();
      List<TSDataType> columnTypes = new ArrayList<>();
      String detectedTimePrecision = null;

      for (Type field : parquetSchema.getFields()) {
        String name = field.getName();
        columnNames.add(name);

        if (field.isPrimitive()) {
          PrimitiveType pt = field.asPrimitiveType();
          TSDataType tsType = mapParquetType(pt);
          columnTypes.add(tsType);

          if (("time".equals(name) || "TIME".equals(name)) && detectedTimePrecision == null) {
            detectedTimePrecision = detectTimestampPrecision(pt);
          }
        } else {
          columnTypes.add(TSDataType.STRING);
        }
      }

      String timeColumn = AutoSchemaInferer.detectTimeColumn(columnNames);
      TSDataType[] types = columnTypes.toArray(new TSDataType[0]);

      String tableName =
          overrideTableName != null
              ? overrideTableName
              : AutoSchemaInferer.deriveTableName(sourceFile.getName(), "parquet_data");

      String timePrecision;
      if (overrideTimePrecision != null) {
        timePrecision = overrideTimePrecision;
      } else if (detectedTimePrecision != null) {
        timePrecision = detectedTimePrecision;
      } else {
        timePrecision = "ms";
      }

      schema =
          AutoSchemaInferer.buildAutoSchema(
              tableName, timeColumn, columnNames, types, timePrecision);
      return schema;
    } catch (IOException e) {
      throw new RuntimeException(
          Messages.format("error.tools.infer_schema_failed", sourceFile.getAbsolutePath()), e);
    }
  }

  @Override
  public SourceBatch readBatch() {
    if (exhausted) {
      return null;
    }

    try {
      ensureReaderOpen();

      PageReadStore rowGroup = parquetReader.readNextRowGroup();
      if (rowGroup == null) {
        exhausted = true;
        return null;
      }

      long rowCount = rowGroup.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(parquetSchema);
      RecordReader<Group> recordReader =
          columnIO.getRecordReader(rowGroup, new GroupRecordConverter(parquetSchema));

      List<String> schemaColumnNames = getSchemaColumnNames();
      Map<String, Integer> parquetColIndex = buildParquetColumnIndex();

      int numCols = schemaColumnNames.size();
      List<Object[]> rows = new ArrayList<>((int) rowCount);

      for (long r = 0; r < rowCount; r++) {
        Group group = recordReader.read();
        Object[] row = new Object[numCols];

        for (int c = 0; c < numCols; c++) {
          String colName = schemaColumnNames.get(c);
          Integer pIdx = parquetColIndex.get(colName);
          if (pIdx == null) {
            row[c] = null;
            continue;
          }

          try {
            if (group.getFieldRepetitionCount(pIdx) == 0) {
              row[c] = null;
            } else {
              row[c] = extractValue(group, pIdx);
            }
          } catch (RuntimeException e) {
            row[c] = null;
          }
        }
        rows.add(row);
      }

      return SourceBatch.fromRows(schemaColumnNames, rows);
    } catch (IOException e) {
      LOGGER.error(
          Messages.format("log.tools.parquet_read_error", sourceFile.getAbsolutePath()), e);
      exhausted = true;
      return null;
    }
  }

  @Override
  public void close() {
    if (parquetReader != null) {
      try {
        parquetReader.close();
      } catch (IOException e) {
        LOGGER.error(Messages.get("log.tools.parquet_close_reader_error"), e);
      }
      parquetReader = null;
    }
  }

  private void ensureReaderOpen() throws IOException {
    if (parquetReader == null) {
      parquetReader = ParquetFileReader.open(new LocalInputFile(sourceFile.toPath()));
      parquetSchema = parquetReader.getFooter().getFileMetaData().getSchema();
    }
  }

  private List<String> getSchemaColumnNames() {
    List<String> names = new ArrayList<>();
    List<ImportSchema.SourceColumn> srcCols = schema.getSourceColumns();
    for (int i = 0; i < srcCols.size(); i++) {
      ImportSchema.SourceColumn col = srcCols.get(i);
      names.add(col.isSkip() ? "_skip_" + i : col.getName());
    }
    return names;
  }

  private Map<String, Integer> buildParquetColumnIndex() {
    Map<String, Integer> index = new HashMap<>();
    List<Type> fields = parquetSchema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      index.put(fields.get(i).getName(), i);
    }
    return index;
  }

  private Object extractValue(Group group, int fieldIndex) {
    Type fieldType = parquetSchema.getType(fieldIndex);
    if (!fieldType.isPrimitive()) {
      return group.getGroup(fieldIndex, 0).toString();
    }

    PrimitiveType pt = fieldType.asPrimitiveType();
    switch (pt.getPrimitiveTypeName()) {
      case BOOLEAN:
        return group.getBoolean(fieldIndex, 0);
      case INT32:
        return group.getInteger(fieldIndex, 0);
      case INT64:
        return group.getLong(fieldIndex, 0);
      case FLOAT:
        return group.getFloat(fieldIndex, 0);
      case DOUBLE:
        return group.getDouble(fieldIndex, 0);
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        LogicalTypeAnnotation logical = pt.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
          return group.getString(fieldIndex, 0);
        }
        return group.getBinary(fieldIndex, 0).getBytes();
      case INT96:
        return group.getBinary(fieldIndex, 0).getBytes();
      default:
        return group.getValueToString(fieldIndex, 0);
    }
  }

  static TSDataType mapParquetType(PrimitiveType pt) {
    LogicalTypeAnnotation logical = pt.getLogicalTypeAnnotation();

    if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      return TSDataType.INT64;
    }
    if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
      return TSDataType.DATE;
    }
    if (logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      return TSDataType.STRING;
    }

    switch (pt.getPrimitiveTypeName()) {
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case INT32:
        return TSDataType.INT32;
      case INT64:
        return TSDataType.INT64;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return TSDataType.STRING;
      case INT96:
        return TSDataType.INT64;
      default:
        return TSDataType.STRING;
    }
  }

  static String detectTimestampPrecision(PrimitiveType pt) {
    LogicalTypeAnnotation logical = pt.getLogicalTypeAnnotation();
    if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts =
          (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logical;
      switch (ts.getUnit()) {
        case MILLIS:
          return "ms";
        case MICROS:
          return "us";
        case NANOS:
          return "ns";
        default:
          return null;
      }
    }
    return null;
  }
}
