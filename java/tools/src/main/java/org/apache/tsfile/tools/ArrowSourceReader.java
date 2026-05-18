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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArrowSourceReader implements SourceReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowSourceReader.class);

  private final File sourceFile;
  private ImportSchema schema;
  private BufferAllocator allocator;
  private FileInputStream fileInputStream;
  private ArrowFileReader arrowReader;
  private Schema arrowSchema;
  private List<ArrowBlock> recordBatches;
  private int currentBatchIndex;
  private boolean exhausted;

  private String overrideTableName;
  private String overrideTimePrecision;

  public ArrowSourceReader(File sourceFile, ImportSchema schema) {
    this.sourceFile = sourceFile;
    this.schema = schema;
    this.exhausted = false;
    this.currentBatchIndex = 0;
  }

  public ArrowSourceReader(File sourceFile) {
    this.sourceFile = sourceFile;
    this.schema = null;
    this.exhausted = false;
    this.currentBatchIndex = 0;
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
      throw new UnsupportedOperationException("inferSchema() is only available in auto mode");
    }

    try {
      ensureReaderOpen();

      List<String> columnNames = new ArrayList<>();
      List<TSDataType> columnTypes = new ArrayList<>();
      String detectedTimePrecision = null;

      for (Field field : arrowSchema.getFields()) {
        String name = field.getName();
        columnNames.add(name);
        TSDataType tsType = mapArrowType(field.getType());
        columnTypes.add(tsType);

        if (("time".equals(name) || "TIME".equals(name)) && detectedTimePrecision == null) {
          detectedTimePrecision = detectTimestampPrecision(field.getType());
        }
      }

      String timeColumn = AutoSchemaInferer.detectTimeColumn(columnNames);
      TSDataType[] types = columnTypes.toArray(new TSDataType[0]);

      String tableName =
          overrideTableName != null
              ? overrideTableName
              : AutoSchemaInferer.deriveTableName(sourceFile.getName(), "arrow_data");

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
      throw new RuntimeException("Failed to infer schema from: " + sourceFile.getAbsolutePath(), e);
    }
  }

  @Override
  public SourceBatch readBatch() {
    if (exhausted) {
      return null;
    }

    try {
      ensureReaderOpen();

      if (currentBatchIndex >= recordBatches.size()) {
        exhausted = true;
        return null;
      }

      arrowReader.loadRecordBatch(recordBatches.get(currentBatchIndex));
      currentBatchIndex++;

      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      int rowCount = root.getRowCount();
      if (rowCount == 0) {
        if (currentBatchIndex >= recordBatches.size()) {
          exhausted = true;
          return null;
        }
        return readBatch();
      }

      List<String> schemaColumnNames = getSchemaColumnNames();
      Map<String, FieldVector> vectorMap = new HashMap<>();
      for (FieldVector vec : root.getFieldVectors()) {
        vectorMap.put(vec.getName(), vec);
      }

      int numCols = schemaColumnNames.size();
      List<Object[]> rows = new ArrayList<>(rowCount);

      for (int r = 0; r < rowCount; r++) {
        Object[] row = new Object[numCols];
        for (int c = 0; c < numCols; c++) {
          String colName = schemaColumnNames.get(c);
          FieldVector vec = vectorMap.get(colName);
          if (vec == null || vec.isNull(r)) {
            row[c] = null;
          } else {
            row[c] = extractValue(vec, r);
          }
        }
        rows.add(row);
      }

      return SourceBatch.fromRows(schemaColumnNames, rows);
    } catch (IOException e) {
      LOGGER.error("Error reading Arrow file: " + sourceFile.getAbsolutePath(), e);
      exhausted = true;
      return null;
    }
  }

  @Override
  public void close() {
    if (arrowReader != null) {
      try {
        arrowReader.close();
      } catch (IOException e) {
        LOGGER.error("Error closing Arrow reader", e);
      }
      arrowReader = null;
    }
    if (fileInputStream != null) {
      try {
        fileInputStream.close();
      } catch (IOException e) {
        LOGGER.error("Error closing FileInputStream", e);
      }
      fileInputStream = null;
    }
    if (allocator != null) {
      allocator.close();
      allocator = null;
    }
  }

  private void ensureReaderOpen() throws IOException {
    if (arrowReader == null) {
      allocator = new RootAllocator();
      fileInputStream = new FileInputStream(sourceFile);
      arrowReader = new ArrowFileReader(fileInputStream.getChannel(), allocator);
      arrowSchema = arrowReader.getVectorSchemaRoot().getSchema();
      recordBatches = arrowReader.getRecordBlocks();
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

  private Object extractValue(FieldVector vec, int row) {
    // Date / Timestamp checks must come BEFORE the BigIntVector/IntVector branches: although
    // they hold int/long underneath, DateDayVector / TimeStampVector do NOT extend
    // IntVector / BigIntVector, so without these branches Date columns fall through to the
    // generic getObject().toString() path and produce strings that don't match TSDataType.DATE.
    if (vec instanceof DateDayVector) {
      // Days since 1970-01-01. ValueConverter.toLocalDate handles Integer → LocalDate.
      return ((DateDayVector) vec).get(row);
    } else if (vec instanceof DateMilliVector) {
      // Millis since 1970-01-01; collapse to date.
      long millis = ((DateMilliVector) vec).get(row);
      return LocalDate.ofEpochDay(Math.floorDiv(millis, 86_400_000L));
    } else if (vec instanceof TimeStampVector) {
      // Long in the vector's native precision; matches the precision detected by
      // detectTimestampPrecision() and stored on the schema.
      return ((TimeStampVector) vec).get(row);
    } else if (vec instanceof BigIntVector) {
      return ((BigIntVector) vec).get(row);
    } else if (vec instanceof IntVector) {
      return ((IntVector) vec).get(row);
    } else if (vec instanceof Float4Vector) {
      return ((Float4Vector) vec).get(row);
    } else if (vec instanceof Float8Vector) {
      return ((Float8Vector) vec).get(row);
    } else if (vec instanceof BitVector) {
      return ((BitVector) vec).get(row) != 0;
    } else if (vec instanceof VarCharVector) {
      byte[] bytes = ((VarCharVector) vec).get(row);
      return new String(bytes, StandardCharsets.UTF_8);
    } else if (vec instanceof VarBinaryVector) {
      return ((VarBinaryVector) vec).get(row);
    } else {
      Object obj = vec.getObject(row);
      return obj != null ? obj.toString() : null;
    }
  }

  static TSDataType mapArrowType(ArrowType type) {
    if (type instanceof ArrowType.Int) {
      int bitWidth = ((ArrowType.Int) type).getBitWidth();
      return bitWidth <= 32 ? TSDataType.INT32 : TSDataType.INT64;
    } else if (type instanceof ArrowType.FloatingPoint) {
      switch (((ArrowType.FloatingPoint) type).getPrecision()) {
        case SINGLE:
          return TSDataType.FLOAT;
        case DOUBLE:
          return TSDataType.DOUBLE;
        default:
          return TSDataType.DOUBLE;
      }
    } else if (type instanceof ArrowType.Bool) {
      return TSDataType.BOOLEAN;
    } else if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8) {
      return TSDataType.STRING;
    } else if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) {
      return TSDataType.BLOB;
    } else if (type instanceof ArrowType.Timestamp) {
      return TSDataType.INT64;
    } else if (type instanceof ArrowType.Date) {
      return TSDataType.DATE;
    }
    return TSDataType.STRING;
  }

  static String detectTimestampPrecision(ArrowType type) {
    if (type instanceof ArrowType.Timestamp) {
      switch (((ArrowType.Timestamp) type).getUnit()) {
        case MILLISECOND:
          return "ms";
        case MICROSECOND:
          return "us";
        case NANOSECOND:
          return "ns";
        case SECOND:
          return "s";
        default:
          return null;
      }
    }
    return null;
  }
}
