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
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TsFileTablePartitionWriter implements DataWriter<InternalRow> {

  private final TsFileTableWriteContext context;
  private final String queryId;
  private final int partitionId;
  private final long taskId;
  private Path tempFile;
  private Path finalFile;
  private TsFileWriter writer;
  private Tablet tablet;
  private boolean closed;

  public TsFileTablePartitionWriter(
      TsFileTableWriteContext context, String queryId, int partitionId, long taskId) {
    this.context = context;
    this.queryId = queryId;
    this.partitionId = partitionId;
    this.taskId = taskId;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    initializeWriterIfNeeded();
    int rowIndex = tablet.getRowSize();
    tablet.addTimestamp(rowIndex, readTime(record));
    for (TsFileTableWriteContext.WriteColumn column : context.columns()) {
      addColumnValue(record, rowIndex, column);
    }
    if (tablet.getRowSize() >= context.options().maxRowsPerTablet()) {
      flushTablet();
    }
  }

  private void initializeWriterIfNeeded() throws IOException {
    if (writer != null) {
      return;
    }
    Path outputPath = context.outputPath();
    Path tempDir = outputPath.resolve("_temporary").resolve(queryId);
    Files.createDirectories(tempDir);
    String fileName = String.format("part-%s-%05d-%020d.tsfile", queryId, partitionId, taskId);
    tempFile = tempDir.resolve(fileName);
    finalFile = outputPath.resolve(fileName);
    writer = new TsFileWriter(tempFile.toFile());
    writer.registerTableSchema(context.tableSchema());
    tablet =
        new Tablet(
            context.options().table(),
            context.columnNames(),
            context.dataTypes(),
            context.categories(),
            context.options().maxRowsPerTablet());
  }

  private long readTime(InternalRow record) {
    if (record.isNullAt(context.timeColumnIndex())) {
      throw new TsFileSparkException("timeColumn must not be null");
    }
    if (context.timeColumnType().sameType(DataTypes.TimestampType)) {
      return TsFileTableTypeConverter.timestampMicrosToRaw(
          record.getLong(context.timeColumnIndex()), context.options().timestampPrecision());
    }
    return record.getLong(context.timeColumnIndex());
  }

  private void addColumnValue(
      InternalRow record, int rowIndex, TsFileTableWriteContext.WriteColumn column) {
    if (column.category() == ColumnCategory.TAG && record.isNullAt(column.inputIndex())) {
      throw new TsFileSparkException("TAG column must not be null: " + column.name());
    }
    Object value = record.isNullAt(column.inputIndex()) ? null : readValue(record, column);
    tablet.addValue(column.name(), rowIndex, value);
  }

  private Object readValue(InternalRow record, TsFileTableWriteContext.WriteColumn column) {
    TSDataType type = column.tsType();
    int index = column.inputIndex();
    switch (type) {
      case BOOLEAN:
        return record.getBoolean(index);
      case INT32:
        return record.getInt(index);
      case INT64:
        return record.getLong(index);
      case FLOAT:
        return record.getFloat(index);
      case DOUBLE:
        return record.getDouble(index);
      case TEXT:
      case STRING:
        UTF8String string = record.getUTF8String(index);
        return string == null ? null : string.toString();
      case DATE:
        return TsFileTableTypeConverter.fromSparkDate(record.getInt(index));
      case TIMESTAMP:
        return TsFileTableTypeConverter.timestampMicrosToRaw(
            record.getLong(index), context.options().timestampPrecision());
      case BLOB:
        return new Binary(record.getBinary(index));
      case VECTOR:
      case UNKNOWN:
      case OBJECT:
      default:
        throw new TsFileSparkException("Unsupported TsFile data type for write: " + type);
    }
  }

  private void flushTablet() throws IOException {
    if (tablet == null || tablet.getRowSize() == 0) {
      return;
    }
    try {
      writer.writeTable(tablet);
      tablet.reset();
    } catch (WriteProcessException e) {
      throw new IOException("Failed to write TsFile tablet", e);
    }
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    if (writer == null) {
      return new TsFileTableWriterCommitMessage(null, null);
    }
    flushTablet();
    closeWriter();
    return new TsFileTableWriterCommitMessage(tempFile.toString(), finalFile.toString());
  }

  @Override
  public void abort() throws IOException {
    closeWriter();
    if (tempFile != null) {
      Files.deleteIfExists(tempFile);
    }
  }

  @Override
  public void close() throws IOException {
    closeWriter();
  }

  private void closeWriter() throws IOException {
    if (closed || writer == null) {
      return;
    }
    closed = true;
    writer.close();
  }
}
