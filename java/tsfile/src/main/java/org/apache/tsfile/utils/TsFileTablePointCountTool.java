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
package org.apache.tsfile.utils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Detects and backfills table-level point-count properties in a complete TsFile. */
public final class TsFileTablePointCountTool {

  private TsFileTablePointCountTool() {}

  public enum UpdateStatus {
    UPDATED,
    ALREADY_PRESENT,
    NO_TABLE
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println(Messages.get("error.utils.table_point_count_tool_usage"));
      return;
    }
    File file = new File(args[0]);
    UpdateStatus status = updateTablePointCountIfMissing(file);
    System.out.println(
        Messages.format(
            "info.utils.table_point_count_tool_result", file.getAbsolutePath(), status));
  }

  /**
   * Returns whether every table in the file has a valid non-negative point-count property.
   * Tree-model-only files return {@code false}.
   */
  public static boolean containsTablePointCount(File file) throws IOException {
    try (TsFileSequenceReader reader = openCompleteFile(file)) {
      Map<String, TableSchema> tableSchemas = reader.getTableSchemaMap();
      return !tableSchemas.isEmpty()
          && hasAllTablePointCountProperties(reader.getTsFileProperties(), tableSchemas.keySet());
    }
  }

  /**
   * Adds table point-count properties when they are missing. Existing complete properties are left
   * untouched. The source file is replaced only after a complete rewritten copy has been forced to
   * disk.
   */
  public static UpdateStatus updateTablePointCountIfMissing(File file) throws IOException {
    TsFileMetadata metadata;
    long fileMetadataPosition;
    Map<String, Long> tablePointCounts;
    try (TsFileSequenceReader reader = openCompleteFile(file)) {
      reader.setEnableCacheTableSchemaMap();
      metadata = reader.readFileMetadata();
      Map<String, TableSchema> tableSchemas = metadata.getTableSchemaMap();
      if (tableSchemas.isEmpty()) {
        return UpdateStatus.NO_TABLE;
      }
      if (hasAllTablePointCountProperties(metadata.getTsFileProperties(), tableSchemas.keySet())) {
        return UpdateStatus.ALREADY_PRESENT;
      }
      tablePointCounts = scanTablePointCounts(reader, tableSchemas);
      fileMetadataPosition = reader.getFileMetadataPos();
    }

    tablePointCounts.forEach(
        (tableName, pointCount) ->
            metadata.addProperty(
                TsFileIOWriter.TABLE_POINT_COUNT_PROPERTY_PREFIX + tableName,
                Long.toString(pointCount)));
    rewriteFileMetadata(file.toPath(), fileMetadataPosition, metadata);
    return UpdateStatus.UPDATED;
  }

  static Map<String, Long> scanTablePointCounts(
      TsFileSequenceReader reader, Map<String, TableSchema> tableSchemas) throws IOException {
    Map<String, Set<String>> fieldNamesByTable = new HashMap<>();
    Map<String, Long> tablePointCounts = new HashMap<>();
    for (Map.Entry<String, TableSchema> tableEntry : tableSchemas.entrySet()) {
      List<IMeasurementSchema> columns = tableEntry.getValue().getColumnSchemas();
      List<ColumnCategory> columnCategories = tableEntry.getValue().getColumnTypes();
      Set<String> fieldNames = new HashSet<>();
      for (int i = 0; i < columns.size(); i++) {
        if (columnCategories.get(i) == ColumnCategory.FIELD) {
          fieldNames.add(columns.get(i).getMeasurementName());
        }
      }
      fieldNamesByTable.put(tableEntry.getKey(), fieldNames);
      tablePointCounts.put(tableEntry.getKey(), 0L);
    }

    for (IDeviceID device : reader.getAllDevices()) {
      String tableName = device.getTableName();
      Set<String> fieldNames = fieldNamesByTable.getOrDefault(tableName, Collections.emptySet());
      if (fieldNames.isEmpty()) {
        continue;
      }
      Map<String, List<ChunkMetadata>> chunkMetadataByMeasurement =
          reader.readChunkMetadataInDevice(device);
      for (String fieldName : fieldNames) {
        for (ChunkMetadata chunkMetadata :
            chunkMetadataByMeasurement.getOrDefault(fieldName, Collections.emptyList())) {
          tablePointCounts.merge(
              tableName, (long) chunkMetadata.getStatistics().getCount(), Long::sum);
        }
      }
    }
    return tablePointCounts;
  }

  private static TsFileSequenceReader openCompleteFile(File file) throws IOException {
    if (!file.isFile()) {
      throw new IOException(
          Messages.format("error.utils.table_point_count_tool_file_not_found", file));
    }
    TsFileSequenceReader reader = new TsFileSequenceReader(file.getAbsolutePath());
    try {
      if (!reader.isComplete()) {
        throw new IOException(
            Messages.format("error.utils.table_point_count_tool_incomplete_file", file));
      }
      return reader;
    } catch (IOException | RuntimeException e) {
      try {
        reader.close();
      } catch (IOException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    }
  }

  private static boolean hasAllTablePointCountProperties(
      Map<String, String> properties, Set<String> tableNames) {
    if (properties == null) {
      return false;
    }
    for (String tableName : tableNames) {
      String value = properties.get(TsFileIOWriter.TABLE_POINT_COUNT_PROPERTY_PREFIX + tableName);
      try {
        if (value == null || Long.parseLong(value) < 0) {
          return false;
        }
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return true;
  }

  private static void rewriteFileMetadata(
      Path sourceFile, long fileMetadataPosition, TsFileMetadata metadata) throws IOException {
    Path absoluteSource = sourceFile.toAbsolutePath();
    Path temporaryFile =
        Files.createTempFile(
            absoluteSource.getParent(),
            absoluteSource.getFileName().toString(),
            ".point-count.tmp");
    try {
      try (FileChannel sourceChannel = FileChannel.open(absoluteSource, StandardOpenOption.READ);
          FileChannel targetChannel =
              FileChannel.open(
                  temporaryFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
        copyPrefix(sourceChannel, targetChannel, fileMetadataPosition);
        targetChannel.position(fileMetadataPosition);
        OutputStream output = Channels.newOutputStream(targetChannel);
        int metadataSize = metadata.serializeTo(output);
        ReadWriteIOUtils.write(metadataSize, output);
        output.write(TSFileConfig.MAGIC_STRING.getBytes(TSFileConfig.STRING_CHARSET));
        output.flush();
        targetChannel.force(true);
      }
      replaceSourceFile(temporaryFile, absoluteSource);
    } finally {
      Files.deleteIfExists(temporaryFile);
    }
  }

  private static void copyPrefix(
      FileChannel sourceChannel, FileChannel targetChannel, long prefixLength) throws IOException {
    long copied = 0;
    while (copied < prefixLength) {
      long currentCopied = sourceChannel.transferTo(copied, prefixLength - copied, targetChannel);
      if (currentCopied <= 0) {
        throw new IOException(Messages.get("error.utils.table_point_count_tool_copy_failed"));
      }
      copied += currentCopied;
    }
  }

  private static void replaceSourceFile(Path temporaryFile, Path sourceFile) throws IOException {
    try {
      Files.move(
          temporaryFile,
          sourceFile,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(temporaryFile, sourceFile, StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
