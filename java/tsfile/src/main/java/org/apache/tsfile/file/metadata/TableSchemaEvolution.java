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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Serializes the table-model schema-evolution history (column renames as aliases, and column drops
 * as tombstones) into a single file property so that old readers ignore it and new readers can
 * resolve data written under historical column names.
 *
 * <p>The property is written only when at least one table has evolved, so files written without any
 * schema evolution are byte-for-byte identical to files written before this feature existed.
 */
public class TableSchemaEvolution {

  /** Key of the {@link TsFileMetadata} property that stores the evolution history. */
  public static final String TABLE_SCHEMA_EVOLUTION_PROPERTY = "tableSchemaEvolution";

  private TableSchemaEvolution() {}

  /** Serialize the evolution history of every evolved table, or null when there is none. */
  public static byte[] serialize(Map<String, TableSchema> tableSchemaMap) throws IOException {
    final List<String> evolvedTableNames = new ArrayList<>();
    for (TableSchema tableSchema : tableSchemaMap.values()) {
      if (tableSchema.hasSchemaEvolution()) {
        evolvedTableNames.add(tableSchema.getTableName());
      }
    }
    if (evolvedTableNames.isEmpty()) {
      return null;
    }
    Collections.sort(evolvedTableNames);
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReadWriteForEncodingUtils.writeUnsignedVarInt(evolvedTableNames.size(), out);
    for (String tableName : evolvedTableNames) {
      ReadWriteIOUtils.writeVar(tableName, out);
      tableSchemaMap.get(tableName).serializeEvolution(out);
    }
    return out.toByteArray();
  }

  /** Apply the serialized evolution history to the matching {@link TableSchema}s in the map. */
  public static void deserializeAndApply(Map<String, TableSchema> tableSchemaMap, byte[] data) {
    if (data == null || data.length == 0) {
      return;
    }
    final ByteBuffer buffer = ByteBuffer.wrap(data);
    final int tableCount = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    for (int i = 0; i < tableCount; i++) {
      final String tableName = ReadWriteIOUtils.readVarIntString(buffer);
      final TableSchema tableSchema = tableSchemaMap.get(tableName);
      if (tableSchema != null) {
        TableSchema.deserializeEvolution(tableSchema, buffer);
      } else {
        // The table is unknown in this read context; consume the record so the cursor stays
        // aligned.
        TableSchema.deserializeEvolution(new TableSchema(tableName), buffer);
      }
    }
  }
}
