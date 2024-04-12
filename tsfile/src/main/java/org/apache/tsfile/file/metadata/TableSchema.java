/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.compatibility.DeserializeContext;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet.ColumnType;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableSchema {
  // the tableName is not serialized since the TableSchema is always stored in a Map, from whose
  // key the tableName can be known
  protected String tableName;
  protected List<MeasurementSchema> columnSchemas;
  protected List<ColumnType> columnTypes;
  protected boolean updatable = false;

  // columnName -> pos in columnSchemas;
  private Map<String, Integer> columnPosIndex;

  public TableSchema(String tableName) {
    this.tableName = tableName;
    this.columnSchemas = new ArrayList<>();
    this.columnTypes = new ArrayList<>();
    this.updatable = true;
  }

  public TableSchema(
      String tableName, List<MeasurementSchema> columnSchemas, List<ColumnType> columnTypes) {
    this.tableName = tableName;
    this.columnSchemas = columnSchemas;
    this.columnTypes = columnTypes;
  }

  public Map<String, Integer> getColumnPosIndex() {
    if (columnPosIndex == null) {
      columnPosIndex = new HashMap<>();
    }
    return columnPosIndex;
  }

  public int findColumnIndex(String columnName) {
    return getColumnPosIndex()
        .computeIfAbsent(
            columnName,
            colName -> {
              for (int i = 0; i < columnSchemas.size(); i++) {
                if (columnSchemas.get(i).getMeasurementId().equals(columnName)) {
                  return i;
                }
              }
              return -1;
            });
  }

  public MeasurementSchema findColumnSchema(String columnName) {
    final int columnIndex = findColumnIndex(columnName);
    return columnIndex >= 0 ? columnSchemas.get(columnIndex) : null;
  }

  public void update(ChunkGroupMetadata chunkGroupMetadata) {
    if (!updatable) {
      return;
    }

    for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
      int columnIndex = findColumnIndex(chunkMetadata.getMeasurementUid());
      // if the measurement is not found in the column list, add it
      if (columnIndex == -1) {
        columnSchemas.add(chunkMetadata.toMeasurementSchema());
        columnTypes.add(ColumnType.MEASUREMENT);
        getColumnPosIndex().put(chunkMetadata.getMeasurementUid(), columnSchemas.size() - 1);
      }
    }
  }

  public List<MeasurementSchema> getColumnSchemas() {
    return columnSchemas;
  }

  public List<ColumnType> getColumnTypes() {
    return columnTypes;
  }

  public int serialize(OutputStream out) throws IOException {
    int cnt = 0;
    if (columnSchemas != null) {
      cnt += ReadWriteIOUtils.write(columnSchemas.size(), out);
      for (int i = 0; i < columnSchemas.size(); i++) {
        MeasurementSchema columnSchema = columnSchemas.get(i);
        ColumnType columnType = columnTypes.get(i);
        cnt += columnSchema.serializeTo(out);
        cnt += ReadWriteIOUtils.write(columnType.ordinal(), out);
      }
    } else {
      cnt += ReadWriteIOUtils.write(0, out);
    }

    return cnt;
  }

  public static TableSchema deserialize(ByteBuffer buffer, DeserializeContext context) {
    final int tableNum = buffer.getInt();
    List<MeasurementSchema> measurementSchemas = new ArrayList<>(tableNum);
    List<ColumnType> columnTypes = new ArrayList<>();
    for (int i = 0; i < tableNum; i++) {
      MeasurementSchema measurementSchema =
          context.measurementSchemaDeserializer.deserialize(buffer, context);
      measurementSchemas.add(measurementSchema);
      columnTypes.add(ColumnType.values()[buffer.getInt()]);
    }
    return new TableSchema(null, measurementSchemas, columnTypes);
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
}
