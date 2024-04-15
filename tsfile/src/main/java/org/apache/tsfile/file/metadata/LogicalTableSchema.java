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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet.ColumnType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * TableSchema for devices with path-based DeviceIds. It generates the Id columns based on the max
 * level of paths.
 */
public class LogicalTableSchema extends TableSchema {

  private int maxLevel;

  public LogicalTableSchema(String tableName) {
    super(tableName);
  }

  @Override
  public void update(ChunkGroupMetadata chunkGroupMetadata) {
    super.update(chunkGroupMetadata);
    this.maxLevel = Math.max(this.maxLevel, chunkGroupMetadata.getDevice().segmentNum());
  }

  private List<IMeasurementSchema> generateIdColumns() {
    List<IMeasurementSchema> generatedIdColumns = new ArrayList<>();
    for (int i = 0; i < maxLevel; i++) {
      generatedIdColumns.add(
          new MeasurementSchema(
              "__level" + i, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
    }
    return generatedIdColumns;
  }

  /** Once called, the schema is no longer updatable. */
  @Override
  public List<IMeasurementSchema> getColumnSchemas() {
    if (!updatable) {
      return columnSchemas;
    }

    List<IMeasurementSchema> allColumns = new ArrayList<>(generateIdColumns());
    List<ColumnType> allColumnTypes = ColumnType.nCopy(ColumnType.ID, allColumns.size());
    allColumns.addAll(columnSchemas);
    allColumnTypes.addAll(columnTypes);
    columnSchemas = allColumns;
    updatable = false;
    return allColumns;
  }

  @Override
  public List<ColumnType> getColumnTypes() {
    // make sure the columns are finalized
    getColumnSchemas();
    return super.getColumnTypes();
  }
}
