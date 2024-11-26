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

package org.apache.tsfile.write.v4;

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.exception.write.ConflictDataTypeException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.WriteUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeviceTableModelWriter extends AbstractTableModelTsFileWriter {

  private String tableName;
  private boolean isTableWriteAligned = true;

  public DeviceTableModelWriter(File file, TableSchema tableSchema, long memoryThreshold)
      throws IOException {
    super(file, memoryThreshold);
    registerTableSchema(tableSchema);
  }

  /**
   * Write the tablet in to the TsFile with the table-view. The method will try to split the tablet
   * by device.
   *
   * @param table data to write
   * @throws IOException if the file cannot be written
   * @throws WriteProcessException if the schema is not registered first
   */
  @TsFileApi
  public void write(Tablet table) throws IOException, WriteProcessException {
    // make sure the ChunkGroupWriter for this Tablet exist and there is no type conflict
    checkIsTableExistAndSetColumnCategoryList(table);
    // spilt the tablet by deviceId
    List<Pair<IDeviceID, Integer>> deviceIdEndIndexPairs = WriteUtils.splitTabletByDevice(table);

    int startIndex = 0;
    for (Pair<IDeviceID, Integer> pair : deviceIdEndIndexPairs) {
      // get corresponding ChunkGroupWriter and write this Tablet
      recordCount +=
          tryToInitialGroupWriter(pair.left, isTableWriteAligned)
              .write(table, startIndex, pair.right);
      startIndex = pair.right;
    }
    checkMemorySizeAndMayFlushChunks();
  }

  private void checkIsTableExistAndSetColumnCategoryList(Tablet tablet)
      throws WriteProcessException {
    String tabletTableName = tablet.getTableName();
    if (tabletTableName != null && !this.tableName.equals(tabletTableName)) {
      throw new NoTableException(tabletTableName);
    }
    tablet.setTableName(this.tableName);
    final TableSchema tableSchema = getSchema().getTableSchemaMap().get(tableName);

    List<Tablet.ColumnCategory> columnCategoryListForTablet =
        new ArrayList<>(tablet.getSchemas().size());
    for (IMeasurementSchema writingColumnSchema : tablet.getSchemas()) {
      final int columnIndex = tableSchema.findColumnIndex(writingColumnSchema.getMeasurementName());
      if (columnIndex < 0) {
        throw new NoMeasurementException(writingColumnSchema.getMeasurementName());
      }
      final IMeasurementSchema registeredColumnSchema =
          tableSchema.getColumnSchemas().get(columnIndex);
      if (!writingColumnSchema.getType().equals(registeredColumnSchema.getType())) {
        throw new ConflictDataTypeException(
            writingColumnSchema.getType(), registeredColumnSchema.getType());
      }
      columnCategoryListForTablet.add(tableSchema.getColumnTypes().get(columnIndex));
    }
    tablet.setColumnCategories(columnCategoryListForTablet);
  }

  private void registerTableSchema(TableSchema tableSchema) {
    this.tableName = tableSchema.getTableName();
    getSchema().registerTableSchema(tableSchema);
  }
}
