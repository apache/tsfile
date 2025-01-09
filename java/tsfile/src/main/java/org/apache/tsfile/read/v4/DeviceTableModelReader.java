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

package org.apache.tsfile.read.v4;

import org.apache.tsfile.annotations.TsFileApi;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.TableResultSet;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DeviceTableModelReader implements ITsFileReader {

  protected TsFileSequenceReader fileReader;
  protected IMetadataQuerier metadataQuerier;
  protected IChunkLoader chunkLoader;
  protected TableQueryExecutor queryExecutor;
  private static final Logger LOG = LoggerFactory.getLogger(DeviceTableModelReader.class);

  public DeviceTableModelReader(File file) throws IOException {
    this.fileReader = new TsFileSequenceReader(file.getPath());
    this.fileReader.setEnableCacheTableSchemaMap();
    this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
    this.chunkLoader = new CachedChunkLoaderImpl(fileReader);
    this.queryExecutor =
        new TableQueryExecutor(
            metadataQuerier, chunkLoader, TableQueryExecutor.TableQueryOrdering.DEVICE);
  }

  @TsFileApi
  public List<TableSchema> getAllTableSchema() throws IOException {
    Map<String, TableSchema> tableSchemaMap = fileReader.getTableSchemaMap();
    return new ArrayList<>(tableSchemaMap.values());
  }

  @TsFileApi
  public Optional<TableSchema> getTableSchemas(String tableName) throws IOException {
    Map<String, TableSchema> tableSchemaMap = fileReader.getTableSchemaMap();
    return Optional.ofNullable(tableSchemaMap.get(tableName.toLowerCase()));
  }

  @TsFileApi
  public ResultSet query(String tableName, List<String> columnNames, long startTime, long endTime)
      throws IOException, NoTableException, NoMeasurementException, ReadProcessException {
    String lowerCaseTableName = tableName.toLowerCase();
    TableSchema tableSchema = fileReader.getTableSchemaMap().get(lowerCaseTableName);
    if (tableSchema == null) {
      throw new NoTableException(tableName);
    }
    List<TSDataType> dataTypeList = new ArrayList<>(columnNames.size());
    List<String> lowerCaseColumnNames = new ArrayList<>(columnNames.size());
    for (String columnName : columnNames) {
      Map<String, Integer> column2IndexMap = tableSchema.buildColumnPosIndex();
      Integer columnIndex = column2IndexMap.get(columnName.toLowerCase());
      if (columnIndex == null) {
        throw new NoMeasurementException(columnName);
      }
      lowerCaseColumnNames.add(columnName.toLowerCase());
      dataTypeList.add(tableSchema.getColumnSchemas().get(columnIndex).getType());
    }
    TsBlockReader tsBlockReader =
        queryExecutor.query(
            lowerCaseTableName,
            lowerCaseColumnNames,
            new ExpressionTree.TimeBetweenAnd(startTime, endTime),
            null,
            null);
    return new TableResultSet(tsBlockReader, columnNames, dataTypeList);
  }

  @Override
  public void close() {
    try {
      this.fileReader.close();
    } catch (IOException e) {
      LOG.warn("Meet exception when close file reader: ", e);
    }
  }
}
