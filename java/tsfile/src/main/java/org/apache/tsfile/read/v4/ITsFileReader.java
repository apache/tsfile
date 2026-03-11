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
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.query.dataset.ResultSet;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface ITsFileReader extends AutoCloseable {

  @TsFileApi
  ResultSet query(String tableName, List<String> columnNames, long startTime, long endTime)
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException;

  @TsFileApi
  ResultSet query(
      String tableName, List<String> columnNames, long startTime, long endTime, Filter tagFilter)
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException;

  @TsFileApi
  Optional<TableSchema> getTableSchemas(String tableName) throws IOException;

  @TsFileApi
  List<TableSchema> getAllTableSchema() throws IOException;

  /**
   * Query table model data by row range.
   *
   * <p>Internally queries the full time range and applies offset/limit at the result-set level.
   * Once {@code limit} rows are returned, no further data is loaded from storage.
   *
   * @param tableName target table name
   * @param columnNames list of column names to query
   * @param offset number of leading rows to skip (&gt;= 0)
   * @param limit maximum number of rows to return; &lt; 0 means unlimited
   * @return a {@link ResultSet} containing the query results
   * @throws ReadProcessException if a read processing error occurs
   * @throws IOException if an I/O error occurs
   * @throws NoTableException if the table does not exist
   * @throws NoMeasurementException if a column does not exist
   */
  @TsFileApi
  ResultSet queryByRow(String tableName, List<String> columnNames, int offset, int limit)
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException;

  @TsFileApi
  void close();
}
