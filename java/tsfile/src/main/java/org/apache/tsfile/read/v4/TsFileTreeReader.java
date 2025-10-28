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

import org.apache.tsfile.annotations.TreeModel;
import org.apache.tsfile.annotations.TsFileApi;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.IExpression;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.TreeResultSet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * TsFileTreeReader is an implementation of ITsFileTreeReader that provides query capability for
 * TsFile with a tree-style interface. It internally wraps {@link TsFileReader}.
 */
public class TsFileTreeReader implements ITsFileTreeReader {
  private final TsFileReader tsfileReader;

  /**
   * Construct a TsFileTreeReader with the given TsFile.
   *
   * @param file the TsFile to read
   * @throws IOException if an I/O error occurs during file opening
   */
  @TsFileApi
  @TreeModel
  public TsFileTreeReader(File file) throws IOException {
    tsfileReader = new TsFileReader(file);
  }

  /**
   * Execute a query on the given devices and measurements within the specified time range.
   *
   * @param deviceIds list of device IDs to query
   * @param measurementNames list of measurement names to query
   * @param startTime query start timestamp (inclusive)
   * @param endTime query end timestamp (inclusive)
   * @return a {@link ResultSet} containing the query results
   * @throws IOException if an I/O error occurs during query execution
   */
  @TsFileApi
  @TreeModel
  @Override
  public ResultSet query(
      List<String> deviceIds, List<String> measurementNames, long startTime, long endTime)
      throws IOException {
    List<Path> paths = new ArrayList<>();
    for (String deviceId : deviceIds) {
      for (String measurementName : measurementNames) {
        paths.add(new Path(deviceId, measurementName, true));
      }
    }
    IExpression expression =
        new GlobalTimeExpression(new TimeFilterOperators.TimeBetweenAnd(startTime, endTime));
    QueryExpression queryExpression = QueryExpression.create(paths, expression);
    QueryDataSet queryDataSet = tsfileReader.query(queryExpression);
    return new TreeResultSet(queryDataSet, deviceIds, measurementNames);
  }

  /**
   * Get all device IDs existing in the TsFile.
   *
   * @return list of all device IDs
   * @throws IOException if an I/O error occurs during metadata fetching
   */
  @TsFileApi
  @TreeModel
  @Override
  public List<String> getAllDeviceIds() throws IOException {
    return tsfileReader.getAllDeviceIds();
  }

  /**
   * Get the measurement schema of the given device.
   *
   * @param deviceId the target device ID
   * @return list of {@link MeasurementSchema} for the device
   * @throws IOException if an I/O error occurs during schema fetching
   */
  @TsFileApi
  @TreeModel
  @Override
  public List<MeasurementSchema> getDeviceSchema(String deviceId) throws IOException {
    return tsfileReader.getMeasurement(new StringArrayDeviceID(deviceId));
  }

  /**
   * Close the TsFileTreeReader and release resources.
   *
   * @throws IOException if an I/O error occurs during closing
   */
  @TsFileApi
  @TreeModel
  @Override
  public void close() throws IOException {
    tsfileReader.close();
  }
}
