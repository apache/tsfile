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
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.List;

/** New tree-model read interface for TsFile. */
public interface ITsFileTreeReader extends AutoCloseable {

  /** Execute a query and return a ResultSet wrapper. */
  @TsFileApi
  @TreeModel
  ResultSet query(
      List<String> deviceIds, List<String> measurementNames, long startTime, long endTime)
      throws IOException;

  /** Return all device IDs found in the file. */
  @TsFileApi
  @TreeModel
  List<String> getAllDeviceIds() throws IOException;

  /** Return measurement schema list for a given device. */
  @TsFileApi
  @TreeModel
  List<MeasurementSchema> getDeviceSchema(String deviceId) throws IOException;

  /** Close underlying resources. */
  @TsFileApi
  @TreeModel
  @Override
  void close() throws IOException;
}
