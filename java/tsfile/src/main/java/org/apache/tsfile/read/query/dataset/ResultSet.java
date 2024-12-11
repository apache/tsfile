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

package org.apache.tsfile.read.query.dataset;

import org.apache.tsfile.annotations.TsFileApi;

import java.io.IOException;
import java.time.LocalDate;

public interface ResultSet extends AutoCloseable {

  @TsFileApi
  ResultSetMetadata getMetadata();

  @TsFileApi
  boolean next() throws IOException;

  @TsFileApi
  int getInt(String columnName);

  @TsFileApi
  int getInt(int columnIndex);

  @TsFileApi
  long getLong(String columnName);

  @TsFileApi
  long getLong(int columnIndex);

  @TsFileApi
  float getFloat(String columnName);

  @TsFileApi
  float getFloat(int columnIndex);

  @TsFileApi
  double getDouble(String columnName);

  @TsFileApi
  double getDouble(int columnIndex);

  @TsFileApi
  boolean getBoolean(String columnName);

  @TsFileApi
  boolean getBoolean(int columnIndex);

  @TsFileApi
  String getString(String columnName);

  @TsFileApi
  String getString(int columnIndex);

  @TsFileApi
  LocalDate getDate(String columnName);

  @TsFileApi
  LocalDate getDate(int columnIndex);

  @TsFileApi
  byte[] getBinary(String columnName);

  @TsFileApi
  byte[] getBinary(int columnIndex);

  @TsFileApi
  boolean isNull(String columnName);

  @TsFileApi
  boolean isNull(int columnIndex);

  @TsFileApi
  public abstract void close();
}
