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
package org.apache.tsfile.tools;

/**
 * Unified interface for reading source data from different formats (CSV, Parquet, Arrow).
 *
 * <p>Usage in schema mode: construct with an ImportSchema, then call readBatch() repeatedly.
 *
 * <p>Usage in auto mode: call inferSchema() first, then readBatch() repeatedly.
 */
public interface SourceReader extends AutoCloseable {

  /**
   * Infer schema from the source data (auto mode). Examines column names and types to produce an
   * ImportSchema where the time column is identified and all other columns become FIELD.
   *
   * @return inferred ImportSchema
   */
  ImportSchema inferSchema();

  /**
   * Read the next batch of data. Returns null when no more data is available.
   *
   * @return next batch, or null if exhausted
   */
  SourceBatch readBatch();

  @Override
  void close();
}
