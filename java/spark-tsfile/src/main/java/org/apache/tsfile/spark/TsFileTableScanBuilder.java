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

package org.apache.tsfile.spark;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

public class TsFileTableScanBuilder
    implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

  private final TsFileTableOptions options;
  private final TsFileTableSchemaInferer.InferenceResult inferenceResult;
  private final TsFileTableFilterTranslator filterTranslator;
  private StructType readSchema;

  public TsFileTableScanBuilder(TsFileTableOptions options) {
    this.options = options;
    this.inferenceResult = TsFileTableSchemaInferer.infer(options);
    this.filterTranslator = new TsFileTableFilterTranslator(inferenceResult.tableSchema(), options);
    this.readSchema = inferenceResult.tableSchema().sparkSchema();
  }

  @Override
  public Filter[] pushFilters(Filter[] filters) {
    return filterTranslator.pushFilters(filters);
  }

  @Override
  public Filter[] pushedFilters() {
    return filterTranslator.pushedFilters();
  }

  @Override
  public void pruneColumns(StructType requiredSchema) {
    this.readSchema = requiredSchema;
  }

  @Override
  public Scan build() {
    TsFileTableReadContext context =
        new TsFileTableReadContext(
            options,
            inferenceResult.files(),
            inferenceResult.tableSchema(),
            readSchema,
            filterTranslator.startTime(),
            filterTranslator.endTime(),
            filterTranslator.tagEqualities());
    return new TsFileTableScan(context);
  }
}
