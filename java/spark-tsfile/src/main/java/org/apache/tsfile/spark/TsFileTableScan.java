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

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

public class TsFileTableScan implements Scan, Batch {

  private final TsFileTableReadContext context;

  public TsFileTableScan(TsFileTableReadContext context) {
    this.context = context;
  }

  @Override
  public StructType readSchema() {
    return context.readSchema();
  }

  @Override
  public String description() {
    return "TsFile table scan: table=" + context.tableSchema().tableName();
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    InputPartition[] partitions = new InputPartition[context.files().size()];
    for (int i = 0; i < context.files().size(); i++) {
      partitions[i] = new TsFileTableInputPartition(context.files().get(i), context);
    }
    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new TsFileTablePartitionReaderFactory();
  }
}
