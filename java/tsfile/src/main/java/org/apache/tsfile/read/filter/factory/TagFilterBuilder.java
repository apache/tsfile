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

package org.apache.tsfile.read.filter.factory;

import org.apache.tsfile.annotations.TsFileApi;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.TagFilterOperators.ValueEq;
import org.apache.tsfile.write.schema.IMeasurementSchema;

public class TagFilterBuilder {
  private TableSchema tableSchema;

  public TagFilterBuilder(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  @TsFileApi
  public Filter eq(String columnName, Object value) {
    int idColumnOrder = tableSchema.findIdColumnOrder(columnName);
    if (idColumnOrder == -1) {
      throw new IllegalArgumentException("Column '" + columnName + "' is not a tag column");
    }
    IMeasurementSchema columnSchema = tableSchema.findColumnSchema(columnName);

    // +1 for table name
    return new ValueEq(idColumnOrder + 1, (String) value);
  }
}
