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

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TsFileTableWriteBuilder implements WriteBuilder {

  private final LogicalWriteInfo info;
  private final Map<String, String> tableProperties;

  public TsFileTableWriteBuilder(LogicalWriteInfo info) {
    this(info, Collections.emptyMap());
  }

  public TsFileTableWriteBuilder(LogicalWriteInfo info, Map<String, String> tableProperties) {
    this.info = info;
    this.tableProperties = tableProperties;
  }

  @Override
  public Write build() {
    TsFileTableOptions options = TsFileTableOptions.forWrite(mergedOptions());
    TsFileTableWriteContext context = TsFileTableWriteContext.build(options, info.schema());
    return new TsFileTableWrite(context, info.queryId());
  }

  private CaseInsensitiveStringMap mergedOptions() {
    Map<String, String> merged = new HashMap<>(tableProperties);
    merged.putAll(info.options().asCaseSensitiveMap());
    return new CaseInsensitiveStringMap(merged);
  }
}
