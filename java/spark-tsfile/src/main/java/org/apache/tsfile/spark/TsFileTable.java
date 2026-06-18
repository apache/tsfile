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

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TsFileTable implements SupportsRead, SupportsWrite {

  private static final Set<TableCapability> CAPABILITIES;

  static {
    Set<TableCapability> capabilities = new HashSet<>();
    capabilities.add(TableCapability.BATCH_READ);
    capabilities.add(TableCapability.BATCH_WRITE);
    capabilities.add(TableCapability.ACCEPT_ANY_SCHEMA);
    CAPABILITIES = Collections.unmodifiableSet(capabilities);
  }

  private final StructType schema;
  private final Map<String, String> properties;

  public TsFileTable(StructType schema, Map<String, String> properties) {
    this.schema = schema;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
  }

  @Override
  public String name() {
    String path = properties.get("path");
    return path == null ? "tsfile" : "tsfile:" + path;
  }

  @Override
  public StructType schema() {
    return schema;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new TsFileTableScanBuilder(TsFileTableOptions.forRead(mergedOptions(options)), schema);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new TsFileTableWriteBuilder(info, properties);
  }

  private CaseInsensitiveStringMap mergedOptions(CaseInsensitiveStringMap options) {
    Map<String, String> merged = new HashMap<>(properties);
    merged.putAll(options.asCaseSensitiveMap());
    return new CaseInsensitiveStringMap(merged);
  }
}
