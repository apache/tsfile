/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.file.metadata;

import java.util.HashMap;
import java.util.Map;
import org.apache.tsfile.file.metadata.evolution.SchemaEvolution;
import org.apache.tsfile.file.metadata.evolution.SchemaEvolution.Builder;

/**
 * A map from table names to their corresponding TableSchema objects.
 */
public class TableSchemaMap extends HashMap<String,TableSchema> {

  public TableSchemaMap() {
  }

  public TableSchemaMap(Map<String, TableSchema> tableSchemaMap) {
    this.putAll(tableSchemaMap);
  }

  /**
   * Update this TableSchemaMap according to the given TSFile properties.
   *
   * @param tsFileProperties the TSFile properties
   */
  public void update(Map<String, String> tsFileProperties) {
    Builder schemaEvolutionBuilder = new Builder();
    tsFileProperties.entrySet().forEach(entry -> {
      SchemaEvolution evolution = schemaEvolutionBuilder.fromProperty(entry);
      if (evolution != null) {
        evolution.applyTo(this);
      }
    });
  }
}
