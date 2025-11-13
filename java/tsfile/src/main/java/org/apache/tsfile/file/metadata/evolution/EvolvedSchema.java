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

package org.apache.tsfile.file.metadata.evolution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.tsfile.file.metadata.TableSchema;

public class EvolvedSchema {
  // the evolved table names after applying all schema evolution operations
  private final Map<String, String> originalTableNames = new HashMap<>();
  /**
   * the first key is the evolved table name, the second key is the evolved column name,
   * and the value is the original column name before any schema evolution.
   */
  private final Map<String, Map<String, String>> originalColumnNames = new HashMap<>();

  public void renameTable(String oldTableName, String newTableName) {
    if (!originalTableNames.containsKey(oldTableName)) {
      originalTableNames.put(newTableName, oldTableName);
      // mark the old table name as non-exists
      originalTableNames.put(oldTableName, "");
    } else {
      // mark the old table name as non-exists
      String originalName = originalTableNames.put(oldTableName, "");
      originalTableNames.put(newTableName, originalName);
    }

    if (originalColumnNames.containsKey(oldTableName)) {
      Map<String, String> columnMap = originalColumnNames.remove(oldTableName);
      originalColumnNames.put(newTableName, columnMap);
    }
  }

  public void renameColumn(String tableName, String oldColumnName, String newColumnName) {
    Map<String, String> columnNameMap = originalColumnNames.computeIfAbsent(tableName,
        t -> new LinkedHashMap<>());
    if (!columnNameMap.containsKey(oldColumnName)) {
      columnNameMap.put(newColumnName, oldColumnName);
      // mark the old column name as non-exists
      columnNameMap.put(oldColumnName, "");
    } else {
      String originalName = columnNameMap.put(oldColumnName, "");
      columnNameMap.put(newColumnName, originalName);
    }
  }

  public String getOriginalTableName(String evolvedTableName) {
    return originalTableNames.getOrDefault(evolvedTableName, evolvedTableName);
  }

  public String getOriginalColumnName(String tableName, String evolvedColumnName) {
    Map<String, String> columnNameMap = originalColumnNames.get(tableName);
    if (columnNameMap == null) {
      return evolvedColumnName;
    }
    return columnNameMap.getOrDefault(evolvedColumnName, evolvedColumnName);
  }

  public Map<String, TableSchema> getEvolvedTableSchemaMap(Map<String, TableSchema> tableSchemaMap) {
    Map<String, TableSchema> evolvedTableSchemaMap = new HashMap<>(tableSchemaMap.size());
    Set<String> renamedTables = new HashSet<>(originalTableNames.values());
    // add renamed tables
    for (Entry<String, String> finalAndOriginalTableName : originalTableNames.entrySet()) {
      String finalName = finalAndOriginalTableName.getKey();
      String originalName = finalAndOriginalTableName.getValue();

      TableSchema tableSchema = tableSchemaMap.get(originalName);
      if (tableSchema != null) {
        tableSchema = new TableSchema(tableSchema);
        tableSchema.setTableName(finalName);
        evolvedTableSchemaMap.put(finalName, tableSchema);
      }
    }
    // add non-renamed tables
    tableSchemaMap.entrySet().stream().filter(e -> !renamedTables.contains(e.getKey())).forEach(e -> evolvedTableSchemaMap.put(e.getKey(), new TableSchema(e.getValue())));

    // rename columns
    for (Entry<String, Map<String, String>> tableColumnNameMap : originalColumnNames.entrySet()) {
      String tableName = tableColumnNameMap.getKey();
      Map<String, String> columnFinalAndOriginalNames = tableColumnNameMap.getValue();
      TableSchema tableSchema = evolvedTableSchemaMap.get(tableName);

      for (Entry<String, String> columnFinalAndOriginalName : columnFinalAndOriginalNames.entrySet()) {
        String columnFinalName = columnFinalAndOriginalName.getKey();
        String columnOriginalName = columnFinalAndOriginalName.getValue();
        tableSchema.renameColumn(columnOriginalName, columnFinalName);
      }
    }

    return evolvedTableSchemaMap;
  }
}
