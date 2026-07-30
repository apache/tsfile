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

import org.apache.tsfile.i18n.Messages;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileTableCatalog implements TableCatalog {

  private final Map<String, StoredTable> tables = new HashMap<>();
  private String name;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    List<Identifier> identifiers = new ArrayList<>();
    for (StoredTable table : tables.values()) {
      if (sameNamespace(namespace, table.identifier.namespace())) {
        identifiers.add(table.identifier);
      }
    }
    return identifiers.toArray(new Identifier[0]);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    StoredTable storedTable = tables.get(key(ident));
    if (storedTable == null) {
      throw new NoSuchTableException(ident);
    }
    return storedTable.table();
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    String key = key(ident);
    if (tables.containsKey(key)) {
      throw new TableAlreadyExistsException(ident);
    }
    Map<String, String> connectorOptions = connectorOptions(properties);
    StoredTable storedTable = new StoredTable(ident, schema, connectorOptions);
    tables.put(key, storedTable);
    return storedTable.table();
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException(Messages.get("error.spark.catalog_alter_unsupported"));
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return tables.remove(key(ident)) != null;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    StoredTable storedTable = tables.remove(key(oldIdent));
    if (storedTable == null) {
      throw new NoSuchTableException(oldIdent);
    }
    String newKey = key(newIdent);
    if (tables.containsKey(newKey)) {
      tables.put(key(oldIdent), storedTable);
      throw new TableAlreadyExistsException(newIdent);
    }
    tables.put(newKey, storedTable.withIdentifier(newIdent));
  }

  private static Map<String, String> connectorOptions(Map<String, String> properties) {
    Map<String, String> options = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(TableCatalog.OPTION_PREFIX)) {
        key = key.substring(TableCatalog.OPTION_PREFIX.length());
      }
      if (TsFileTableOptions.isKnownOption(key)) {
        options.put(key, entry.getValue());
      }
    }
    return options;
  }

  private static String key(Identifier ident) {
    return String.join("\u0001", ident.namespace()) + "\u0002" + ident.name();
  }

  private static boolean sameNamespace(String[] left, String[] right) {
    if (left.length != right.length) {
      return false;
    }
    for (int i = 0; i < left.length; i++) {
      if (!left[i].equals(right[i])) {
        return false;
      }
    }
    return true;
  }

  private static class StoredTable {
    private final Identifier identifier;
    private final StructType schema;
    private final Map<String, String> properties;

    private StoredTable(Identifier identifier, StructType schema, Map<String, String> properties) {
      this.identifier = identifier;
      this.schema = schema;
      this.properties = properties;
    }

    private StoredTable withIdentifier(Identifier identifier) {
      return new StoredTable(identifier, schema, properties);
    }

    private Table table() {
      return new TsFileTable(schema, properties);
    }
  }
}
