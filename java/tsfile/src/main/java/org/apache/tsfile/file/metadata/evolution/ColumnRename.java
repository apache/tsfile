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

import java.util.Map;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.utils.Pair;

/**
 * A schema evolution operation that renames a column in a table schema.
 */
public class ColumnRename implements SchemaEvolution {

  static final String KEY_PREFIX = SchemaEvolution.KEY_PREFIX + "ColumnRename:";

  private final String tableName;
  private final String nameBefore;
  private final String nameAfter;

  public ColumnRename(String tableName, String nameBefore, String nameAfter) {
    this.tableName = tableName.toLowerCase();
    this.nameBefore = nameBefore.toLowerCase();
    this.nameAfter = nameAfter.toLowerCase();
  }

  @Override
  public void applyTo(TsFileMetadata metadata) {
    metadata.getEvolvedSchema(true).renameColumn(tableName, nameBefore, nameAfter);
  }

  /**
   * @return "SEV:ColumnRename:{tableNameLength},{nameBeforeLength},{tableName},{nameBefore}"
   */
  @Override
  public String propertyKey() {
    return KEY_PREFIX + tableName.length() + "," + nameBefore.length() + "," + tableName + "," + nameBefore;
  }

  /**
   * @return "{nameAfter}"
   */
  @Override
  public String propertyValue() {
    return nameAfter;
  }

  public static ColumnRename fromProperty(Pair<String, String> property) {
    String key = property.getLeft();
    String nameAfter = property.getRight();

    int i = KEY_PREFIX.length();
    int tableNameLengthStart = i;
    for (; i < key.length(); i++) {
      if (key.charAt(i) == ',') {
        break;
      }
    }
    int tableNameLength = Integer.parseInt(key.substring(tableNameLengthStart, i));

    // move past ','
    i ++;
    int nameBeforeLengthStart = i;
    for (; i < key.length(); i++) {
      if (key.charAt(i) == ',') {
        break;
      }
    }
    int nameBeforeLength = Integer.parseInt(key.substring(nameBeforeLengthStart, i));

    // move past ','
    i ++;
    String tableName = key.substring(i, i + tableNameLength);
    i += tableNameLength;

    // move past ','
    i ++;
    String nameBefore = key.substring(i, i + nameBeforeLength);

    return new ColumnRename(tableName, nameBefore, nameAfter);
  }
}
