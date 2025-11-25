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
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.utils.Pair;

/**
 * A schema evolution operation that renames a table in a schema map.
 */
public class TableRename implements SchemaEvolution{

  static final String KEY_PREFIX = SchemaEvolution.KEY_PREFIX + "TableRename:";

  private final String nameBefore;
  private final String nameAfter;

  public TableRename(String nameBefore, String nameAfter) {
    this.nameBefore = nameBefore.toLowerCase();
    this.nameAfter = nameAfter.toLowerCase();
  }

  @Override
  public void applyTo(TsFileMetadata tsFileMetadata) {
    MetadataIndexNode indexNode = tsFileMetadata.getTableMetadataIndexNodeMap().remove(nameBefore);
    if (indexNode != null) {
      tsFileMetadata.getTableMetadataIndexNodeMap().put(nameAfter, indexNode);
    }

    tsFileMetadata.getEvolvedSchema(true).renameTable(nameBefore, nameAfter);
  }

  /**
   * @return "SEV:TableRename:{nameBefore}"
   */
  @Override
  public String propertyKey() {
    return KEY_PREFIX + nameBefore;
  }

  /**
   * @return "{nameAfter}"
   */
  @Override
  public String propertyValue() {
    return nameAfter;
  }

  public static TableRename fromProperty(Pair<String, String> property) {
    String key = property.getLeft();
    String nameAfter = property.getRight();

    String nameBefore = key.substring(TableRename.KEY_PREFIX.length());

    return new TableRename(nameBefore, nameAfter);
  }
}
