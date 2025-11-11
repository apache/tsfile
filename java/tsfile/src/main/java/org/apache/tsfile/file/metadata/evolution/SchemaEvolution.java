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

import java.util.Map.Entry;
import org.apache.tsfile.file.metadata.TsFileMetadata;

/**
 * A schema evolution operation that can be applied to a TableSchemaMap.
 */
public interface SchemaEvolution {
  String KEY_PREFIX = "SEV:";

  /**
   * Apply this schema evolution operation to the given metadata.
   *
   * @param metadata the metadata to apply the operation to
   */
  void applyTo(TsFileMetadata metadata);

  /**
   * Get the property key representing this schema evolution operation.
   * All keys should start with the {@link #KEY_PREFIX}.
   *
   * @return the property key
   */
  String propertyKey();

  /**
   * Get the property value representing this schema evolution operation.
   *
   * @return the property value
   */
  String propertyValue();

  class Builder {
    /**
     * Create a SchemaEvolution instance from the given property entry.
     *
     * @param property the property entry
     * @return the SchemaEvolution instance, or null if the property key does not match any known
     *         schema evolution operation
     */
    public SchemaEvolution fromProperty(Entry<String, String> property) {
      String key = property.getKey();

      if (!key.startsWith(KEY_PREFIX)) {
        return null;
      }

      if (key.startsWith(TableRename.KEY_PREFIX)) {
        return TableRename.fromProperty(property);
      } else if (key.startsWith(ColumnRename.KEY_PREFIX)) {
        return ColumnRename.fromProperty(property);
      } else {
        return null;
      }
    }
  }
}
