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
package org.apache.tsfile.tools;

import java.util.ArrayList;
import java.util.List;

/** Utilities for preparing {@link ImportSchema} instances used in hybrid CSV import. */
public final class ImportSchemaUtils {

  public static final String DEFAULT_BATCH_ID_TAG = "batch_id";

  private ImportSchemaUtils() {}

  /**
   * Returns a copy of {@code base} with a virtual tag column for batch identification. The tag is
   * filled on every row via the default value (see {@link TabletBuilder}).
   */
  public static ImportSchema withBatchIdTag(
      ImportSchema base, String batchIdTagName, String batchIdValue) {
    ImportSchema copy = copyOf(base);
    List<ImportSchema.TagColumn> tags = new ArrayList<>(copy.getTagColumns());
    boolean replaced = false;
    for (int i = 0; i < tags.size(); i++) {
      if (tags.get(i).getName().equals(batchIdTagName)) {
        tags.set(i, new ImportSchema.TagColumn(batchIdTagName, batchIdValue));
        replaced = true;
        break;
      }
    }
    if (!replaced) {
      tags.add(new ImportSchema.TagColumn(batchIdTagName, batchIdValue));
    }
    copy.setTagColumns(tags);
    return copy;
  }

  /** Source columns present in supplement CSV files (time column excluded). */
  public static List<ImportSchema.SourceColumn> supplementSourceColumns(ImportSchema schema) {
    List<ImportSchema.SourceColumn> result = new ArrayList<>();
    String timeName = schema.getTimeColumnName();
    for (ImportSchema.SourceColumn col : schema.getSourceColumns()) {
      if (col.isSkip()) {
        continue;
      }
      if (col.getName().equals(timeName)) {
        continue;
      }
      result.add(col);
    }
    return result;
  }

  public static ImportSchema copyOf(ImportSchema base) {
    ImportSchema copy = new ImportSchema();
    copy.setTableName(base.getTableName());
    copy.setTimePrecision(base.getTimePrecision());
    copy.setHasHeader(base.isHasHeader());
    copy.setSeparator(base.getSeparator());
    copy.setNullFormat(base.getNullFormat());
    copy.setTimeColumnName(base.getTimeColumnName());
    copy.setTagColumns(new ArrayList<>(base.getTagColumns()));
    copy.setSourceColumns(new ArrayList<>(base.getSourceColumns()));
    return copy;
  }
}
