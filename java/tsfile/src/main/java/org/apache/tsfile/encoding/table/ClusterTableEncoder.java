/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.encoding.table;

import java.io.IOException;
import java.util.Arrays;

/** Fits selection once on up to 10,000 leading rows, then encodes independent blocks. */
public final class ClusterTableEncoder {
  private final ClusterTableOptions options;
  private String[] names;
  private org.apache.tsfile.enums.TSDataType[] types;
  private int[] selected;

  public ClusterTableEncoder(ClusterTableOptions options) {
    if (options == null) throw new IllegalArgumentException("Missing options");
    this.options = options;
  }

  /** Calling fit again checks the schema; it never silently refits a stream. */
  public void fit(ClusterTable sample) {
    if (names == null) {
      if (sample.rowCount() == 0) return;
      selected = ClusterColumnSelector.select(sample);
      names = sample.columnNames();
      types = sample.columnTypes();
    } else if (!Arrays.equals(names, sample.names) || !Arrays.equals(types, sample.types)) {
      throw new IllegalArgumentException("Schema changed within cluster table stream");
    }
  }

  public ClusterTableCodec.Encoded encode(ClusterTable page) throws IOException {
    fit(page);
    return ClusterTableCodec.encode(page, options, selected == null ? new int[0] : selected);
  }

  public int[] selectedColumns() {
    return selected == null ? new int[0] : selected.clone();
  }

  public static int pageSize(ClusterTable table) {
    return Math.min(ClusterTableCodec.MAX_ROWS, ClusterTableCodec.MAX_CELLS / table.columnCount());
  }
}
