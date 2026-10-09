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

package org.apache.tsfile.write.page;

import org.apache.tsfile.encoding.table.ClusterColumnBuffer;
import org.apache.tsfile.encoding.table.ClusterNativePageCodec;
import org.apache.tsfile.encoding.table.ClusterTableOptions;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.chunk.ValueChunkWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** Coordinates page-local joint encoding before any of the aligned column pages are sealed. */
public final class ClusterAlignedPageWriter {
  private final Map<TSEncoding, ClusterNativePageCodec.Selection> selections =
      new EnumMap<>(TSEncoding.class);

  public static void attach(TimePageWriter time, Collection<ValueChunkWriter> writers) {
    for (ValueChunkWriter writer : writers) {
      if (writer.getPageWriter().getClusterBuffer() != null) {
        time.enableClusterTimestamps();
        return;
      }
    }
  }

  public void prepare(TimePageWriter time, Collection<ValueChunkWriter> writers) {
    if (time == null || time.getPointNumber() == 0) return;
    for (TSEncoding method : new TSEncoding[] {TSEncoding.ACLUSTER, TSEncoding.KCLUSTER}) {
      List<ValueChunkWriter> group = new ArrayList<>();
      for (ValueChunkWriter writer : writers) {
        ValuePageWriter page = writer.getPageWriter();
        if (writer.getEncodingType() == method && page != null && page.getClusterBuffer() != null)
          group.add(writer);
      }
      if (group.isEmpty() || group.get(0).getPageWriter().hasClusterPage()) continue;
      String[] names = new String[group.size()];
      TSDataType[] types = new TSDataType[group.size()];
      ClusterColumnBuffer[] columns = new ClusterColumnBuffer[group.size()];
      for (int c = 0; c < group.size(); c++) {
        names[c] = group.get(c).getMeasurementId();
        types[c] = group.get(c).getDataType();
        columns[c] = group.get(c).getPageWriter().getClusterBuffer();
      }
      ClusterNativePageCodec.Selection selection =
          selections.computeIfAbsent(method, ignored -> new ClusterNativePageCodec.Selection());
      try {
        byte[][] pages =
            ClusterNativePageCodec.encode(
                time.getClusterTimestamps(),
                names,
                types,
                columns,
                method == TSEncoding.ACLUSTER
                    ? ClusterTableOptions.aCluster()
                    : ClusterTableOptions.kCluster(),
                selection);
        for (int c = 0; c < group.size(); c++)
          group.get(c).getPageWriter().setClusterPage(pages[c]);
      } catch (IOException failure) {
        // Never fall back to independently reordered scalar streams or silently drop a page.
        throw new UncheckedIOException("Cannot encode joint aligned cluster page", failure);
      }
    }
  }
}
