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

import org.apache.tsfile.enums.TSDataType;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Optional bounded real-data check; no benchmarks or modifications to the C++ repository. */
public final class ClusterTableDataCheck {
  private ClusterTableDataCheck() {}

  public static void main(String[] args) throws Exception {
    Path workspace = Paths.get(args[0]);
    List<String> manifest =
        Files.readAllLines(
            workspace.resolve("data/selected_columns_manifest.csv"), StandardCharsets.UTF_8);
    System.out.println(
        "dataset,method,selection_rows,tested_rows,selected_dim,clustered_dim,references,bytes,status");
    for (String dataset : Arrays.asList("CT", "Crop", "Gas", "BT", "Musk")) {
      String entry =
          manifest.stream().filter(line -> line.startsWith(dataset + ",")).findFirst().get();
      String[] fields = entry.split(",");
      int dimensions = Integer.parseInt(fields[7]);
      int[] expected = Arrays.stream(fields[9].split(";")).mapToInt(Integer::parseInt).toArray();
      List<Object[]> rows = new ArrayList<>();
      try (BufferedReader reader =
          Files.newBufferedReader(
              workspace.resolve("data/all/" + dataset + ".csv"), StandardCharsets.UTF_8)) {
        String line;
        while (rows.size() < 10000 && (line = reader.readLine()) != null) {
          String[] tokens = line.split(",");
          if (tokens.length != dimensions) throw new AssertionError("Unexpected CSV width");
          Object[] row = new Object[dimensions];
          for (int c = 0; c < dimensions; c++) row[c] = Double.parseDouble(tokens[c]);
          rows.add(row);
        }
      }
      long[] times = new long[rows.size()];
      for (int r = 0; r < times.length; r++) times[r] = r;
      String[] names = new String[dimensions];
      TSDataType[] types = new TSDataType[dimensions];
      for (int c = 0; c < dimensions; c++) {
        names[c] = "c" + c;
        types[c] = TSDataType.DOUBLE;
      }
      ClusterTable sample = new ClusterTable(times, names, types, rows.toArray(new Object[0][]));
      int[] selected = ClusterColumnSelector.select(sample);
      if (!Arrays.equals(expected, selected)) {
        throw new AssertionError(dataset + " selector differs: " + Arrays.toString(selected));
      }
      for (ClusterTableOptions.Method method : ClusterTableOptions.Method.values()) {
        ClusterTableEncoder encoder =
            new ClusterTableEncoder(new ClusterTableOptions(method, 100, 10, 0));
        encoder.fit(sample);
        long bytes = 0, refs = 0;
        int tested = 0, clustered = Integer.MAX_VALUE;
        for (int from : new int[] {0, 256, rows.size() - 127}) {
          int to = Math.min(rows.size(), from + 256);
          ClusterTable page = sample.slice(from, to);
          ClusterTableCodec.Encoded result = encoder.encode(page);
          ClusterTable restored = ClusterTableCodec.decode(result.bytes());
          if (!ClusterTableCodecTest.records(page)
              .equals(ClusterTableCodecTest.records(restored))) {
            throw new AssertionError(dataset + " complete-record round trip failed");
          }
          clustered = Math.min(clustered, result.clusteredColumns().length);
          bytes += result.bytes().length;
          refs += result.referenceCount;
          tested += page.rowCount();
        }
        System.out.println(
            dataset
                + ","
                + method
                + ","
                + rows.size()
                + ","
                + tested
                + ","
                + selected.length
                + ","
                + clustered
                + ","
                + refs
                + ","
                + bytes
                + ",PASS");
      }
    }
  }
}
