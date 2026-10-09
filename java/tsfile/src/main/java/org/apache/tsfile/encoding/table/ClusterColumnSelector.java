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

import java.util.ArrayList;
import java.util.List;

/** Port of select_end_to_end_columns: Pearson, alpha=.5, global CV elbow, 64 rounds. */
public final class ClusterColumnSelector {
  private ClusterColumnSelector() {}

  public static int[] select(ClusterTable table) {
    int dim = table.columnCount();
    int n = Math.min(10000, table.rowCount());
    List<Integer> active = new ArrayList<>();
    for (int c = 0; c < dim; c++) active.add(c);
    if (dim == 1 || n == 0) return array(active);
    double[][] centered = new double[dim][n];
    double[] norm = new double[dim];
    for (int c = 0; c < dim; c++) {
      double mean = 0;
      boolean finite = true;
      for (int r = 0; r < n; r++) {
        double x = table.number(r, c);
        finite &= Double.isFinite(x);
        mean += x;
      }
      mean /= n;
      if (!finite || !Double.isFinite(mean)) continue;
      for (int r = 0; r < n; r++) {
        centered[c][r] = table.number(r, c) - mean;
        norm[c] += centered[c][r] * centered[c][r];
      }
      norm[c] = Math.sqrt(norm[c]);
    }
    double[][] corr = new double[dim][dim];
    for (int c = 0; c < dim; c++) {
      for (int j = c + 1; j < dim; j++) {
        double denominator = norm[c] * norm[j];
        if (denominator <= 1e-18 || !Double.isFinite(denominator)) continue;
        double dot = 0;
        for (int r = 0; r < n; r++) dot += centered[c][r] * centered[j][r];
        double v = Math.abs(dot / denominator);
        if (Double.isFinite(v)) corr[c][j] = corr[j][c] = v;
      }
    }
    int[] result = array(active);
    double previous = 0;
    double bestDrop = Double.NEGATIVE_INFINITY;
    boolean hasBest = false;
    for (int step = 0; step < 64 && active.size() > 1; step++) {
      double[] scores = new double[active.size()];
      double mean = 0;
      for (int i = 0; i < active.size(); i++) {
        for (int j : active) scores[i] += corr[active.get(i)][j];
        scores[i] /= active.size() - 1;
        mean += scores[i];
      }
      mean /= scores.length;
      double variance = 0;
      for (double score : scores) variance += (score - mean) * (score - mean);
      double sd = Math.sqrt(variance / scores.length);
      double cv = sd / Math.max(Math.abs(mean), 1e-18);
      if (step > 0 && previous - cv > bestDrop + 1e-15) {
        result = array(active);
        bestDrop = previous - cv;
        hasBest = true;
      }
      previous = cv;
      List<Integer> next = new ArrayList<>();
      int weakest = 0;
      for (int i = 0; i < scores.length; i++) {
        if (scores[i] < scores[weakest]) weakest = i;
        if (scores[i] >= mean - .5 * sd) next.add(active.get(i));
      }
      // This fallback is part of the retained Python/C++ implementation.
      if (next.size() == active.size()) next.remove(weakest);
      active = next;
    }
    return hasBest ? result : array(active);
  }

  private static int[] array(List<Integer> values) {
    int[] result = new int[values.size()];
    for (int i = 0; i < result.length; i++) result[i] = values.get(i);
    return result;
  }
}
