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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Multidimensional bit-cost medoids. All coordinates are checked, normalized nonnegative longs. */
final class MultidimensionalCluster {
  private MultidimensionalCluster() {}

  static final class Result {
    final long[][] references;
    final int[] counts;
    final int[] order;

    Result(long[][] references, int[] counts, int[] order) {
      this.references = references;
      this.counts = counts;
      this.order = order;
    }
  }

  private static final class Key {
    final long[] row;

    Key(long[] row) {
      this.row = row;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(row);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Key && Arrays.equals(row, ((Key) other).row);
    }
  }

  static long cost(long[] a, long[] b) {
    long sum = 0;
    for (int c = 0; c < a.length; c++) sum += 1 + TableBlockIO.width(Math.abs(a[c] - b[c]));
    return sum;
  }

  static Result run(long[][] data, ClusterTableOptions options) {
    List<long[]> refs = new ArrayList<>();
    int[] assignment = new int[data.length];
    if (options.method == ClusterTableOptions.Method.ACLUSTER) adaptive(data, refs, assignment);
    else fixed(data, refs, assignment, options);
    int[] counts = new int[refs.size()];
    for (int id : assignment) counts[id]++;
    List<Integer> ids = new ArrayList<>();
    for (int i = 0; i < counts.length; i++) if (counts[i] != 0) ids.add(i);
    ids.sort(Comparator.comparingInt((Integer id) -> counts[id]).thenComparingInt(id -> id));
    int[] offsets = new int[counts.length];
    int[] sortedCounts = new int[ids.size()];
    long[][] sortedRefs = new long[ids.size()][];
    int position = 0;
    for (int i = 0; i < ids.size(); i++) {
      int id = ids.get(i);
      offsets[id] = position;
      position += counts[id];
      sortedCounts[i] = counts[id];
      sortedRefs[i] = refs.get(id);
    }
    int[] order = new int[data.length];
    for (int r = 0; r < data.length; r++) order[offsets[assignment[r]]++] = r;
    return new Result(sortedRefs, sortedCounts, order);
  }

  private static int nearest(long[] point, List<long[]> refs) {
    int best = 0;
    long min = Long.MAX_VALUE;
    for (int i = 0; i < refs.size(); i++) {
      long score = cost(point, refs.get(i));
      if (score < min || (score == min && Arrays.equals(point, refs.get(i)))) {
        best = i;
        min = score;
      }
    }
    return best;
  }

  private static void adaptive(long[][] data, List<long[]> refs, int[] assignment) {
    refs.add(data[0]);
    List<List<Integer>> members = new ArrayList<>();
    members.add(new ArrayList<>());
    members.get(0).add(0);
    Map<Key, Integer> known = new HashMap<>();
    known.put(new Key(data[0]), 0);
    for (int r = 1; r < data.length; r++) {
      Integer duplicate = known.get(new Key(data[r]));
      if (duplicate != null) {
        assignment[r] = duplicate;
        members.get(duplicate).add(r);
        continue;
      }
      int best = nearest(data[r], refs);
      long gain = cost(data[r], refs.get(best)) - data[r].length;
      List<Integer> move = new ArrayList<>();
      for (int p : members.get(best)) {
        long saving = cost(data[p], refs.get(best)) - cost(data[p], data[r]);
        if (saving > 0) {
          gain += saving;
          move.add(p);
        }
      }
      long referenceCost = 0;
      for (long value : data[r]) referenceCost += 1 + TableBlockIO.width(value);
      if (gain > referenceCost) {
        int id = refs.size();
        refs.add(data[r]);
        known.put(new Key(data[r]), id);
        for (int p : move) assignment[p] = id;
        members.get(best).removeIf(p -> assignment[p] == id);
        move.add(r);
        members.add(move);
        assignment[r] = id;
      } else {
        assignment[r] = best;
        members.get(best).add(r);
      }
    }
  }

  private static double distance(long[] a, long[] b) {
    double sum = 0;
    for (int c = 0; c < a.length; c++) sum += Math.abs(a[c] - b[c]);
    return sum;
  }

  private static void fixed(
      long[][] data, List<long[]> refs, int[] assignment, ClusterTableOptions options) {
    Map<Key, long[]> unique = new LinkedHashMap<>();
    for (long[] row : data) unique.putIfAbsent(new Key(row), row);
    int k = Math.min(options.k, unique.size());
    Random random = new Random(options.seed);
    refs.add(data[random.nextInt(data.length)]);
    double[] nearestDistance = new double[data.length];
    Arrays.fill(nearestDistance, Double.POSITIVE_INFINITY);
    while (refs.size() < k) {
      double total = 0;
      long[] newest = refs.get(refs.size() - 1);
      for (int r = 0; r < data.length; r++) {
        nearestDistance[r] = Math.min(nearestDistance[r], distance(data[r], newest));
        total += nearestDistance[r];
      }
      double sample = random.nextDouble() * total;
      int chosen = 0;
      for (; chosen < data.length - 1; chosen++) {
        sample -= nearestDistance[chosen];
        if (sample < 0) break;
      }
      // Never select the same value twice, including random/rounding boundary cases.
      while (nearestDistance[chosen] == 0) chosen = (chosen + 1) % data.length;
      refs.add(data[chosen]);
    }
    for (int iteration = 0; iteration < options.maxIterations; iteration++) {
      List<Map<Key, Integer>> groups = new ArrayList<>();
      for (int id = 0; id < k; id++) groups.add(new LinkedHashMap<>());
      for (int r = 0; r < data.length; r++) {
        assignment[r] = nearest(data[r], refs);
        groups.get(assignment[r]).merge(new Key(data[r]), 1, Integer::sum);
      }
      boolean changed = false;
      for (int id = 0; id < k; id++) {
        Map<Key, Integer> group = groups.get(id);
        if (group.isEmpty()) continue;
        long bestCost = Long.MAX_VALUE;
        long[] best = refs.get(id);
        for (Key candidate : group.keySet()) {
          long score = 0;
          for (Map.Entry<Key, Integer> member : group.entrySet()) {
            score += cost(candidate.row, member.getKey().row) * member.getValue();
            if (score >= bestCost) break;
          }
          if (score < bestCost) {
            bestCost = score;
            best = candidate.row;
          }
        }
        changed |= !Arrays.equals(best, refs.get(id));
        refs.set(id, best);
      }
      if (!changed) break;
    }
    // The final update must be followed by assignment against the final medoids.
    for (int r = 0; r < data.length; r++) assignment[r] = nearest(data[r], refs);
  }
}
