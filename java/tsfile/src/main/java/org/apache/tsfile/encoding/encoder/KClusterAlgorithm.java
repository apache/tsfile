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

package org.apache.tsfile.encoding.encoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class KClusterAlgorithm {

  /** Private constructor to prevent instantiation. */
  private KClusterAlgorithm() {}

  public static ClusterResult run(long[] data, int k) {
    return run(data, k, 2, 0L);
  }

  public static ClusterResult run(long[] data, int k, int maxIterations, long seed) {
    ClusterResult.validateInput(data);
    if (k <= 0 || maxIterations <= 0) {
      throw new IllegalArgumentException("k and maxIterations must be positive");
    }
    if (data.length == 0) {
      return new ClusterResult(new long[0], new int[0], new long[0]);
    }
    return kMedoidLogCost(data, k, maxIterations, 0.01, new Random(seed));
  }

  /** Helper class for sorting medoids based on their cluster size (frequency). */
  private static class MedoidSortHelper implements Comparable<KClusterAlgorithm.MedoidSortHelper> {
    long medoid;
    long size;
    int originalIndex;

    MedoidSortHelper(long medoid, long size, int originalIndex) {
      this.medoid = medoid;
      this.size = size;
      this.originalIndex = originalIndex;
    }

    @Override
    public int compareTo(KClusterAlgorithm.MedoidSortHelper other) {
      return Long.compare(this.size, other.size);
    }
  }

  /** The core K-Medoids algorithm, specifically implemented for 1D data. */
  private static ClusterResult kMedoidLogCost(
      long[] data, int k, int maxIter, double tol, Random random) {
    int n = data.length;

    // Use a HashSet for efficient uniqueness checking on primitive longs.
    Set<Long> uniquePoints = new HashSet<>();
    for (long point : data) {
      uniquePoints.add(point);
      if (uniquePoints.size() > k) {
        break;
      }
    }
    int distinctCount = uniquePoints.size();
    if (distinctCount < k) {
      k = distinctCount;
    }

    // 1. Initialize medoids using a K-Medoids++ style approach.
    long[] medoids = acceleratedInitialization(data, k, random);
    int[] clusterAssignment = new int[n];
    long previousTotalCost = Long.MAX_VALUE;

    // 2. Main iterative loop (Build and Swap phases).
    for (int iteration = 0; iteration < maxIter; iteration++) {
      // --- Assignment Step ---
      long totalCostThisRound = assign(data, medoids, clusterAssignment);

      // --- Convergence Check (Cost) ---
      if (iteration > 0 && Math.abs(previousTotalCost - totalCostThisRound) < tol) {
        break;
      }
      previousTotalCost = totalCostThisRound;

      // --- Update Step ---
      long[] newMedoids = updateMedoids(data, clusterAssignment, medoids);

      // --- Convergence Check (Medoids) ---
      if (Arrays.equals(medoids, newMedoids)) {
        break;
      }
      medoids = newMedoids;
    }

    // 3. Calculate final cluster sizes and sort results.
    assign(data, medoids, clusterAssignment);
    long[] finalClusterSizes = new long[k];
    for (int assignment : clusterAssignment) {
      if (assignment != -1) finalClusterSizes[assignment]++;
    }
    return sortResults(medoids, clusterAssignment, finalClusterSizes);
  }

  /** K-Medoids++ style initialization for 1D data. */
  private static long[] acceleratedInitialization(long[] data, int k, Random rand) {
    long[] medoids = new long[k];
    Set<Long> selectedMedoids = new HashSet<>();

    int firstIndex = rand.nextInt(data.length);
    medoids[0] = data[firstIndex];
    selectedMedoids.add(medoids[0]);

    long[] distances = new long[data.length];
    for (int i = 0; i < data.length; i++) {
      distances[i] = Math.abs(data[i] - medoids[0]);
    }

    for (int i = 1; i < k; i++) {
      long[] prefixSums = new long[data.length];
      prefixSums[0] = distances[0];
      for (int p = 1; p < data.length; p++) {
        prefixSums[p] = prefixSums[p - 1] + distances[p];
      }
      long totalDistance = prefixSums[data.length - 1];
      if (totalDistance == 0) {
        int idx = rand.nextInt(data.length);
        while (selectedMedoids.contains(data[idx])) {
          idx = (idx + 1) % data.length;
        }
        medoids[i] = data[idx];
        selectedMedoids.add(medoids[i]);
        continue;
      }

      long randValue = (long) (rand.nextDouble() * totalDistance);
      int chosenIdx = binarySearch(prefixSums, randValue);

      while (selectedMedoids.contains(data[chosenIdx])) {
        chosenIdx = (chosenIdx + 1) % data.length;
      }
      medoids[i] = data[chosenIdx];
      selectedMedoids.add(medoids[i]);

      for (int idx = 0; idx < data.length; idx++) {
        long distNewMedoid = Math.abs(data[idx] - medoids[i]);
        if (distNewMedoid < distances[idx]) {
          distances[idx] = distNewMedoid;
        }
      }
    }
    return medoids;
  }

  /**
   * Updates medoids by finding the point within each cluster that minimizes the total intra-cluster
   * cost.
   */
  private static long[] updateMedoids(long[] data, int[] clusterAssignment, long[] medoids) {
    int k = medoids.length;
    long[] newMedoids = medoids.clone();
    List<Long>[] clusterPoints = new ArrayList[k];
    for (int i = 0; i < k; i++) {
      clusterPoints[i] = new ArrayList<>();
    }
    for (int i = 0; i < data.length; i++) {
      if (clusterAssignment[i] != -1) {
        clusterPoints[clusterAssignment[i]].add(data[i]);
      }
    }

    for (int m = 0; m < k; m++) {
      List<Long> members = clusterPoints[m];
      if (members.isEmpty()) continue;

      long minTotalClusterCost = Long.MAX_VALUE;
      long newMedoid = members.get(0);

      for (Long candidate : members) {
        long currentCandidateTotalCost = 0L;
        for (Long otherMember : members) {
          currentCandidateTotalCost += calculateResidualCost(candidate, otherMember);
        }
        if (currentCandidateTotalCost < minTotalClusterCost) {
          minTotalClusterCost = currentCandidateTotalCost;
          newMedoid = candidate;
        }
      }
      newMedoids[m] = newMedoid;
    }
    return newMedoids;
  }

  /**
   * Sorts the final medoids and their cluster information based on cluster frequency.
   *
   * @param medoids The discovered medoids.
   * @param clusterAssignment The assignment map for each data point.
   * @param clusterSize The frequency of each cluster.
   * @return Sorted references with matching assignments and counts.
   */
  private static ClusterResult sortResults(
      long[] medoids, int[] clusterAssignment, long[] clusterSize) {
    int k = medoids.length;
    List<KClusterAlgorithm.MedoidSortHelper> sorters = new ArrayList<>();
    for (int i = 0; i < k; i++) {
      sorters.add(new KClusterAlgorithm.MedoidSortHelper(medoids[i], clusterSize[i], i));
    }
    Collections.sort(sorters);

    long[] sortedMedoids = new long[k];
    long[] sortedClusterSize = new long[k];
    int[] oldToNewIndexMap = new int[k];

    for (int i = 0; i < k; i++) {
      KClusterAlgorithm.MedoidSortHelper sortedItem = sorters.get(i);
      sortedMedoids[i] = sortedItem.medoid;
      sortedClusterSize[i] = sortedItem.size;
      oldToNewIndexMap[sortedItem.originalIndex] = i;
    }

    int[] sortedClusterAssignment = new int[clusterAssignment.length];
    for (int i = 0; i < clusterAssignment.length; i++) {
      int oldIndex = clusterAssignment[i];
      sortedClusterAssignment[i] = oldToNewIndexMap[oldIndex];
    }

    return new ClusterResult(sortedMedoids, sortedClusterAssignment, sortedClusterSize);
  }

  // --- Cost Calculation Functions ---

  private static long assign(long[] data, long[] medoids, int[] assignments) {
    long total = 0;
    for (int i = 0; i < data.length; i++) {
      long bestCost = Long.MAX_VALUE;
      int best = 0;
      for (int m = 0; m < medoids.length; m++) {
        long cost = calculateResidualCost(data[i], medoids[m]);
        if (cost < bestCost || (cost == bestCost && data[i] == medoids[m])) {
          bestCost = cost;
          best = m;
        }
        if (data[i] == medoids[m]) {
          break;
        }
      }
      assignments[i] = best;
      total += bestCost;
    }
    return total;
  }

  private static long bitLengthCost(long value) {
    if (value == 0) return 1;
    return 64 - Long.numberOfLeadingZeros(value);
  }

  private static long calculateResidualCost(long p1, long p2) {
    return 1 + bitLengthCost(Math.abs(p1 - p2));
  }

  private static int binarySearch(long[] prefixSums, long value) {
    int low = 0;
    int high = prefixSums.length - 1;
    int ans = -1;
    while (low <= high) {
      int mid = low + (high - low) / 2;
      if (prefixSums[mid] > value) {
        ans = mid;
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    return ans == -1 ? prefixSums.length - 1 : ans;
  }
}
