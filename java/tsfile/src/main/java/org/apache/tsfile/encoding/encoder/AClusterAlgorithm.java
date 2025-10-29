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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class AClusterAlgorithm {

  /** Private constructor to prevent instantiation of this utility class. */
  private AClusterAlgorithm() {}

  /**
   * The main entry point for the ACluster algorithm. It processes a page of data and returns the
   * results as an Object array.
   *
   * @param data The input time series data for a single page, represented as a long array.
   * @return An Object array where: <br>
   *     - index 0: long[] medoids (sorted by cluster frequency) <br>
   *     - index 1: int[] clusterAssignments (mapped to the sorted medoids) <br>
   *     - index 2: long[] clusterFrequencies (sorted)
   */
  public static Object[] run(long[] data) {
    int n = data.length;
    if (n == 0) {
      return new Object[] {new long[0], new int[0], new long[0]};
    }

    // --- Initialization ---
    List<Long> medoids = new ArrayList<>();
    Set<Long> existingMedoids = new HashSet<>();
    List<Set<Integer>> pointsInClusters = new ArrayList<>();
    int[] pointToMedoidMap = new int[n];

    // --- Step 1: Initialize with the first data point ---
    long firstPoint = data[0];
    medoids.add(firstPoint);
    existingMedoids.add(firstPoint);
    Set<Integer> firstCluster = new HashSet<>();
    firstCluster.add(0);
    pointsInClusters.add(firstCluster);
    pointToMedoidMap[0] = 0;

    // --- Step 2: Iteratively process the rest of the points ---
    for (int i = 1; i < n; i++) {
      long currentPoint = data[i];

      if (existingMedoids.contains(currentPoint)) {
        int medoidIndex = medoids.indexOf(currentPoint);
        pointsInClusters.get(medoidIndex).add(i);
        pointToMedoidMap[i] = medoidIndex;
        continue;
      }

      // --- Step 3: Find the best existing medoid ---
      int bestMedoidIndex = -1;
      long minCostToExistingMedoid = Long.MAX_VALUE;
      for (int j = 0; j < medoids.size(); j++) {
        long cost = calculateResidualCost(currentPoint, medoids.get(j));
        if (cost < minCostToExistingMedoid) {
          minCostToExistingMedoid = cost;
          bestMedoidIndex = j;
        }
      }

      // --- Step 4: Calculate potential savings ---
      long savingsFromCurrentPoint = minCostToExistingMedoid;
      long savingsFromReassignment = 0;
      Set<Integer> pointsInBestCluster = pointsInClusters.get(bestMedoidIndex);
      for (int pointIndexInCluster : pointsInBestCluster) {
        long p = data[pointIndexInCluster];
        long costToOldMedoid = calculateResidualCost(p, medoids.get(bestMedoidIndex));
        long costToNewPotentialMedoid = calculateResidualCost(p, currentPoint);
        if (costToNewPotentialMedoid < costToOldMedoid) {
          savingsFromReassignment += (costToOldMedoid - costToNewPotentialMedoid);
        }
      }
      long totalSavings = savingsFromCurrentPoint + savingsFromReassignment;

      // --- Step 5: Make the decision ---
      long storageCostForNewPoint = calculateBasePointStorageCost(currentPoint);
      if (totalSavings > storageCostForNewPoint) {
        // Decision: Create a new medoid.
        int newMedoidId = medoids.size();
        medoids.add(currentPoint);
        existingMedoids.add(currentPoint);
        Set<Integer> newCluster = new HashSet<>();
        newCluster.add(i);
        pointsInClusters.add(newCluster);
        pointToMedoidMap[i] = newMedoidId;

        Set<Integer> pointsToReEvaluate = new HashSet<>(pointsInClusters.get(bestMedoidIndex));
        for (int pointIndexToReEvaluate : pointsToReEvaluate) {
          long p = data[pointIndexToReEvaluate];
          if (calculateResidualCost(p, currentPoint)
              < calculateResidualCost(p, medoids.get(bestMedoidIndex))) {
            pointsInClusters.get(bestMedoidIndex).remove(pointIndexToReEvaluate);
            pointsInClusters.get(newMedoidId).add(pointIndexToReEvaluate);
            pointToMedoidMap[pointIndexToReEvaluate] = newMedoidId;
          }
        }
      } else {
        // Decision: Assign to the existing best medoid.
        pointsInClusters.get(bestMedoidIndex).add(i);
        pointToMedoidMap[i] = bestMedoidIndex;
      }
    }

    // --- Step 6: Finalize and sort the results ---
    int k = medoids.size();
    long[] finalMedoids = medoids.stream().mapToLong(l -> l).toArray();
    long[] rawClusterSizes = new long[k];
    for (int i = 0; i < k; i++) {
      rawClusterSizes[i] = pointsInClusters.get(i).size();
    }

    return sortResults(finalMedoids, pointToMedoidMap, rawClusterSizes);
  }

  /** Helper class for sorting medoids based on their cluster size (frequency). */
  private static class MedoidSortHelper implements Comparable<MedoidSortHelper> {
    long medoid;
    long size;
    int originalIndex;

    MedoidSortHelper(long medoid, long size, int originalIndex) {
      this.medoid = medoid;
      this.size = size;
      this.originalIndex = originalIndex;
    }

    @Override
    public int compareTo(MedoidSortHelper other) {
      return Long.compare(this.size, other.size);
    }
  }

  /**
   * Sorts the final medoids and their cluster information based on cluster frequency.
   *
   * @param medoids The discovered medoids.
   * @param clusterAssignment The assignment map for each data point.
   * @param clusterSize The frequency of each cluster.
   * @return A sorted and correctly mapped Object array.
   */
  private static Object[] sortResults(long[] medoids, int[] clusterAssignment, long[] clusterSize) {
    int k = medoids.length;
    List<MedoidSortHelper> sorters = new ArrayList<>();
    for (int i = 0; i < k; i++) {
      sorters.add(new MedoidSortHelper(medoids[i], clusterSize[i], i));
    }
    Collections.sort(sorters);

    long[] sortedMedoids = new long[k];
    long[] sortedClusterSize = new long[k];
    int[] oldToNewIndexMap = new int[k];

    for (int i = 0; i < k; i++) {
      MedoidSortHelper sortedItem = sorters.get(i);
      sortedMedoids[i] = sortedItem.medoid;
      sortedClusterSize[i] = sortedItem.size;
      oldToNewIndexMap[sortedItem.originalIndex] = i;
    }

    int[] sortedClusterAssignment = new int[clusterAssignment.length];
    for (int i = 0; i < clusterAssignment.length; i++) {
      int oldIndex = clusterAssignment[i];
      sortedClusterAssignment[i] = oldToNewIndexMap[oldIndex];
    }

    return new Object[] {sortedMedoids, sortedClusterAssignment, sortedClusterSize};
  }

  // --- Cost Calculation Functions ---

  private static long bitLengthCost(long value) {
    if (value == 0) return 1;
    return 64 - Long.numberOfLeadingZeros(value);
  }

  private static long calculateResidualCost(long p1, long p2) {
    return 1 + bitLengthCost(Math.abs(p1 - p2));
  }

  private static long calculateBasePointStorageCost(long basePoint) {
    return 1 + bitLengthCost(Math.abs(basePoint));
  }
}
