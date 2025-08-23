package org.apache.tsfile.encoding.encoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

public class KClusterAlgorithm {

    private static final Random rand = new Random();

    /**
     * Private constructor to prevent instantiation.
     */
    private KClusterAlgorithm() {}

    public static Object[] run(long[] data, int k) {
        if (data == null || data.length == 0) {
            return new Object[] {new long[0], new int[0], new long[0]};
        }

        long[][] data2D = new long[data.length][1];
        for (int i = 0; i < data.length; i++) {
            data2D[i][0] = data[i];
        }

        return kMedoidLogCost(data, k, 2, 0.01);
    }

    /**
     * Helper class for sorting medoids based on their cluster size (frequency).
     */
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

    /**
     * The core K-Medoids algorithm, specifically implemented for 1D data.
     */
    private static Object[] kMedoidLogCost(long[] data, int k, int maxIter, double tol) {
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
            System.err.println("Warning: Distinct data points (" + distinctCount +
                    ") is less than input k (" + k + "), setting k to " + distinctCount);
            k = distinctCount;
        }

        if (k <= 0) {
            return new Object[]{new long[0], new int[n], new long[0]};
        }

        // 1. Initialize medoids using a K-Medoids++ style approach.
        long[] medoids = acceleratedInitialization(data, k);
        int[] clusterAssignment = new int[n];
        long previousTotalCost = Long.MAX_VALUE;

        // 2. Main iterative loop (Build and Swap phases).
        for (int iteration = 0; iteration < maxIter; iteration++) {
            // --- Assignment Step ---
            long totalCostThisRound = 0L;
            for (int i = 0; i < n; i++) {
                long minCost = Long.MAX_VALUE;
                int assignedMedoidIndex = -1;
                for (int m = 0; m < k; m++) {
                    long cost = calculateResidualCost(data[i], medoids[m]);
                    if (cost < minCost) {
                        minCost = cost;
                        assignedMedoidIndex = m;
                        if (minCost == 0) break; // Optimization: A perfect match is unbeatable.
                    }
                }
                clusterAssignment[i] = assignedMedoidIndex;
                totalCostThisRound += minCost;
            }

            // --- Convergence Check (Cost) ---
            if (iteration > 0 && (previousTotalCost - totalCostThisRound) < tol) {
                break;
            }
            previousTotalCost = totalCostThisRound;

            // --- Update Step ---
            long[] newMedoids = updateMedoids(data, clusterAssignment, k);

            // --- Convergence Check (Medoids) ---
            if (Arrays.equals(medoids, newMedoids)) {
                break;
            }
            medoids = newMedoids;
        }

        // 3. Calculate final cluster sizes and sort results.
        long[] finalClusterSizes = new long[k];
        for (int assignment : clusterAssignment) {
            if (assignment != -1) finalClusterSizes[assignment]++;
        }
        return sortResults(medoids, clusterAssignment, finalClusterSizes);
    }

    /**
     * K-Medoids++ style initialization for 1D data.
     */
    private static long[] acceleratedInitialization(long[] data, int k) {
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
     * Updates medoids by finding the point within each cluster that minimizes the total intra-cluster cost.
     */
    private static long[] updateMedoids(long[] data, int[] clusterAssignment, int k) {
        long[] newMedoids = new long[k];
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
     * @return A sorted and correctly mapped Object array.
     */
    private static Object[] sortResults(long[] medoids, int[] clusterAssignment, long[] clusterSize) {
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

        return new Object[]{sortedMedoids, sortedClusterAssignment, sortedClusterSize};
    }

    // --- Cost Calculation Functions ---

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
            if (prefixSums[mid] >= value) {
                ans = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return ans == -1 ? prefixSums.length - 1 : ans;
    }

}
