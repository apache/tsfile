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

import java.util.Arrays;

/** References, original-input assignments and frequency-sorted cluster sizes. */
public final class ClusterResult {
  public final long[] references;
  public final int[] assignments;
  public final long[] counts;

  public ClusterResult(long[] references, int[] assignments, long[] counts) {
    this.references = references;
    this.assignments = assignments;
    this.counts = counts;
  }

  public void validate(int pointCount) {
    if (references.length == 0
        || references.length > pointCount
        || counts.length != references.length
        || assignments.length != pointCount) {
      throw new IllegalArgumentException("Invalid cluster result dimensions");
    }
    long[] actualCounts = new long[counts.length];
    for (int id : assignments) {
      if (id < 0 || id >= counts.length) {
        throw new IllegalArgumentException("Invalid cluster assignment");
      }
      actualCounts[id]++;
    }
    for (int i = 0; i < counts.length; i++) {
      if (references[i] < 0 || counts[i] < 0 || (i > 0 && counts[i] < counts[i - 1])) {
        throw new IllegalArgumentException("Invalid references or unsorted cluster counts");
      }
    }
    if (!Arrays.equals(counts, actualCounts)) {
      throw new IllegalArgumentException("Cluster counts do not match assignments");
    }
  }

  static void validateInput(long[] data) {
    if (data == null) {
      throw new IllegalArgumentException("Cluster input cannot be null");
    }
    long limit = Long.MAX_VALUE / (2L * Math.max(1, data.length));
    for (long value : data) {
      if (value < 0 || value > limit) {
        throw new IllegalArgumentException("Cluster input exceeds safe normalized range");
      }
    }
  }
}
