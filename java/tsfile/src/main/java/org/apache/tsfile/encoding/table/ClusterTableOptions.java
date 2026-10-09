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

/** Explicit, reproducible algorithm configuration; no dataset-specific branches. */
public final class ClusterTableOptions {
  public enum Method {
    ACLUSTER,
    KCLUSTER
  }

  public final Method method;
  public final int k;
  public final int maxIterations;
  public final long seed;

  public ClusterTableOptions(Method method, int k, int maxIterations, long seed) {
    if (method == null
        || k < 1
        || k > ClusterTableCodec.MAX_ROWS
        || maxIterations < 1
        || maxIterations > 100) {
      throw new IllegalArgumentException("Invalid cluster options");
    }
    this.method = method;
    this.k = k;
    this.maxIterations = maxIterations;
    this.seed = seed;
  }

  public static ClusterTableOptions aCluster() {
    return new ClusterTableOptions(Method.ACLUSTER, 100, 10, 0);
  }

  public static ClusterTableOptions kCluster() {
    return new ClusterTableOptions(Method.KCLUSTER, 100, 10, 0);
  }
}
