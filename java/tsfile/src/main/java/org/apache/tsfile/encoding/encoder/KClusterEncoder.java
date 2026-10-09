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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

/** Fixed-reference-count encoding for an unordered, single-column table. */
public class KClusterEncoder extends ClusterEncoder {
  private final int k;
  private final int maxIterations;
  private final long seed;

  public KClusterEncoder(TSDataType dataType, int k) {
    this(dataType, k, 2, 0L);
  }

  public KClusterEncoder(TSDataType dataType, int k, int maxIterations, long seed) {
    super(TSEncoding.KCLUSTER, dataType);
    if (k <= 0 || maxIterations <= 0) {
      throw new IllegalArgumentException("k and maxIterations must be positive");
    }
    this.k = k;
    this.maxIterations = maxIterations;
    this.seed = seed;
  }

  @Override
  protected ClusterResult cluster(long[] data) {
    return KClusterAlgorithm.run(data, k, maxIterations, seed);
  }
}
