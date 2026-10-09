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

import java.util.Arrays;
import java.util.BitSet;

/** Page-local raw numeric cells awaiting joint encoding by an aligned writer. */
public final class ClusterColumnBuffer {
  private long[] bits = new long[128];
  private final BitSet nulls = new BitSet();
  private int size;

  public void append(long valueBits, boolean isNull) {
    if (size >= ClusterNativePageCodec.MAX_PAGE_ROWS) {
      throw new IllegalArgumentException("Cluster native page exceeds row limit");
    }
    if (size == bits.length) bits = Arrays.copyOf(bits, bits.length * 2);
    bits[size] = valueBits;
    if (isNull) nulls.set(size);
    size++;
  }

  public int size() {
    return size;
  }

  public long bits(int row) {
    return bits[row];
  }

  public boolean isNull(int row) {
    return nulls.get(row);
  }

  public long memoryBytes() {
    return 8L * bits.length + nulls.size() / 8;
  }

  public void reset() {
    size = 0;
    nulls.clear();
  }
}
