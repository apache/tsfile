/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.tools;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SupplementSequentialIdTest {

  @Test
  public void testConsecutiveTimestamps() {
    assertArrayEquals(new long[] {1, 2, 3}, HybridCsvTsFileAssembler.buildConsecutiveTimestamps(1, 3));
    assertArrayEquals(
        new long[] {4, 5}, HybridCsvTsFileAssembler.buildConsecutiveTimestamps(4, 2));
  }

  @Test
  public void testChainedIdsAcrossTwoFiles() {
    long nextId = 1;
    int file1Rows = 2;
    long[] t1 = HybridCsvTsFileAssembler.buildConsecutiveTimestamps(nextId, file1Rows);
    nextId += file1Rows;
    assertEquals(3, nextId);

    int file2Rows = 1;
    long[] t2 = HybridCsvTsFileAssembler.buildConsecutiveTimestamps(nextId, file2Rows);
    assertArrayEquals(new long[] {1, 2}, t1);
    assertArrayEquals(new long[] {3}, t2);
  }
}
