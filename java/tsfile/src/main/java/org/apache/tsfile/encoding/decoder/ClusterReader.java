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

package org.apache.tsfile.encoding.decoder;

import java.nio.ByteBuffer;

public class ClusterReader {
  private final ByteBuffer buffer;
  private byte currentByte;
  private int bitPosition; // from 7 down to 0

  public ClusterReader(ByteBuffer buffer) {
    this.buffer = buffer;
    this.currentByte = 0;
    this.bitPosition = -1; // Start at -1 to force reading a new byte first
  }

  public long read(int numBits) {
    if (numBits > 64 || numBits <= 0) {
      throw new IllegalArgumentException(
          "Cannot read more than 64 bits or non-positive bits at once.");
    }

    long result = 0;
    for (int i = 0; i < numBits; i++) {
      if (bitPosition < 0) {
        currentByte = buffer.get();
        bitPosition = 7;
      }
      // Read the bit at the current position
      long bit = (currentByte >> bitPosition) & 1;
      // Shift the result and add the new bit
      result = (result << 1) | bit;
      bitPosition--;
    }
    return result;
  }
}
