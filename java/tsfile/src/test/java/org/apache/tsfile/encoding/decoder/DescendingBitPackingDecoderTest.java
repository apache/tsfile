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

import org.apache.tsfile.encoding.encoder.DescendingBitPackingEncoder;
import org.apache.tsfile.encoding.encoder.SeparateStorageEncoder;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class DescendingBitPackingDecoderTest {
  @Test
  public void test() throws Exception {
    long[] original =
        new long[] {
          0,
          -1,
          1,
          -2,
          2,
          0,
          0,
          -3,
          3,
          0,
          -4,
          0,
          0,
          0,
          4,
          -5,
          5,
          Long.MIN_VALUE,
          Long.MAX_VALUE,
          Long.MAX_VALUE - 1,
          Long.MIN_VALUE + 1,
          0,
          0
        };
    compressDecompressAndAssert(original);
    compressDecompressAndAssertSeparateStorage(original);
  }

  private static void compressDecompressAndAssert(long[] original) throws Exception {
    DescendingBitPackingEncoder encoder = new DescendingBitPackingEncoder();
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    for (long v : original) {
      encoder.encode(v, bout);
    }
    encoder.flush(bout);
    // Decode and verify
    DescendingBitPackingDecoder decoder = new DescendingBitPackingDecoder();
    ByteBuffer buffer = ByteBuffer.wrap(bout.toByteArray());

    int i = 0;
    while (decoder.hasNext(buffer)) {
      long actual = decoder.readLong(buffer);
      long expected = original[i];
      assertEquals("Mismatch at index " + i, expected, actual);
      i++;
    }
    assertEquals(original.length, i);
  }

  private static void compressDecompressAndAssertSeparateStorage(long[] original) throws Exception {
    SeparateStorageEncoder encoder = new SeparateStorageEncoder();
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    for (long v : original) {
      encoder.encode(v, bout);
    }
    encoder.flush(bout);
    // Decode and verify
    SeparateStorageDecoder decoder = new SeparateStorageDecoder();
    ByteBuffer buffer = ByteBuffer.wrap(bout.toByteArray());

    int i = 0;
    while (decoder.hasNext(buffer)) {
      long actual = decoder.readLong(buffer);
      long expected = original[i];
      assertEquals("Mismatch at index " + i, expected, actual);
      i++;
    }
    assertEquals(original.length, i);
  }
}
