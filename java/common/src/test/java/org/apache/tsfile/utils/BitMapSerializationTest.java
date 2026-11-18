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

package org.apache.tsfile.utils;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BitMapSerializationTest {

  @Test
  public void testSerializeToMatchesTruncatedBytes() {
    for (int size : new int[] {1, 7, 8, 9, 63, 64, 65, 128}) {
      BitMap bitMap = BitMap.createBitMapDynamically(size);
      bitMap.mark(0);
      if (size > 1) {
        bitMap.mark(size - 1);
      }

      byte[] bitmapBytes = bitMap.getTruncatedByteArray(size);
      ByteBuffer expected =
          ByteBuffer.allocate(Integer.BYTES + bitmapBytes.length).order(ByteOrder.LITTLE_ENDIAN);
      expected.putInt(bitmapBytes.length).put(bitmapBytes);

      ByteBuffer actual =
          ByteBuffer.allocateDirect(bitMap.serializedSize(size) + 2).order(ByteOrder.LITTLE_ENDIAN);
      actual.position(2);
      assertEquals(bitMap.serializedSize(size), bitMap.serializeTo(actual, size));
      assertEquals(2 + bitMap.serializedSize(size), actual.position());

      byte[] actualBytes = new byte[bitMap.serializedSize(size)];
      actual.position(2);
      actual.get(actualBytes);
      assertArrayEquals(expected.array(), actualBytes);
    }
  }
}
