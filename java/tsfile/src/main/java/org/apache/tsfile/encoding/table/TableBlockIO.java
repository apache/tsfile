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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Bounds-checked, byte-aligned unsigned bit packing, including widths zero and 64. */
final class TableBlockIO {
  private TableBlockIO() {}

  static int width(long value) {
    return 64 - Long.numberOfLeadingZeros(value);
  }

  static void pack(DataOutputStream out, long[] values, int start, int end, int width)
      throws IOException {
    int current = 0;
    int used = 0;
    for (int i = start; i < end; i++) {
      for (int b = width - 1; b >= 0; b--) {
        current = (current << 1) | (int) ((values[i] >>> b) & 1);
        if (++used == 8) {
          out.writeByte(current);
          current = 0;
          used = 0;
        }
      }
    }
    if (used != 0) out.writeByte(current << (8 - used));
  }

  static long[] unpack(DataInputStream in, int count, int width) throws IOException {
    if (width < 0
        || width > 64
        || count < 0
        || count > ClusterTableCodec.MAX_ROWS
        || ((long) count * width + 7) / 8 > in.available()) {
      throw new IOException("Invalid packed column");
    }
    long[] result = new long[count];
    int current = 0;
    int left = 0;
    for (int i = 0; i < count; i++) {
      for (int b = 0; b < width; b++) {
        if (left == 0) {
          current = in.readUnsignedByte();
          left = 8;
        }
        result[i] = (result[i] << 1) | ((current >>> --left) & 1);
      }
    }
    if (left != 0 && (current & ((1 << left) - 1)) != 0) {
      throw new IOException("Nonzero bit padding");
    }
    return result;
  }

  static byte[] packs(long[] values) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    for (int start = 0; start < values.length; start += 10) {
      int end = Math.min(values.length, start + 10);
      long mask = 0;
      for (int i = start; i < end; i++) mask |= values[i];
      int width = width(mask);
      out.writeByte(width);
      pack(out, values, start, end, width);
    }
    return bytes.toByteArray();
  }

  static long[] readPacks(DataInputStream in, int count) throws IOException {
    long[] result = new long[count];
    for (int start = 0; start < count; start += 10) {
      int n = Math.min(10, count - start);
      long[] block = unpack(in, n, in.readUnsignedByte());
      System.arraycopy(block, 0, result, start, n);
    }
    return result;
  }

  static void payload(DataOutputStream out, byte[] bytes) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  static DataInputStream payload(DataInputStream in) throws IOException {
    int size = in.readInt();
    if (size < 0 || size > in.available()) throw new IOException("Invalid payload length");
    byte[] bytes = new byte[size];
    in.readFully(bytes);
    return new DataInputStream(new ByteArrayInputStream(bytes));
  }

  static void exhausted(DataInputStream in) throws IOException {
    if (in.available() != 0) throw new IOException("Trailing data in block");
  }
}
