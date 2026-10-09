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

import org.apache.tsfile.enums.TSDataType;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Java port of the repository ALP path: exponent/factor search, integer FOR, exceptions, 1024-value
 * vectors and RAW fallback. A versioned table-block format, not upstream ALP bytes.
 */
final class AlpColumnCodec {
  private static final int VECTOR = 1024;
  private static final double[] POW = new double[19];
  private static final double[] INV = new double[19];

  static {
    for (int i = 0; i < POW.length; i++) {
      POW[i] = Double.parseDouble("1e" + i);
      INV[i] = Double.parseDouble("1e-" + i);
    }
  }

  private AlpColumnCodec() {}

  static byte[] encode(long[] raw, TSDataType type) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    for (int offset = 0; offset < raw.length; offset += VECTOR) {
      int n = Math.min(VECTOR, raw.length - offset);
      int wordBytes = type == TSDataType.INT32 || type == TSDataType.FLOAT ? 4 : 8;
      long bestBytes = 1L + n * wordBytes;
      int bestE = -1, bestF = 0, bestWidth = 0;
      long bestBase = 0;
      long[] integers = new long[n];
      boolean[] exact = new boolean[n];
      for (int e = 0; e <= 18; e++) {
        for (int f = 0; f <= e; f++) {
          int exceptions = 0;
          long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
          for (int i = 0; i < n; i++) {
            exact[i] = convert(raw[offset + i], type, e, f, integers, i);
            if (!exact[i]) exceptions++;
            else {
              min = Math.min(min, integers[i]);
              max = Math.max(max, integers[i]);
            }
          }
          if (exceptions == n) continue;
          int width = TableBlockIO.width(max - min); // unsigned span, possibly 64 bits
          long size = 14L + exceptions * (2L + wordBytes) + ((long) n * width + 7) / 8;
          if (size < bestBytes) {
            bestBytes = size;
            bestE = e;
            bestF = f;
            bestBase = min;
            bestWidth = width;
          }
        }
      }
      if (bestE < 0) {
        out.writeByte(0);
        for (int i = 0; i < n; i++) writeRaw(out, raw[offset + i], type);
        continue;
      }
      out.writeByte(1);
      out.writeByte(bestE);
      out.writeByte(bestF);
      out.writeByte(bestWidth);
      out.writeLong(bestBase);
      int exceptions = 0;
      for (int i = 0; i < n; i++) {
        exact[i] = convert(raw[offset + i], type, bestE, bestF, integers, i);
        if (!exact[i]) exceptions++;
      }
      out.writeShort(exceptions);
      for (int i = 0; i < n; i++) {
        if (!exact[i]) {
          out.writeShort(i);
          writeRaw(out, raw[offset + i], type);
          integers[i] = bestBase;
        }
        integers[i] -= bestBase;
      }
      TableBlockIO.pack(out, integers, 0, n, bestWidth);
    }
    return bytes.toByteArray();
  }

  private static boolean convert(long raw, TSDataType type, int e, int f, long[] ints, int i) {
    // INT64 values outside binary64's exact-integer range are always literal exceptions.
    if (type == TSDataType.INT64 && (raw < -(1L << 53) || raw > (1L << 53))) return false;
    double value = ClusterTable.numeric(raw, type);
    double scaled = value * POW[e] * INV[f];
    if (!Double.isFinite(scaled) || scaled <= -0x1p63 || scaled >= 0x1p63) return false;
    // llround-style ties away from zero, without adding .5 to large integral doubles.
    double absolute = Math.abs(scaled);
    double floor = Math.floor(absolute);
    long integer = (long) (floor + (absolute - floor >= .5 ? 1 : 0));
    if (scaled < 0) integer = -integer;
    double restored = (double) integer * POW[f] * INV[e];
    if (type == TSDataType.INT64
        && (restored < -0x1p53 || restored > 0x1p53 || restored != Math.rint(restored))) {
      return false;
    }
    if (type == TSDataType.INT32
        && (restored < Integer.MIN_VALUE
            || restored > Integer.MAX_VALUE
            || restored != Math.rint(restored))) return false;
    if (ClusterTable.raw(restored, type) != raw) return false;
    ints[i] = integer;
    return true;
  }

  static long[] decode(DataInputStream in, int rows, TSDataType type) throws IOException {
    long[] result = new long[rows];
    for (int offset = 0; offset < rows; offset += VECTOR) {
      int n = Math.min(VECTOR, rows - offset);
      int mode = in.readUnsignedByte();
      if (mode == 0) {
        for (int i = 0; i < n; i++) result[offset + i] = readRaw(in, type);
      } else if (mode == 1) {
        int e = in.readUnsignedByte(), f = in.readUnsignedByte(), width = in.readUnsignedByte();
        if (e > 18 || f > e || width > 64) throw new IOException("Invalid ALP parameters");
        long base = in.readLong();
        int count = in.readUnsignedShort();
        if (count > n) throw new IOException("Invalid ALP exception count");
        int[] positions = new int[count];
        long[] exceptions = new long[count];
        for (int i = 0; i < count; i++) {
          positions[i] = in.readUnsignedShort();
          if (positions[i] >= n || (i > 0 && positions[i] <= positions[i - 1])) {
            throw new IOException("Invalid ALP exception positions");
          }
          exceptions[i] = readRaw(in, type);
        }
        long[] values = TableBlockIO.unpack(in, n, width);
        for (int i = 0; i < n; i++) {
          long integer = base + values[i]; // defined modular arithmetic for unsigned FOR
          double value = (double) integer * POW[f] * INV[e];
          result[offset + i] = ClusterTable.raw(value, type);
        }
        for (int i = 0; i < count; i++) result[offset + positions[i]] = exceptions[i];
      } else throw new IOException("Unknown ALP vector mode");
    }
    TableBlockIO.exhausted(in);
    return result;
  }

  static void writeRaw(DataOutputStream out, long raw, TSDataType type) throws IOException {
    if (type == TSDataType.INT32 || type == TSDataType.FLOAT) out.writeInt((int) raw);
    else out.writeLong(raw);
  }

  static long readRaw(DataInputStream in, TSDataType type) throws IOException {
    return type == TSDataType.INT32 || type == TSDataType.FLOAT ? in.readInt() : in.readLong();
  }
}
