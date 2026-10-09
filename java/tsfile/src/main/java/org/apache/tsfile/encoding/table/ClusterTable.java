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

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/** Numeric records with explicit timestamps. Physical row order is not part of the contract. */
public final class ClusterTable {
  final long[] timestamps;
  final String[] names;
  final TSDataType[] types;
  final long[][] bits;
  final BitSet[] nulls;

  public ClusterTable(long[] timestamps, String[] names, TSDataType[] types, Object[][] rows) {
    validateSchema(names, types);
    if (timestamps.length != rows.length) {
      throw new IllegalArgumentException("Timestamp/row count mismatch");
    }
    this.timestamps = timestamps.clone();
    this.names = names.clone();
    this.types = types.clone();
    this.bits = new long[rows.length][names.length];
    this.nulls = new BitSet[names.length];
    for (int c = 0; c < names.length; c++) {
      nulls[c] = new BitSet(rows.length);
    }
    for (int r = 0; r < rows.length; r++) {
      if (rows[r] == null || rows[r].length != names.length) {
        throw new IllegalArgumentException("Ragged table");
      }
      for (int c = 0; c < names.length; c++) {
        Object value = rows[r][c];
        if (value == null) {
          nulls[c].set(r);
          continue;
        }
        // Reject implicit numeric casts: a long must never silently pass through binary64.
        switch (types[c]) {
          case INT32:
            if (!(value instanceof Integer)) throw wrongType(c);
            bits[r][c] = (Integer) value;
            break;
          case INT64:
            if (!(value instanceof Long)) throw wrongType(c);
            bits[r][c] = (Long) value;
            break;
          case FLOAT:
            if (!(value instanceof Float)) throw wrongType(c);
            bits[r][c] = Float.floatToRawIntBits((Float) value);
            break;
          case DOUBLE:
            if (!(value instanceof Double)) throw wrongType(c);
            bits[r][c] = Double.doubleToRawLongBits((Double) value);
            break;
          default:
            throw wrongType(c);
        }
      }
    }
  }

  ClusterTable(
      long[] timestamps, String[] names, TSDataType[] types, long[][] bits, BitSet[] nulls) {
    validateSchema(names, types);
    this.timestamps = timestamps;
    this.names = names;
    this.types = types;
    this.bits = bits;
    this.nulls = nulls;
  }

  static void validateSchema(String[] names, TSDataType[] types) {
    if (names == null
        || types == null
        || names.length == 0
        || names.length > ClusterTableCodec.MAX_COLUMNS
        || names.length != types.length) {
      throw new IllegalArgumentException("Expected 1..1024 named numeric columns");
    }
    Set<String> unique = new HashSet<>();
    for (int c = 0; c < names.length; c++) {
      if (names[c] == null
          || names[c].isEmpty()
          || names[c].length() > 1024
          || !unique.add(names[c])) {
        throw new IllegalArgumentException("Invalid or duplicate column name");
      }
      if (types[c] != TSDataType.INT32
          && types[c] != TSDataType.INT64
          && types[c] != TSDataType.FLOAT
          && types[c] != TSDataType.DOUBLE) {
        throw new IllegalArgumentException("Supported types: INT32, INT64, FLOAT, DOUBLE");
      }
    }
  }

  private static IllegalArgumentException wrongType(int column) {
    return new IllegalArgumentException("Value does not match declared type at column " + column);
  }

  public int rowCount() {
    return timestamps.length;
  }

  public int columnCount() {
    return names.length;
  }

  public String[] columnNames() {
    return names.clone();
  }

  public TSDataType[] columnTypes() {
    return types.clone();
  }

  public long timestamp(int row) {
    return timestamps[row];
  }

  public boolean isNull(int row, int column) {
    return nulls[column].get(row);
  }

  public long rawBits(int row, int column) {
    return bits[row][column];
  }

  public Number value(int row, int column) {
    if (isNull(row, column)) return null;
    switch (types[column]) {
      case INT32:
        return (int) bits[row][column];
      case INT64:
        return bits[row][column];
      case FLOAT:
        return Float.intBitsToFloat((int) bits[row][column]);
      case DOUBLE:
        return Double.longBitsToDouble(bits[row][column]);
      default:
        throw wrongType(column);
    }
  }

  double number(int row, int column) {
    return isNull(row, column) ? Double.NaN : numeric(bits[row][column], types[column]);
  }

  static double numeric(long raw, TSDataType type) {
    switch (type) {
      case DOUBLE:
        return Double.longBitsToDouble(raw);
      case FLOAT:
        return Float.intBitsToFloat((int) raw);
      default:
        return raw;
    }
  }

  static long raw(double value, TSDataType type) {
    switch (type) {
      case DOUBLE:
        return Double.doubleToRawLongBits(value);
      case FLOAT:
        return Float.floatToRawIntBits((float) value);
      case INT32:
        return (int) value;
      default:
        return (long) value;
    }
  }

  public ClusterTable slice(int from, int to) {
    if (from < 0 || to < from || to > rowCount()) throw new IndexOutOfBoundsException();
    int[] indices = new int[to - from];
    for (int i = 0; i < indices.length; i++) indices[i] = from + i;
    return selectRows(indices);
  }

  /** Inclusive bounds on the ORIGINAL timestamps, including duplicate timestamps. */
  public ClusterTable timeRange(long start, long end) {
    if (start > end) throw new IllegalArgumentException("Reversed time range");
    int[] indices = new int[rowCount()];
    int count = 0;
    for (int r = 0; r < rowCount(); r++) {
      if (timestamps[r] >= start && timestamps[r] <= end) indices[count++] = r;
    }
    return selectRows(Arrays.copyOf(indices, count));
  }

  private ClusterTable selectRows(int[] indices) {
    long[] times = new long[indices.length];
    long[][] values = new long[indices.length][];
    BitSet[] missing = new BitSet[columnCount()];
    for (int c = 0; c < columnCount(); c++) missing[c] = new BitSet(indices.length);
    for (int i = 0; i < indices.length; i++) {
      int r = indices[i];
      times[i] = timestamps[r];
      values[i] = bits[r].clone();
      for (int c = 0; c < columnCount(); c++) if (nulls[c].get(r)) missing[c].set(i);
    }
    return new ClusterTable(times, names.clone(), types.clone(), values, missing);
  }
}
