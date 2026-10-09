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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Complete independent numeric table blocks: CV-elbow selection, joint A/K clustering, ALP
 * companions and timestamps in the same traversal. No permutation is stored or restored.
 */
public final class ClusterTableCodec {
  public static final int MAX_ROWS = 10000;
  public static final int MAX_COLUMNS = 1024;
  public static final int MAX_CELLS = 2000000;
  public static final int MAX_ENCODED_BYTES = 64 * 1024 * 1024;
  private static final int MAGIC = 0x434c5442; // CLTB
  private static final int VERSION = 1;

  private ClusterTableCodec() {}

  public static final class Encoded {
    private final byte[] bytes;
    private final int[] selected;
    private final int[] clustered;
    public final int referenceCount;
    public final long clusterBytes;
    public final long alpBytes;

    private Encoded(
        byte[] bytes,
        int[] selected,
        int[] clustered,
        int references,
        long clusterBytes,
        long alpBytes) {
      this.bytes = bytes;
      this.selected = selected.clone();
      this.clustered = clustered;
      this.referenceCount = references;
      this.clusterBytes = clusterBytes;
      this.alpBytes = alpBytes;
    }

    public byte[] bytes() {
      return bytes.clone();
    }

    public int[] selectedColumns() {
      return selected.clone();
    }

    public int[] clusteredColumns() {
      return clustered.clone();
    }
  }

  private static final class Scaled {
    final int column;
    final int scale;
    final long min;
    final long[] values;

    Scaled(int column, int scale, long min, long[] values) {
      this.column = column;
      this.scale = scale;
      this.min = min;
      this.values = values;
    }
  }

  public static Encoded encode(ClusterTable table, ClusterTableOptions options) throws IOException {
    return encode(table, options, ClusterColumnSelector.select(table));
  }

  /**
   * Reuse the first sample's selection across later blocks; every block records it independently.
   */
  public static Encoded encode(ClusterTable table, ClusterTableOptions options, int[] selected)
      throws IOException {
    int n = table.rowCount(), d = table.columnCount();
    shape(n, d);
    if (options == null) throw new IllegalArgumentException("Missing algorithm options");
    boolean[] requested = new boolean[d];
    for (int c : selected) {
      if (c < 0 || c >= d || requested[c]) throw new IllegalArgumentException("Invalid selection");
      requested[c] = true;
    }
    List<Scaled> scaled = new ArrayList<>();
    for (int c : selected) {
      Scaled column = scale(table, c);
      if (column != null) scaled.add(column);
    }
    int[] order = new int[n];
    for (int r = 0; r < n; r++) order[r] = r;
    MultidimensionalCluster.Result clusters = null;
    if (!scaled.isEmpty()) {
      long[][] points = new long[n][scaled.size()];
      for (int c = 0; c < scaled.size(); c++) {
        for (int r = 0; r < n; r++) points[r][c] = scaled.get(c).values[r];
      }
      clusters = MultidimensionalCluster.run(points, options);
      order = clusters.order;
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeInt(MAGIC);
    out.writeByte(VERSION);
    out.writeByte(options.method.ordinal());
    out.writeInt(n);
    out.writeShort(d);
    out.writeShort(options.k);
    out.writeByte(options.maxIterations);
    out.writeLong(options.seed);
    out.writeShort(selected.length);
    for (int c : selected) out.writeShort(c);
    for (int c = 0; c < d; c++) {
      out.writeUTF(table.names[c]);
      out.writeByte(table.types[c].serialize());
    }
    int k = clusters == null ? 0 : clusters.counts.length;
    out.writeShort(k);
    if (clusters != null) for (int count : clusters.counts) out.writeShort(count);
    // Original timestamps travel with entire records; these are not outer block identifiers.
    for (int r : order) out.writeLong(table.timestamps[r]);
    long clusterBytes = 0, alpBytes = 0;
    int[] clustered = new int[scaled.size()];
    for (int i = 0; i < scaled.size(); i++) clustered[i] = scaled.get(i).column;
    for (int c = 0; c < d; c++) {
      byte[] missing = new byte[(n + 7) / 8];
      for (int i = 0; i < n; i++) if (table.nulls[c].get(order[i])) missing[i / 8] |= 1 << (i % 8);
      out.write(missing);
      int index = -1;
      for (int i = 0; i < scaled.size(); i++) if (scaled.get(i).column == c) index = i;
      out.writeByte(index < 0 ? 0 : 1);
      byte[] payload;
      if (index < 0) {
        long[] values = new long[n];
        for (int i = 0; i < n; i++) values[i] = table.bits[order[i]][c];
        payload = AlpColumnCodec.encode(values, table.types[c]);
        alpBytes += payload.length;
      } else {
        Scaled column = scaled.get(index);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(body);
        data.writeByte(column.scale);
        data.writeLong(column.min);
        for (long[] reference : clusters.references) data.writeLong(reference[index]);
        long[] residuals = new long[n];
        int position = 0;
        for (int id = 0; id < k; id++) {
          for (int j = 0; j < clusters.counts[id]; j++, position++) {
            long delta = column.values[order[position]] - clusters.references[id][index];
            residuals[position] = (delta << 1) ^ (delta >> 63);
          }
        }
        data.write(TableBlockIO.packs(residuals));
        payload = body.toByteArray();
        clusterBytes += payload.length;
      }
      TableBlockIO.payload(out, payload);
    }
    if (bytes.size() + 4 > MAX_ENCODED_BYTES) throw new IOException("Encoded block too large");
    CRC32 crc = new CRC32();
    byte[] body = bytes.toByteArray();
    crc.update(body);
    out.writeInt((int) crc.getValue());
    return new Encoded(bytes.toByteArray(), selected, clustered, k, clusterBytes, alpBytes);
  }

  private static Scaled scale(ClusterTable table, int c) {
    int n = table.rowCount();
    if (n == 0 || !table.nulls[c].isEmpty()) return null;
    TSDataType type = table.types[c];
    long[] values = new long[n];
    int scale = 0;
    BigDecimal[] decimals = null;
    if (type == TSDataType.DOUBLE || type == TSDataType.FLOAT) {
      decimals = new BigDecimal[n];
      for (int r = 0; r < n; r++) {
        double value = table.number(r, c);
        if (!Double.isFinite(value) || (value == 0 && table.bits[r][c] != 0)) return null;
        decimals[r] =
            type == TSDataType.FLOAT
                ? new BigDecimal(Float.toString((float) value))
                : BigDecimal.valueOf(value);
        scale = Math.max(scale, decimals[r].stripTrailingZeros().scale());
      }
      if (scale > 18) return null;
    }
    long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    try {
      for (int r = 0; r < n; r++) {
        values[r] =
            decimals == null
                ? table.bits[r][c]
                : decimals[r].movePointRight(scale).longValueExact();
        if (decimals != null
            && ClusterTable.raw(values[r] / Math.pow(10, scale), type) != table.bits[r][c]) {
          return null;
        }
        min = Math.min(min, values[r]);
        max = Math.max(max, values[r]);
      }
      long span = Math.subtractExact(max, min);
      if (span > Long.MAX_VALUE / 2) return null;
      for (int r = 0; r < n; r++) values[r] = Math.subtractExact(values[r], min);
    } catch (ArithmeticException unsafe) {
      return null;
    }
    return new Scaled(c, scale, min, values);
  }

  public static ClusterTable decode(byte[] bytes) throws IOException {
    if (bytes == null || bytes.length < 8 || bytes.length > MAX_ENCODED_BYTES) {
      throw new IOException("Invalid encoded block length");
    }
    CRC32 crc = new CRC32();
    crc.update(bytes, 0, bytes.length - 4);
    DataInputStream checksum =
        new DataInputStream(new ByteArrayInputStream(bytes, bytes.length - 4, 4));
    if ((int) crc.getValue() != checksum.readInt())
      throw new IOException("Table block checksum mismatch");
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 4));
    try {
      if (in.readInt() != MAGIC || in.readUnsignedByte() != VERSION)
        throw new IOException("Unknown table block format");
      int method = in.readUnsignedByte();
      if (method >= ClusterTableOptions.Method.values().length)
        throw new IOException("Unknown clustering method");
      int n = in.readInt(), d = in.readUnsignedShort();
      shape(n, d);
      new ClusterTableOptions(
          ClusterTableOptions.Method.values()[method],
          in.readUnsignedShort(),
          in.readUnsignedByte(),
          in.readLong());
      int selectionCount = in.readUnsignedShort();
      if (selectionCount > d) throw new IOException("Invalid selected column count");
      boolean[] selected = new boolean[d];
      for (int i = 0; i < selectionCount; i++) {
        int c = in.readUnsignedShort();
        if (c >= d || selected[c]) throw new IOException("Invalid selected column ID");
        selected[c] = true;
      }
      String[] names = new String[d];
      TSDataType[] types = new TSDataType[d];
      for (int c = 0; c < d; c++) {
        names[c] = in.readUTF();
        types[c] = TSDataType.deserialize(in.readByte());
      }
      ClusterTable.validateSchema(names, types);
      int k = in.readUnsignedShort();
      if (k > n) throw new IOException("Invalid reference count");
      int[] counts = new int[k];
      int total = 0;
      for (int id = 0; id < k; id++) {
        counts[id] = in.readUnsignedShort();
        if (counts[id] == 0 || (id > 0 && counts[id] < counts[id - 1]))
          throw new IOException("Invalid cluster count");
        total += counts[id];
      }
      if (k > 0 && total != n)
        throw new IOException("Cluster frequency sum differs from row count");
      // Check available timestamp bytes before any row-sized allocation.
      if (8L * n > in.available()) throw new IOException("Truncated timestamp stream");
      long[] times = new long[n];
      for (int r = 0; r < n; r++) times[r] = in.readLong();
      long[][] values = new long[n][d];
      BitSet[] nulls = new BitSet[d];
      int clusterColumns = 0;
      for (int c = 0; c < d; c++) {
        byte[] bitmap = new byte[(n + 7) / 8];
        in.readFully(bitmap);
        nulls[c] = BitSet.valueOf(bitmap);
        if (nulls[c].length() > n) throw new IOException("Invalid null bitmap");
        int mode = in.readUnsignedByte();
        DataInputStream body = TableBlockIO.payload(in);
        long[] column;
        if (mode == 0) column = AlpColumnCodec.decode(body, n, types[c]);
        else if (mode == 1) {
          if (!selected[c] || k == 0 || !nulls[c].isEmpty())
            throw new IOException("Invalid clustered column");
          clusterColumns++;
          int scale = body.readUnsignedByte();
          if (scale > 18
              || ((types[c] == TSDataType.INT32 || types[c] == TSDataType.INT64) && scale != 0)) {
            throw new IOException("Invalid scale");
          }
          long min = body.readLong();
          long[] refs = new long[k];
          for (int id = 0; id < k; id++) {
            refs[id] = body.readLong();
            if (refs[id] < 0 || refs[id] > Long.MAX_VALUE / 2)
              throw new IOException("Invalid reference");
          }
          column = TableBlockIO.readPacks(body, n);
          int position = 0;
          for (int id = 0; id < k; id++) {
            for (int j = 0; j < counts[id]; j++, position++) {
              long residual = (column[position] >>> 1) ^ -(column[position] & 1);
              long normalized = Math.addExact(refs[id], residual);
              if (normalized < 0 || normalized > Long.MAX_VALUE / 2)
                throw new IOException("Invalid residual");
              long integer = Math.addExact(min, normalized);
              if (types[c] == TSDataType.INT64) column[position] = integer;
              else if (types[c] == TSDataType.INT32) {
                if (integer != (int) integer) throw new IOException("INT32 overflow");
                column[position] = integer;
              } else column[position] = ClusterTable.raw(integer / Math.pow(10, scale), types[c]);
            }
          }
          TableBlockIO.exhausted(body);
        } else throw new IOException("Unknown column codec");
        for (int r = 0; r < n; r++) values[r][c] = column[r];
      }
      if ((k > 0) != (clusterColumns > 0)) throw new IOException("Inconsistent cluster metadata");
      TableBlockIO.exhausted(in);
      return new ClusterTable(times, names, types, values, nulls);
    } catch (IllegalArgumentException | ArithmeticException bad) {
      throw new IOException("Invalid table block", bad);
    }
  }

  private static void shape(int rows, int columns) {
    if (rows < 0
        || rows > MAX_ROWS
        || columns < 1
        || columns > MAX_COLUMNS
        || (long) rows * columns > MAX_CELLS) {
      throw new IllegalArgumentException("Block exceeds row/column/cell limits");
    }
  }
}
