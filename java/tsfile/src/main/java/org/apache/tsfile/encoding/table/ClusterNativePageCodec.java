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
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

/**
 * Native typed-column pages produced by one joint table encoding. Columns carry independent
 * projections of the joint stream, so projecting a column never requires reading another column.
 * Timestamp keys are data, not input positions. The SQL adapter visits these keys in time order;
 * the table codec itself continues to decode in cluster order without restoring input order.
 */
public final class ClusterNativePageCodec {
  public static final int MAGIC = 0xC34C4E50;
  public static final int MAX_PAGE_ROWS = 100000;
  private static final int VERSION = 1;

  private ClusterNativePageCodec() {}

  public static final class Selection {
    private String[] names;
    private TSDataType[] types;
    private int[] selected;

    private int[] fit(ClusterTable table) {
      if (!Arrays.equals(names, table.names) || !Arrays.equals(types, table.types)) {
        names = table.names.clone();
        types = table.types.clone();
        selected = ClusterColumnSelector.select(table);
      }
      return selected;
    }
  }

  public static byte[][] encode(
      long[] times,
      String[] names,
      TSDataType[] types,
      ClusterColumnBuffer[] columns,
      ClusterTableOptions options,
      Selection selection)
      throws IOException {
    ClusterTable.validateSchema(names, types);
    int n = times.length, d = names.length;
    if (n < 1 || n > MAX_PAGE_ROWS || columns.length != d) {
      throw new IOException("Invalid native cluster page shape");
    }
    for (int r = 1; r < n; r++) {
      if (times[r] <= times[r - 1]) throw new IOException("Native time keys must increase");
    }
    for (ClusterColumnBuffer column : columns) {
      if (column.size() != n) throw new IOException("Unequal aligned column lengths");
    }
    int blockRows = Math.min(ClusterTableCodec.MAX_ROWS, ClusterTableCodec.MAX_CELLS / d);
    int blocks = (n + blockRows - 1) / blockRows;
    ByteArrayOutputStream[] buffers = new ByteArrayOutputStream[d];
    DataOutputStream[] outputs = new DataOutputStream[d];
    for (int c = 0; c < d; c++) {
      buffers[c] = new ByteArrayOutputStream();
      outputs[c] = new DataOutputStream(buffers[c]);
      outputs[c].writeInt(MAGIC);
      outputs[c].writeByte(VERSION);
      outputs[c].writeInt(n);
      outputs[c].writeInt(blocks);
    }
    for (int start = 0; start < n; start += blockRows) {
      int end = Math.min(n, start + blockRows), count = end - start;
      long[][] values = new long[count][d];
      BitSet[] nulls = new BitSet[d];
      for (int c = 0; c < d; c++) {
        nulls[c] = new BitSet(count);
        for (int r = 0; r < count; r++) {
          values[r][c] = columns[c].bits(start + r);
          if (columns[c].isNull(start + r)) nulls[c].set(r);
        }
      }
      ClusterTable table =
          new ClusterTable(Arrays.copyOfRange(times, start, end), names, types, values, nulls);
      byte[] joint = ClusterTableCodec.encode(table, options, selection.fit(table)).bytes();
      byte[][] projected = project(joint);
      for (int c = 0; c < d; c++) TableBlockIO.payload(outputs[c], projected[c]);
    }
    byte[][] result = new byte[d][];
    for (int c = 0; c < d; c++) {
      if (buffers[c].size() > ClusterTableCodec.MAX_ENCODED_BYTES) {
        throw new IOException("Native column page too large");
      }
      result[c] = buffers[c].toByteArray();
    }
    return result;
  }

  // Only called on bytes just produced by the checked table encoder. Copy the existing compressed
  // column bodies verbatim: this does not rerun clustering or recompress columns independently.
  private static byte[][] project(byte[] joint) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(joint));
    int magic = in.readInt(), version = in.readUnsignedByte(), method = in.readUnsignedByte();
    int n = in.readInt(), d = in.readUnsignedShort(), requestedK = in.readUnsignedShort();
    int iterations = in.readUnsignedByte();
    long seed = in.readLong();
    boolean[] selected = new boolean[d];
    int selectedCount = in.readUnsignedShort();
    for (int i = 0; i < selectedCount; i++) selected[in.readUnsignedShort()] = true;
    String[] names = new String[d];
    byte[] types = new byte[d];
    for (int c = 0; c < d; c++) {
      names[c] = in.readUTF();
      types[c] = in.readByte();
    }
    int k = in.readUnsignedShort();
    byte[] counts = new byte[k * 2], times = new byte[n * 8];
    in.readFully(counts);
    in.readFully(times);
    byte[][] result = new byte[d][];
    for (int c = 0; c < d; c++) {
      byte[] bitmap = new byte[(n + 7) / 8];
      in.readFully(bitmap);
      int mode = in.readUnsignedByte();
      int length = in.readInt();
      byte[] payload = new byte[length];
      in.readFully(payload);
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(buffer);
      out.writeInt(magic);
      out.writeByte(version);
      out.writeByte(method);
      out.writeInt(n);
      out.writeShort(1);
      out.writeShort(requestedK);
      out.writeByte(iterations);
      out.writeLong(seed);
      out.writeShort(selected[c] ? 1 : 0);
      if (selected[c]) out.writeShort(0);
      out.writeUTF(names[c]);
      out.writeByte(types[c]);
      out.writeShort(mode == 1 ? k : 0);
      if (mode == 1) out.write(counts);
      out.write(times);
      out.write(bitmap);
      out.writeByte(mode);
      TableBlockIO.payload(out, payload);
      CRC32 crc = new CRC32();
      crc.update(buffer.toByteArray());
      out.writeInt((int) crc.getValue());
      result[c] = buffer.toByteArray();
    }
    return result;
  }

  public static boolean isNativePage(ByteBuffer buffer) {
    return buffer.remaining() >= 4 && buffer.getInt(buffer.position()) == MAGIC;
  }

  public static final class Decoded {
    public final long[] times;
    public final byte[] bitmap;
    public final ByteBuffer values;

    private Decoded(long[] times, byte[] bitmap, ByteBuffer values) {
      this.times = times;
      this.bitmap = bitmap;
      this.values = values;
    }
  }

  /** Native scan adapter: expose actual timestamp keys in the order expected by IoTDB readers. */
  public static Decoded decode(ByteBuffer source, TSDataType type) throws IOException {
    if (source.remaining() > ClusterTableCodec.MAX_ENCODED_BYTES)
      throw new IOException("Native page too large");
    byte[] bytes = new byte[source.remaining()];
    source.duplicate().get(bytes);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    if (in.readInt() != MAGIC || in.readUnsignedByte() != VERSION)
      throw new IOException("Unknown native cluster page");
    int n = in.readInt(), blocks = in.readInt();
    if (n < 1 || n > MAX_PAGE_ROWS || blocks < 1 || blocks > n)
      throw new IOException("Invalid native page counts");
    TreeMap<Long, Long> cells = new TreeMap<>();
    int total = 0;
    String name = null;
    for (int b = 0; b < blocks; b++) {
      DataInputStream body = TableBlockIO.payload(in);
      byte[] encoded = new byte[body.available()];
      body.readFully(encoded);
      ClusterTable column = ClusterTableCodec.decode(encoded);
      if (column.columnCount() != 1
          || column.types[0] != type
          || column.rowCount() == 0
          || (name != null && !name.equals(column.names[0])))
        throw new IOException("Native column schema mismatch");
      name = column.names[0];
      total += column.rowCount();
      if (total > n) throw new IOException("Native page row count overflow");
      for (int r = 0; r < column.rowCount(); r++) {
        long time = column.timestamps[r];
        if (cells.containsKey(time)) throw new IOException("Duplicate native timestamp key");
        cells.put(time, column.nulls[0].get(r) ? null : column.bits[r][0]);
      }
    }
    if (total != n) throw new IOException("Native page row count mismatch");
    TableBlockIO.exhausted(in);
    long[] times = new long[n];
    byte[] bitmap = new byte[(n + 7) / 8];
    ByteArrayOutputStream values = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(values);
    int r = 0;
    for (Map.Entry<Long, Long> cell : cells.entrySet()) {
      times[r] = cell.getKey();
      if (cell.getValue() != null) {
        bitmap[r / 8] |= 0x80 >>> (r % 8);
        long bits = cell.getValue();
        switch (type) {
          case INT32:
            ReadWriteForEncodingUtils.writeVarInt((int) bits, values);
            break;
          case FLOAT:
            out.writeInt((int) bits);
            break;
          case INT64:
          case DOUBLE:
            out.writeLong(bits);
            break;
          default:
            throw new IOException("Unsupported native cluster type");
        }
      }
      r++;
    }
    return new Decoded(times, bitmap, ByteBuffer.wrap(values.toByteArray()));
  }
}
