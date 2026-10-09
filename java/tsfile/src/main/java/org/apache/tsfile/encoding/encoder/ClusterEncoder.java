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

package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Shared single-column table-data codec. Values may be reordered within each block; only the
 * multiset of values is preserved. No row-position or cluster-ID stream is stored.
 */
public abstract class ClusterEncoder extends Encoder {
  public static final int MAX_BLOCK_VALUES = 10_000;
  private static final int PACK_SIZE = 10;
  private final TSDataType dataType;
  // Integers or raw IEEE bits, so even NaN payloads and negative zero survive a RAW block.
  private long[] values = new long[128];
  private int size;

  protected ClusterEncoder(TSEncoding encoding, TSDataType dataType) {
    super(encoding);
    switch (dataType) {
      case INT32:
      case DATE:
      case INT64:
      case TIMESTAMP:
      case FLOAT:
      case DOUBLE:
        this.dataType = dataType;
        break;
      default:
        throw new TsFileEncodingException(encoding + " does not support " + dataType);
    }
  }

  protected abstract ClusterResult cluster(long[] data);

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    requireType(dataType == TSDataType.INT32 || dataType == TSDataType.DATE);
    append(value, out);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    requireType(dataType == TSDataType.INT64 || dataType == TSDataType.TIMESTAMP);
    append(value, out);
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    requireType(dataType == TSDataType.FLOAT);
    append(Float.floatToRawIntBits(value) & 0xffffffffL, out);
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    requireType(dataType == TSDataType.DOUBLE);
    append(Double.doubleToRawLongBits(value), out);
  }

  private void requireType(boolean valid) {
    if (!valid) {
      throw new TsFileEncodingException("Value type does not match cluster schema " + dataType);
    }
  }

  private void append(long value, ByteArrayOutputStream out) {
    if (size == MAX_BLOCK_VALUES) {
      try {
        flush(out);
      } catch (IOException e) {
        throw new TsFileEncodingException("Cannot flush cluster block: " + e.getMessage());
      }
    }
    if (size == values.length) {
      values = Arrays.copyOf(values, Math.min(MAX_BLOCK_VALUES, values.length * 2));
    }
    values[size++] = value;
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    if (size == 0) {
      return;
    }
    long[] scaled = new long[size];
    int scale = scaleValues(scaled);
    ByteArrayOutputStream candidate = null;
    if (scale >= 0) {
      long min = scaled[0];
      for (long value : scaled) {
        min = Math.min(min, value);
      }
      boolean safe = min != Long.MIN_VALUE;
      for (int i = 0; safe && i < size; i++) {
        try {
          scaled[i] = Math.subtractExact(scaled[i], min);
        } catch (ArithmeticException e) {
          safe = false;
        }
        // Bound residual ZigZag and every KCluster initialization distance sum.
        safe &= scaled[i] >= 0 && scaled[i] <= Long.MAX_VALUE / (2L * size);
      }
      if (safe) {
        candidate = new ByteArrayOutputStream();
        boolean constant = true;
        for (long value : scaled) {
          constant &= value == 0;
        }
        writeCluster(candidate, scale, min, scaled, constant ? null : cluster(scaled));
      }
    }

    int rawBytes = 10 + size * valueByteWidth();
    if (candidate != null && candidate.size() <= rawBytes) {
      candidate.writeTo(out);
    } else {
      writeRaw(out);
    }
    size = 0;
  }

  // A negative result selects RAW. A scale is accepted only after exact bitwise round-trip.
  private int scaleValues(long[] scaled) {
    if (dataType != TSDataType.FLOAT && dataType != TSDataType.DOUBLE) {
      System.arraycopy(values, 0, scaled, 0, size);
      return 0;
    }
    int scale = 0;
    for (int i = 0; i < size; i++) {
      double value = floatingValue(i);
      if (!Double.isFinite(value)) {
        return -1;
      }
      scale = Math.max(scale, decimalValue(i).stripTrailingZeros().scale());
      if (scale > 18) {
        return -1;
      }
    }
    double factor = Math.pow(10, scale);
    for (int i = 0; i < size; i++) {
      try {
        scaled[i] = decimalValue(i).movePointRight(scale).longValueExact();
      } catch (ArithmeticException e) {
        return -1;
      }
      double restored = scaled[i] / factor;
      long bits =
          dataType == TSDataType.FLOAT
              ? Float.floatToRawIntBits((float) restored) & 0xffffffffL
              : Double.doubleToRawLongBits(restored);
      if (bits != values[i]) {
        return -1;
      }
    }
    return scale;
  }

  private double floatingValue(int i) {
    return dataType == TSDataType.FLOAT
        ? Float.intBitsToFloat((int) values[i])
        : Double.longBitsToDouble(values[i]);
  }

  private BigDecimal decimalValue(int i) {
    return dataType == TSDataType.FLOAT
        ? new BigDecimal(Float.toString(Float.intBitsToFloat((int) values[i])))
        : BigDecimal.valueOf(Double.longBitsToDouble(values[i]));
  }

  private void writeHeader(ClusterSupport writer, int scale, int k) throws IOException {
    writer.write(scale, 8);
    writer.write(k, 16);
    writer.write(size, 16);
    writer.write(PACK_SIZE, 16);
  }

  private void writeRaw(ByteArrayOutputStream out) throws IOException {
    ClusterSupport writer = new ClusterSupport(out);
    writeHeader(writer, 0, 0);
    // A zero min-value width is impossible in the legacy writer. Old readers reject it.
    writer.write(0, 8);
    writer.write(1, 8); // RAW extension version
    writer.write(dataType.serialize(), 8);
    for (int i = 0; i < size; i++) {
      writer.write(values[i], valueByteWidth() * 8);
    }
    writer.flush();
  }

  private void writeCluster(
      ByteArrayOutputStream out, int scale, long min, long[] data, ClusterResult result)
      throws IOException {
    ClusterSupport writer = new ClusterSupport(out);
    int k = result == null ? 0 : result.references.length;
    writeHeader(writer, scale, k);
    int minBits = ClusterSupport.bitsRequired(Math.abs(min));
    writer.write(minBits, 8);
    writer.write(min < 0 ? 1 : 0, 1);
    writer.write(Math.abs(min), minBits);
    if (k == 0) {
      writer.flush();
      return;
    }
    result.validate(size);
    long minReference = result.references[0];
    for (long reference : result.references) {
      minReference = Math.min(minReference, reference);
    }
    writer.write(ClusterSupport.bitsRequired(minReference), 8);
    writer.write(0, 1);
    writer.write(minReference, ClusterSupport.bitsRequired(minReference));
    int referenceBits = 1;
    for (long reference : result.references) {
      referenceBits =
          Math.max(referenceBits, ClusterSupport.bitsRequired(reference - minReference));
    }
    writer.write(referenceBits, 8);
    for (long reference : result.references) {
      writer.write(reference - minReference, referenceBits);
    }
    long[] countDeltas = new long[k];
    for (int i = 0; i < k; i++) {
      countDeltas[i] = result.counts[i] - (i == 0 ? 0 : result.counts[i - 1]);
    }
    writePacks(writer, countDeltas, 16);

    long[] residuals = new long[size];
    int[] positions = new int[k];
    for (int i = 1; i < k; i++) {
      positions[i] = positions[i - 1] + (int) result.counts[i - 1];
    }
    for (int i = 0; i < size; i++) {
      int id = result.assignments[i];
      long residual = data[i] - result.references[id];
      residuals[positions[id]++] = (residual << 1) ^ (residual >> 63);
    }
    writePacks(writer, residuals, 32);
    writer.flush();
  }

  private void writePacks(ClusterSupport writer, long[] data, int countBits) throws IOException {
    int packs = (data.length + PACK_SIZE - 1) / PACK_SIZE;
    writer.write(packs, countBits);
    int[] widths = new int[packs];
    for (int i = 0; i < packs; i++) {
      int width = 1;
      for (int j = i * PACK_SIZE; j < Math.min(data.length, (i + 1) * PACK_SIZE); j++) {
        width = Math.max(width, ClusterSupport.bitsRequired(data[j]));
      }
      widths[i] = width;
      writer.write(width, 8);
    }
    for (int i = 0; i < packs; i++) {
      for (int j = i * PACK_SIZE; j < Math.min(data.length, (i + 1) * PACK_SIZE); j++) {
        writer.write(data[j], widths[i]);
      }
    }
  }

  @Override
  public int getOneItemMaxSize() {
    return Long.BYTES;
  }

  private int valueByteWidth() {
    return dataType == TSDataType.INT32
            || dataType == TSDataType.DATE
            || dataType == TSDataType.FLOAT
        ? 4
        : 8;
  }

  @Override
  public long getMaxByteSize() {
    // Current retained memory, including unused array capacity; flush scratch is transient.
    return 64L + 8L * values.length;
  }
}
