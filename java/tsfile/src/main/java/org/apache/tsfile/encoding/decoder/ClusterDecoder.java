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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Decodes cluster-grouped table values; input row order is intentionally not restored. */
public class ClusterDecoder extends Decoder {
  private final TSDataType dataType;
  private long[] values;
  private int readIndex;

  public ClusterDecoder(TSDataType dataType) {
    this(TSEncoding.ACLUSTER, dataType);
  }

  public ClusterDecoder(TSEncoding encoding, TSDataType dataType) {
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
        throw new TsFileDecodingException("Unsupported cluster type: " + dataType);
    }
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    return ensureBlock(buffer);
  }

  @Override
  public void reset() {
    values = null;
    readIndex = 0;
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    require(dataType == TSDataType.INT32 || dataType == TSDataType.DATE, "Wrong read type");
    return (int) nextValue(buffer);
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    require(dataType == TSDataType.INT64 || dataType == TSDataType.TIMESTAMP, "Wrong read type");
    return nextValue(buffer);
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    require(dataType == TSDataType.FLOAT, "Wrong read type");
    return Float.intBitsToFloat((int) nextValue(buffer));
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    require(dataType == TSDataType.DOUBLE, "Wrong read type");
    return Double.longBitsToDouble(nextValue(buffer));
  }

  private long nextValue(ByteBuffer buffer) {
    require(ensureBlock(buffer), "No more cluster values");
    return values[readIndex++];
  }

  private boolean ensureBlock(ByteBuffer buffer) {
    if (values != null && readIndex < values.length) {
      return true;
    }
    if (!buffer.hasRemaining()) {
      return false;
    }
    // Only consume the caller's bytes after a whole block has passed validation.
    ByteBuffer input = buffer.duplicate();
    try {
      long[] decoded = decodeBlock(new ClusterReader(input));
      buffer.position(input.position());
      values = decoded;
      readIndex = 0;
      return true;
    } catch (ArithmeticException | IllegalArgumentException e) {
      throw new TsFileDecodingException("Invalid cluster block: " + e.getMessage());
    }
  }

  private long[] decodeBlock(ClusterReader reader) {
    int scale = (int) reader.read(8);
    int k = (int) reader.read(16);
    int count = (int) reader.read(16);
    int packSize = (int) reader.read(16);
    require(count > 0 && k <= count && packSize > 0, "Invalid cluster header");
    int minBits = (int) reader.read(8);
    if (minBits == 0) {
      require(scale == 0 && k == 0, "Invalid RAW block header");
      require(reader.read(8) == 1, "Unsupported cluster RAW version");
      require(reader.read(8) == (dataType.serialize() & 0xff), "RAW block type mismatch");
      int width = is32Bit() ? 32 : 64;
      require(reader.remainingBits() >= (long) count * width, "Truncated RAW block");
      long[] raw = new long[count];
      for (int i = 0; i < count; i++) {
        long bits = reader.read(width);
        raw[i] = dataType == TSDataType.INT32 || dataType == TSDataType.DATE ? (int) bits : bits;
      }
      return raw;
    }
    require(isFloating() || scale == 0, "Integer cluster block has a decimal scale");
    long min = readSignedMagnitude(reader, minBits);
    long[] result = new long[count];
    if (k == 0) {
      Arrays.fill(result, toValueBits(min, scale));
      return result;
    }

    int referenceMinBits = (int) reader.read(8);
    long referenceMin = readSignedMagnitude(reader, referenceMinBits);
    int referenceBits = (int) reader.read(8);
    require(referenceBits >= 1 && referenceBits <= 63, "Invalid reference width");
    require(referenceMin >= 0, "Negative normalized reference");
    long[] references = new long[k];
    for (int i = 0; i < k; i++) {
      references[i] = Math.addExact(referenceMin, reader.read(referenceBits));
    }
    long[] counts = readPacks(reader, k, packSize, 16, false);
    long previousCount = 0;
    long total = 0;
    for (int i = 0; i < k; i++) {
      // Stored values are differences between sorted cluster sizes, not prefix positions.
      counts[i] = Math.addExact(previousCount, counts[i]);
      require(counts[i] >= 0 && counts[i] <= count, "Invalid cluster frequency");
      previousCount = counts[i];
      total = Math.addExact(total, counts[i]);
    }
    require(total == count, "Cluster frequencies do not sum to the value count");
    long[] residuals = readPacks(reader, count, packSize, 32, true);
    int position = 0;
    for (int i = 0; i < k; i++) {
      for (int j = 0; j < counts[i]; j++) {
        long zigzag = residuals[position];
        long residual = (zigzag >>> 1) ^ -(zigzag & 1);
        long offset = Math.addExact(references[i], residual);
        require(offset >= 0, "Negative normalized value");
        result[position++] = toValueBits(Math.addExact(min, offset), scale);
      }
    }
    return result;
  }

  private long[] readPacks(
      ClusterReader reader, int count, int packSize, int packCountBits, boolean allowZeroWidth) {
    int expected = (count + packSize - 1) / packSize;
    require(reader.read(packCountBits) == expected, "Invalid pack count");
    int[] widths = new int[expected];
    long requiredBits = 0;
    for (int i = 0; i < expected; i++) {
      widths[i] = (int) reader.read(8);
      require(widths[i] >= (allowZeroWidth ? 0 : 1) && widths[i] <= 64, "Invalid pack width");
      requiredBits += (long) widths[i] * Math.min(packSize, count - i * packSize);
    }
    require(reader.remainingBits() >= requiredBits, "Truncated cluster payload");
    long[] data = new long[count];
    for (int i = 0; i < count; i++) {
      int width = widths[i / packSize];
      data[i] = width == 0 ? 0 : reader.read(width);
    }
    return data;
  }

  private long readSignedMagnitude(ClusterReader reader, int width) {
    require(width >= 1 && width <= 64, "Invalid signed value width");
    boolean negative = reader.read(1) != 0;
    long magnitude = reader.read(width);
    require(
        magnitude >= 0 || (negative && magnitude == Long.MIN_VALUE),
        "Signed magnitude out of range");
    return negative ? -magnitude : magnitude;
  }

  private long toValueBits(long scaled, int scale) {
    if (isFloating()) {
      double value = scaled / Math.pow(10, scale);
      return dataType == TSDataType.FLOAT
          ? Float.floatToRawIntBits((float) value) & 0xffffffffL
          : Double.doubleToRawLongBits(value);
    }
    if (is32Bit()) {
      require(
          scaled >= Integer.MIN_VALUE && scaled <= Integer.MAX_VALUE,
          "Cluster value exceeds INT32");
    }
    return scaled;
  }

  private boolean isFloating() {
    return dataType == TSDataType.FLOAT || dataType == TSDataType.DOUBLE;
  }

  private boolean is32Bit() {
    return dataType == TSDataType.INT32
        || dataType == TSDataType.DATE
        || dataType == TSDataType.FLOAT;
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new TsFileDecodingException(message);
    }
  }
}
