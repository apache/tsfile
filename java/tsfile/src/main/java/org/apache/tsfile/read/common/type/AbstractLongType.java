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

package org.apache.tsfile.read.common.type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.tsfile.encoding.decoder.LongChimpDecoder;
import org.apache.tsfile.encoding.decoder.LongGorillaDecoder;
import org.apache.tsfile.encoding.decoder.LongRLBEDecoder;
import org.apache.tsfile.encoding.decoder.LongRleDecoder;
import org.apache.tsfile.encoding.decoder.LongSprintzDecoder;
import org.apache.tsfile.encoding.decoder.LongZigzagDecoder;
import org.apache.tsfile.encoding.decoder.RegularDataDecoder;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public abstract class AbstractLongType extends AbstractType {

  @Override
  public void toBytes(TsPrimitiveType value, byte[] valueBytes, int offset) {
    BytesUtils.longToBytes(value.getLong(), valueBytes, offset);
  }

  @Override
  public int calcTypeSize(TsPrimitiveType value) {
    return Long.BYTES;
  }

  @Override
  public int calcTypeSize(Object value) {
    return Long.BYTES;
  }

  @Override
  public void init(BatchData batchData) {
    batchData.initLongValues();
  }

  @Override
  public void put(BatchData batchData, long timestamp, Object value) {
    batchData.putLong(timestamp, (long) value);
  }

  @Override
  public void serialize(BatchData batchData, DataOutputStream outputStream, boolean isDesc)
      throws IOException {
    for (int i = 0; i < batchData.length(); i++) {
      int index = isDesc ? batchData.length() - 1 - i : i;
      outputStream.writeLong(batchData.getTimeByIndex(index));
      outputStream.writeLong(batchData.getLongByIndex(index));
    }
  }

  @Override
  public void serialize(TsPrimitiveType value, DataOutputStream stream) throws IOException {
    stream.writeLong(value.getLong());
  }

  @Override
  public TSEncoding getDefaultEncoding(TSFileConfig config) {
    return TSEncoding.valueOf(config.getInt64Encoding());
  }

  @Override
  public CompressionType getDefaultCompressor(TSFileConfig config) {
    return config.getInt64Compressor();
  }

  @Override
  public Decoder getDecoder(TSEncoding encoding) {
    return switch (encoding) {
      case PLAIN, DICTIONARY -> super.getDecoder(encoding);
      case RLE -> new LongRleDecoder();
      case TS_2DIFF -> new DeltaBinaryDecoder.LongDeltaDecoder();
      case REGULAR -> new RegularDataDecoder.LongRegularDecoder();
      case GORILLA -> new LongGorillaDecoder();
      case ZIGZAG -> new LongZigzagDecoder();
      case CHIMP -> new LongChimpDecoder();
      case SPRINTZ -> new LongSprintzDecoder();
      case RLBE -> new LongRLBEDecoder();
      default -> throw decoderNotFound(encoding);
    };
  }

  @Override
  public int getOneItemMaxSize(int valveLength) {
    return Long.BYTES;
  }

  @Override
  public void update(Statistics<?> stats, long timestamp, TsPrimitiveType value) {
    stats.update(timestamp, value.getLong());
  }

  @Override
  public void update(Statistics<?> stats, BatchData batchData) {
    stats.update(batchData.currentTime(), batchData.getLong());
  }

  @Override
  public Object getCurrentValue(BatchData batchData) {
    return batchData.getLong();
  }

  @Override
  public String toString(Field field) {
    return String.valueOf(field.getLongV());
  }

  @Override
  public Object getValue(Field field) {
    return field.getLongV();
  }

  @Override
  public void setTo(TsPrimitiveType from, Field to) {
    to.setLongV(from.getLong());
  }

  @Override
  public void addPoint(TSRecord record, String columnName, ResultSet resultSet) {
    record.addPoint(columnName, resultSet.getLong(columnName));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, String value) {
    return new LongDataPoint(measurementId, Long.parseLong(value));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, long value) {
    return new LongDataPoint(measurementId, value);
  }

  @Override
  public void write(ValueChunkWriter writer, long time, Object value, boolean isNull) {
    writer.write(time, (long) value, isNull);
  }

  @Override
  public void write(ValueChunkWriter writer, long time, TsPrimitiveType value) {
    writer.write(time, value != null ? value.getLong() : Long.MAX_VALUE, value == null);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Object array, int rowIndex, boolean isNull) {
    writer.write(time, ((long[]) array)[rowIndex], isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Column column, int rowIndex, boolean isNull) {
    writer.write(time, isNull ? 0 : column.getLong(rowIndex), isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long[] times, Column column, int batchSize, int arrayOffset) {
    writer.write(times, column.getLongs(), column.isNull(), batchSize, arrayOffset);
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    writer.write(time, ((long[]) column)[rowIndex]);
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType() {
    return new TsPrimitiveType.TsLong();
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(Object value) {
    return new TsPrimitiveType.TsLong((long) value);
  }

  @Override
  public int getInt(Column c, int position) {
    return (int) c.getLong(position);
  }

  @Override
  public long getLong(Column c, int position) {
    return c.getLong(position);
  }

  @Override
  public float getFloat(Column c, int position) {
    return c.getLong(position);
  }

  @Override
  public double getDouble(Column c, int position) {
    return c.getLong(position);
  }

  @Override
  public void writeInt(ColumnBuilder builder, int value) {
    builder.writeLong(value);
  }

  @Override
  public void writeLong(ColumnBuilder builder, long value) {
    builder.writeLong(value);
  }

  @Override
  public void writeFloat(ColumnBuilder builder, float value) {
    builder.writeLong((long) value);
  }

  @Override
  public void writeDouble(ColumnBuilder builder, double value) {
    builder.writeLong((long) value);
  }

  @Override
  public void addValue(int rowIndex, Object value, Object column) {
    checkValueType(value, Long.class, "Long");
    ((long[]) column)[rowIndex] = value != null ? (long) value : Long.MIN_VALUE;
  }

  @Override
  public void copyArrayElement(Object source, int sourceIndex, Object target, int targetIndex) {
    ((long[]) target)[targetIndex] = ((long[]) source)[sourceIndex];
  }

  @Override
  public Object createArray(int capacity) {
    return new long[capacity];
  }

  @Override
  public long estimateArraySize(int size) {
    return RamUsageEstimator.sizeOfLongArray(size);
  }

  @Override
  public long estimateValueSize() {
    return Long.BYTES;
  }

  @Override
  public int serializedSize(Object column, int rowSize) {
    return Math.multiplyExact(Long.BYTES, rowSize);
  }

  @Override
  public void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    long[] values = (long[]) array;
    for (int i = 0; i < rowSize; i++) {
      ReadWriteIOUtils.write(values[i], stream);
    }
  }

  @Override
  public Object deserializeArray(ByteBuffer buffer, int rowSize) {
    long[] values = new long[rowSize];
    for (int i = 0; i < rowSize; i++) {
      values[i] = ReadWriteIOUtils.readLong(buffer);
    }
    return values;
  }

  @Override
  public Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    long[] intValues = new long[rowSize];
    if (nullIndicators == null) {
      for (int i = 0; i < rowSize; i++) {
        intValues[i] = buffer.getLong();
      }
    } else {
      for (int i = 0; i < rowSize; i++) {
        if (!nullIndicators[i]) {
          intValues[i] = buffer.getLong();
        }
      }
    }
    return new LongColumn(0, rowSize, nullIndicators, intValues);
  }

  @Override
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return hasEnoughLength(left, right, rowSize)
        && Arrays.equals((long[]) left, 0, rowSize, (long[]) right, 0, rowSize);
  }

  @Override
  public long arrayRamBytesUsed(Object array) {
    return RamUsageEstimator.sizeOf((long[]) array);
  }

  @Override
  public Column createColumnWithMaxPosition(int positionCount) {
    return new LongColumn(
        positionCount, Optional.of(new boolean[positionCount]), new long[positionCount]);
  }

  @Override
  public Column createColumnWithZeroPosition(int positionCount) {
    return new LongColumn(positionCount);
  }

  @Override
  public Column createNullColumn(int positionCount) {
    return new RunLengthEncodedColumn(LongColumnBuilder.NULL_VALUE_BLOCK, positionCount);
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    return new LongColumnBuilder(null, expectedEntries);
  }

  @Override
  public boolean isComparable() {
    return true;
  }

  @Override
  public boolean isOrderable() {
    return true;
  }

  @Override
  public List<Type> getTypeParameters() {
    return Collections.emptyList();
  }
}
