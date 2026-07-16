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
import org.apache.tsfile.encoding.decoder.IntChimpDecoder;
import org.apache.tsfile.encoding.decoder.IntGorillaDecoder;
import org.apache.tsfile.encoding.decoder.IntRLBEDecoder;
import org.apache.tsfile.encoding.decoder.IntRleDecoder;
import org.apache.tsfile.encoding.decoder.IntSprintzDecoder;
import org.apache.tsfile.encoding.decoder.IntZigzagDecoder;
import org.apache.tsfile.encoding.decoder.RegularDataDecoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
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
import org.apache.tsfile.write.record.datapoint.IntDataPoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public abstract class AbstractIntType extends AbstractType {

  @Override
  public void toBytes(TsPrimitiveType value, byte[] valueBytes, int offset) {
    BytesUtils.intToBytes(value.getInt(), valueBytes, offset);
  }

  @Override
  public int calcTypeSize(TsPrimitiveType value) {
    return Integer.BYTES;
  }

  @Override
  public int calcTypeSize(Object value) {
    return Integer.BYTES;
  }

  @Override
  public void init(BatchData batchData) {
    batchData.initIntValues();
  }

  @Override
  public void put(BatchData batchData, long timestamp, Object value) {
    batchData.putInt(timestamp, (int) value);
  }

  @Override
  public void serialize(BatchData batchData, DataOutputStream outputStream, boolean isDesc)
      throws IOException {
    for (int i = 0; i < batchData.length(); i++) {
      int index = isDesc ? batchData.length() - 1 - i : i;
      outputStream.writeLong(batchData.getTimeByIndex(index));
      outputStream.writeInt(batchData.getIntByIndex(index));
    }
  }

  @Override
  public void serialize(TsPrimitiveType value, DataOutputStream stream) throws IOException {
    stream.writeInt(value.getInt());
  }

  @Override
  public void serializeValue(Object value, ByteBuffer buffer) {
    buffer.putInt((int) value);
  }

  @Override
  public void serializeValue(Object value, DataOutputStream stream) throws IOException {
    stream.writeInt((int) value);
  }

  @Override
  public TSEncoding getDefaultEncoding(TSFileConfig config) {
    return TSEncoding.valueOf(config.getInt32Encoding());
  }

  @Override
  public CompressionType getDefaultCompressor(TSFileConfig config) {
    return config.getInt32Compressor();
  }

  @Override
  public Decoder getDecoder(TSEncoding encoding) {
    return switch (encoding) {
      case PLAIN, DICTIONARY -> super.getDecoder(encoding);
      case RLE -> new IntRleDecoder();
      case TS_2DIFF -> new DeltaBinaryDecoder.IntDeltaDecoder();
      case REGULAR -> new RegularDataDecoder.IntRegularDecoder();
      case GORILLA -> new IntGorillaDecoder();
      case ZIGZAG -> new IntZigzagDecoder();
      case CHIMP -> new IntChimpDecoder();
      case SPRINTZ -> new IntSprintzDecoder();
      case RLBE -> new IntRLBEDecoder();
      default -> throw decoderNotFound(encoding);
    };
  }

  @Override
  public int getOneItemMaxSize(int valveLength) {
    return Integer.BYTES;
  }

  @Override
  public void update(Statistics<?> stats, long timestamp, TsPrimitiveType value) {
    stats.update(timestamp, value.getInt());
  }

  @Override
  public void update(Statistics<?> stats, BatchData batchData) {
    stats.update(batchData.currentTime(), batchData.getInt());
  }

  @Override
  public Object getCurrentValue(BatchData batchData) {
    return batchData.getInt();
  }

  @Override
  public String toString(Field field) {
    return String.valueOf(field.getIntV());
  }

  @Override
  public Object getValue(Field field) {
    return field.getIntV();
  }

  @Override
  public void setTo(TsPrimitiveType from, Field to) {
    to.setIntV(from.getInt());
  }

  @Override
  public void setTo(TsPrimitiveType from, Column to, int index) {
    to.getInts()[index] = from.getInt();
  }

  @Override
  public void setTo(Object value, Column to, int startIndex, int endIndex) {
    Arrays.fill(to.getInts(), startIndex, endIndex, (int) value);
  }

  @Override
  public void setTo(Field from, Field to) {
    to.setIntV(from.getIntV());
  }

  @Override
  public void setTo(BatchData from, Field to) {
    to.setIntV(from.getInt());
  }

  @Override
  public void setTo(BatchData from, Column to, int index) {
    to.getInts()[index] = from.getInt();
  }

  @Override
  public void setTo(Column from, int fromIndex, Object toArray, int toIndex) {
    ((int[]) toArray)[toIndex] = from.getInt(fromIndex);
  }

  @Override
  public void addPoint(TSRecord record, String columnName, Field field) {
    record.addPoint(columnName, field.getIntV());
  }

  @Override
  public void addPoint(TSRecord record, String columnName, ResultSet resultSet) {
    record.addPoint(columnName, resultSet.getInt(columnName));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, String value) {
    return new IntDataPoint(measurementId, Integer.parseInt(value));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, long value) {
    return new IntDataPoint(measurementId, (int) value);
  }

  @Override
  public void write(ValueChunkWriter writer, long time, Object value, boolean isNull) {
    writer.write(time, isNull ? 0 : (int) value, isNull);
  }

  @Override
  public void write(ValueChunkWriter writer, long time, TsPrimitiveType value) {
    writer.write(time, value != null ? value.getInt() : Integer.MAX_VALUE, value == null);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Object array, int rowIndex, boolean isNull) {
    writer.write(time, ((int[]) array)[rowIndex], isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Column column, int rowIndex, boolean isNull) {
    writer.write(time, isNull ? 0 : column.getInt(rowIndex), isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long[] times, Column column, int batchSize, int arrayOffset) {
    writer.write(times, column.getInts(), column.isNull(), batchSize, arrayOffset);
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    writer.write(time, ((int[]) column)[rowIndex]);
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType() {
    return new TsPrimitiveType.TsInt();
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(Object value) {
    return new TsPrimitiveType.TsInt((int) value);
  }

  @Override
  public int getInt(Column c, int position) {
    return c.getInt(position);
  }

  @Override
  public long getLong(Column c, int position) {
    return c.getInt(position);
  }

  @Override
  public float getFloat(Column c, int position) {
    return c.getInt(position);
  }

  @Override
  public double getDouble(Column c, int position) {
    return c.getInt(position);
  }

  @Override
  public void writeInt(ColumnBuilder builder, int value) {
    builder.writeInt(value);
  }

  @Override
  public void writeLong(ColumnBuilder builder, long value) {
    builder.writeInt((int) value);
  }

  @Override
  public void writeFloat(ColumnBuilder builder, float value) {
    builder.writeInt((int) value);
  }

  @Override
  public void writeDouble(ColumnBuilder builder, double value) {
    builder.writeInt((int) value);
  }

  @Override
  public void addValue(int rowIndex, Object value, Object array) {
    checkValueType(value, Integer.class, "Integer");
    ((int[]) array)[rowIndex] = value != null ? (int) value : Integer.MIN_VALUE;
  }

  @Override
  public void copyArrayElement(Object source, int sourceIndex, Object target, int targetIndex) {
    ((int[]) target)[targetIndex] = ((int[]) source)[sourceIndex];
  }

  @Override
  public Object arrayCopyOf(Object array, int newLength) {
    return Arrays.copyOf((int[]) array, newLength);
  }

  @Override
  public Object createArray(int capacity) {
    return new int[capacity];
  }

  @Override
  public long estimateArraySize(int size) {
    return RamUsageEstimator.sizeOfIntArray(size);
  }

  @Override
  public long estimateValueSize() {
    return Integer.BYTES;
  }

  @Override
  public int serializedSize(Object column, int rowSize) {
    return Math.multiplyExact(Integer.BYTES, rowSize);
  }

  @Override
  public void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    int[] values = (int[]) array;
    for (int i = 0; i < rowSize; i++) {
      ReadWriteIOUtils.write(values[i], stream);
    }
  }

  @Override
  public Object deserializeArray(ByteBuffer buffer, int rowSize) {
    int[] values = new int[rowSize];
    for (int i = 0; i < rowSize; i++) {
      values[i] = ReadWriteIOUtils.readInt(buffer);
    }
    return values;
  }

  @Override
  public void deserialize(Object[] array, int index, ByteBuffer buffer) {
    array[index] = ReadWriteIOUtils.readInt(buffer);
  }

  @Override
  public void deserialize(Object[] array, int index, InputStream stream) throws IOException {
    array[index] = ReadWriteIOUtils.readInt(stream);
  }

  @Override
  public Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    int[] intValues = new int[rowSize];
    if (nullIndicators == null) {
      for (int i = 0; i < rowSize; i++) {
        intValues[i] = buffer.getInt();
      }
    } else {
      for (int i = 0; i < rowSize; i++) {
        if (!nullIndicators[i]) {
          intValues[i] = buffer.getInt();
        }
      }
    }
    return new IntColumn(0, rowSize, nullIndicators, intValues);
  }

  @Override
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return hasEnoughLength(left, right, rowSize)
        && Arrays.equals((int[]) left, 0, rowSize, (int[]) right, 0, rowSize);
  }

  @Override
  public long estimateArraySize(Object array) {
    return RamUsageEstimator.sizeOf((int[]) array);
  }

  @Override
  public Column createColumnWithMaxPosition(int positionCount) {
    return new IntColumn(
        positionCount,
        Optional.of(new boolean[positionCount]),
        new int[positionCount],
        TSDataType.INT32);
  }

  @Override
  public Column createColumnWithZeroPosition(int positionCount) {
    return new IntColumn(positionCount);
  }

  @Override
  public Column createNullColumn(int positionCount) {
    return new RunLengthEncodedColumn(IntColumnBuilder.NULL_VALUE_BLOCK, positionCount);
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    return new IntColumnBuilder(null, expectedEntries);
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
