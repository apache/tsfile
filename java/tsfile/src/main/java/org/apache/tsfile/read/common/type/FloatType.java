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
import org.apache.tsfile.encoding.decoder.FloatDecoder;
import org.apache.tsfile.encoding.decoder.FloatRLBEDecoder;
import org.apache.tsfile.encoding.decoder.FloatSprintzDecoder;
import org.apache.tsfile.encoding.decoder.SinglePrecisionChimpDecoder;
import org.apache.tsfile.encoding.decoder.SinglePrecisionDecoderV1;
import org.apache.tsfile.encoding.decoder.SinglePrecisionDecoderV2;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
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
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class FloatType extends AbstractType {

  public static final FloatType FLOAT = new FloatType();

  private FloatType() {}

  @Override
  public void toBytes(TsPrimitiveType value, byte[] valueBytes, int offset) {
    BytesUtils.floatToBytes(value.getFloat(), valueBytes, offset);
  }

  @Override
  public int calcTypeSize(TsPrimitiveType value) {
    return Float.BYTES;
  }

  @Override
  public int calcTypeSize(Object value) {
    return Float.BYTES;
  }

  @Override
  public void init(BatchData batchData) {
    batchData.initFloatValues();
  }

  @Override
  public void put(BatchData batchData, long timestamp, Object value) {
    batchData.putFloat(timestamp, (float) value);
  }

  @Override
  public void serialize(BatchData batchData, DataOutputStream outputStream, boolean isDesc)
      throws IOException {
    for (int i = 0; i < batchData.length(); i++) {
      int index = isDesc ? batchData.length() - 1 - i : i;
      outputStream.writeLong(batchData.getTimeByIndex(index));
      outputStream.writeFloat(batchData.getFloatByIndex(index));
    }
  }

  @Override
  public void serialize(TsPrimitiveType value, DataOutputStream stream) throws IOException {
    stream.writeFloat(value.getFloat());
  }

  @Override
  public TSEncoding getDefaultEncoding(TSFileConfig config) {
    return TSEncoding.valueOf(config.getFloatEncoding());
  }

  @Override
  public CompressionType getDefaultCompressor(TSFileConfig config) {
    return config.getFloatCompressor();
  }

  @Override
  public Decoder getDecoder(TSEncoding encoding) {
    return switch (encoding) {
      case PLAIN, DICTIONARY -> super.getDecoder(encoding);
      case RLE, TS_2DIFF -> new FloatDecoder(encoding, TSDataType.FLOAT);
      case GORILLA_V1 -> new SinglePrecisionDecoderV1();
      case GORILLA -> new SinglePrecisionDecoderV2();
      case CHIMP -> new SinglePrecisionChimpDecoder();
      case SPRINTZ -> new FloatSprintzDecoder();
      case RLBE -> new FloatRLBEDecoder();
      default -> throw decoderNotFound(encoding);
    };
  }

  @Override
  public int getOneItemMaxSize(int valveLength) {
    return Float.BYTES;
  }

  @Override
  public void update(Statistics<?> stats, long timestamp, TsPrimitiveType value) {
    stats.update(timestamp, value.getFloat());
  }

  @Override
  public void update(Statistics<?> stats, BatchData batchData) {
    stats.update(batchData.currentTime(), batchData.getFloat());
  }

  @Override
  public Object getCurrentValue(BatchData batchData) {
    return batchData.getFloat();
  }

  @Override
  public String toString(Field field) {
    return String.valueOf(field.getFloatV());
  }

  @Override
  public Object getValue(Field field) {
    return field.getFloatV();
  }

  @Override
  public void setTo(TsPrimitiveType from, Field to) {
    to.setFloatV(from.getFloat());
  }

  @Override
  public void setTo(Column from, int fromIndex, Object toArray, int toIndex) {
    ((float[]) toArray)[toIndex] = from.getFloat(fromIndex);
  }

  @Override
  public void addPoint(TSRecord record, String columnName, ResultSet resultSet) {
    record.addPoint(columnName, resultSet.getFloat(columnName));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, String value) {
    return new FloatDataPoint(measurementId, Float.parseFloat(value));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, long value) {
    return new FloatDataPoint(measurementId, (float) value);
  }

  @Override
  public void write(ValueChunkWriter writer, long time, Object value, boolean isNull) {
    writer.write(time, (float) value, isNull);
  }

  @Override
  public void write(ValueChunkWriter writer, long time, TsPrimitiveType value) {
    writer.write(time, value != null ? value.getFloat() : Float.MAX_VALUE, value == null);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Object array, int rowIndex, boolean isNull) {
    writer.write(time, ((float[]) array)[rowIndex], isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Column column, int rowIndex, boolean isNull) {
    writer.write(time, isNull ? 0 : column.getFloat(rowIndex), isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long[] times, Column column, int batchSize, int arrayOffset) {
    writer.write(times, column.getFloats(), column.isNull(), batchSize, arrayOffset);
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    writer.write(time, ((float[]) column)[rowIndex]);
  }

  @Override
  public Field getField(Object value) {
    Field field = new Field(TSDataType.FLOAT);
    field.setFloatV((float) value);
    return field;
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType() {
    return new TsPrimitiveType.TsFloat();
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(Object value) {
    return new TsPrimitiveType.TsFloat((float) value);
  }

  @Override
  public int getInt(Column c, int position) {
    return (int) c.getFloat(position);
  }

  @Override
  public long getLong(Column c, int position) {
    return (long) c.getFloat(position);
  }

  @Override
  public float getFloat(Column c, int position) {
    return c.getFloat(position);
  }

  @Override
  public double getDouble(Column c, int position) {
    return c.getFloat(position);
  }

  @Override
  public void writeInt(ColumnBuilder builder, int value) {
    builder.writeFloat(value);
  }

  @Override
  public void writeLong(ColumnBuilder builder, long value) {
    builder.writeFloat(value);
  }

  @Override
  public void writeFloat(ColumnBuilder builder, float value) {
    builder.writeFloat(value);
  }

  @Override
  public void writeDouble(ColumnBuilder builder, double value) {
    builder.writeFloat((float) value);
  }

  @Override
  public void addValue(int rowIndex, Object value, Object column) {
    checkValueType(value, Float.class, "Float");
    ((float[]) column)[rowIndex] = value != null ? (float) value : Float.MIN_VALUE;
  }

  @Override
  public void copyArrayElement(Object source, int sourceIndex, Object target, int targetIndex) {
    ((float[]) target)[targetIndex] = ((float[]) source)[sourceIndex];
  }

  @Override
  public Object arrayCopyOf(Object array, int newLength) {
    return Arrays.copyOf((float[]) array, newLength);
  }

  @Override
  public Object createArray(int capacity) {
    return new float[capacity];
  }

  @Override
  public long estimateArraySize(int size) {
    return RamUsageEstimator.sizeOfFloatArray(size);
  }

  @Override
  public long estimateValueSize() {
    return Float.BYTES;
  }

  @Override
  public int serializedSize(Object column, int rowSize) {
    return Math.multiplyExact(Float.BYTES, rowSize);
  }

  @Override
  public void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    float[] values = (float[]) array;
    for (int i = 0; i < rowSize; i++) {
      ReadWriteIOUtils.write(values[i], stream);
    }
  }

  @Override
  public Object deserializeArray(ByteBuffer buffer, int rowSize) {
    float[] values = new float[rowSize];
    for (int i = 0; i < rowSize; i++) {
      values[i] = ReadWriteIOUtils.readFloat(buffer);
    }
    return values;
  }

  @Override
  public Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    float[] values = new float[rowSize];
    if (nullIndicators == null) {
      for (int i = 0; i < rowSize; i++) {
        values[i] = Float.intBitsToFloat(buffer.getInt());
      }
    } else {
      for (int i = 0; i < rowSize; i++) {
        if (!nullIndicators[i]) {
          values[i] = Float.intBitsToFloat(buffer.getInt());
        }
      }
    }
    return new FloatColumn(rowSize, Optional.ofNullable(nullIndicators), values);
  }

  @Override
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return hasEnoughLength(left, right, rowSize)
        && Arrays.equals((float[]) left, 0, rowSize, (float[]) right, 0, rowSize);
  }

  @Override
  public long arrayRamBytesUsed(Object array) {
    return RamUsageEstimator.sizeOf((float[]) array);
  }

  @Override
  public Column createColumnWithMaxPosition(int positionCount) {
    return new FloatColumn(
        positionCount, Optional.of(new boolean[positionCount]), new float[positionCount]);
  }

  @Override
  public Column createColumnWithZeroPosition(int positionCount) {
    return new FloatColumn(positionCount);
  }

  @Override
  public Column createNullColumn(int positionCount) {
    return new RunLengthEncodedColumn(FloatColumnBuilder.NULL_VALUE_BLOCK, positionCount);
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    return new FloatColumnBuilder(null, expectedEntries);
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.FLOAT;
  }

  @Override
  public String getDisplayName() {
    return "FLOAT";
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

  public static FloatType getInstance() {
    return FLOAT;
  }
}
