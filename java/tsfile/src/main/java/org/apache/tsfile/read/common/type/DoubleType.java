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
import org.apache.tsfile.block.column.ColumnBuilderStatus;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.encoding.decoder.CamelDecoder;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.decoder.DoublePrecisionChimpDecoder;
import org.apache.tsfile.encoding.decoder.DoublePrecisionDecoderV1;
import org.apache.tsfile.encoding.decoder.DoublePrecisionDecoderV2;
import org.apache.tsfile.encoding.decoder.DoubleRLBEDecoder;
import org.apache.tsfile.encoding.decoder.DoubleSprintzDecoder;
import org.apache.tsfile.encoding.decoder.FloatDecoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumnBuilder;
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
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DoubleType extends AbstractType {

  public static final DoubleType DOUBLE = new DoubleType();

  private DoubleType() {}

  @Override
  public void toBytes(TsPrimitiveType value, byte[] valueBytes, int offset) {
    BytesUtils.doubleToBytes(value.getDouble(), valueBytes, offset);
  }

  @Override
  public int calcTypeSize(TsPrimitiveType value) {
    return Double.BYTES;
  }

  @Override
  public int calcTypeSize(Object value) {
    return Double.BYTES;
  }

  @Override
  public void init(BatchData batchData) {
    batchData.initDoubleValues();
  }

  @Override
  public void put(BatchData batchData, long timestamp, Object value) {
    batchData.putDouble(timestamp, (double) value);
  }

  @Override
  public int serialize(BatchData batchData, DataOutputStream outputStream, boolean isDesc)
      throws IOException {
    for (int i = 0; i < batchData.length(); i++) {
      int index = isDesc ? batchData.length() - 1 - i : i;
      outputStream.writeLong(batchData.getTimeByIndex(index));
      outputStream.writeDouble(batchData.getDoubleByIndex(index));
    }
    return Math.multiplyExact(Long.BYTES + Double.BYTES, batchData.length());
  }

  @Override
  public void deserialize(ByteBuffer buffer, BatchData batchData, int length) {
    for (int i = 0; i < length; i++) {
      batchData.putDouble(buffer.getLong(), buffer.getDouble());
    }
  }

  @Override
  public int serialize(TsPrimitiveType value, DataOutputStream stream) throws IOException {
    stream.writeDouble(value.getDouble());
    return Double.BYTES;
  }

  @Override
  public int serializeValue(Object value, ByteBuffer buffer) {
    buffer.putDouble((double) value);
    return Double.BYTES;
  }

  @Override
  public int serializeValue(Object value, DataOutputStream stream) throws IOException {
    stream.writeDouble((double) value);
    return Double.BYTES;
  }

  @Override
  public TSEncoding getDefaultEncoding(TSFileConfig config) {
    return TSEncoding.valueOf(config.getDoubleEncoding());
  }

  @Override
  public CompressionType getDefaultCompressor(TSFileConfig config) {
    return config.getDoubleCompressor();
  }

  @Override
  public Decoder getDecoder(TSEncoding encoding) {
    return switch (encoding) {
      case PLAIN, DICTIONARY -> super.getDecoder(encoding);
      case RLE, TS_2DIFF -> new FloatDecoder(encoding, TSDataType.DOUBLE);
      case GORILLA_V1 -> new DoublePrecisionDecoderV1();
      case GORILLA -> new DoublePrecisionDecoderV2();
      case CHIMP -> new DoublePrecisionChimpDecoder();
      case SPRINTZ -> new DoubleSprintzDecoder();
      case RLBE -> new DoubleRLBEDecoder();
      case CAMEL -> new CamelDecoder();
      default -> throw decoderNotFound(encoding);
    };
  }

  @Override
  public int getOneItemMaxSize(int valveLength) {
    return Double.BYTES;
  }

  @Override
  public Statistics<?> createStatistics() {
    return new DoubleStatistics();
  }

  @Override
  public long getStatisticsInstanceSize() {
    return DoubleStatistics.INSTANCE_SIZE;
  }

  @Override
  public void update(Statistics<?> stats, long timestamp, TsPrimitiveType value) {
    stats.update(timestamp, value.getDouble());
  }

  @Override
  public void update(Statistics<?> stats, BatchData batchData) {
    stats.update(batchData.currentTime(), batchData.getDouble());
  }

  @Override
  public void update(Statistics<?> stats, TsBlock block, int columnIndex, int rowIndex) {
    stats.update(block.getTimeByIndex(rowIndex), block.getColumn(columnIndex).getDouble(rowIndex));
  }

  @Override
  public Object getCurrentValue(BatchData batchData) {
    return batchData.getDouble();
  }

  @Override
  public String toString(Field field) {
    return String.valueOf(field.getDoubleV());
  }

  @Override
  public Object getValue(Field field) {
    return field.getDoubleV();
  }

  @Override
  public void setTo(TsPrimitiveType from, Field to) {
    to.setDoubleV(from.getDouble());
  }

  @Override
  public void setTo(TsPrimitiveType from, Column to, int index) {
    to.getDoubles()[index] = from.getDouble();
  }

  @Override
  public void setTo(Object value, Column to, int startIndex, int endIndex) {
    Arrays.fill(to.getDoubles(), startIndex, endIndex, (double) value);
  }

  @Override
  public void setTo(Field from, Field to) {
    to.setDoubleV(from.getDoubleV());
  }

  @Override
  public void setTo(BatchData from, Field to) {
    to.setDoubleV(from.getDouble());
  }

  @Override
  public void setTo(BatchData from, Column to, int index) {
    to.getDoubles()[index] = from.getDouble();
  }

  @Override
  public void setTo(Column from, int fromIndex, Column to, int toIndex) {
    to.getDoubles()[toIndex] = from.getDouble(fromIndex);
  }

  @Override
  public void setTo(Column from, int fromIndex, Object toArray, int toIndex) {
    ((double[]) toArray)[toIndex] = from.getDouble(fromIndex);
  }

  @Override
  public void addPoint(TSRecord record, String columnName, Field field) {
    record.addPoint(columnName, field.getDoubleV());
  }

  @Override
  public void addPoint(TSRecord record, String columnName, ResultSet resultSet) {
    record.addPoint(columnName, resultSet.getDouble(columnName));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, String value) {
    return new DoubleDataPoint(measurementId, Double.parseDouble(value));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, long value) {
    return new DoubleDataPoint(measurementId, (double) value);
  }

  @Override
  public void write(ValueChunkWriter writer, long time, Object value, boolean isNull) {
    writer.write(time, isNull ? 0.0D : (double) value, isNull);
  }

  @Override
  public void write(ValueChunkWriter writer, long time, TsPrimitiveType value) {
    writer.write(time, value != null ? value.getDouble() : Double.MAX_VALUE, value == null);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Object array, int rowIndex, boolean isNull) {
    writer.write(time, ((double[]) array)[rowIndex], isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Column column, int rowIndex, boolean isNull) {
    writer.write(time, isNull ? 0 : column.getDouble(rowIndex), isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long[] times, Column column, int batchSize, int arrayOffset) {
    writer.write(times, column.getDoubles(), column.isNull(), batchSize, arrayOffset);
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    writer.write(time, ((double[]) column)[rowIndex]);
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, TsPrimitiveType value) {
    writer.write(time, value.getDouble());
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, BatchData data) {
    writer.write(time, data.getDouble());
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object value) {
    writer.write(time, (double) value);
  }

  @Override
  public Field getField(Object value) {
    Field field = new Field(TSDataType.DOUBLE);
    field.setDoubleV((double) value);
    return field;
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType() {
    return new TsPrimitiveType.TsDouble();
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(Object value) {
    return new TsPrimitiveType.TsDouble((double) value);
  }

  @Override
  public TsPrimitiveType getValueAsTsPrimitiveType(Object array, int rowIndex) {
    return new TsPrimitiveType.TsDouble(((double[]) array)[rowIndex]);
  }

  @Override
  public int getInt(Column c, int position) {
    return (int) c.getDouble(position);
  }

  @Override
  public long getLong(Column c, int position) {
    return (long) c.getDouble(position);
  }

  @Override
  public float getFloat(Column c, int position) {
    return (float) c.getDouble(position);
  }

  @Override
  public double getDouble(Column c, int position) {
    return c.getDouble(position);
  }

  @Override
  public void write(ColumnBuilder builder, TsPrimitiveType value) {
    builder.writeDouble(value.getDouble());
  }

  @Override
  public void write(ColumnBuilder builder, byte[] bytes, int offset) {
    builder.writeDouble(BytesUtils.bytesToDouble(bytes, offset));
  }

  @Override
  public void write(ColumnBuilder builder, Column column, int index) {
    builder.writeDouble(column.getDouble(index));
  }

  @Override
  public void write(TsPrimitiveType from, Object toArray, int index) {
    ((double[]) toArray)[index] = from.getDouble();
  }

  @Override
  public void write(BatchData from, Object toArray, int index) {
    ((double[]) toArray)[index] = from.getDouble();
  }

  @Override
  public void writeInt(ColumnBuilder builder, int value) {
    builder.writeDouble(value);
  }

  @Override
  public void writeLong(ColumnBuilder builder, long value) {
    builder.writeDouble(value);
  }

  @Override
  public void writeFloat(ColumnBuilder builder, float value) {
    builder.writeDouble(value);
  }

  @Override
  public void writeDouble(ColumnBuilder builder, double value) {
    builder.writeDouble(value);
  }

  @Override
  public void addValue(int rowIndex, Object value, Object array) {
    checkValueType(value, Double.class, "Double");
    ((double[]) array)[rowIndex] = value != null ? (double) value : Double.MIN_VALUE;
  }

  @Override
  public void copyArrayElement(Object source, int sourceIndex, Object target, int targetIndex) {
    ((double[]) target)[targetIndex] = ((double[]) source)[sourceIndex];
  }

  @Override
  public Object arrayCopyOf(Object array, int newLength) {
    return Arrays.copyOf((double[]) array, newLength);
  }

  @Override
  public Object createArray(int capacity) {
    return new double[capacity];
  }

  @Override
  public long estimateArraySize(int size) {
    return RamUsageEstimator.sizeOfDoubleArray(size);
  }

  @Override
  public long estimateValueSize() {
    return Double.BYTES;
  }

  @Override
  public int serializedSize(Object array, int rowSize) {
    return Math.multiplyExact(Double.BYTES, rowSize);
  }

  @Override
  public void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    double[] values = (double[]) array;
    for (int i = 0; i < rowSize; i++) {
      ReadWriteIOUtils.write(values[i], stream);
    }
  }

  @Override
  public void serializeArray(Object array, int length, ByteBuffer buffer) {
    double[] values = (double[]) array;
    for (int i = 0; i < length; i++) {
      buffer.putDouble(values[i]);
    }
  }

  @Override
  public Object deserializeArray(ByteBuffer buffer, int rowSize) {
    double[] values = new double[rowSize];
    for (int i = 0; i < rowSize; i++) {
      values[i] = ReadWriteIOUtils.readDouble(buffer);
    }
    return values;
  }

  @Override
  public Object deserializeArray(DataInputStream stream, int rowSize) throws IOException {
    double[] values = new double[rowSize];
    for (int i = 0; i < rowSize; i++) {
      values[i] = ReadWriteIOUtils.readDouble(stream);
    }
    return values;
  }

  @Override
  public void deserialize(Object[] array, int index, ByteBuffer buffer) {
    array[index] = ReadWriteIOUtils.readDouble(buffer);
  }

  @Override
  public void deserialize(Object[] array, int index, InputStream stream) throws IOException {
    array[index] = ReadWriteIOUtils.readDouble(stream);
  }

  @Override
  public Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    double[] values = new double[rowSize];
    if (nullIndicators == null) {
      for (int i = 0; i < rowSize; i++) {
        values[i] = Double.longBitsToDouble(buffer.getLong());
      }
    } else {
      for (int i = 0; i < rowSize; i++) {
        if (!nullIndicators[i]) {
          values[i] = Double.longBitsToDouble(buffer.getLong());
        }
      }
    }
    return new DoubleColumn(rowSize, Optional.ofNullable(nullIndicators), values);
  }

  @Override
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return hasEnoughLength(left, right, rowSize)
        && Arrays.equals((double[]) left, 0, rowSize, (double[]) right, 0, rowSize);
  }

  @Override
  public long estimateArraySize(Object array) {
    return RamUsageEstimator.sizeOf((double[]) array);
  }

  @Override
  public Column createColumnWithMaxPosition(int positionCount) {
    return new DoubleColumn(
        positionCount, Optional.of(new boolean[positionCount]), new double[positionCount]);
  }

  @Override
  public Column createColumnWithZeroPosition(int positionCount) {
    return new DoubleColumn(positionCount);
  }

  @Override
  public Column createNullColumn(int positionCount) {
    return new RunLengthEncodedColumn(DoubleColumnBuilder.NULL_VALUE_BLOCK, positionCount);
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    return new DoubleColumnBuilder(null, expectedEntries);
  }

  @Override
  public ColumnBuilder createColumnBuilder(
      ColumnBuilderStatus columnBuilderStatus, int expectedEntries) {
    return new DoubleColumnBuilder(columnBuilderStatus, expectedEntries);
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.DOUBLE;
  }

  @Override
  public String getDisplayName() {
    return "DOUBLE";
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

  public static DoubleType getInstance() {
    return DOUBLE;
  }
}
