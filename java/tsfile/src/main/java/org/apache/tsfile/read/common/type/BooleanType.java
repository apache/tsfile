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
import org.apache.tsfile.encoding.decoder.IntRleDecoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.tsfile.read.common.block.column.ColumnEncoder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.record.datapoint.DataPoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BooleanType extends AbstractType {

  public static final BooleanType BOOLEAN = new BooleanType();

  private BooleanType() {}

  @Override
  public void toBytes(TsPrimitiveType value, byte[] valueBytes, int offset) {
    BytesUtils.boolToBytes(value.getBoolean(), valueBytes, offset);
  }

  @Override
  public int calcTypeSize(TsPrimitiveType value) {
    return Byte.BYTES;
  }

  @Override
  public int calcTypeSize(Object value) {
    return Byte.BYTES;
  }

  @Override
  public void init(BatchData batchData) {
    batchData.initBooleanValues();
  }

  @Override
  public void put(BatchData batchData, long timestamp, Object value) {
    batchData.putBoolean(timestamp, (boolean) value);
  }

  @Override
  public void serialize(BatchData batchData, DataOutputStream outputStream, boolean isDesc)
      throws IOException {
    for (int i = 0; i < batchData.length(); i++) {
      int index = isDesc ? batchData.length() - 1 - i : i;
      outputStream.writeLong(batchData.getTimeByIndex(index));
      outputStream.writeBoolean(batchData.getBooleanByIndex(index));
    }
  }

  @Override
  public void serialize(TsPrimitiveType value, DataOutputStream stream) throws IOException {
    stream.writeBoolean(value.getBoolean());
  }

  @Override
  public TSEncoding getDefaultEncoding(TSFileConfig config) {
    return TSEncoding.valueOf(config.getBooleanEncoding());
  }

  @Override
  public CompressionType getDefaultCompressor(TSFileConfig config) {
    return config.getBooleanCompressor();
  }

  @Override
  public Decoder getDecoder(TSEncoding encoding) {
    return switch (encoding) {
      case PLAIN, DICTIONARY -> super.getDecoder(encoding);
      case RLE -> new IntRleDecoder();
      default -> throw decoderNotFound(encoding);
    };
  }

  @Override
  public int getOneItemMaxSize(int valveLength) {
    return Byte.BYTES;
  }

  @Override
  public void update(Statistics<?> stats, long timestamp, TsPrimitiveType value) {
    stats.update(timestamp, value.getBoolean());
  }

  @Override
  public void update(Statistics<?> stats, BatchData batchData) {
    stats.update(batchData.currentTime(), batchData.getBoolean());
  }

  @Override
  public Object getCurrentValue(BatchData batchData) {
    return batchData.getBoolean();
  }

  @Override
  public String toString(Field field) {
    return String.valueOf(field.getBoolV());
  }

  @Override
  public Object getValue(Field field) {
    return field.getBoolV();
  }

  @Override
  public void setTo(TsPrimitiveType from, Field to) {
    to.setBoolV(from.getBoolean());
  }

  @Override
  public void addPoint(TSRecord record, String columnName, ResultSet resultSet) {
    record.addPoint(columnName, resultSet.getBoolean(columnName));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, String value) {
    return new BooleanDataPoint(measurementId, Boolean.parseBoolean(value));
  }

  @Override
  public void write(ValueChunkWriter writer, long time, Object value, boolean isNull) {
    writer.write(time, (boolean) value, isNull);
  }

  @Override
  public Field getField(Object value) {
    Field field = new Field(TSDataType.BOOLEAN);
    field.setBoolV((boolean) value);
    return field;
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType() {
    return new TsPrimitiveType.TsBoolean();
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(Object value) {
    return new TsPrimitiveType.TsBoolean((boolean) value);
  }

  @Override
  public boolean getBoolean(Column c, int position) {
    return c.getBoolean(position);
  }

  @Override
  public void writeBoolean(ColumnBuilder builder, boolean value) {
    builder.writeBoolean(value);
  }

  @Override
  public void addValue(int rowIndex, Object value, Object column) {
    checkValueType(value, Boolean.class, "Boolean");
    ((boolean[]) column)[rowIndex] = value != null && (boolean) value;
  }

  @Override
  public Object createArray(int capacity) {
    return new boolean[capacity];
  }

  @Override
  public long estimateArraySize(int size) {
    return RamUsageEstimator.sizeOfBooleanArray(size);
  }

  @Override
  public long estimateValueSize() {
    return Byte.BYTES;
  }

  @Override
  public int serializedSize(Object column, int rowSize) {
    return rowSize;
  }

  @Override
  public void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    boolean[] values = (boolean[]) array;
    for (int i = 0; i < rowSize; i++) {
      ReadWriteIOUtils.write(BytesUtils.boolToByte(values[i]), stream);
    }
  }

  @Override
  public Object deserializeArray(ByteBuffer buffer, int rowSize) {
    boolean[] values = new boolean[rowSize];
    for (int i = 0; i < rowSize; i++) {
      values[i] = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(buffer));
    }
    return values;
  }

  @Override
  public Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    if (nullIndicators == null) {
      boolean[] values = ColumnEncoder.deserializeBooleanArray(buffer, rowSize);
      return new BooleanColumn(rowSize, Optional.empty(), values);
    }

    int nonNullCount = 0;
    for (boolean isNull : nullIndicators) {
      if (!isNull) {
        nonNullCount++;
      }
    }

    boolean[] nonNullValues = ColumnEncoder.deserializeBooleanArray(buffer, nonNullCount);
    boolean[] values = new boolean[rowSize];
    int nonNullIndex = 0;
    for (int i = 0; i < rowSize; i++) {
      if (!nullIndicators[i]) {
        values[i] = nonNullValues[nonNullIndex++];
      }
    }
    return new BooleanColumn(rowSize, Optional.of(nullIndicators), values);
  }

  @Override
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return hasEnoughLength(left, right, rowSize)
        && Arrays.equals((boolean[]) left, 0, rowSize, (boolean[]) right, 0, rowSize);
  }

  @Override
  public long arrayRamBytesUsed(Object array) {
    return RamUsageEstimator.sizeOf((boolean[]) array);
  }

  @Override
  public Column createColumnWithMaxPosition(int positionCount) {
    return new BooleanColumn(
        positionCount, Optional.of(new boolean[positionCount]), new boolean[positionCount]);
  }

  @Override
  public Column createColumnWithZeroPosition(int positionCount) {
    return new BooleanColumn(positionCount);
  }

  @Override
  public Column createNullColumn(int positionCount) {
    return new RunLengthEncodedColumn(BooleanColumnBuilder.NULL_VALUE_BLOCK, positionCount);
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    return new BooleanColumnBuilder(null, expectedEntries);
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.BOOLEAN;
  }

  @Override
  public String getDisplayName() {
    return "BOOLEAN";
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

  public static BooleanType getInstance() {
    return BOOLEAN;
  }
}
