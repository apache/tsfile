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
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.tsfile.utils.BytesUtils.parseObjectByteArrayToString;

public class ObjectType extends AbstractType {

  public static final ObjectType OBJECT = new ObjectType();

  private ObjectType() {}

  @Override
  public void toBytes(TsPrimitiveType value, byte[] valueBytes, int offset) {
    binaryToBytes(value, valueBytes, offset);
  }

  @Override
  public int calcTypeSize(TsPrimitiveType value) {
    return binaryTypeSize(value);
  }

  @Override
  public int calcTypeSize(Object value) {
    return binaryTypeSize(value);
  }

  @Override
  public void init(BatchData batchData) {
    batchData.initBinaryValues();
  }

  @Override
  public void put(BatchData batchData, long timestamp, Object value) {
    batchData.putBinary(timestamp, (Binary) value);
  }

  @Override
  public void serialize(BatchData batchData, DataOutputStream outputStream, boolean isDesc)
      throws IOException {
    for (int i = 0; i < batchData.length(); i++) {
      int index = isDesc ? batchData.length() - 1 - i : i;
      outputStream.writeLong(batchData.getTimeByIndex(index));
      Binary binary = batchData.getBinaryByIndex(index);
      outputStream.writeInt(binary.getLength());
      outputStream.write(binary.getValues());
    }
  }

  @Override
  public void serialize(TsPrimitiveType value, DataOutputStream stream) throws IOException {
    Binary binary = value.getBinary();
    stream.writeInt(binary.getLength());
    stream.write(binary.getValues());
  }

  @Override
  public TSEncoding getDefaultEncoding(TSFileConfig config) {
    return TSEncoding.valueOf(config.getTextEncoding());
  }

  @Override
  public CompressionType getDefaultCompressor(TSFileConfig config) {
    return config.getTextCompressor();
  }

  @Override
  public int getOneItemMaxSize(int valveLength) {
    return Integer.BYTES + TSFileConfig.BYTE_SIZE_PER_CHAR * valveLength;
  }

  @Override
  public void update(Statistics<?> stats, long timestamp, TsPrimitiveType value) {
    stats.update(timestamp, value.getBinary());
  }

  @Override
  public void update(Statistics<?> stats, BatchData batchData) {
    stats.update(batchData.currentTime(), batchData.getBinary());
  }

  @Override
  public Object getCurrentValue(BatchData batchData) {
    return batchData.getBinary();
  }

  @Override
  public String toString(Field field) {
    return parseObjectByteArrayToString(field.getBinaryV().getValues());
  }

  @Override
  public Object getValue(Field field) {
    return field.getStringValue();
  }

  @Override
  public void setTo(TsPrimitiveType from, Field to) {
    to.setBinaryV(from.getBinary());
  }

  @Override
  public void addPoint(TSRecord record, String columnName, ResultSet resultSet) {
    record.addPoint(columnName, resultSet.getBinary(columnName));
  }

  @Override
  public DataPoint getDataPoint(String measurementId, String value) {
    return new StringDataPoint(measurementId, new Binary(value, TSFileConfig.STRING_CHARSET));
  }

  @Override
  public void write(ValueChunkWriter writer, long time, Object value, boolean isNull) {
    writer.write(time, (Binary) value, isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Object column, int rowIndex, boolean isNull) {
    writer.write(time, ((Binary[]) column)[rowIndex], isNull);
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    writer.write(time, ((Binary[]) column)[rowIndex]);
  }

  @Override
  public Field getField(Object value) {
    Field field = new Field(TSDataType.OBJECT);
    field.setBinaryV((Binary) value);
    return field;
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType() {
    return new TsPrimitiveType.TsBinary();
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(Object value) {
    return new TsPrimitiveType.TsBinary((Binary) value);
  }

  @Override
  public Binary getBinary(Column c, int position) {
    return c.getBinary(position);
  }

  @Override
  public void writeBinary(ColumnBuilder builder, Binary value) {
    builder.writeBinary(value);
  }

  @Override
  public void addValue(int rowIndex, Object value, Object column) {
    if (value != null && !(value instanceof Binary) && !(value instanceof String)) {
      throw new IllegalArgumentException(
          Messages.format(
              "error.write.tablet_expected_type",
              "Binary or String",
              getDisplayName(),
              value.getClass().getName()));
    }
    if (value instanceof Binary) {
      ((Binary[]) column)[rowIndex] = (Binary) value;
    } else {
      ((Binary[]) column)[rowIndex] =
          value != null
              ? new Binary(((String) value).getBytes(TSFileConfig.STRING_CHARSET))
              : Binary.EMPTY_VALUE;
    }
  }

  @Override
  public Object createArray(int capacity) {
    return new Binary[capacity];
  }

  @Override
  public long estimateArraySize(int size) {
    return RamUsageEstimator.sizeOfObjectArray(size);
  }

  @Override
  public long estimateValueSize() {
    return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
  }

  @Override
  public int serializedSize(Object column, int rowSize) {
    return serializedSizeOfBinaryValues(column, rowSize);
  }

  @Override
  public void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    serializeBinaryValues(array, rowSize, stream);
  }

  @Override
  public Object deserializeArray(ByteBuffer buffer, int rowSize) {
    return deserializeBinaryValues(buffer, rowSize);
  }

  @Override
  public Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    Binary[] values = deserializeBinaryValues(buffer, rowSize, nullIndicators);
    return new BinaryColumn(0, rowSize, nullIndicators, values);
  }

  @Override
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return binaryArrayEquals(left, right, rowSize);
  }

  @Override
  public long arrayRamBytesUsed(Object array) {
    return RamUsageEstimator.sizeOf((Binary[]) array);
  }

  @Override
  public Column createColumnWithMaxPosition(int positionCount) {
    return new BinaryColumn(
        positionCount, Optional.of(new boolean[positionCount]), new Binary[positionCount]);
  }

  @Override
  public Column createColumnWithZeroPosition(int positionCount) {
    return new BinaryColumn(positionCount);
  }

  @Override
  public Column createNullColumn(int positionCount) {
    return new RunLengthEncodedColumn(BinaryColumnBuilder.NULL_VALUE_BLOCK, positionCount);
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    return new BinaryColumnBuilder(null, expectedEntries);
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.OBJECT;
  }

  @Override
  public String getDisplayName() {
    return "OBJECT";
  }

  @Override
  public boolean isComparable() {
    return false;
  }

  @Override
  public boolean isOrderable() {
    return false;
  }

  @Override
  public List<Type> getTypeParameters() {
    return Collections.emptyList();
  }

  public static ObjectType getInstance() {
    return OBJECT;
  }
}
