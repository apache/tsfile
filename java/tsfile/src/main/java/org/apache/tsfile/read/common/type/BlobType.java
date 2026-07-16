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
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BlobType extends AbstractType {

  public static final BlobType BLOB = new BlobType();

  private BlobType() {}

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
  public void serializeValue(Object value, ByteBuffer buffer) {
    ReadWriteIOUtils.write((Binary) value, buffer);
  }

  @Override
  public void serializeValue(Object value, DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write((Binary) value, stream);
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
    return BytesUtils.parseBlobByteArrayToString(field.getBinaryV().getValues());
  }

  @Override
  public Object getValue(Field field) {
    return field.getBinaryV();
  }

  @Override
  public void setTo(TsPrimitiveType from, Field to) {
    to.setBinaryV(from.getBinary());
  }

  @Override
  public void setTo(Field from, Field to) {
    to.setBinaryV(from.getBinaryV());
  }

  @Override
  public void setTo(BatchData from, Field to) {
    to.setBinaryV(from.getBinary());
  }

  @Override
  public void setTo(Column from, int fromIndex, Object toArray, int toIndex) {
    ((Binary[]) toArray)[toIndex] = from.getBinary(fromIndex);
  }

  @Override
  public void addPoint(TSRecord record, String columnName, Field field) {
    record.addPoint(columnName, field.getBinaryV().getValues());
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
  public void write(ValueChunkWriter writer, long time, TsPrimitiveType value) {
    writer.write(time, value != null ? value.getBinary() : Binary.EMPTY_VALUE, value == null);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Object array, int rowIndex, boolean isNull) {
    writer.write(time, ((Binary[]) array)[rowIndex], isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Column column, int rowIndex, boolean isNull) {
    writer.write(time, isNull ? null : column.getBinary(rowIndex), isNull);
  }

  @Override
  public void write(
      ValueChunkWriter writer, long[] times, Column column, int batchSize, int arrayOffset) {
    writer.write(times, column.getBinaries(), column.isNull(), batchSize, arrayOffset);
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    writer.write(time, ((Binary[]) column)[rowIndex]);
  }

  @Override
  public Field getField(Object value) {
    Field field = new Field(TSDataType.BLOB);
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
  public void addValue(int rowIndex, Object value, Object array) {
    if (value != null && !(value instanceof Binary) && !(value instanceof String)) {
      throw new IllegalArgumentException(
          Messages.format(
              "error.write.tablet_expected_type",
              "Binary or String",
              getDisplayName(),
              value.getClass().getName()));
    }
    if (value instanceof Binary) {
      ((Binary[]) array)[rowIndex] = (Binary) value;
    } else {
      ((Binary[]) array)[rowIndex] =
          value != null
              ? new Binary(((String) value).getBytes(TSFileConfig.STRING_CHARSET))
              : Binary.EMPTY_VALUE;
    }
  }

  @Override
  public void copyArrayElement(Object source, int sourceIndex, Object target, int targetIndex) {
    ((Binary[]) target)[targetIndex] = ((Binary[]) source)[sourceIndex];
  }

  @Override
  public Object arrayCopyOf(Object array, int newLength) {
    return Arrays.copyOf((Binary[]) array, newLength);
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
  public void deserialize(Object[] array, int index, ByteBuffer buffer) {
    array[index] = ReadWriteIOUtils.readBinary(buffer);
  }

  @Override
  public void deserialize(Object[] array, int index, InputStream stream) throws IOException {
    array[index] = ReadWriteIOUtils.readBinary(stream);
  }

  @Override
  public Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    Binary[] binaries = deserializeBinaryValues(buffer, rowSize, nullIndicators);
    return new BinaryColumn(0, rowSize, nullIndicators, binaries);
  }

  @Override
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return binaryArrayEquals(left, right, rowSize);
  }

  @Override
  public long estimateArraySize(Object array) {
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
    return TypeEnum.BLOB;
  }

  @Override
  public String getDisplayName() {
    return "BLOB";
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

  public static BlobType getInstance() {
    return BLOB;
  }
}
