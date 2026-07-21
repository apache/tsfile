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
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Type provides common interfaces associated with a specific TsDataType to remove uses of
 * "switch(dataType)", which make the extension of datatypes very difficult.
 *
 * <p>Interfaces that are less generic should be defined using TypeService and checked in a proper
 * context.
 */
public interface Type {

  static Type fromTsDataType(TSDataType tsDataType) {
    return TypeFactory.getType(tsDataType);
  }

  /** Gets a boolean at {@code position}. */
  default boolean getBoolean(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a little endian int at {@code position}. */
  default int getInt(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a little endian long at {@code position}. */
  default long getLong(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a float at {@code position}. */
  default float getFloat(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a double at {@code position}. */
  default double getDouble(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a Binary at {@code position}. */
  default Binary getBinary(Column c, int position) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Gets a Object at {@code position}. */
  default Object getObject(Column c, int position) {
    return c.getObject(position);
  }

  /** Creates an empty TsPrimitiveType for this type. */
  default TsPrimitiveType getTsPrimitiveType() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Creates a TsPrimitiveType initialized with {@code value}. */
  default TsPrimitiveType getTsPrimitiveType(Object value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Serializes {@code value} into {@code valueBytes} starting at {@code offset}. */
  default void toBytes(TsPrimitiveType value, byte[] valueBytes, int offset) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the number of bytes required to serialize {@code value}. */
  default int calcTypeSize(TsPrimitiveType value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the number of bytes required to serialize {@code value}. */
  default int calcTypeSize(Object value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Initializes the value storage in {@code batchData} for this type. */
  default void init(BatchData batchData) {
    throw new UnSupportedDataTypeException(String.valueOf(batchData.getDataType()));
  }

  /** Adds a timestamp-value pair to {@code batchData}. */
  default void put(BatchData batchData, long timestamp, Object value) {
    throw new UnSupportedDataTypeException(String.valueOf(batchData.getDataType()));
  }

  /** Serializes all timestamp-value pairs in {@code batchData}. */
  default void serialize(BatchData batchData, DataOutputStream outputStream, boolean isDesc)
      throws IOException {
    throw new IllegalArgumentException(
        Messages.format("error.read.batch_data_unknown_type", batchData.getDataType()));
  }

  /** Serializes {@code value} to {@code stream}. */
  default void serialize(TsPrimitiveType value, DataOutputStream stream) throws IOException {
    throw new IllegalArgumentException(
        Messages.format("error.read.batch_data_unknown_type", value.getDataType()));
  }

  /** Serializes {@code value} to {@code buffer}. */
  default void serializeValue(Object value, ByteBuffer buffer) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Serializes {@code value} to {@code stream}. */
  default void serializeValue(Object value, DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a boolean to the current entry; */
  default void writeBoolean(ColumnBuilder builder, boolean value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write an int to the current entry; */
  default void writeInt(ColumnBuilder builder, int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a long to the current entry; */
  default void writeLong(ColumnBuilder builder, long value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a float to the current entry; */
  default void writeFloat(ColumnBuilder builder, float value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a double to the current entry; */
  default void writeDouble(ColumnBuilder builder, double value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a Binary to the current entry; */
  default void writeBinary(ColumnBuilder builder, Binary value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Write a Object to the current entry; */
  default void writeObject(ColumnBuilder builder, Object value) {
    builder.writeObject(value);
  }

  /** Adds a value to the array at {@code rowIndex}. */
  default void addValue(int rowIndex, Object value, Object array) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Copies one element from {@code source} to {@code target}. */
  default void copyArrayElement(Object source, int sourceIndex, Object target, int targetIndex) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Copies {@code array}, truncating or padding it to {@code newLength}. */
  default Object arrayCopyOf(Object array, int newLength) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Creates an array column with {@code capacity} entries for this type. */
  default Object createArray(int capacity) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Estimates the retained memory size of an array with {@code size} entries. */
  default long estimateArraySize(int size) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Estimates the retained memory size of {@code array}. */
  default long estimateArraySize(Object array) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Estimates the retained memory size of a value of this type. */
  default long estimateValueSize() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the serialized byte size of the array with {@code rowSize} entries. */
  default int serializedSize(Object array, int rowSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the serialized byte size of the array within the given range. */
  default int serializedSize(Object array, int startRow, int endRow) {
    return serializedSize(array, endRow - startRow);
  }

  /** Serializes the array column with {@code rowSize} entries. */
  default void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Serializes the first {@code length} entries in {@code array} to {@code buffer}. */
  default void serializeArray(Object array, int length, ByteBuffer buffer) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Deserializes an array with {@code rowSize} entries from {@code buffer}. */
  default Object deserializeArray(ByteBuffer buffer, int rowSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Deserializes one value from {@code buffer} into {@code array} at {@code index}. */
  default void deserialize(Object[] array, int index, ByteBuffer buffer) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Deserializes one value from {@code stream} into {@code array} at {@code index}. */
  default void deserialize(Object[] array, int index, InputStream stream) throws IOException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Deserializes an array with {@code rowSize} entries from {@code buffer}. */
  default Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns whether two array columns are equal in the first {@code rowSize} entries. */
  default boolean arrayEquals(Object left, Object right, int rowSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the value at {@code rowIndex} from an array. */
  default Object getValue(Object array, int rowIndex) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the value at {@code rowIndex} from an array as a TsPrimitiveType. */
  default TsPrimitiveType getValueAsTsPrimitiveType(Object array, int rowIndex) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the value in {@code field}. */
  default Object getValue(Field field) {
    throw new UnSupportedDataTypeException(getTypeEnum().toString());
  }

  /** Creates a field initialized with {@code value}. */
  default Field getField(Object value) {
    throw new UnSupportedDataTypeException(getTypeEnum().toString());
  }

  /** Copies the value from {@code from} to {@code to}. */
  default void setTo(TsPrimitiveType from, Field to) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", from.getDataType()));
  }

  /** Copies the value from {@code from} to {@code to} at {@code index}. */
  default void setTo(TsPrimitiveType from, Column to, int index) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", from.getDataType()));
  }

  /** Copies {@code value} to {@code to} from {@code startIndex} to {@code endIndex}. */
  default void setTo(Object value, Column to, int startIndex, int endIndex) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Copies the value from {@code from} to {@code to}. */
  default void setTo(Field from, Field to) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", from.getDataType()));
  }

  /** Copies the current value from {@code from} to {@code to}. */
  default void setTo(BatchData from, Field to) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", from.getDataType()));
  }

  /** Copies the current value from {@code from} to {@code to} at {@code index}. */
  default void setTo(BatchData from, Column to, int index) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", from.getDataType()));
  }

  /** Copies a value from {@code from} at {@code fromIndex} to {@code to} at {@code toIndex}. */
  default void setTo(Column from, int fromIndex, Column to, int toIndex) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Copies a value from {@code from} at {@code fromIndex} to {@code toArray} at {@code toIndex}.
   */
  default void setTo(Column from, int fromIndex, Object toArray, int toIndex) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Adds the value of {@code columnName} in {@code resultSet} to {@code record}. */
  default void addPoint(TSRecord record, String columnName, ResultSet resultSet) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", this.getTypeEnum()));
  }

  /** Adds the value in {@code field} to {@code record}. */
  default void addPoint(TSRecord record, String columnName, Field field) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.common.unsupported_data_type", this.getTypeEnum()));
  }

  /** Creates a data point by parsing a string value. */
  default DataPoint getDataPoint(String measurementId, String value) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  /** Creates a data point from a long value for test file generation. */
  default DataPoint getDataPoint(String measurementId, long value) {
    return new StringDataPoint(
        measurementId, new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET));
  }

  /** Writes a value to an aligned value chunk. */
  default void write(ValueChunkWriter writer, long time, Object value, boolean isNull) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  /** Writes a TsPrimitiveType value to an aligned value chunk. */
  default void write(ValueChunkWriter writer, long time, TsPrimitiveType value) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  /** Writes a value at {@code rowIndex} in a Tablet column to an aligned value chunk. */
  default void write(
      ValueChunkWriter writer, long time, Object array, int rowIndex, boolean isNull) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  /** Writes a value at {@code rowIndex} in a TsBlock column to an aligned value chunk. */
  default void write(
      ValueChunkWriter writer, long time, Column column, int rowIndex, boolean isNull) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  /** Writes a batch of values in a TsBlock column to an aligned value chunk. */
  default void write(
      ValueChunkWriter writer, long[] times, Column column, int batchSize, int arrayOffset) {
    throw new UnsupportedOperationException(
        Messages.format("error.write.chunk_unknown_type", getTypeEnum()));
  }

  /** Writes a value at {@code rowIndex} in a Tablet column to a non-aligned chunk. */
  default void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  /** Writes a TsPrimitiveType value to a non-aligned chunk. */
  default void write(ChunkWriterImpl writer, long time, TsPrimitiveType value) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  /** Writes a value to a non-aligned chunk. */
  default void write(ChunkWriterImpl writer, long time, Object value) {
    throw new UnSupportedDataTypeException(
        Messages.format("error.write.type_not_supported", getTypeEnum()));
  }

  /** Creates a column with {@code positionCount} capacity for this type. */
  default Column createColumnWithMaxPosition(int positionCount) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Creates a column with {@code positionCount} capacity for this type. */
  default Column createColumnWithZeroPosition(int positionCount) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Creates an all-null column with {@code positionCount} positions. */
  default Column createNullColumn(int positionCount) {
    throw new IllegalArgumentException(
        Messages.format("error.read.null_col_unknown_type", getTypeEnum()));
  }

  /** Returns the default encoding configured for this type. */
  default TSEncoding getDefaultEncoding(TSFileConfig config) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the default compressor configured for this type. */
  default CompressionType getDefaultCompressor(TSFileConfig config) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns a decoder for the specified encoding. */
  default Decoder getDecoder(TSEncoding encoding) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the maximum encoded size of one item. */
  default int getOneItemMaxSize(int valveLength) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Creates empty statistics for this type. */
  default Statistics<?> createStatistics() {
    throw new UnknownColumnTypeException(getTypeEnum().toString());
  }

  /** Returns the estimated instance size of statistics for this type. */
  default long getStatisticsInstanceSize() {
    throw new UnknownColumnTypeException(getTypeEnum().toString());
  }

  /** Updates statistics with a timestamp-value pair. */
  default void update(Statistics<?> stats, long timestamp, TsPrimitiveType value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Updates statistics with the current timestamp-value pair in {@code batchData}. */
  default void update(Statistics<?> stats, BatchData batchData) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Updates statistics with the value at {@code rowIndex} in a TsBlock column. */
  default void update(Statistics<?> stats, TsBlock block, int columnIndex, int rowIndex) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the current value in {@code batchData}. */
  default Object getCurrentValue(BatchData batchData) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the string representation of the value in {@code field}. */
  default String toString(Field field) {
    throw new UnSupportedDataTypeException(field.getDataType().toString());
  }

  /**
   * Creates the preferred column builder for this type. This is the builder used to store values
   * after an expression projection within the read.
   */
  ColumnBuilder createColumnBuilder(int expectedEntries);

  /** Creates the preferred column builder for this type with memory tracking. */
  default ColumnBuilder createColumnBuilder(
      ColumnBuilderStatus columnBuilderStatus, int expectedEntries) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  TypeEnum getTypeEnum();

  /** Returns the name of this type that should be displayed to end-users. */
  String getDisplayName();

  /** True if the type supports equalTo and hash. */
  boolean isComparable();

  /** True if the type supports compareTo. */
  boolean isOrderable();

  /** For parameterized types returns the list of parameters. */
  List<Type> getTypeParameters();
}
