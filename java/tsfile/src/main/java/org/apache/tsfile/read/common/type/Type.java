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
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

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

  /** Adds a value to the array column at {@code rowIndex}. */
  default void addValue(int rowIndex, Object value, Object column) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Creates an array column with {@code capacity} entries for this type. */
  default Object createArray(int capacity) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns the serialized byte size of the array column with {@code rowSize} entries. */
  default int serializedSize(Object column, int rowSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Serializes the array column with {@code rowSize} entries. */
  default void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Deserializes an array with {@code rowSize} entries from {@code buffer}. */
  default Object deserializeArray(ByteBuffer buffer, int rowSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** Returns whether two array columns are equal in the first {@code rowSize} entries. */
  default boolean arrayEquals(Object left, Object right, int rowSize) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Creates the preferred column builder for this type. This is the builder used to store values
   * after an expression projection within the read.
   */
  ColumnBuilder createColumnBuilder(int expectedEntries);

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
