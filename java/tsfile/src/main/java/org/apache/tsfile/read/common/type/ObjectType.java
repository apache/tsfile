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
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BinaryColumnBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ObjectType extends AbstractType {

  public static final ObjectType OBJECT = new ObjectType();

  private ObjectType() {}

  @Override
  public void toBytes(TsPrimitiveType value, byte[] valueBytes, int offset) {
    binaryToBytes(value, valueBytes, offset);
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
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return binaryArrayEquals(left, right, rowSize);
  }

  @Override
  public long arrayRamBytesUsed(Object array) {
    return RamUsageEstimator.sizeOf((Binary[]) array);
  }

  @Override
  public Column createColumn(int positionCount) {
    return new BinaryColumn(
        positionCount, Optional.of(new boolean[positionCount]), new Binary[positionCount]);
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
