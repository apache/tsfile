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
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;

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
  public TSEncoding getDefaultEncoding(TSFileConfig config) {
    return TSEncoding.valueOf(config.getInt64Encoding());
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
  public Object createArray(int capacity) {
    return new long[capacity];
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
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return hasEnoughLength(left, right, rowSize)
        && Arrays.equals((long[]) left, 0, rowSize, (long[]) right, 0, rowSize);
  }

  @Override
  public long arrayRamBytesUsed(Object array) {
    return RamUsageEstimator.sizeOf((long[]) array);
  }

  @Override
  public Column createColumn(int positionCount) {
    return new LongColumn(
        positionCount, Optional.of(new boolean[positionCount]), new long[positionCount]);
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
