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
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.IntColumnBuilder;
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

public abstract class AbstractIntType extends AbstractType {

  @Override
  public TSEncoding getDefaultEncoding(TSFileConfig config) {
    return TSEncoding.valueOf(config.getInt32Encoding());
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
  public void addValue(int rowIndex, Object value, Object column) {
    checkValueType(value, Integer.class, "Integer");
    ((int[]) column)[rowIndex] = value != null ? (int) value : Integer.MIN_VALUE;
  }

  @Override
  public Object createArray(int capacity) {
    return new int[capacity];
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
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return hasEnoughLength(left, right, rowSize)
        && Arrays.equals((int[]) left, 0, rowSize, (int[]) right, 0, rowSize);
  }

  @Override
  public long arrayRamBytesUsed(Object array) {
    return RamUsageEstimator.sizeOf((int[]) array);
  }

  @Override
  public Column createColumn(int positionCount) {
    return new IntColumn(
        positionCount,
        Optional.of(new boolean[positionCount]),
        new int[positionCount],
        TSDataType.INT32);
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
