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
import org.apache.tsfile.read.common.block.column.FloatColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FloatType extends AbstractType {

  public static final FloatType FLOAT = new FloatType();

  private FloatType() {}

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
  public Object createArray(int capacity) {
    return new float[capacity];
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
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    return hasEnoughLength(left, right, rowSize)
        && Arrays.equals((float[]) left, 0, rowSize, (float[]) right, 0, rowSize);
  }

  @Override
  public long arrayRamBytesUsed(Object array) {
    return RamUsageEstimator.sizeOf((float[]) array);
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
