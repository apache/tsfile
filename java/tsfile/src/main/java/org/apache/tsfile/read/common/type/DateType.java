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
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Optional;

public class DateType extends AbstractIntType {

  public static final DateType DATE = new DateType();
  private static final LocalDate EMPTY_DATE = LocalDate.of(1000, 1, 1);

  private DateType() {}

  @Override
  public TsPrimitiveType getTsPrimitiveType() {
    return new TsInt(TSDataType.DATE);
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(Object value) {
    return new TsInt((int) value, TSDataType.DATE);
  }

  @Override
  public DataPoint getDataPoint(String measurementId, String value) {
    return new IntDataPoint(measurementId, DateUtils.parseDateExpressionToInt(value));
  }

  @Override
  public void write(
      ValueChunkWriter writer, long time, Object array, int rowIndex, boolean isNull) {
    if (array instanceof int[]) {
      super.write(writer, time, array, rowIndex, isNull);
      return;
    }
    writer.write(
        time,
        isNull ? 0 : DateUtils.parseDateExpressionToInt(((LocalDate[]) array)[rowIndex]),
        isNull);
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    if (column instanceof int[]) {
      super.write(writer, time, column, rowIndex);
      return;
    }
    writer.write(time, DateUtils.parseDateExpressionToInt(((LocalDate[]) column)[rowIndex]));
  }

  @Override
  public void addValue(int rowIndex, Object value, Object array) {
    if (array instanceof int[]) {
      if (value instanceof Integer) {
        ((int[]) array)[rowIndex] = (int) value;
      } else {
        checkValueType(value, LocalDate.class, "LocalDate");
        ((int[]) array)[rowIndex] =
            value != null
                ? DateUtils.parseDateExpressionToInt((LocalDate) value)
                : DateUtils.EMPTY_DATE_INT;
      }
      return;
    }
    checkValueType(value, LocalDate.class, "LocalDate");
    ((LocalDate[]) array)[rowIndex] = value != null ? (LocalDate) value : EMPTY_DATE;
  }

  @Override
  public void setTo(Column from, int fromIndex, Object toArray, int toIndex) {
    if (toArray instanceof int[]) {
      super.setTo(from, fromIndex, toArray, toIndex);
      return;
    }
    ((LocalDate[]) toArray)[toIndex] = DateUtils.parseIntToLocalDate(from.getInt(fromIndex));
  }

  @Override
  public void copyArrayElement(Object source, int sourceIndex, Object target, int targetIndex) {
    if (target instanceof int[]) {
      ((int[]) target)[targetIndex] = getDateInt(source, sourceIndex);
      return;
    }
    ((LocalDate[]) target)[targetIndex] =
        source instanceof int[]
            ? DateUtils.parseIntToLocalDate(((int[]) source)[sourceIndex])
            : ((LocalDate[]) source)[sourceIndex];
  }

  @Override
  public Object arrayCopyOf(Object array, int newLength) {
    return array instanceof int[]
        ? Arrays.copyOf((int[]) array, newLength)
        : Arrays.copyOf((LocalDate[]) array, newLength);
  }

  @Override
  public Object createArray(int capacity) {
    return new LocalDate[capacity];
  }

  @Override
  public long estimateArraySize(int size) {
    return RamUsageEstimator.sizeOfObjectArray(size);
  }

  @Override
  public Object getValue(Field field) {
    return field.getDateV();
  }

  @Override
  public Field getField(Object value) {
    Field field = new Field(TSDataType.DATE);
    field.setIntV((int) value);
    return field;
  }

  @Override
  public void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    if (array instanceof int[]) {
      for (int i = 0; i < rowSize; i++) {
        ReadWriteIOUtils.write(((int[]) array)[i], stream);
      }
      return;
    }
    LocalDate[] values = (LocalDate[]) array;
    for (int i = 0; i < rowSize; i++) {
      ReadWriteIOUtils.write(
          values[i] == null
              ? DateUtils.EMPTY_DATE_INT
              : DateUtils.parseDateExpressionToInt(values[i]),
          stream);
    }
  }

  @Override
  public void serializeArray(Object array, int length, ByteBuffer buffer) {
    if (array instanceof int[]) {
      super.serializeArray(array, length, buffer);
      return;
    }
    LocalDate[] values = (LocalDate[]) array;
    for (int i = 0; i < length; i++) {
      buffer.putInt(
          values[i] == null
              ? DateUtils.EMPTY_DATE_INT
              : DateUtils.parseDateExpressionToInt(values[i]));
    }
  }

  @Override
  public Object deserializeArray(ByteBuffer buffer, int rowSize) {
    LocalDate[] values = new LocalDate[rowSize];
    for (int i = 0; i < rowSize; i++) {
      values[i] = DateUtils.parseIntToLocalDate(ReadWriteIOUtils.readInt(buffer));
    }
    return values;
  }

  @Override
  public Object deserializeColumn(ByteBuffer buffer, int rowSize, boolean[] nullIndicators) {
    return ((IntColumn) super.deserializeColumn(buffer, rowSize, nullIndicators))
        .modifyDataType(TSDataType.DATE);
  }

  @Override
  public boolean arrayEquals(Object left, Object right, int rowSize) {
    if (!hasEnoughLength(left, right, rowSize)) {
      return false;
    }
    if (left instanceof int[] && right instanceof int[]) {
      return Arrays.equals((int[]) left, 0, rowSize, (int[]) right, 0, rowSize);
    }
    if (left instanceof LocalDate[] && right instanceof LocalDate[]) {
      return Arrays.equals((LocalDate[]) left, 0, rowSize, (LocalDate[]) right, 0, rowSize);
    }
    for (int i = 0; i < rowSize; i++) {
      if (getDateInt(left, i) != getDateInt(right, i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public long estimateArraySize(Object array) {
    return array instanceof int[]
        ? RamUsageEstimator.sizeOf((int[]) array)
        : RamUsageEstimator.sizeOf((LocalDate[]) array);
  }

  @Override
  public Column createColumnWithMaxPosition(int positionCount) {
    return new IntColumn(
        positionCount,
        Optional.of(new boolean[positionCount]),
        new int[positionCount],
        TSDataType.DATE);
  }

  @Override
  public Column createColumnWithZeroPosition(int positionCount) {
    return new IntColumn(positionCount, TSDataType.DATE);
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.DATE;
  }

  @Override
  public String getDisplayName() {
    return "DATE";
  }

  private int getDateInt(Object array, int index) {
    if (array instanceof int[]) {
      return ((int[]) array)[index];
    }
    LocalDate value = ((LocalDate[]) array)[index];
    return value != null ? DateUtils.parseDateExpressionToInt(value) : DateUtils.EMPTY_DATE_INT;
  }

  public static DateType getInstance() {
    return DATE;
  }
}
