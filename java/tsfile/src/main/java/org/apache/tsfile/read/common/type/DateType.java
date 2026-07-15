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
    writer.write(
        time,
        isNull ? 0 : DateUtils.parseDateExpressionToInt(((LocalDate[]) array)[rowIndex]),
        isNull);
  }

  @Override
  public void write(ChunkWriterImpl writer, long time, Object column, int rowIndex) {
    writer.write(time, DateUtils.parseDateExpressionToInt(((LocalDate[]) column)[rowIndex]));
  }

  @Override
  public void addValue(int rowIndex, Object value, Object column) {
    checkValueType(value, LocalDate.class, "LocalDate");
    ((LocalDate[]) column)[rowIndex] = value != null ? (LocalDate) value : EMPTY_DATE;
  }

  @Override
  public void copyArrayElement(Object source, int sourceIndex, Object target, int targetIndex) {
    ((LocalDate[]) target)[targetIndex] = ((LocalDate[]) source)[sourceIndex];
  }

  @Override
  public Object arrayCopyOf(Object array, int newLength) {
    return Arrays.copyOf((LocalDate[]) array, newLength);
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
    return hasEnoughLength(left, right, rowSize)
        && Arrays.equals((LocalDate[]) left, 0, rowSize, (LocalDate[]) right, 0, rowSize);
  }

  @Override
  public long arrayRamBytesUsed(Object array) {
    return RamUsageEstimator.sizeOf((LocalDate[]) array);
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

  public static DateType getInstance() {
    return DATE;
  }
}
