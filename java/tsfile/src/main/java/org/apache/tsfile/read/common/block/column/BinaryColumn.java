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

package org.apache.tsfile.read.common.block.column;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.block.column.ColumnEncoding;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkArrayRange;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkReadablePosition;
import static org.apache.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfBooleanArray;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfObjectArray;

public class BinaryColumn implements Column {

  private static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(BinaryColumn.class);

  public static final int SHALLOW_SIZE_IN_BYTES_PER_POSITION = NUM_BYTES_OBJECT_REF + Byte.BYTES;

  private final int arrayOffset;
  private int positionCount;
  private boolean[] valueIsNull;
  private final Binary[] values;

  private final long retainedSizeInBytes;
  // A view uses the same value bytes and row count as the column they were measured on.
  private long measuredValueSizeInBytes;
  private int measuredPositionCount;

  public BinaryColumn(int initialCapacity) {
    this(0, 0, null, new Binary[initialCapacity]);
  }

  public BinaryColumn(int positionCount, Optional<boolean[]> valueIsNull, Binary[] values) {
    this(0, positionCount, valueIsNull.orElse(null), values);
  }

  public BinaryColumn(int arrayOffset, int positionCount, boolean[] valueIsNull, Binary[] values) {
    this(arrayOffset, positionCount, valueIsNull, values, null);
  }

  private BinaryColumn(
      int arrayOffset,
      int positionCount,
      boolean[] valueIsNull,
      Binary[] values,
      BinaryColumn parent) {
    if (arrayOffset < 0) {
      throw new IllegalArgumentException(Messages.get("error.read.col_array_offset_negative"));
    }
    this.arrayOffset = arrayOffset;
    if (positionCount < 0) {
      throw new IllegalArgumentException(Messages.get("error.read.col_position_count_negative"));
    }
    this.positionCount = positionCount;

    if (values.length - arrayOffset < positionCount) {
      throw new IllegalArgumentException(Messages.get("error.read.col_values_length_lt_position"));
    }
    this.values = values;

    if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
      throw new IllegalArgumentException(Messages.get("error.read.col_isnull_length_lt_position"));
    }
    this.valueIsNull = valueIsNull;
    if (parent == null) {
      int end = arrayOffset + positionCount;
      measuredPositionCount = positionCount;
      measuredValueSizeInBytes = sizeOfValues(arrayOffset, end);
      retainedSizeInBytes =
          INSTANCE_SIZE
              + sizeOfBooleanArray(positionCount)
              + sizeOfObjectArray(values.length)
              + measuredValueSizeInBytes
              + sizeOfValues(0, arrayOffset)
              + sizeOfValues(end, values.length);
    } else {
      retainedSizeInBytes = parent.retainedSizeInBytes;
      measuredPositionCount = parent.measuredPositionCount;
      measuredValueSizeInBytes = parent.measuredValueSizeInBytes;
    }
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.TEXT;
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.BINARY_ARRAY;
  }

  @Override
  public Column convertTo(TSDataType type) {
    if (type == TSDataType.TEXT) {
      return this;
    }
    ColumnUtil.checkConversion(TSDataType.TEXT, type);

    ColumnBuilder builder = Type.fromTsDataType(type).createColumnBuilder(positionCount);
    for (int position = 0; position < positionCount; position++) {
      if (isNull(position)) {
        builder.appendNull();
      } else {
        builder.writeBinary(getBinary(position));
      }
    }
    return builder.build();
  }

  @Override
  public Binary getBinary(int position) {
    return values[position + arrayOffset];
  }

  @Override
  public Binary[] getBinaries() {
    return values;
  }

  @Override
  public Object getObject(int position) {
    return getBinary(position);
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(int position) {
    return new TsPrimitiveType.TsBinary(getBinary(position));
  }

  @Override
  public void writeTo(int index, ByteBuffer buffer) {
    Binary value = values[index + arrayOffset];
    buffer.putInt(value.getLength());
    buffer.put(value.getValues());
  }

  @Override
  public void writeTo(int index, DataOutputStream stream) throws IOException {
    Binary value = values[index + arrayOffset];
    stream.writeInt(value.getLength());
    stream.write(value.getValues());
  }

  @Override
  public void serializeWithoutNulls(DataOutputStream output) throws IOException {
    for (int i = 0; i < positionCount; i++) {
      if (!isNull(i)) {
        Binary value = values[i + arrayOffset];
        output.writeInt(value.getLength());
        output.write(value.getValues());
      }
    }
  }

  @Override
  public boolean arePositionsEqual(int thisPos, Column that, int thatPos) {
    boolean thisIsNull = isNull(thisPos);
    if (thisIsNull) {
      return that.isNull(thatPos);
    }
    return !that.isNull(thatPos) && getBinary(thisPos).equals(that.getBinary(thatPos));
  }

  @Override
  public boolean mayHaveNull() {
    return valueIsNull != null;
  }

  @Override
  public boolean isNull(int position) {
    return values[position + arrayOffset] == null
        || valueIsNull != null && valueIsNull[position + arrayOffset];
  }

  @Override
  public boolean[] isNull() {
    if (valueIsNull == null) {
      boolean[] res = new boolean[positionCount];
      Arrays.fill(res, false);
      return res;
    }
    return valueIsNull;
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return retainedSizeInBytes;
  }

  /** Shared regions estimate their value bytes using the average over the measured positions. */
  @Override
  public long getSizeInBytes() {
    long shallowSize =
        INSTANCE_SIZE + sizeOfBooleanArray(positionCount) + sizeOfObjectArray(positionCount);
    if (positionCount == measuredPositionCount) {
      return shallowSize + measuredValueSizeInBytes;
    }
    // Scale only the values. Dividing first avoids overflowing when the column is large.
    long bytesPerPosition = measuredValueSizeInBytes / measuredPositionCount;
    long remainder = measuredValueSizeInBytes % measuredPositionCount;
    return shallowSize
        + bytesPerPosition * positionCount
        + remainder * positionCount / measuredPositionCount;
  }

  private long sizeOfValues(int from, int end) {
    long size = 0;
    for (int i = from; i < end; i++) {
      Binary value = values[i];
      if (value != null) {
        size += value.ramBytesUsed();
      }
    }
    return size;
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(getPositionCount(), positionOffset, length);
    return new BinaryColumn(positionOffset + arrayOffset, length, valueIsNull, values, this);
  }

  @Override
  public Column getRegionCopy(int positionOffset, int length) {
    checkValidRegion(getPositionCount(), positionOffset, length);

    int from = positionOffset + arrayOffset;
    int to = from + length;
    boolean[] valueIsNullCopy =
        valueIsNull != null ? Arrays.copyOfRange(valueIsNull, from, to) : null;
    Binary[] valuesCopy = Arrays.copyOfRange(values, from, to);

    return new BinaryColumn(0, length, valueIsNullCopy, valuesCopy);
  }

  @Override
  public Column subColumn(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException(Messages.get("error.read.col_from_index_invalid"));
    }
    return getRegion(fromIndex, positionCount - fromIndex);
  }

  @Override
  public Column subColumnCopy(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException(Messages.get("error.read.col_from_index_invalid"));
    }

    int from = arrayOffset + fromIndex;
    boolean[] valueIsNullCopy =
        valueIsNull != null ? Arrays.copyOfRange(valueIsNull, from, positionCount) : null;
    Binary[] valuesCopy = Arrays.copyOfRange(values, from, positionCount);

    int length = positionCount - fromIndex;
    return new BinaryColumn(0, length, valueIsNullCopy, valuesCopy);
  }

  @Override
  public void reverse() {
    for (int i = arrayOffset, j = arrayOffset + positionCount - 1; i < j; i++, j--) {
      Binary valueTmp = values[i];
      values[i] = values[j];
      values[j] = valueTmp;
    }
    if (valueIsNull != null) {
      for (int i = arrayOffset, j = arrayOffset + positionCount - 1; i < j; i++, j--) {
        boolean isNullTmp = valueIsNull[i];
        valueIsNull[i] = valueIsNull[j];
        valueIsNull[j] = isNullTmp;
      }
    }
  }

  @Override
  public Column getPositions(int[] positions, int offset, int length) {
    checkArrayRange(positions, offset, length);

    return DictionaryColumn.createInternal(
        offset, length, this, positions, DictionaryId.randomDictionaryId());
  }

  @Override
  public Column copyPositions(int[] positions, int offset, int length) {
    checkArrayRange(positions, offset, length);

    boolean[] newValueIsNull = null;
    if (valueIsNull != null) {
      newValueIsNull = new boolean[length];
    }
    Binary[] newValues = new Binary[length];
    for (int i = 0; i < length; i++) {
      int position = positions[offset + i];
      checkReadablePosition(this, position);
      if (newValueIsNull != null) {
        newValueIsNull[i] = valueIsNull[position + arrayOffset];
      }
      newValues[i] = values[position + arrayOffset];
    }
    return new BinaryColumn(0, length, newValueIsNull, newValues);
  }

  @Override
  public int getInstanceSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public void setPositionCount(int count) {
    int capacity =
        valueIsNull == null ? values.length : Math.min(values.length, valueIsNull.length);
    checkValidRegion(capacity - arrayOffset, 0, count);
    if (count > positionCount && positionCount == measuredPositionCount) {
      // Readers fill the backing array in place and publish the newly written positions here.
      measuredValueSizeInBytes += sizeOfValues(arrayOffset + positionCount, arrayOffset + count);
      measuredPositionCount = count;
    } else if (count >= positionCount && count > 0) {
      // The previous range was estimated, or existing positions have been filled again.
      measuredValueSizeInBytes = sizeOfValues(arrayOffset, arrayOffset + count);
      measuredPositionCount = count;
    }
    positionCount = count;
  }

  @Override
  public void setNull(int start, int end) {
    if (valueIsNull == null) {
      valueIsNull = new boolean[values.length];
    }
    Arrays.fill(valueIsNull, start, end, true);
  }
}
