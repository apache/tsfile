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

package org.apache.tsfile.enums;

import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public enum TSDataType {
  /** BOOLEAN. */
  BOOLEAN(
      (byte) 0,
      1,
      false,
      true,
      TSDataType::castIdenticalSingleValue,
      TSDataType::castIdenticalArray),

  /** INT32. */
  INT32(
      (byte) 1,
      4,
      true,
      true,
      TSDataType::castIdenticalSingleValue,
      TSDataType::castIdenticalArray),

  /** INT64. */
  INT64((byte) 2, 8, true, true, TSDataType::castToLongSingleValue, TSDataType::castToLongArray),

  /** FLOAT. */
  FLOAT((byte) 3, 4, true, true, TSDataType::castToFloatSingleValue, TSDataType::castToFloatArray),

  /** DOUBLE. */
  DOUBLE(
      (byte) 4, 8, true, true, TSDataType::castToDoubleSingleValue, TSDataType::castToDoubleArray),

  /** TEXT. */
  TEXT((byte) 5, 8, false, true, TSDataType::castToTextSingleValue, TSDataType::castToTextArray),

  /** VECTOR. */
  VECTOR(
      (byte) 6,
      8,
      false,
      false,
      TSDataType::unsupportedSingleValueCast,
      TSDataType::unsupportedArrayCast),

  /** UNKNOWN. */
  UNKNOWN(
      (byte) 7,
      TSDataType.UNSUPPORTED_DATA_TYPE_SIZE,
      false,
      false,
      TSDataType::unsupportedSingleValueCast,
      TSDataType::unsupportedArrayCast),

  /** TIMESTAMP. */
  TIMESTAMP(
      (byte) 8,
      8,
      false,
      true,
      TSDataType::castToTimestampSingleValue,
      TSDataType::castToTimestampArray),

  /** DATE. */
  DATE(
      (byte) 9,
      4,
      false,
      true,
      TSDataType::castIdenticalSingleValue,
      TSDataType::castIdenticalArray),

  /** BLOB. */
  BLOB((byte) 10, 8, false, false, TSDataType::castToBlobSingleValue, TSDataType::castToBlobArray),

  /** STRING */
  STRING((byte) 11, 8, false, true, TSDataType::castToTextSingleValue, TSDataType::castToTextArray),

  /** OBJECT */
  OBJECT(
      (byte) 12,
      8,
      false,
      false,
      TSDataType::castIdenticalSingleValue,
      TSDataType::castIdenticalArray);

  private static final int UNSUPPORTED_DATA_TYPE_SIZE = -1;
  private static final Object UNSUPPORTED_CAST = new Object();
  private final byte type;
  private final int dataTypeSize;
  private final boolean numeric;
  private final boolean comparable;
  private final SingleValueCaster singleValueCaster;
  private final ArrayCaster arrayCaster;
  private static final Map<TSDataType, Set<TSDataType>> compatibleTypes;

  static {
    compatibleTypes = new EnumMap<>(TSDataType.class);

    compatibleTypes.put(BOOLEAN, Collections.emptySet());

    compatibleTypes.put(INT32, Collections.emptySet());

    Set<TSDataType> i64CompatibleTypes = new HashSet<>();
    i64CompatibleTypes.add(INT32);
    i64CompatibleTypes.add(TIMESTAMP);
    compatibleTypes.put(INT64, i64CompatibleTypes);

    Set<TSDataType> floatCompatibleTypes = new HashSet<>();
    floatCompatibleTypes.add(INT32);
    compatibleTypes.put(FLOAT, floatCompatibleTypes);

    Set<TSDataType> doubleCompatibleTypes = new HashSet<>();
    doubleCompatibleTypes.add(INT32);
    doubleCompatibleTypes.add(INT64);
    doubleCompatibleTypes.add(FLOAT);
    doubleCompatibleTypes.add(TIMESTAMP);
    compatibleTypes.put(DOUBLE, doubleCompatibleTypes);

    Set<TSDataType> textCompatibleTypes = new HashSet<>();
    textCompatibleTypes.add(STRING);
    textCompatibleTypes.add(INT32);
    textCompatibleTypes.add(INT64);
    textCompatibleTypes.add(FLOAT);
    textCompatibleTypes.add(DOUBLE);
    textCompatibleTypes.add(BOOLEAN);
    textCompatibleTypes.add(BLOB);
    textCompatibleTypes.add(DATE);
    textCompatibleTypes.add(TIMESTAMP);
    compatibleTypes.put(TEXT, textCompatibleTypes);

    compatibleTypes.put(VECTOR, Collections.emptySet());

    compatibleTypes.put(UNKNOWN, Collections.emptySet());

    Set<TSDataType> timestampCompatibleTypes = new HashSet<>();
    timestampCompatibleTypes.add(INT32);
    timestampCompatibleTypes.add(INT64);
    compatibleTypes.put(TIMESTAMP, timestampCompatibleTypes);

    compatibleTypes.put(DATE, Collections.emptySet());

    Set<TSDataType> blobCompatibleTypes = new HashSet<>();
    blobCompatibleTypes.add(STRING);
    blobCompatibleTypes.add(TEXT);
    compatibleTypes.put(BLOB, blobCompatibleTypes);

    Set<TSDataType> stringCompatibleTypes = new HashSet<>();
    stringCompatibleTypes.add(TEXT);
    // add
    stringCompatibleTypes.add(INT32);
    stringCompatibleTypes.add(INT64);
    stringCompatibleTypes.add(FLOAT);
    stringCompatibleTypes.add(DOUBLE);
    stringCompatibleTypes.add(BOOLEAN);
    stringCompatibleTypes.add(BLOB);
    stringCompatibleTypes.add(DATE);
    stringCompatibleTypes.add(TIMESTAMP);
    compatibleTypes.put(STRING, stringCompatibleTypes);

    compatibleTypes.put(OBJECT, Collections.emptySet());
  }

  TSDataType(
      byte type,
      int dataTypeSize,
      boolean numeric,
      boolean comparable,
      SingleValueCaster singleValueCaster,
      ArrayCaster arrayCaster) {
    this.type = type;
    this.dataTypeSize = dataTypeSize;
    this.numeric = numeric;
    this.comparable = comparable;
    this.singleValueCaster = singleValueCaster;
    this.arrayCaster = arrayCaster;
  }

  /**
   * give an integer to return a data type.
   *
   * @param type -param to judge enum type
   * @return -enum type
   */
  public static TSDataType deserialize(byte type) {
    return getTsDataType(type);
  }

  public byte getType() {
    return type;
  }

  public static TSDataType getTsDataType(byte type) {
    switch (type) {
      case 0:
        return TSDataType.BOOLEAN;
      case 1:
        return TSDataType.INT32;
      case 2:
        return TSDataType.INT64;
      case 3:
        return TSDataType.FLOAT;
      case 4:
        return TSDataType.DOUBLE;
      case 5:
        return TSDataType.TEXT;
      case 6:
        return TSDataType.VECTOR;
      case 7:
        return TSDataType.UNKNOWN;
      case 8:
        return TSDataType.TIMESTAMP;
      case 9:
        return TSDataType.DATE;
      case 10:
        return TSDataType.BLOB;
      case 11:
        return TSDataType.STRING;
      case 12:
        return TSDataType.OBJECT;
      default:
        throw new IllegalArgumentException(Messages.format("error.common.invalid_input", type));
    }
  }

  /**
   * @return if the source type can be cast to this type.
   */
  public boolean isCompatible(TSDataType source) {
    return this == source
        || compatibleTypes.getOrDefault(this, Collections.emptySet()).contains(source);
  }

  @SuppressWarnings({"java:S3012", "java:S3776", "java:S6541"})
  public Object castFromSingleValue(TSDataType sourceType, Object value) {
    if (Objects.isNull(value)) {
      return null;
    }
    Object result = singleValueCaster.cast(this, sourceType, value);
    if (result != UNSUPPORTED_CAST) {
      return result;
    }
    throw new ClassCastException(
        Messages.format("error.common.unsupported_cast", sourceType, this));
  }

  private static Object castIdenticalSingleValue(
      TSDataType targetType, TSDataType sourceType, Object value) {
    return sourceType == targetType ? value : UNSUPPORTED_CAST;
  }

  private static Object castToLongSingleValue(
      TSDataType targetType, TSDataType sourceType, Object value) {
    if (sourceType == INT64 || sourceType == TIMESTAMP) {
      return value;
    }
    return sourceType == INT32 ? (long) ((int) value) : UNSUPPORTED_CAST;
  }

  private static Object castToFloatSingleValue(
      TSDataType targetType, TSDataType sourceType, Object value) {
    if (sourceType == FLOAT) {
      return value;
    }
    return sourceType == INT32 ? (float) ((int) value) : UNSUPPORTED_CAST;
  }

  private static Object castToDoubleSingleValue(
      TSDataType targetType, TSDataType sourceType, Object value) {
    if (sourceType == DOUBLE) {
      return value;
    }
    if (sourceType == INT32) {
      return (double) ((int) value);
    }
    if (sourceType == INT64 || sourceType == TIMESTAMP) {
      return (double) ((long) value);
    }
    return sourceType == FLOAT ? (double) ((float) value) : UNSUPPORTED_CAST;
  }

  private static Object castToTextSingleValue(
      TSDataType targetType, TSDataType sourceType, Object value) {
    if (sourceType == TEXT || sourceType == STRING) {
      return value;
    }
    if (sourceType == INT32
        || sourceType == INT64
        || sourceType == FLOAT
        || sourceType == DOUBLE
        || sourceType == BOOLEAN
        || sourceType == TIMESTAMP) {
      return new Binary(String.valueOf(value), StandardCharsets.UTF_8);
    }
    if (sourceType == DATE) {
      return new Binary(getDateStringValue((int) value), StandardCharsets.UTF_8);
    }
    return sourceType == BLOB
        ? new Binary(value.toString(), StandardCharsets.UTF_8)
        : UNSUPPORTED_CAST;
  }

  private static Object castToTimestampSingleValue(
      TSDataType targetType, TSDataType sourceType, Object value) {
    if (sourceType == TIMESTAMP || sourceType == INT64) {
      return value;
    }
    return sourceType == INT32 ? (long) ((int) value) : UNSUPPORTED_CAST;
  }

  private static Object castToBlobSingleValue(
      TSDataType targetType, TSDataType sourceType, Object value) {
    return sourceType == BLOB || sourceType == STRING || sourceType == TEXT
        ? value
        : UNSUPPORTED_CAST;
  }

  private static Object unsupportedSingleValueCast(
      TSDataType targetType, TSDataType sourceType, Object value) {
    return UNSUPPORTED_CAST;
  }

  public Object castFromArray(TSDataType sourceType, Object array) {
    Object result = arrayCaster.cast(this, sourceType, array);
    if (result != UNSUPPORTED_CAST) {
      return result;
    }
    throw new ClassCastException(
        Messages.format("error.common.unsupported_cast", sourceType, this));
  }

  private static Object castIdenticalArray(
      TSDataType targetType, TSDataType sourceType, Object array) {
    return sourceType == targetType ? array : UNSUPPORTED_CAST;
  }

  private static Object castToLongArray(
      TSDataType targetType, TSDataType sourceType, Object array) {
    if (sourceType == INT64 || sourceType == TIMESTAMP) {
      return array;
    }
    return sourceType == INT32
        ? Arrays.stream((int[]) array).mapToLong(Long::valueOf).toArray()
        : UNSUPPORTED_CAST;
  }

  private static Object castToFloatArray(
      TSDataType targetType, TSDataType sourceType, Object array) {
    if (sourceType == FLOAT) {
      return array;
    }
    if (sourceType != INT32) {
      return UNSUPPORTED_CAST;
    }
    int[] values = (int[]) array;
    float[] result = new float[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = values[i];
    }
    return result;
  }

  private static Object castToDoubleArray(
      TSDataType targetType, TSDataType sourceType, Object array) {
    if (sourceType == DOUBLE) {
      return array;
    }
    if (sourceType == INT32) {
      return Arrays.stream((int[]) array).mapToDouble(Double::valueOf).toArray();
    }
    if (sourceType == INT64 || sourceType == TIMESTAMP) {
      return Arrays.stream((long[]) array).mapToDouble(Double::valueOf).toArray();
    }
    if (sourceType != FLOAT) {
      return UNSUPPORTED_CAST;
    }
    float[] values = (float[]) array;
    double[] result = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      result[i] = values[i];
    }
    return result;
  }

  private static Object castToTimestampArray(
      TSDataType targetType, TSDataType sourceType, Object array) {
    if (sourceType == TIMESTAMP || sourceType == INT64) {
      return array;
    }
    return sourceType == INT32
        ? Arrays.stream((int[]) array).mapToLong(Long::valueOf).toArray()
        : UNSUPPORTED_CAST;
  }

  private static Object castToBlobArray(
      TSDataType targetType, TSDataType sourceType, Object array) {
    return sourceType == BLOB || sourceType == STRING || sourceType == TEXT
        ? array
        : UNSUPPORTED_CAST;
  }

  private static Object castToTextArray(
      TSDataType targetType, TSDataType sourceType, Object array) {
    if (sourceType == STRING || sourceType == TEXT || sourceType == BLOB) {
      return array;
    }
    if (sourceType == INT32) {
      int[] values = (int[]) array;
      Binary[] result = new Binary[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = new Binary(String.valueOf(values[i]), StandardCharsets.UTF_8);
      }
      return result;
    }
    if (sourceType == DATE) {
      int[] values = (int[]) array;
      Binary[] result = new Binary[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = new Binary(getDateStringValue(values[i]), StandardCharsets.UTF_8);
      }
      return result;
    }
    if (sourceType == INT64 || sourceType == TIMESTAMP) {
      long[] values = (long[]) array;
      Binary[] result = new Binary[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = new Binary(String.valueOf(values[i]), StandardCharsets.UTF_8);
      }
      return result;
    }
    if (sourceType == FLOAT) {
      float[] values = (float[]) array;
      Binary[] result = new Binary[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = new Binary(String.valueOf(values[i]), StandardCharsets.UTF_8);
      }
      return result;
    }
    if (sourceType == DOUBLE) {
      double[] values = (double[]) array;
      Binary[] result = new Binary[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = new Binary(String.valueOf(values[i]), StandardCharsets.UTF_8);
      }
      return result;
    }
    if (sourceType == BOOLEAN) {
      boolean[] values = (boolean[]) array;
      Binary[] result = new Binary[values.length];
      for (int i = 0; i < values.length; i++) {
        result[i] = new Binary(String.valueOf(values[i]), StandardCharsets.UTF_8);
      }
      return result;
    }
    return UNSUPPORTED_CAST;
  }

  private static Object unsupportedArrayCast(
      TSDataType targetType, TSDataType sourceType, Object array) {
    return UNSUPPORTED_CAST;
  }

  public static TSDataType deserializeFrom(ByteBuffer buffer) {
    return deserialize(buffer.get());
  }

  public static TSDataType deserializeFrom(InputStream stream) throws IOException {
    return deserialize((byte) stream.read());
  }

  public static int getSerializedSize() {
    return Byte.BYTES;
  }

  public void serializeTo(ByteBuffer byteBuffer) {
    byteBuffer.put(serialize());
  }

  public void serializeTo(DataOutputStream outputStream) throws IOException {
    outputStream.write(serialize());
  }

  public void serializeTo(FileOutputStream outputStream) throws IOException {
    outputStream.write(serialize());
  }

  public int getDataTypeSize() {
    if (dataTypeSize == UNSUPPORTED_DATA_TYPE_SIZE) {
      throw new UnSupportedDataTypeException(this.toString());
    }
    return dataTypeSize;
  }

  /**
   * get type byte.
   *
   * @return byte number
   */
  public byte serialize() {
    return type;
  }

  /**
   * numeric datatype judgement.
   *
   * @return whether it is a numeric datatype
   * @throws UnSupportedDataTypeException when meets unSupported DataType
   */
  public boolean isNumeric() {
    if (this == UNKNOWN) {
      throw new UnSupportedDataTypeException(this.toString());
    }
    return numeric;
  }

  /**
   * comparable datatype judgement.
   *
   * @return whether it is a comparable datatype
   * @throws UnSupportedDataTypeException when meets unSupported DataType
   */
  public boolean isComparable() {
    if (this == UNKNOWN) {
      throw new UnSupportedDataTypeException(this.toString());
    }
    return comparable;
  }

  public boolean isBinary() {
    return this == TEXT || this == STRING || this == BLOB || this == OBJECT;
  }

  public boolean isTextStringOrBlob() {
    return this == TEXT || this == STRING || this == BLOB;
  }

  // Indicating the statistics don't contain values, such as first, last, min, max...
  public boolean hasNoValueInStatistics() {
    return this == BLOB || this == OBJECT;
  }

  public static String getDateStringValue(int value) {
    return String.format("%04d-%02d-%02d", value / 10000, (value % 10000) / 100, value % 100);
  }

  @FunctionalInterface
  private interface SingleValueCaster {

    Object cast(TSDataType targetType, TSDataType sourceType, Object value);
  }

  @FunctionalInterface
  private interface ArrayCaster {

    Object cast(TSDataType targetType, TSDataType sourceType, Object array);
  }
}
