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
  BOOLEAN((byte) 0, TSDataType::castIdenticalSingleValue),

  /** INT32. */
  INT32((byte) 1, TSDataType::castIdenticalSingleValue),

  /** INT64. */
  INT64((byte) 2, TSDataType::castToLongSingleValue),

  /** FLOAT. */
  FLOAT((byte) 3, TSDataType::castToFloatSingleValue),

  /** DOUBLE. */
  DOUBLE((byte) 4, TSDataType::castToDoubleSingleValue),

  /** TEXT. */
  TEXT((byte) 5, TSDataType::castToTextSingleValue),

  /** VECTOR. */
  VECTOR((byte) 6, TSDataType::unsupportedSingleValueCast),

  /** UNKNOWN. */
  UNKNOWN((byte) 7, TSDataType::unsupportedSingleValueCast),

  /** TIMESTAMP. */
  TIMESTAMP((byte) 8, TSDataType::castToTimestampSingleValue),

  /** DATE. */
  DATE((byte) 9, TSDataType::castIdenticalSingleValue),

  /** BLOB. */
  BLOB((byte) 10, TSDataType::castToBlobSingleValue),

  /** STRING */
  STRING((byte) 11, TSDataType::castToTextSingleValue),

  /** OBJECT */
  OBJECT((byte) 12, TSDataType::castIdenticalSingleValue);

  private static final Object UNSUPPORTED_CAST = new Object();
  private final byte type;
  private final SingleValueCaster singleValueCaster;
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

  TSDataType(byte type, SingleValueCaster singleValueCaster) {
    this.type = type;
    this.singleValueCaster = singleValueCaster;
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

  @SuppressWarnings({"java:S3012", "java:S3776", "java:S6541"})
  public Object castFromArray(TSDataType sourceType, Object array) {
    switch (this) {
      case BOOLEAN:
        if (sourceType == TSDataType.BOOLEAN) {
          return array;
        } else {
          break;
        }
      case INT32:
        if (sourceType == TSDataType.INT32) {
          return array;
        } else {
          break;
        }
      case INT64:
        if (sourceType == TSDataType.INT64) {
          return array;
        } else if (sourceType == INT32) {
          return Arrays.stream((int[]) array).mapToLong(Long::valueOf).toArray();
        } else if (sourceType == TIMESTAMP) {
          return array;
        } else {
          break;
        }
      case FLOAT:
        if (sourceType == TSDataType.FLOAT) {
          return array;
        } else if (sourceType == INT32) {
          int[] tmp = (int[]) array;
          float[] result = new float[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = tmp[i];
          }
          return result;
        } else {
          break;
        }
      case DOUBLE:
        if (sourceType == TSDataType.DOUBLE) {
          return array;
        } else if (sourceType == INT32) {
          return Arrays.stream((int[]) array).mapToDouble(Double::valueOf).toArray();
        } else if (sourceType == INT64) {
          return Arrays.stream((long[]) array).mapToDouble(Double::valueOf).toArray();
        } else if (sourceType == FLOAT) {
          float[] tmp = (float[]) array;
          double[] result = new double[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = tmp[i];
          }
          return result;
        } else if (sourceType == TIMESTAMP) {
          return Arrays.stream((long[]) array).mapToDouble(Double::valueOf).toArray();
        } else {
          break;
        }
      case TIMESTAMP:
        if (sourceType == TSDataType.TIMESTAMP) {
          return array;
        } else if (sourceType == INT32) {
          return Arrays.stream((int[]) array).mapToLong(Long::valueOf).toArray();
        } else if (sourceType == INT64) {
          return array;
        } else {
          break;
        }
      case DATE:
        if (sourceType == TSDataType.DATE) {
          return array;
        } else {
          break;
        }
      case BLOB:
        if (sourceType == TSDataType.BLOB
            || sourceType == TSDataType.STRING
            || sourceType == TSDataType.TEXT) {
          return array;
        } else {
          break;
        }
      case TEXT:
      case STRING:
        if (sourceType == TSDataType.STRING
            || sourceType == TSDataType.TEXT
            || sourceType == TSDataType.BLOB) {
          return array;
        } else if (sourceType == TSDataType.INT32) {
          int[] tmp = (int[]) array;
          Binary[] result = new Binary[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = new Binary(String.valueOf(tmp[i]), StandardCharsets.UTF_8);
          }
          return result;
        } else if (sourceType == TSDataType.DATE) {
          int[] tmp = (int[]) array;
          Binary[] result = new Binary[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = new Binary(TSDataType.getDateStringValue(tmp[i]), StandardCharsets.UTF_8);
          }
          return result;
        } else if (sourceType == TSDataType.INT64 || sourceType == TSDataType.TIMESTAMP) {
          long[] tmp = (long[]) array;
          Binary[] result = new Binary[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = new Binary(String.valueOf(tmp[i]), StandardCharsets.UTF_8);
          }
          return result;
        } else if (sourceType == TSDataType.FLOAT) {
          float[] tmp = (float[]) array;
          Binary[] result = new Binary[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = new Binary(String.valueOf(tmp[i]), StandardCharsets.UTF_8);
          }
          return result;
        } else if (sourceType == TSDataType.DOUBLE) {
          double[] tmp = (double[]) array;
          Binary[] result = new Binary[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = new Binary(String.valueOf(tmp[i]), StandardCharsets.UTF_8);
          }
          return result;
        } else if (sourceType == TSDataType.BOOLEAN) {
          boolean[] tmp = (boolean[]) array;
          Binary[] result = new Binary[tmp.length];
          for (int i = 0; i < tmp.length; i++) {
            result[i] = new Binary(String.valueOf(tmp[i]), StandardCharsets.UTF_8);
          }
          return result;
        } else {
          break;
        }
      case OBJECT:
        if (sourceType == TSDataType.OBJECT) {
          return array;
        } else {
          break;
        }
      case VECTOR:
      case UNKNOWN:
      default:
        break;
    }
    throw new ClassCastException(
        Messages.format("error.common.unsupported_cast", sourceType, this));
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
    switch (this) {
      case BOOLEAN:
        return 1;
      case INT32:
      case FLOAT:
      case DATE:
        return 4;
      // For text: return the size of reference here
      case TEXT:
      case INT64:
      case DOUBLE:
      case VECTOR:
      case BLOB:
      case OBJECT:
      case STRING:
      case TIMESTAMP:
        return 8;
      default:
        throw new UnSupportedDataTypeException(this.toString());
    }
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
    switch (this) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
        return true;
      // For text: return the size of reference here
      case BLOB:
      case TIMESTAMP:
      case DATE:
      case STRING:
      case BOOLEAN:
      case TEXT:
      case VECTOR:
      case OBJECT:
        return false;
      default:
        throw new UnSupportedDataTypeException(this.toString());
    }
  }

  /**
   * comparable datatype judgement.
   *
   * @return whether it is a comparable datatype
   * @throws UnSupportedDataTypeException when meets unSupported DataType
   */
  public boolean isComparable() {
    switch (this) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case TEXT:
      case BOOLEAN:
      case TIMESTAMP:
      case DATE:
      case STRING:
        return true;
      case VECTOR:
      case BLOB:
      case OBJECT:
        return false;
      default:
        throw new UnSupportedDataTypeException(this.toString());
    }
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
}
