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

import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public enum TSDataType {
  /** BOOLEAN. */
  BOOLEAN((byte) 0),

  /** INT32. */
  INT32((byte) 1),

  /** INT64. */
  INT64((byte) 2),

  /** FLOAT. */
  FLOAT((byte) 3),

  /** DOUBLE. */
  DOUBLE((byte) 4),

  /** TEXT. */
  TEXT((byte) 5),

  /** VECTOR. */
  VECTOR((byte) 6),

  /** UNKNOWN. */
  UNKNOWN((byte) 7),

  /** TIMESTAMP. */
  TIMESTAMP((byte) 8),

  /** DATE. */
  DATE((byte) 9),

  /** BLOB. */
  BLOB((byte) 10),

  /** STRING */
  STRING((byte) 11);

  private final byte type;
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
    compatibleTypes.put(STRING, stringCompatibleTypes);
  }

  TSDataType(byte type) {
    this.type = type;
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
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
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
    switch (this) {
      case BOOLEAN:
        if (sourceType == TSDataType.BOOLEAN) {
          return value;
        } else {
          break;
        }
      case INT32:
        if (sourceType == TSDataType.INT32) {
          return value;
        } else {
          break;
        }
      case INT64:
        if (sourceType == TSDataType.INT64) {
          return value;
        } else if (sourceType == INT32) {
          return (long) ((int) value);
        } else if (sourceType == TIMESTAMP) {
          return value;
        } else {
          break;
        }
      case FLOAT:
        if (sourceType == TSDataType.FLOAT) {
          return value;
        } else if (sourceType == INT32) {
          return (float) ((int) value);
        } else {
          break;
        }
      case DOUBLE:
        if (sourceType == TSDataType.DOUBLE) {
          return value;
        } else if (sourceType == INT32) {
          return (double) ((int) value);
        } else if (sourceType == INT64) {
          return (double) ((long) value);
        } else if (sourceType == FLOAT) {
          return (double) ((float) value);
        } else if (sourceType == TIMESTAMP) {
          return (double) ((long) value);
        } else {
          break;
        }
      case TEXT:
        if (sourceType == TSDataType.TEXT || sourceType == TSDataType.STRING) {
          return value;
        } else {
          break;
        }
      case TIMESTAMP:
        if (sourceType == TSDataType.TIMESTAMP) {
          return value;
        } else if (sourceType == INT32) {
          return (long) ((int) value);
        } else if (sourceType == INT64) {
          return value;
        } else {
          break;
        }
      case DATE:
        if (sourceType == TSDataType.DATE) {
          return value;
        } else {
          break;
        }
      case BLOB:
        if (sourceType == TSDataType.BLOB
            || sourceType == TSDataType.STRING
            || sourceType == TSDataType.TEXT) {
          return value;
        } else {
          break;
        }
      case STRING:
        if (sourceType == TSDataType.STRING || sourceType == TSDataType.TEXT) {
          return value;
        } else {
          break;
        }
      case VECTOR:
      case UNKNOWN:
      default:
        break;
    }
    throw new ClassCastException(
        String.format("Unsupported cast: from %s to %s", sourceType, this));
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
      case TEXT:
        if (sourceType == TSDataType.TEXT || sourceType == STRING) {
          return array;
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
      case STRING:
        if (sourceType == TSDataType.STRING || sourceType == TSDataType.TEXT) {
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
        String.format("Unsupported cast: from %s to %s", sourceType, this));
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
        return false;
      default:
        throw new UnSupportedDataTypeException(this.toString());
    }
  }

  public boolean isBinary() {
    return this == TEXT || this == STRING || this == BLOB;
  }
}
