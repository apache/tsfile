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
package org.apache.tsfile.tools;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;

import java.nio.charset.StandardCharsets;

public class ValueConverter {

  public static Object convert(Object value, TSDataType targetType, boolean isMeasurement) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return fromString((String) value, targetType, isMeasurement);
    }
    return fromObject(value, targetType, isMeasurement);
  }

  private static Object fromString(String value, TSDataType targetType, boolean isMeasurement) {
    switch (targetType) {
      case BOOLEAN:
        return Boolean.valueOf(value);
      case INT32:
        return Integer.valueOf(value);
      case INT64:
        return Long.valueOf(value);
      case FLOAT:
        return Float.valueOf(value);
      case DOUBLE:
        return Double.valueOf(value);
      case TEXT:
      case STRING:
        if (isMeasurement) {
          return new Binary(value, StandardCharsets.UTF_8);
        }
        return value;
      case BLOB:
        return new Binary(value.getBytes(StandardCharsets.UTF_8));
      default:
        return value;
    }
  }

  private static Object fromObject(Object value, TSDataType targetType, boolean isMeasurement) {
    switch (targetType) {
      case BOOLEAN:
        if (value instanceof Boolean) {
          return value;
        }
        return Boolean.valueOf(value.toString());
      case INT32:
        if (value instanceof Integer) {
          return value;
        }
        if (value instanceof Number) {
          return ((Number) value).intValue();
        }
        return Integer.valueOf(value.toString());
      case INT64:
        if (value instanceof Long) {
          return value;
        }
        if (value instanceof Number) {
          return ((Number) value).longValue();
        }
        return Long.valueOf(value.toString());
      case FLOAT:
        if (value instanceof Float) {
          return value;
        }
        if (value instanceof Number) {
          return ((Number) value).floatValue();
        }
        return Float.valueOf(value.toString());
      case DOUBLE:
        if (value instanceof Double) {
          return value;
        }
        if (value instanceof Number) {
          return ((Number) value).doubleValue();
        }
        return Double.valueOf(value.toString());
      case TEXT:
      case STRING:
        if (isMeasurement) {
          if (value instanceof Binary) {
            return value;
          }
          return new Binary(value.toString(), StandardCharsets.UTF_8);
        }
        if (value instanceof String) {
          return value;
        }
        return value.toString();
      case BLOB:
        if (value instanceof Binary) {
          return value;
        }
        if (value instanceof byte[]) {
          return new Binary((byte[]) value);
        }
        return new Binary(value.toString().getBytes(StandardCharsets.UTF_8));
      default:
        return value;
    }
  }
}
