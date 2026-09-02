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
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;

public class ValueConverter {

  private static final TypeService<StringConverter> FROM_STRING_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> (value, isMeasurement, timePrecision) -> Boolean.valueOf(value);
            case INT32 -> (value, isMeasurement, timePrecision) -> Integer.valueOf(value);
            case INT64 -> (value, isMeasurement, timePrecision) -> Long.valueOf(value);
            case FLOAT -> (value, isMeasurement, timePrecision) -> Float.valueOf(value);
            case DOUBLE -> (value, isMeasurement, timePrecision) -> Double.valueOf(value);
            case TEXT, STRING ->
                (value, isMeasurement, timePrecision) ->
                    isMeasurement ? new Binary(value, StandardCharsets.UTF_8) : value;
            case BLOB ->
                (value, isMeasurement, timePrecision) ->
                    new Binary(value.getBytes(StandardCharsets.UTF_8));
            case DATE -> (value, isMeasurement, timePrecision) -> parseDate(value);
            case TIMESTAMP ->
                (value, isMeasurement, timePrecision) -> parseTimestamp(value, timePrecision);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (value, isMeasurement, timePrecision) -> value;
          };

  private static final TypeService<ObjectConverter> FROM_OBJECT_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (value, isMeasurement, timePrecision) ->
                    value instanceof Boolean ? value : Boolean.valueOf(value.toString());
            case INT32 ->
                (value, isMeasurement, timePrecision) -> {
                  if (value instanceof Integer) {
                    return value;
                  }
                  if (value instanceof Number) {
                    return ((Number) value).intValue();
                  }
                  return Integer.valueOf(value.toString());
                };
            case INT64 ->
                (value, isMeasurement, timePrecision) -> {
                  if (value instanceof Long) {
                    return value;
                  }
                  if (value instanceof Number) {
                    return ((Number) value).longValue();
                  }
                  return Long.valueOf(value.toString());
                };
            case FLOAT ->
                (value, isMeasurement, timePrecision) -> {
                  if (value instanceof Float) {
                    return value;
                  }
                  if (value instanceof Number) {
                    return ((Number) value).floatValue();
                  }
                  return Float.valueOf(value.toString());
                };
            case DOUBLE ->
                (value, isMeasurement, timePrecision) -> {
                  if (value instanceof Double) {
                    return value;
                  }
                  if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                  }
                  return Double.valueOf(value.toString());
                };
            case TEXT, STRING ->
                (value, isMeasurement, timePrecision) -> {
                  if (isMeasurement) {
                    return value instanceof Binary
                        ? value
                        : new Binary(value.toString(), StandardCharsets.UTF_8);
                  }
                  return value instanceof String ? value : value.toString();
                };
            case BLOB ->
                (value, isMeasurement, timePrecision) -> {
                  if (value instanceof Binary) {
                    return value;
                  }
                  if (value instanceof byte[]) {
                    return new Binary((byte[]) value);
                  }
                  return new Binary(value.toString().getBytes(StandardCharsets.UTF_8));
                };
            case DATE -> (value, isMeasurement, timePrecision) -> toLocalDate(value);
            case TIMESTAMP ->
                (value, isMeasurement, timePrecision) -> toEpochLong(value, timePrecision);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (value, isMeasurement, timePrecision) -> value;
          };

  static {
    FROM_STRING_SERVICE.check();
    FROM_OBJECT_SERVICE.check();
  }

  public static Object convert(Object value, TSDataType targetType, boolean isMeasurement) {
    return convert(value, targetType, isMeasurement, "ms");
  }

  public static Object convert(
      Object value, TSDataType targetType, boolean isMeasurement, String timePrecision) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return fromString((String) value, targetType, isMeasurement, timePrecision);
    }
    return fromObject(value, targetType, isMeasurement, timePrecision);
  }

  private static Object fromString(
      String value, TSDataType targetType, boolean isMeasurement, String timePrecision) {
    return FROM_STRING_SERVICE
        .call(Type.fromTsDataType(targetType))
        .convert(value, isMeasurement, timePrecision);
  }

  private static Object fromObject(
      Object value, TSDataType targetType, boolean isMeasurement, String timePrecision) {
    return FROM_OBJECT_SERVICE
        .call(Type.fromTsDataType(targetType))
        .convert(value, isMeasurement, timePrecision);
  }

  private static LocalDate parseDate(String value) {
    String s = value.trim();
    try {
      return LocalDate.parse(s, DateTimeUtils.ISO_LOCAL_DATE_WIDTH_1_2);
    } catch (DateTimeParseException ignored) {
      // try next format
    }
    try {
      return LocalDate.parse(s, DateTimeUtils.ISO_LOCAL_DATE_WITH_SLASH);
    } catch (DateTimeParseException ignored) {
      // try next format
    }
    try {
      return LocalDate.parse(s, DateTimeUtils.ISO_LOCAL_DATE_WITH_DOT);
    } catch (DateTimeParseException ignored) {
      // try LocalDate.parse default
    }
    try {
      return LocalDate.parse(s);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException("Cannot parse DATE value: " + value, e);
    }
  }

  private static LocalDate toLocalDate(Object value) {
    if (value instanceof LocalDate) {
      return (LocalDate) value;
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toLocalDate();
    }
    if (value instanceof Instant) {
      return ((Instant) value).atZone(ZoneId.systemDefault()).toLocalDate();
    }
    if (value instanceof Number) {
      // Parquet/Arrow DATE is days since 1970-01-01
      return LocalDate.ofEpochDay(((Number) value).longValue());
    }
    return parseDate(value.toString());
  }

  private static long parseTimestamp(String value, String timePrecision) {
    String s = value.trim();
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException ignored) {
      // not numeric — fall through to datetime parsing
    }
    return DateTimeUtils.convertDatetimeStrToLong(s, ZoneId.systemDefault(), timePrecision);
  }

  private static long toEpochLong(Object value, String timePrecision) {
    if (value instanceof Long) {
      return (Long) value;
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value instanceof Instant) {
      Instant instant = (Instant) value;
      switch (timePrecision) {
        case "ns":
          return Math.addExact(
              Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
        case "us":
          return Math.addExact(
              Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000);
        case "s":
          return instant.getEpochSecond();
        case "ms":
        default:
          return instant.toEpochMilli();
      }
    }
    return parseTimestamp(value.toString(), timePrecision);
  }

  @FunctionalInterface
  private interface StringConverter {
    Object convert(String value, boolean isMeasurement, String timePrecision);
  }

  @FunctionalInterface
  private interface ObjectConverter {
    Object convert(Object value, boolean isMeasurement, String timePrecision);
  }
}
