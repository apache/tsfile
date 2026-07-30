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

package org.apache.tsfile.spark;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.i18n.Messages;

import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Locale;

public final class TsFileTableTypeConverter {

  private TsFileTableTypeConverter() {}

  public static DataType toSparkType(TSDataType type, TsFileTableOptions.TimestampAs timestampAs) {
    switch (type) {
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INT32:
        return DataTypes.IntegerType;
      case INT64:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case TEXT:
      case STRING:
        return DataTypes.StringType;
      case DATE:
        return DataTypes.DateType;
      case TIMESTAMP:
        return timestampAs == TsFileTableOptions.TimestampAs.TIMESTAMP
            ? DataTypes.TimestampType
            : DataTypes.LongType;
      case BLOB:
        return DataTypes.BinaryType;
      case VECTOR:
      case UNKNOWN:
      case OBJECT:
      default:
        throw new TsFileSparkException(
            Messages.format("error.spark.unsupported_tsfile_type_connector", type));
    }
  }

  public static TSDataType toTsFileFieldType(
      DataType sparkType, TsFileTableOptions.TimestampAs timestampAs) {
    if (sparkType instanceof BooleanType) {
      return TSDataType.BOOLEAN;
    }
    if (sparkType instanceof IntegerType) {
      return TSDataType.INT32;
    }
    if (sparkType instanceof LongType) {
      return TSDataType.INT64;
    }
    if (sparkType instanceof FloatType) {
      return TSDataType.FLOAT;
    }
    if (sparkType instanceof DoubleType) {
      return TSDataType.DOUBLE;
    }
    if (sparkType instanceof StringType) {
      return TSDataType.STRING;
    }
    if (sparkType instanceof DateType) {
      return TSDataType.DATE;
    }
    if (sparkType instanceof TimestampType) {
      return TSDataType.TIMESTAMP;
    }
    if (sparkType instanceof BinaryType) {
      return TSDataType.BLOB;
    }
    throw new TsFileSparkException(
        Messages.format("error.spark.unsupported_spark_field_type", sparkType));
  }

  public static long timestampMicrosToRaw(
      long micros, TsFileTableOptions.TimestampPrecision precision) {
    switch (precision) {
      case MS:
        return Math.floorDiv(micros, 1_000L);
      case US:
        return micros;
      case NS:
        return Math.multiplyExact(micros, 1_000L);
      default:
        throw new TsFileSparkException(
            Messages.format("error.spark.unsupported_timestamp_precision", precision));
    }
  }

  public static long rawToTimestampMicros(
      long raw, TsFileTableOptions.TimestampPrecision precision) {
    switch (precision) {
      case MS:
        return Math.multiplyExact(raw, 1_000L);
      case US:
        return raw;
      case NS:
        return Math.floorDiv(raw, 1_000L);
      default:
        throw new TsFileSparkException(
            Messages.format("error.spark.unsupported_timestamp_precision", precision));
    }
  }

  public static int toSparkDate(LocalDate date) {
    return Math.toIntExact(date.toEpochDay());
  }

  public static LocalDate fromSparkDate(int days) {
    return LocalDate.ofEpochDay(days);
  }

  public static LocalDate millisToDate(long millis) {
    return Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC).toLocalDate();
  }

  public static TSEncoding parseEncoding(String encoding, TSDataType type) {
    if (encoding == null) {
      return null;
    }
    TSEncoding parsed;
    try {
      parsed = TSEncoding.valueOf(encoding.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new TsFileSparkException(Messages.format("error.spark.encoding_invalid", encoding), e);
    }
    if (!TSEncoding.isSupported(type, parsed)) {
      throw new TsFileSparkException(
          Messages.format("error.spark.encoding_not_supported", parsed, type));
    }
    return parsed;
  }

  public static CompressionType parseCompression(String compression) {
    if (compression == null) {
      return null;
    }
    try {
      return CompressionType.valueOf(compression.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new TsFileSparkException(
          Messages.format("error.spark.compression_invalid", compression), e);
    }
  }
}
