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

import org.apache.tsfile.i18n.Messages;

import java.time.Instant;

public class TimeConverter {

  private static final long THRESHOLD_NS = (long) 1e15;
  private static final long THRESHOLD_US = (long) 1e12;
  private static final long THRESHOLD_MS = (long) 1e11;

  private final String targetPrecision;

  public TimeConverter(String targetPrecision) {
    this.targetPrecision = targetPrecision;
  }

  public long convert(Object value) {
    if (value == null) {
      throw new IllegalArgumentException(Messages.get("error.tools.time_value_null"));
    }
    if (value instanceof Instant) {
      return fromInstant((Instant) value);
    }
    if (value instanceof Number) {
      return fromNumeric(((Number) value).longValue());
    }
    return fromString(value.toString());
  }

  public long convert(Object value, String sourcePrecision) {
    if (value == null) {
      throw new IllegalArgumentException(Messages.get("error.tools.time_value_null"));
    }
    if (value instanceof Instant) {
      return fromInstant((Instant) value);
    }
    if (value instanceof Number) {
      return rescale(((Number) value).longValue(), sourcePrecision, targetPrecision);
    }
    return fromStringWithPrecision(value.toString(), sourcePrecision);
  }

  private long fromStringWithPrecision(String value, String sourcePrecision) {
    try {
      long numeric = Long.parseLong(value);
      return rescale(numeric, sourcePrecision, targetPrecision);
    } catch (NumberFormatException e) {
      return DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
          value, targetPrecision);
    }
  }

  private long fromInstant(Instant instant) {
    switch (targetPrecision) {
      case "ns":
        return Math.addExact(
            Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
      case "us":
        return Math.addExact(
            Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000);
      case "ms":
      default:
        return instant.toEpochMilli();
    }
  }

  private long fromNumeric(long value) {
    String inferred = inferPrecision(value);
    return rescale(value, inferred, targetPrecision);
  }

  private long fromString(String value) {
    try {
      long numeric = Long.parseLong(value);
      return fromNumeric(numeric);
    } catch (NumberFormatException e) {
      return DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
          value, targetPrecision);
    }
  }

  static String inferPrecision(long value) {
    long abs = Math.abs(value);
    if (abs > THRESHOLD_NS) {
      return "ns";
    } else if (abs > THRESHOLD_US) {
      return "us";
    } else if (abs > THRESHOLD_MS) {
      return "ms";
    }
    return "s";
  }

  static long rescale(long value, String from, String to) {
    if (from.equals(to)) {
      return value;
    }
    long valueInNs = toNanos(value, from);
    return fromNanos(valueInNs, to);
  }

  private static long toNanos(long value, String precision) {
    switch (precision) {
      case "s":
        return Math.multiplyExact(value, 1_000_000_000L);
      case "ms":
        return Math.multiplyExact(value, 1_000_000L);
      case "us":
        return Math.multiplyExact(value, 1_000L);
      case "ns":
        return value;
      default:
        throw new IllegalArgumentException(
            Messages.format("error.tools.unknown_precision", precision));
    }
  }

  private static long fromNanos(long nanos, String precision) {
    switch (precision) {
      case "s":
        return nanos / 1_000_000_000L;
      case "ms":
        return nanos / 1_000_000L;
      case "us":
        return nanos / 1_000L;
      case "ns":
        return nanos;
      default:
        throw new IllegalArgumentException(
            Messages.format("error.tools.unknown_precision", precision));
    }
  }
}
