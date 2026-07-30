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

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.i18n.Messages;

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileTableFilterTranslator {

  private static final BigInteger BIG_LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger BIG_LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
  private static final BigInteger BIG_ONE = BigInteger.ONE;
  private static final BigInteger BIG_NS_PER_MICRO = BigInteger.valueOf(1_000L);

  private final TsFileTableSchema tableSchema;
  private final TsFileTableOptions options;
  private long startTime = Long.MIN_VALUE;
  private long endTime = Long.MAX_VALUE;
  private final Map<String, String> tagEqualities = new HashMap<>();
  private final List<Filter> pushedFilters = new ArrayList<>();
  private final List<Filter> residualFilters = new ArrayList<>();

  public TsFileTableFilterTranslator(TsFileTableSchema tableSchema, TsFileTableOptions options) {
    this.tableSchema = tableSchema;
    this.options = options;
  }

  public Filter[] pushFilters(Filter[] filters) {
    pushedFilters.clear();
    residualFilters.clear();
    startTime = Long.MIN_VALUE;
    endTime = Long.MAX_VALUE;
    tagEqualities.clear();
    if (!options.pushdown()) {
      Collections.addAll(residualFilters, filters);
      return residualFilters.toArray(new Filter[0]);
    }
    for (Filter filter : filters) {
      translate(filter);
    }
    return residualFilters.toArray(new Filter[0]);
  }

  private void translate(Filter filter) {
    if (filter instanceof And) {
      translate(((And) filter).left());
      translate(((And) filter).right());
      return;
    }
    if (filter instanceof EqualTo) {
      EqualTo equalTo = (EqualTo) filter;
      String attribute = normalizeAttribute(equalTo.attribute());
      if (isTimeColumn(attribute)) {
        pushTimeFilter(filter, equalTo.value(), TimeComparison.EQUAL);
        return;
      }
      if (isTagColumn(attribute) && equalTo.value() instanceof String) {
        String previous = tagEqualities.putIfAbsent(attribute, (String) equalTo.value());
        if (previous == null || previous.equals(equalTo.value())) {
          pushedFilters.add(filter);
        } else {
          residualFilters.add(filter);
        }
        return;
      }
    } else if (filter instanceof GreaterThan) {
      GreaterThan greaterThan = (GreaterThan) filter;
      if (isTimeColumn(normalizeAttribute(greaterThan.attribute()))) {
        pushTimeFilter(filter, greaterThan.value(), TimeComparison.GREATER_THAN);
        return;
      }
    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
      if (isTimeColumn(normalizeAttribute(greaterThanOrEqual.attribute()))) {
        pushTimeFilter(filter, greaterThanOrEqual.value(), TimeComparison.GREATER_THAN_OR_EQUAL);
        return;
      }
    } else if (filter instanceof LessThan) {
      LessThan lessThan = (LessThan) filter;
      if (isTimeColumn(normalizeAttribute(lessThan.attribute()))) {
        pushTimeFilter(filter, lessThan.value(), TimeComparison.LESS_THAN);
        return;
      }
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
      if (isTimeColumn(normalizeAttribute(lessThanOrEqual.attribute()))) {
        pushTimeFilter(filter, lessThanOrEqual.value(), TimeComparison.LESS_THAN_OR_EQUAL);
        return;
      }
    }
    residualFilters.add(filter);
  }

  private boolean isTimeColumn(String attribute) {
    return TsFileTableOptions.normalizeName(options.timeColumn()).equals(attribute);
  }

  private boolean isTagColumn(String attribute) {
    TsFileTableSchema.ColumnInfo column = tableSchema.column(attribute);
    return column != null && column.category() == ColumnCategory.TAG;
  }

  private String normalizeAttribute(String attribute) {
    return TsFileTableOptions.normalizeName(attribute);
  }

  private void pushTimeFilter(Filter filter, Object value, TimeComparison comparison) {
    RawTimeRange range = toRawTimeRange(value, comparison);
    if (range == null) {
      residualFilters.add(filter);
      return;
    }
    startTime = Math.max(startTime, range.startTime);
    endTime = Math.min(endTime, range.endTime);
    pushedFilters.add(filter);
  }

  private RawTimeRange toRawTimeRange(Object value, TimeComparison comparison) {
    if (value instanceof Number) {
      return rawLongRange(((Number) value).longValue(), comparison);
    }
    if (value instanceof Timestamp) {
      return timestampRange(timestampMicros((Timestamp) value), comparison);
    }
    return null;
  }

  private RawTimeRange timestampRange(long micros, TimeComparison comparison) {
    switch (options.timestampPrecision()) {
      case MS:
        return timestampMillisRange(micros, comparison);
      case US:
        return rawLongRange(micros, comparison);
      case NS:
        return timestampNanosRange(micros, comparison);
      default:
        throw new TsFileSparkException(
            Messages.format(
                "error.spark.unsupported_timestamp_precision", options.timestampPrecision()));
    }
  }

  private RawTimeRange timestampMillisRange(long micros, TimeComparison comparison) {
    switch (comparison) {
      case EQUAL:
        if (Math.floorMod(micros, 1_000L) != 0) {
          return RawTimeRange.empty();
        }
        return RawTimeRange.point(Math.floorDiv(micros, 1_000L));
      case GREATER_THAN:
        return rawLongRange(Math.floorDiv(micros, 1_000L), TimeComparison.GREATER_THAN);
      case GREATER_THAN_OR_EQUAL:
        return rawLongRange(ceilDiv(micros, 1_000L), TimeComparison.GREATER_THAN_OR_EQUAL);
      case LESS_THAN:
        return rawLongRange(ceilDiv(micros, 1_000L), TimeComparison.LESS_THAN);
      case LESS_THAN_OR_EQUAL:
        return rawLongRange(Math.floorDiv(micros, 1_000L), TimeComparison.LESS_THAN_OR_EQUAL);
      default:
        throw new TsFileSparkException(
            Messages.format(
                "error.spark.unsupported_timestamp_precision", options.timestampPrecision()));
    }
  }

  private RawTimeRange timestampNanosRange(long micros, TimeComparison comparison) {
    BigInteger value = BigInteger.valueOf(micros);
    switch (comparison) {
      case EQUAL:
        return RawTimeRange.between(
            value.multiply(BIG_NS_PER_MICRO),
            value.add(BIG_ONE).multiply(BIG_NS_PER_MICRO).subtract(BIG_ONE));
      case GREATER_THAN:
        return RawTimeRange.atLeast(value.add(BIG_ONE).multiply(BIG_NS_PER_MICRO));
      case GREATER_THAN_OR_EQUAL:
        return RawTimeRange.atLeast(value.multiply(BIG_NS_PER_MICRO));
      case LESS_THAN:
        return RawTimeRange.atMost(value.multiply(BIG_NS_PER_MICRO).subtract(BIG_ONE));
      case LESS_THAN_OR_EQUAL:
        return RawTimeRange.atMost(value.add(BIG_ONE).multiply(BIG_NS_PER_MICRO).subtract(BIG_ONE));
      default:
        throw new TsFileSparkException(
            Messages.format(
                "error.spark.unsupported_timestamp_precision", options.timestampPrecision()));
    }
  }

  private RawTimeRange rawLongRange(long rawTime, TimeComparison comparison) {
    switch (comparison) {
      case EQUAL:
        return RawTimeRange.point(rawTime);
      case GREATER_THAN:
        return rawTime == Long.MAX_VALUE ? RawTimeRange.empty() : RawTimeRange.atLeast(rawTime + 1);
      case GREATER_THAN_OR_EQUAL:
        return RawTimeRange.atLeast(rawTime);
      case LESS_THAN:
        return rawTime == Long.MIN_VALUE ? RawTimeRange.empty() : RawTimeRange.atMost(rawTime - 1);
      case LESS_THAN_OR_EQUAL:
        return RawTimeRange.atMost(rawTime);
      default:
        throw new TsFileSparkException(
            Messages.format("error.spark.unsupported_time_filter_literal", rawTime));
    }
  }

  private long timestampMicros(Timestamp timestamp) {
    Instant instant = timestamp.toInstant();
    return Math.addExact(
        Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
  }

  private long ceilDiv(long value, long divisor) {
    long floor = Math.floorDiv(value, divisor);
    return Math.floorMod(value, divisor) == 0 ? floor : floor + 1;
  }

  public Filter[] pushedFilters() {
    return pushedFilters.toArray(new Filter[0]);
  }

  public long startTime() {
    return startTime;
  }

  public long endTime() {
    return endTime;
  }

  public Map<String, String> tagEqualities() {
    return Collections.unmodifiableMap(tagEqualities);
  }

  private enum TimeComparison {
    EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL
  }

  private static class RawTimeRange {
    private final long startTime;
    private final long endTime;

    private RawTimeRange(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    private static RawTimeRange point(long value) {
      return new RawTimeRange(value, value);
    }

    private static RawTimeRange atLeast(long value) {
      return new RawTimeRange(value, Long.MAX_VALUE);
    }

    private static RawTimeRange atLeast(BigInteger value) {
      if (value.compareTo(BIG_LONG_MAX) > 0) {
        return empty();
      }
      return new RawTimeRange(clampLower(value), Long.MAX_VALUE);
    }

    private static RawTimeRange atMost(long value) {
      return new RawTimeRange(Long.MIN_VALUE, value);
    }

    private static RawTimeRange atMost(BigInteger value) {
      if (value.compareTo(BIG_LONG_MIN) < 0) {
        return empty();
      }
      return new RawTimeRange(Long.MIN_VALUE, clampUpper(value));
    }

    private static RawTimeRange between(BigInteger startTime, BigInteger endTime) {
      if (startTime.compareTo(BIG_LONG_MAX) > 0 || endTime.compareTo(BIG_LONG_MIN) < 0) {
        return empty();
      }
      return new RawTimeRange(clampLower(startTime), clampUpper(endTime));
    }

    private static RawTimeRange empty() {
      return new RawTimeRange(1L, 0L);
    }

    private static long clampLower(BigInteger value) {
      return value.compareTo(BIG_LONG_MIN) < 0 ? Long.MIN_VALUE : value.longValue();
    }

    private static long clampUpper(BigInteger value) {
      return value.compareTo(BIG_LONG_MAX) > 0 ? Long.MAX_VALUE : value.longValue();
    }
  }
}
