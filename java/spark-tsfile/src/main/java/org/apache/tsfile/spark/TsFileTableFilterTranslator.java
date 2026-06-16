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

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileTableFilterTranslator {

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
        long value = toRawTime(equalTo.value());
        startTime = Math.max(startTime, value);
        endTime = Math.min(endTime, value);
        pushedFilters.add(filter);
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
        startTime = Math.max(startTime, addOne(toRawTime(greaterThan.value())));
        pushedFilters.add(filter);
        return;
      }
    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
      if (isTimeColumn(normalizeAttribute(greaterThanOrEqual.attribute()))) {
        startTime = Math.max(startTime, toRawTime(greaterThanOrEqual.value()));
        pushedFilters.add(filter);
        return;
      }
    } else if (filter instanceof LessThan) {
      LessThan lessThan = (LessThan) filter;
      if (isTimeColumn(normalizeAttribute(lessThan.attribute()))) {
        endTime = Math.min(endTime, subtractOne(toRawTime(lessThan.value())));
        pushedFilters.add(filter);
        return;
      }
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
      if (isTimeColumn(normalizeAttribute(lessThanOrEqual.attribute()))) {
        endTime = Math.min(endTime, toRawTime(lessThanOrEqual.value()));
        pushedFilters.add(filter);
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

  private long toRawTime(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    if (value instanceof Timestamp) {
      Instant instant = ((Timestamp) value).toInstant();
      long micros =
          Math.addExact(
              Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
      return TsFileTableTypeConverter.timestampMicrosToRaw(micros, options.timestampPrecision());
    }
    throw new TsFileSparkException("Unsupported time filter literal: " + value);
  }

  private long addOne(long value) {
    return value == Long.MAX_VALUE ? Long.MAX_VALUE : value + 1;
  }

  private long subtractOne(long value) {
    return value == Long.MIN_VALUE ? Long.MIN_VALUE : value - 1;
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
}
