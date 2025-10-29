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

package org.apache.tsfile.read.filter.operator;

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.read.filter.basic.TimeFilter;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.time.temporal.ChronoField.ALIGNED_WEEK_OF_YEAR;

/**
 * These are the extract time column operators in a filter predicate expression tree. They are
 * constructed by using the methods in {@link TimeFilterApi}
 */
public final class ExtractTimeFilterOperators {

  public enum Field {
    YEAR,
    QUARTER,
    MONTH,
    WEEK,
    DAY,
    DAY_OF_MONTH,
    DAY_OF_WEEK,
    DOW,
    DAY_OF_YEAR,
    DOY,
    HOUR,
    MINUTE,
    SECOND,
    MS,
    US,
    NS
  }

  private ExtractTimeFilterOperators() {
    // forbidden construction
  }

  private static final String OPERATOR_TO_STRING_FORMAT = "extract %s from time %s %s";

  abstract static class ExtractTimeCompareFilter extends TimeFilter {
    protected final long constant;

    protected final Field field;
    protected final ZoneId zoneId;
    protected final TimeUnit currPrecision;

    private final transient Function<Long, Long> CAST_TIMESTAMP_TO_MS;
    private final transient Function<Long, Long> EXTRACT_TIMESTAMP_MS_PART;
    private final transient Function<Long, Long> EXTRACT_TIMESTAMP_US_PART;
    private final transient Function<Long, Long> EXTRACT_TIMESTAMP_NS_PART;
    protected final transient Function<Integer, Long> GET_YEAR_TIMESTAMP;

    // calculate extraction of time
    protected final transient Function<Long, Long> evaluateFunction;
    // calculate if the truncations of input times are the same
    protected final transient BiFunction<Long, Long, Boolean> truncatedEqualsFunction;

    // constant cannot be null
    protected ExtractTimeCompareFilter(
        long constant, Field field, ZoneId zoneId, TimeUnit currPrecision) {
      this.constant = constant;
      this.field = field;
      this.zoneId = zoneId;
      this.currPrecision = currPrecision;
      // make lifestyles of these functions are same with object to avoid switch case in calculation
      switch (currPrecision) {
        case MICROSECONDS:
          CAST_TIMESTAMP_TO_MS = timestamp -> timestamp / 1000;
          EXTRACT_TIMESTAMP_MS_PART = timestamp -> Math.floorMod(timestamp, 1000_000L) / 1000;
          EXTRACT_TIMESTAMP_US_PART = timestamp -> Math.floorMod(timestamp, 1000L);
          EXTRACT_TIMESTAMP_NS_PART = timestamp -> 0L;
          GET_YEAR_TIMESTAMP =
              year ->
                  Math.multiplyExact(
                      LocalDate.of(year, 1, 1).atStartOfDay(zoneId).toEpochSecond(), 1000_000L);
          break;
        case NANOSECONDS:
          CAST_TIMESTAMP_TO_MS = timestamp -> timestamp / 1000000;
          EXTRACT_TIMESTAMP_MS_PART =
              timestamp -> Math.floorMod(timestamp, 1000_000_000L) / 1000_000;
          EXTRACT_TIMESTAMP_US_PART = timestamp -> Math.floorMod(timestamp, 1000_000L) / 1000;
          EXTRACT_TIMESTAMP_NS_PART = timestamp -> Math.floorMod(timestamp, 1000L);
          GET_YEAR_TIMESTAMP =
              year ->
                  Math.multiplyExact(
                      LocalDate.of(year, 1, 1).atStartOfDay(zoneId).toEpochSecond(), 1000_000_000L);
          break;
        case MILLISECONDS:
        default:
          CAST_TIMESTAMP_TO_MS = timestamp -> timestamp;
          EXTRACT_TIMESTAMP_MS_PART = timestamp -> Math.floorMod(timestamp, 1000L);
          EXTRACT_TIMESTAMP_US_PART = timestamp -> 0L;
          EXTRACT_TIMESTAMP_NS_PART = timestamp -> 0L;
          GET_YEAR_TIMESTAMP =
              year ->
                  Math.multiplyExact(
                      LocalDate.of(year, 1, 1).atStartOfDay(zoneId).toEpochSecond(), 1000L);
          break;
      }
      evaluateFunction = constructEvaluateFunction(field, zoneId);
      truncatedEqualsFunction = constructTruncatedEqualsFunction(field, zoneId);
    }

    protected Function<Long, Long> constructEvaluateFunction(Field field, ZoneId zoneId) {
      switch (field) {
        case YEAR:
          return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getYear();
        case QUARTER:
          return timestamp -> (convertToZonedDateTime(timestamp, zoneId).getMonthValue() + 2L) / 3L;
        case MONTH:
          return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getMonthValue();
        case WEEK:
          return timestamp ->
              convertToZonedDateTime(timestamp, zoneId).getLong(ALIGNED_WEEK_OF_YEAR);
        case DAY:
        case DAY_OF_MONTH:
          return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getDayOfMonth();
        case DAY_OF_WEEK:
        case DOW:
          return timestamp ->
              (long) convertToZonedDateTime(timestamp, zoneId).getDayOfWeek().getValue();
        case DAY_OF_YEAR:
        case DOY:
          return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getDayOfYear();
        case HOUR:
          return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getHour();
        case MINUTE:
          return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getMinute();
        case SECOND:
          return timestamp -> (long) convertToZonedDateTime(timestamp, zoneId).getSecond();
        case MS:
          return EXTRACT_TIMESTAMP_MS_PART;
        case US:
          return EXTRACT_TIMESTAMP_US_PART;
        case NS:
          return EXTRACT_TIMESTAMP_NS_PART;
        default:
          throw new UnsupportedOperationException("Unexpected extract field: " + field);
      }
    }

    /** Truncate timestamps to based unit then compare */
    protected BiFunction<Long, Long, Boolean> constructTruncatedEqualsFunction(
        Field field, ZoneId zoneId) {
      switch (field) {
        case YEAR:
          return (timestamp1, timestamp2) -> true;
          // base YEAR
        case QUARTER:
        case MONTH:
        case WEEK:
        case DAY_OF_YEAR:
        case DOY:
          return (timestamp1, timestamp2) ->
              convertToZonedDateTime(timestamp1, zoneId)
                  .withMonth(1)
                  .withDayOfMonth(1)
                  .truncatedTo(ChronoUnit.DAYS)
                  .equals(
                      convertToZonedDateTime(timestamp2, zoneId)
                          .withMonth(1)
                          .withDayOfMonth(1)
                          .truncatedTo(ChronoUnit.DAYS));
          // base MONTH
        case DAY:
        case DAY_OF_MONTH:
          return (timestamp1, timestamp2) ->
              convertToZonedDateTime(timestamp1, zoneId)
                  .withDayOfMonth(1)
                  .truncatedTo(ChronoUnit.DAYS)
                  .equals(
                      convertToZonedDateTime(timestamp2, zoneId)
                          .withDayOfMonth(1)
                          .truncatedTo(ChronoUnit.DAYS));
          // base WEEK
        case DAY_OF_WEEK:
        case DOW:
          return (timestamp1, timestamp2) ->
              convertToZonedDateTime(timestamp1, zoneId)
                  .with(DayOfWeek.MONDAY)
                  .truncatedTo(ChronoUnit.DAYS)
                  .equals(
                      convertToZonedDateTime(timestamp2, zoneId)
                          .with(DayOfWeek.MONDAY)
                          .truncatedTo(ChronoUnit.DAYS));
          // base DAY
        case HOUR:
          return (timestamp1, timestamp2) ->
              convertToZonedDateTime(timestamp1, zoneId)
                  .truncatedTo(ChronoUnit.DAYS)
                  .equals(convertToZonedDateTime(timestamp2, zoneId).truncatedTo(ChronoUnit.DAYS));
          // base HOUR
        case MINUTE:
          return (timestamp1, timestamp2) ->
              convertToZonedDateTime(timestamp1, zoneId)
                  .truncatedTo(ChronoUnit.HOURS)
                  .equals(convertToZonedDateTime(timestamp2, zoneId).truncatedTo(ChronoUnit.HOURS));
          // base MINUTE
        case SECOND:
          return (timestamp1, timestamp2) ->
              convertToZonedDateTime(timestamp1, zoneId)
                  .truncatedTo(ChronoUnit.MINUTES)
                  .equals(
                      convertToZonedDateTime(timestamp2, zoneId).truncatedTo(ChronoUnit.MINUTES));
          // base SECOND
        case MS:
          return (timestamp1, timestamp2) ->
              convertToZonedDateTime(timestamp1, zoneId)
                  .truncatedTo(ChronoUnit.SECONDS)
                  .equals(
                      convertToZonedDateTime(timestamp2, zoneId).truncatedTo(ChronoUnit.SECONDS));
          // base MS
        case US:
          return (timestamp1, timestamp2) ->
              convertToZonedDateTime(timestamp1, zoneId)
                  .truncatedTo(ChronoUnit.MILLIS)
                  .equals(
                      convertToZonedDateTime(timestamp2, zoneId).truncatedTo(ChronoUnit.MILLIS));
          // base US
        case NS:
          return (timestamp1, timestamp2) ->
              convertToZonedDateTime(timestamp1, zoneId)
                  .truncatedTo(ChronoUnit.MICROS)
                  .equals(
                      convertToZonedDateTime(timestamp2, zoneId).truncatedTo(ChronoUnit.MICROS));
        default:
          throw new UnsupportedOperationException("Unexpected extract field: " + field);
      }
    }

    private ZonedDateTime convertToZonedDateTime(long timestamp, ZoneId zoneId) {
      timestamp = CAST_TIMESTAMP_TO_MS.apply(timestamp);
      return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);
    }

    protected ExtractTimeCompareFilter(ByteBuffer buffer) {
      this(
          ReadWriteIOUtils.readLong(buffer),
          Field.values()[ReadWriteIOUtils.readInt(buffer)],
          ZoneId.of(Objects.requireNonNull(ReadWriteIOUtils.readString(buffer))),
          TimeUnit.values()[ReadWriteIOUtils.readInt(buffer)]);
    }

    public long getConstant() {
      return constant;
    }

    public Field getField() {
      return field;
    }

    public TimeUnit getCurrPrecision() {
      return currPrecision;
    }

    public ZoneId getZoneId() {
      return zoneId;
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(constant, outputStream);
      ReadWriteIOUtils.write(field.ordinal(), outputStream);
      ReadWriteIOUtils.write(zoneId.getId(), outputStream);
      ReadWriteIOUtils.write(currPrecision.ordinal(), outputStream);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ExtractTimeCompareFilter that = (ExtractTimeCompareFilter) o;
      return constant == that.constant
          && zoneId.equals(that.zoneId)
          && currPrecision == that.currPrecision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(constant, field, zoneId, currPrecision);
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, field, getOperatorType().getSymbol(), constant);
    }
  }

  public static final class ExtractTimeEq extends ExtractTimeCompareFilter {

    public ExtractTimeEq(long constant, Field field, ZoneId zoneId, TimeUnit currPrecision) {
      super(constant, field, zoneId, currPrecision);
    }

    public ExtractTimeEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return evaluateFunction.apply(time) == constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return !(truncatedEqualsFunction.apply(startTime, endTime)
          && (evaluateFunction.apply(endTime) < constant
              || evaluateFunction.apply(startTime) > constant));
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(startTime) == constant
          && evaluateFunction.apply(endTime) == constant;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      if (field == Field.YEAR) {
        int year = (int) constant;
        return Collections.singletonList(
            new TimeRange(GET_YEAR_TIMESTAMP.apply(year), GET_YEAR_TIMESTAMP.apply(year + 1) - 1));
      }
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public Filter reverse() {
      return new ExtractTimeNotEq(constant, field, zoneId, currPrecision);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_TIME_EQ;
    }
  }

  public static final class ExtractTimeNotEq extends ExtractTimeCompareFilter {

    public ExtractTimeNotEq(long constant, Field field, ZoneId zoneId, TimeUnit currPrecision) {
      super(constant, field, zoneId, currPrecision);
    }

    public ExtractTimeNotEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return evaluateFunction.apply(time) != constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return !(truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(startTime) == constant
          && evaluateFunction.apply(endTime) == constant);
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return truncatedEqualsFunction.apply(startTime, endTime)
          && (evaluateFunction.apply(startTime) > constant
              || evaluateFunction.apply(endTime) < constant);
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      if (field == Field.YEAR) {
        List<TimeRange> res = new ArrayList<>();
        int year = (int) constant;
        res.add(new TimeRange(Long.MIN_VALUE, GET_YEAR_TIMESTAMP.apply(year) - 1));
        res.add(new TimeRange(GET_YEAR_TIMESTAMP.apply(year + 1), Long.MAX_VALUE));
        return res;
      }
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public Filter reverse() {
      return new ExtractTimeEq(constant, field, zoneId, currPrecision);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_TIME_NEQ;
    }
  }

  public static final class ExtractTimeLt extends ExtractTimeCompareFilter {

    public ExtractTimeLt(long constant, Field field, ZoneId zoneId, TimeUnit currPrecision) {
      super(constant, field, zoneId, currPrecision);
    }

    public ExtractTimeLt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return evaluateFunction.apply(time) < constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return !(truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(startTime) >= constant);
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(endTime) < constant;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      if (field == Field.YEAR) {
        int year = (int) constant;
        return Collections.singletonList(
            new TimeRange(Long.MIN_VALUE, GET_YEAR_TIMESTAMP.apply(year) - 1));
      }
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public Filter reverse() {
      return new ExtractTimeGtEq(constant, field, zoneId, currPrecision);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_TIME_LT;
    }
  }

  public static final class ExtractTimeLtEq extends ExtractTimeCompareFilter {

    public ExtractTimeLtEq(long constant, Field field, ZoneId zoneId, TimeUnit currPrecision) {
      super(constant, field, zoneId, currPrecision);
    }

    public ExtractTimeLtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return evaluateFunction.apply(time) <= constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return !(truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(startTime) > constant);
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(endTime) <= constant;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      if (field == Field.YEAR) {
        int year = (int) constant;
        return Collections.singletonList(
            new TimeRange(Long.MIN_VALUE, GET_YEAR_TIMESTAMP.apply(year + 1) - 1));
      }
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public Filter reverse() {
      return new ExtractTimeGt(constant, field, zoneId, currPrecision);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_TIME_LTEQ;
    }
  }

  public static final class ExtractTimeGt extends ExtractTimeCompareFilter {

    public ExtractTimeGt(long constant, Field field, ZoneId zoneId, TimeUnit currPrecision) {
      super(constant, field, zoneId, currPrecision);
    }

    public ExtractTimeGt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return evaluateFunction.apply(time) > constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return !(truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(endTime) <= constant);
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(startTime) > constant;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      if (field == Field.YEAR) {
        int year = (int) constant;
        return Collections.singletonList(
            new TimeRange(GET_YEAR_TIMESTAMP.apply(year + 1), Long.MAX_VALUE));
      }
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public Filter reverse() {
      return new ExtractTimeLtEq(constant, field, zoneId, currPrecision);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_TIME_GT;
    }
  }

  public static final class ExtractTimeGtEq extends ExtractTimeCompareFilter {

    public ExtractTimeGtEq(long constant, Field field, ZoneId zoneId, TimeUnit currPrecision) {
      super(constant, field, zoneId, currPrecision);
    }

    public ExtractTimeGtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean timeSatisfy(long time) {
      return evaluateFunction.apply(time) >= constant;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
      return !(truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(endTime) < constant);
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
      return truncatedEqualsFunction.apply(startTime, endTime)
          && evaluateFunction.apply(startTime) >= constant;
    }

    @Override
    public List<TimeRange> getTimeRanges() {
      if (field == Field.YEAR) {
        int year = (int) constant;
        return Collections.singletonList(
            new TimeRange(GET_YEAR_TIMESTAMP.apply(year), Long.MAX_VALUE));
      }
      return Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Override
    public Filter reverse() {
      return new ExtractTimeLt(constant, field, zoneId, currPrecision);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_TIME_GTEQ;
    }
  }
}
