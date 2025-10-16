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

import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.LongFilter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class ExtractValueFilterOperators {

  private ExtractValueFilterOperators() {
    // forbidden construction
  }

  private static final String EXTRACT_OPERATOR_TO_STRING_FORMAT =
      "extract %s from measurements[%s] %s %s";

  // The input type of Extract must be INT64
  abstract static class ExtractValueCompareFilter extends LongFilter {

    protected final ExtractTimeFilterOperators.ExtractTimeCompareFilter delegate;

    protected ExtractValueCompareFilter(
        int measurementIndex, ExtractTimeFilterOperators.ExtractTimeCompareFilter delegate) {
      super(measurementIndex);
      this.delegate = delegate;
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      delegate.serialize(outputStream);
    }

    @Override
    public boolean valueSatisfy(Object value) {
      return valueSatisfy((long) value);
    }

    @Override
    public boolean valueSatisfy(long value) {
      return delegate.timeSatisfy(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      if (statistics.isEmpty()) {
        return false;
      }
      // has no intersection
      return !delegate.satisfyStartEndTime(
          (Long) statistics.getMinValue(), (Long) statistics.getMaxValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      if (statistics.isEmpty()) {
        return false;
      }
      // contains all start and end time
      return delegate.containStartEndTime(
          (Long) statistics.getMinValue(), (Long) statistics.getMaxValue());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      ExtractTimeFilterOperators.ExtractTimeCompareFilter thatDelegate =
          ((ExtractValueCompareFilter) o).delegate;
      return delegate.equals(thatDelegate);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), delegate);
    }

    @Override
    public String toString() {
      return String.format(
          EXTRACT_OPERATOR_TO_STRING_FORMAT,
          delegate.getField(),
          measurementIndex,
          getOperatorType().getSymbol(),
          delegate.getConstant());
    }
  }

  public static final class ExtractValueEq extends ExtractValueCompareFilter {

    public ExtractValueEq(
        int measurementIndex,
        long constant,
        ExtractTimeFilterOperators.Field field,
        ZoneId zoneId,
        TimeUnit currPrecision) {
      super(
          measurementIndex,
          new ExtractTimeFilterOperators.ExtractTimeEq(constant, field, zoneId, currPrecision));
    }

    public ExtractValueEq(ByteBuffer buffer) {
      super(
          ReadWriteIOUtils.readInt(buffer),
          (ExtractTimeFilterOperators.ExtractTimeCompareFilter)
              ExtractTimeFilterOperators.ExtractTimeEq.deserialize(buffer));
    }

    @Override
    public Filter reverse() {
      return new ExtractValueNotEq(
          measurementIndex,
          delegate.getConstant(),
          delegate.getField(),
          delegate.getZoneId(),
          delegate.getCurrPrecision());
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_VALUE_EQ;
    }
  }

  public static final class ExtractValueNotEq extends ExtractValueCompareFilter {

    public ExtractValueNotEq(
        int measurementIndex,
        long constant,
        ExtractTimeFilterOperators.Field field,
        ZoneId zoneId,
        TimeUnit currPrecision) {
      super(
          measurementIndex,
          new ExtractTimeFilterOperators.ExtractTimeNotEq(constant, field, zoneId, currPrecision));
    }

    public ExtractValueNotEq(ByteBuffer buffer) {
      super(
          ReadWriteIOUtils.readInt(buffer),
          (ExtractTimeFilterOperators.ExtractTimeCompareFilter)
              ExtractTimeFilterOperators.ExtractTimeNotEq.deserialize(buffer));
    }

    @Override
    public Filter reverse() {
      return new ExtractValueEq(
          measurementIndex,
          delegate.getConstant(),
          delegate.getField(),
          delegate.getZoneId(),
          delegate.getCurrPrecision());
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_VALUE_NEQ;
    }
  }

  public static final class ExtractValueLt extends ExtractValueCompareFilter {

    public ExtractValueLt(
        int measurementIndex,
        long constant,
        ExtractTimeFilterOperators.Field field,
        ZoneId zoneId,
        TimeUnit currPrecision) {
      super(
          measurementIndex,
          new ExtractTimeFilterOperators.ExtractTimeLt(constant, field, zoneId, currPrecision));
    }

    public ExtractValueLt(ByteBuffer buffer) {
      super(
          ReadWriteIOUtils.readInt(buffer),
          (ExtractTimeFilterOperators.ExtractTimeCompareFilter)
              ExtractTimeFilterOperators.ExtractTimeLt.deserialize(buffer));
    }

    @Override
    public Filter reverse() {
      return new ExtractValueGtEq(
          measurementIndex,
          delegate.getConstant(),
          delegate.getField(),
          delegate.getZoneId(),
          delegate.getCurrPrecision());
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_VALUE_LT;
    }
  }

  public static final class ExtractValueLtEq extends ExtractValueCompareFilter {

    public ExtractValueLtEq(
        int measurementIndex,
        long constant,
        ExtractTimeFilterOperators.Field field,
        ZoneId zoneId,
        TimeUnit currPrecision) {
      super(
          measurementIndex,
          new ExtractTimeFilterOperators.ExtractTimeLtEq(constant, field, zoneId, currPrecision));
    }

    public ExtractValueLtEq(ByteBuffer buffer) {
      super(
          ReadWriteIOUtils.readInt(buffer),
          (ExtractTimeFilterOperators.ExtractTimeCompareFilter)
              ExtractTimeFilterOperators.ExtractTimeLtEq.deserialize(buffer));
    }

    @Override
    public Filter reverse() {
      return new ExtractValueGt(
          measurementIndex,
          delegate.getConstant(),
          delegate.getField(),
          delegate.getZoneId(),
          delegate.getCurrPrecision());
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_VALUE_LTEQ;
    }
  }

  public static final class ExtractValueGt extends ExtractValueCompareFilter {

    public ExtractValueGt(
        int measurementIndex,
        long constant,
        ExtractTimeFilterOperators.Field field,
        ZoneId zoneId,
        TimeUnit currPrecision) {
      super(
          measurementIndex,
          new ExtractTimeFilterOperators.ExtractTimeGt(constant, field, zoneId, currPrecision));
    }

    public ExtractValueGt(ByteBuffer buffer) {
      super(
          ReadWriteIOUtils.readInt(buffer),
          (ExtractTimeFilterOperators.ExtractTimeCompareFilter)
              ExtractTimeFilterOperators.ExtractTimeGt.deserialize(buffer));
    }

    @Override
    public Filter reverse() {
      return new ExtractValueLtEq(
          measurementIndex,
          delegate.getConstant(),
          delegate.getField(),
          delegate.getZoneId(),
          delegate.getCurrPrecision());
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_VALUE_GT;
    }
  }

  public static final class ExtractValueGtEq extends ExtractValueCompareFilter {

    public ExtractValueGtEq(
        int measurementIndex,
        long constant,
        ExtractTimeFilterOperators.Field field,
        ZoneId zoneId,
        TimeUnit currPrecision) {
      super(
          measurementIndex,
          new ExtractTimeFilterOperators.ExtractTimeGtEq(constant, field, zoneId, currPrecision));
    }

    public ExtractValueGtEq(ByteBuffer buffer) {
      super(
          ReadWriteIOUtils.readInt(buffer),
          (ExtractTimeFilterOperators.ExtractTimeCompareFilter)
              ExtractTimeFilterOperators.ExtractTimeGtEq.deserialize(buffer));
    }

    @Override
    public Filter reverse() {
      return new ExtractValueLt(
          measurementIndex,
          delegate.getConstant(),
          delegate.getField(),
          delegate.getZoneId(),
          delegate.getCurrPrecision());
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.EXTRACT_VALUE_GTEQ;
    }
  }
}
