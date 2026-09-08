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

package org.apache.tsfile.read.filter.factory;

import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.ExtractTimeFilterOperators;
import org.apache.tsfile.read.filter.operator.ExtractValueFilterOperators;
import org.apache.tsfile.read.filter.operator.ValueIsNotNullOperator;
import org.apache.tsfile.read.filter.operator.ValueIsNullOperator;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_BETWEEN_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_EQ_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_GT_EQ_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_GT_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_IN_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_LIKE_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_LT_EQ_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_LT_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_NOT_BETWEEN_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_NOT_EQ_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_NOT_IN_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_NOT_LIKE_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_NOT_REGEXP_FILTER_SERVICE;
import static org.apache.tsfile.utils.ValueFilterTypeServices.VALUE_REGEXP_FILTER_SERVICE;

public class ValueFilterApi {

  public static final int DEFAULT_MEASUREMENT_INDEX = 0;

  private static final String CONSTANT_CANNOT_BE_NULL_MSG = " constant cannot be null";
  public static final String CANNOT_PUSH_DOWN_MSG = " operator can not be pushed down.";

  private ValueFilterApi() {
    // forbidden construction
  }

  public static Filter gt(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_GT_FILTER_SERVICE.call(Type.fromTsDataType(type)).create(measurementIndex, value);
  }

  public static Filter gtEq(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_GT_EQ_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, value);
  }

  public static Filter lt(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_LT_FILTER_SERVICE.call(Type.fromTsDataType(type)).create(measurementIndex, value);
  }

  public static Filter ltEq(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_LT_EQ_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, value);
  }

  public static Filter eq(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_EQ_FILTER_SERVICE.call(Type.fromTsDataType(type)).create(measurementIndex, value);
  }

  public static Filter notEq(int measurementIndex, Object value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_NOT_EQ_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, value);
  }

  public static Filter isNull(int measurementIndex) {
    return new ValueIsNullOperator(measurementIndex);
  }

  public static Filter isNotNull(int measurementIndex) {
    return new ValueIsNotNullOperator(measurementIndex);
  }

  public static Filter between(
      int measurementIndex, Object value1, Object value2, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value1, CONSTANT_CANNOT_BE_NULL_MSG);
    Objects.requireNonNull(value2, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_BETWEEN_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, value1, value2);
  }

  public static Filter notBetween(
      int measurementIndex, Object value1, Object value2, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value1, CONSTANT_CANNOT_BE_NULL_MSG);
    Objects.requireNonNull(value2, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_NOT_BETWEEN_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, value1, value2);
  }

  public static Filter like(int measurementIndex, LikePattern pattern, TSDataType type) {
    Objects.requireNonNull(pattern, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_LIKE_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, pattern);
  }

  public static Filter notLike(int measurementIndex, LikePattern pattern, TSDataType type) {
    Objects.requireNonNull(pattern, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_NOT_LIKE_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, pattern);
  }

  public static Filter regexp(int measurementIndex, Pattern pattern, TSDataType type) {
    Objects.requireNonNull(pattern, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_REGEXP_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, pattern);
  }

  public static Filter notRegexp(int measurementIndex, Pattern pattern, TSDataType type) {
    Objects.requireNonNull(pattern, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_NOT_REGEXP_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, pattern);
  }

  public static <T extends Comparable<T>> Filter in(
      int measurementIndex, Set<T> values, TSDataType type) {
    // constants cannot be null
    Objects.requireNonNull(values, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_IN_FILTER_SERVICE.call(Type.fromTsDataType(type)).create(measurementIndex, values);
  }

  public static <T extends Comparable<T>> Filter notIn(
      int measurementIndex, Set<T> values, TSDataType type) {
    // constants cannot be null
    Objects.requireNonNull(values, CONSTANT_CANNOT_BE_NULL_MSG);
    return VALUE_NOT_IN_FILTER_SERVICE
        .call(Type.fromTsDataType(type))
        .create(measurementIndex, values);
  }

  public static Filter extractValueGt(
      int measurementIndex,
      long value,
      ExtractTimeFilterOperators.Field field,
      ZoneId zoneId,
      TimeUnit currPrecision) {
    return new ExtractValueFilterOperators.ExtractValueGt(
        measurementIndex, value, field, zoneId, currPrecision);
  }

  public static Filter extractValueGtEq(
      int measurementIndex,
      long value,
      ExtractTimeFilterOperators.Field field,
      ZoneId zoneId,
      TimeUnit currPrecision) {
    return new ExtractValueFilterOperators.ExtractValueGtEq(
        measurementIndex, value, field, zoneId, currPrecision);
  }

  public static Filter extractValueLt(
      int measurementIndex,
      long value,
      ExtractTimeFilterOperators.Field field,
      ZoneId zoneId,
      TimeUnit currPrecision) {
    return new ExtractValueFilterOperators.ExtractValueLt(
        measurementIndex, value, field, zoneId, currPrecision);
  }

  public static Filter extractValueLtEq(
      int measurementIndex,
      long value,
      ExtractTimeFilterOperators.Field field,
      ZoneId zoneId,
      TimeUnit currPrecision) {
    return new ExtractValueFilterOperators.ExtractValueLtEq(
        measurementIndex, value, field, zoneId, currPrecision);
  }

  public static Filter extractValueEq(
      int measurementIndex,
      long value,
      ExtractTimeFilterOperators.Field field,
      ZoneId zoneId,
      TimeUnit currPrecision) {
    return new ExtractValueFilterOperators.ExtractValueEq(
        measurementIndex, value, field, zoneId, currPrecision);
  }

  public static Filter extractValueNotEq(
      int measurementIndex,
      long value,
      ExtractTimeFilterOperators.Field field,
      ZoneId zoneId,
      TimeUnit currPrecision) {
    return new ExtractValueFilterOperators.ExtractValueNotEq(
        measurementIndex, value, field, zoneId, currPrecision);
  }
}
