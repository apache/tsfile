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

package org.apache.tsfile.utils;

import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.BinaryFilterOperators;
import org.apache.tsfile.read.filter.operator.BooleanFilterOperators;
import org.apache.tsfile.read.filter.operator.DoubleFilterOperators;
import org.apache.tsfile.read.filter.operator.FloatFilterOperators;
import org.apache.tsfile.read.filter.operator.IntegerFilterOperators;
import org.apache.tsfile.read.filter.operator.LongFilterOperators;
import org.apache.tsfile.read.filter.operator.StringFilterOperators;

import java.util.Set;
import java.util.regex.Pattern;

@SuppressWarnings("unchecked")
public final class ValueFilterTypeServices {

  public static final TypeService<SingleValueFilterFactory> VALUE_GT_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, value) -> new BooleanFilterOperators.ValueGt(index, (boolean) value);
            case INT32, DATE ->
                (index, value) ->
                    new IntegerFilterOperators.ValueGt(index, ((Number) value).intValue());
            case INT64, TIMESTAMP ->
                (index, value) ->
                    new LongFilterOperators.ValueGt(index, ((Number) value).longValue());
            case FLOAT ->
                (index, value) ->
                    new FloatFilterOperators.ValueGt(index, ((Number) value).floatValue());
            case DOUBLE ->
                (index, value) ->
                    new DoubleFilterOperators.ValueGt(index, ((Number) value).doubleValue());
            case TEXT, BLOB ->
                (index, value) -> new BinaryFilterOperators.ValueGt(index, (Binary) value);
            case STRING ->
                (index, value) -> new StringFilterOperators.ValueGt(index, (Binary) value);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, value) -> unsupportedType(type);
          };

  public static final TypeService<SingleValueFilterFactory> VALUE_GT_EQ_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, value) -> new BooleanFilterOperators.ValueGtEq(index, (boolean) value);
            case INT32, DATE ->
                (index, value) ->
                    new IntegerFilterOperators.ValueGtEq(index, ((Number) value).intValue());
            case INT64, TIMESTAMP ->
                (index, value) ->
                    new LongFilterOperators.ValueGtEq(index, ((Number) value).longValue());
            case FLOAT ->
                (index, value) ->
                    new FloatFilterOperators.ValueGtEq(index, ((Number) value).floatValue());
            case DOUBLE ->
                (index, value) ->
                    new DoubleFilterOperators.ValueGtEq(index, ((Number) value).doubleValue());
            case TEXT, BLOB ->
                (index, value) -> new BinaryFilterOperators.ValueGtEq(index, (Binary) value);
            case STRING ->
                (index, value) -> new StringFilterOperators.ValueGtEq(index, (Binary) value);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, value) -> unsupportedType(type);
          };

  public static final TypeService<SingleValueFilterFactory> VALUE_LT_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, value) -> new BooleanFilterOperators.ValueLt(index, (boolean) value);
            case INT32, DATE ->
                (index, value) ->
                    new IntegerFilterOperators.ValueLt(index, ((Number) value).intValue());
            case INT64, TIMESTAMP ->
                (index, value) ->
                    new LongFilterOperators.ValueLt(index, ((Number) value).longValue());
            case FLOAT ->
                (index, value) ->
                    new FloatFilterOperators.ValueLt(index, ((Number) value).floatValue());
            case DOUBLE ->
                (index, value) ->
                    new DoubleFilterOperators.ValueLt(index, ((Number) value).doubleValue());
            case TEXT, BLOB ->
                (index, value) -> new BinaryFilterOperators.ValueLt(index, (Binary) value);
            case STRING ->
                (index, value) -> new StringFilterOperators.ValueLt(index, (Binary) value);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, value) -> unsupportedType(type);
          };

  public static final TypeService<SingleValueFilterFactory> VALUE_LT_EQ_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, value) -> new BooleanFilterOperators.ValueLtEq(index, (boolean) value);
            case INT32, DATE ->
                (index, value) ->
                    new IntegerFilterOperators.ValueLtEq(index, ((Number) value).intValue());
            case INT64, TIMESTAMP ->
                (index, value) ->
                    new LongFilterOperators.ValueLtEq(index, ((Number) value).longValue());
            case FLOAT ->
                (index, value) ->
                    new FloatFilterOperators.ValueLtEq(index, ((Number) value).floatValue());
            case DOUBLE ->
                (index, value) ->
                    new DoubleFilterOperators.ValueLtEq(index, ((Number) value).doubleValue());
            case TEXT, BLOB ->
                (index, value) -> new BinaryFilterOperators.ValueLtEq(index, (Binary) value);
            case STRING ->
                (index, value) -> new StringFilterOperators.ValueLtEq(index, (Binary) value);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, value) -> unsupportedType(type);
          };

  public static final TypeService<SingleValueFilterFactory> VALUE_EQ_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, value) -> new BooleanFilterOperators.ValueEq(index, (boolean) value);
            case INT32, DATE ->
                (index, value) ->
                    new IntegerFilterOperators.ValueEq(index, ((Number) value).intValue());
            case INT64, TIMESTAMP ->
                (index, value) ->
                    new LongFilterOperators.ValueEq(index, ((Number) value).longValue());
            case FLOAT ->
                (index, value) ->
                    new FloatFilterOperators.ValueEq(index, ((Number) value).floatValue());
            case DOUBLE ->
                (index, value) ->
                    new DoubleFilterOperators.ValueEq(index, ((Number) value).doubleValue());
            case TEXT, BLOB ->
                (index, value) -> new BinaryFilterOperators.ValueEq(index, (Binary) value);
            case STRING ->
                (index, value) -> new StringFilterOperators.ValueEq(index, (Binary) value);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, value) -> unsupportedType(type);
          };

  public static final TypeService<SingleValueFilterFactory> VALUE_NOT_EQ_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, value) -> new BooleanFilterOperators.ValueNotEq(index, (boolean) value);
            case INT32, DATE ->
                (index, value) ->
                    new IntegerFilterOperators.ValueNotEq(index, ((Number) value).intValue());
            case INT64, TIMESTAMP ->
                (index, value) ->
                    new LongFilterOperators.ValueNotEq(index, ((Number) value).longValue());
            case FLOAT ->
                (index, value) ->
                    new FloatFilterOperators.ValueNotEq(index, ((Number) value).floatValue());
            case DOUBLE ->
                (index, value) ->
                    new DoubleFilterOperators.ValueNotEq(index, ((Number) value).doubleValue());
            case TEXT, BLOB ->
                (index, value) -> new BinaryFilterOperators.ValueNotEq(index, (Binary) value);
            case STRING ->
                (index, value) -> new StringFilterOperators.ValueNotEq(index, (Binary) value);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, value) -> unsupportedType(type);
          };

  public static final TypeService<BetweenValueFilterFactory> VALUE_BETWEEN_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, value1, value2) ->
                    new BooleanFilterOperators.ValueBetweenAnd(
                        index, (boolean) value1, (boolean) value2);
            case INT32, DATE ->
                (index, value1, value2) ->
                    new IntegerFilterOperators.ValueBetweenAnd(
                        index, ((Number) value1).intValue(), ((Number) value2).intValue());
            case INT64, TIMESTAMP ->
                (index, value1, value2) ->
                    new LongFilterOperators.ValueBetweenAnd(
                        index, ((Number) value1).longValue(), ((Number) value2).longValue());
            case FLOAT ->
                (index, value1, value2) ->
                    new FloatFilterOperators.ValueBetweenAnd(
                        index, ((Number) value1).floatValue(), ((Number) value2).floatValue());
            case DOUBLE ->
                (index, value1, value2) ->
                    new DoubleFilterOperators.ValueBetweenAnd(
                        index, ((Number) value1).doubleValue(), ((Number) value2).doubleValue());
            case TEXT, BLOB ->
                (index, value1, value2) ->
                    new BinaryFilterOperators.ValueBetweenAnd(
                        index, (Binary) value1, (Binary) value2);
            case STRING ->
                (index, value1, value2) ->
                    new StringFilterOperators.ValueBetweenAnd(
                        index, (Binary) value1, (Binary) value2);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, value1, value2) -> unsupportedType(type);
          };

  public static final TypeService<BetweenValueFilterFactory> VALUE_NOT_BETWEEN_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, value1, value2) ->
                    new BooleanFilterOperators.ValueNotBetweenAnd(
                        index, (boolean) value1, (boolean) value2);
            case INT32, DATE ->
                (index, value1, value2) ->
                    new IntegerFilterOperators.ValueNotBetweenAnd(
                        index, ((Number) value1).intValue(), ((Number) value2).intValue());
            case INT64, TIMESTAMP ->
                (index, value1, value2) ->
                    new LongFilterOperators.ValueNotBetweenAnd(
                        index, ((Number) value1).longValue(), ((Number) value2).longValue());
            case FLOAT ->
                (index, value1, value2) ->
                    new FloatFilterOperators.ValueNotBetweenAnd(
                        index, ((Number) value1).floatValue(), ((Number) value2).floatValue());
            case DOUBLE ->
                (index, value1, value2) ->
                    new DoubleFilterOperators.ValueNotBetweenAnd(
                        index, ((Number) value1).doubleValue(), ((Number) value2).doubleValue());
            case TEXT, BLOB ->
                (index, value1, value2) ->
                    new BinaryFilterOperators.ValueNotBetweenAnd(
                        index, (Binary) value1, (Binary) value2);
            case STRING ->
                (index, value1, value2) ->
                    new StringFilterOperators.ValueNotBetweenAnd(
                        index, (Binary) value1, (Binary) value2);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, value1, value2) -> unsupportedType(type);
          };

  public static final TypeService<LikeFilterFactory> VALUE_LIKE_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> BooleanFilterOperators.ValueLike::new;
            case INT32, DATE -> IntegerFilterOperators.ValueLike::new;
            case INT64, TIMESTAMP -> LongFilterOperators.ValueLike::new;
            case FLOAT -> FloatFilterOperators.ValueLike::new;
            case DOUBLE -> DoubleFilterOperators.ValueLike::new;
            case TEXT, BLOB -> BinaryFilterOperators.ValueLike::new;
            case STRING -> StringFilterOperators.ValueLike::new;
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, pattern) -> unsupportedType(type);
          };

  public static final TypeService<LikeFilterFactory> VALUE_NOT_LIKE_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> BooleanFilterOperators.ValueNotLike::new;
            case INT32, DATE -> IntegerFilterOperators.ValueNotLike::new;
            case INT64, TIMESTAMP -> LongFilterOperators.ValueNotLike::new;
            case FLOAT -> FloatFilterOperators.ValueNotLike::new;
            case DOUBLE -> DoubleFilterOperators.ValueNotLike::new;
            case TEXT, BLOB -> BinaryFilterOperators.ValueNotLike::new;
            case STRING -> StringFilterOperators.ValueNotLike::new;
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, pattern) -> unsupportedType(type);
          };

  public static final TypeService<RegexpFilterFactory> VALUE_REGEXP_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> BooleanFilterOperators.ValueRegexp::new;
            case INT32, DATE -> IntegerFilterOperators.ValueRegexp::new;
            case INT64, TIMESTAMP -> LongFilterOperators.ValueRegexp::new;
            case FLOAT -> FloatFilterOperators.ValueRegexp::new;
            case DOUBLE -> DoubleFilterOperators.ValueRegexp::new;
            case TEXT, BLOB -> BinaryFilterOperators.ValueRegexp::new;
            case STRING -> StringFilterOperators.ValueRegexp::new;
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, pattern) -> unsupportedType(type);
          };

  public static final TypeService<RegexpFilterFactory> VALUE_NOT_REGEXP_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> BooleanFilterOperators.ValueNotRegexp::new;
            case INT32, DATE -> IntegerFilterOperators.ValueNotRegexp::new;
            case INT64, TIMESTAMP -> LongFilterOperators.ValueNotRegexp::new;
            case FLOAT -> FloatFilterOperators.ValueNotRegexp::new;
            case DOUBLE -> DoubleFilterOperators.ValueNotRegexp::new;
            case TEXT, BLOB -> BinaryFilterOperators.ValueNotRegexp::new;
            case STRING -> StringFilterOperators.ValueNotRegexp::new;
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, pattern) -> unsupportedType(type);
          };

  public static final TypeService<SetFilterFactory> VALUE_IN_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, values) -> new BooleanFilterOperators.ValueIn(index, (Set<Boolean>) values);
            case INT32, DATE ->
                (index, values) -> new IntegerFilterOperators.ValueIn(index, (Set<Integer>) values);
            case INT64, TIMESTAMP ->
                (index, values) -> new LongFilterOperators.ValueIn(index, (Set<Long>) values);
            case FLOAT ->
                (index, values) -> new FloatFilterOperators.ValueIn(index, (Set<Float>) values);
            case DOUBLE ->
                (index, values) -> new DoubleFilterOperators.ValueIn(index, (Set<Double>) values);
            case TEXT, BLOB ->
                (index, values) -> new BinaryFilterOperators.ValueIn(index, (Set<Binary>) values);
            case STRING ->
                (index, values) -> new StringFilterOperators.ValueIn(index, (Set<Binary>) values);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, values) -> unsupportedType(type);
          };

  public static final TypeService<SetFilterFactory> VALUE_NOT_IN_FILTER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN ->
                (index, values) ->
                    new BooleanFilterOperators.ValueNotIn(index, (Set<Boolean>) values);
            case INT32, DATE ->
                (index, values) ->
                    new IntegerFilterOperators.ValueNotIn(index, (Set<Integer>) values);
            case INT64, TIMESTAMP ->
                (index, values) -> new LongFilterOperators.ValueNotIn(index, (Set<Long>) values);
            case FLOAT ->
                (index, values) -> new FloatFilterOperators.ValueNotIn(index, (Set<Float>) values);
            case DOUBLE ->
                (index, values) ->
                    new DoubleFilterOperators.ValueNotIn(index, (Set<Double>) values);
            case TEXT, BLOB ->
                (index, values) ->
                    new BinaryFilterOperators.ValueNotIn(index, (Set<Binary>) values);
            case STRING ->
                (index, values) ->
                    new StringFilterOperators.ValueNotIn(index, (Set<Binary>) values);
            case OBJECT, ROW, UNKNOWN, VECTOR -> (index, values) -> unsupportedType(type);
          };

  private ValueFilterTypeServices() {}

  private static Filter unsupportedType(Type type) {
    throw new UnsupportedOperationException(
        Messages.format("error.read.filter_api_unsupported_type", type.getTypeEnum()));
  }

  @FunctionalInterface
  public interface SingleValueFilterFactory {
    Filter create(int measurementIndex, Object value);
  }

  @FunctionalInterface
  public interface BetweenValueFilterFactory {
    Filter create(int measurementIndex, Object value1, Object value2);
  }

  @FunctionalInterface
  public interface LikeFilterFactory {
    Filter create(int measurementIndex, LikePattern pattern);
  }

  @FunctionalInterface
  public interface RegexpFilterFactory {
    Filter create(int measurementIndex, Pattern pattern);
  }

  @FunctionalInterface
  public interface SetFilterFactory {
    Filter create(int measurementIndex, Set<?> values);
  }
}
