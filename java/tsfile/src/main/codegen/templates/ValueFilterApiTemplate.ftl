<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/tsfile/read/filter/factory/ValueFilterApi.java" />
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

import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.BinaryFilterOperators;
import org.apache.tsfile.read.filter.operator.BooleanFilterOperators;
import org.apache.tsfile.read.filter.operator.DoubleFilterOperators;
import org.apache.tsfile.read.filter.operator.FloatFilterOperators;
import org.apache.tsfile.read.filter.operator.IntegerFilterOperators;
import org.apache.tsfile.read.filter.operator.LongFilterOperators;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueBetweenAnd;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueEq;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueGt;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueGtEq;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueIn;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueIsNotNull;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueIsNull;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueLt;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueLtEq;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueNotBetweenAnd;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueNotEq;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueNotIn;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueNotRegexp;
import org.apache.tsfile.read.filter.operator.ValueFilterOperators.ValueRegexp;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RegexUtils;

public class ValueFilterApi {

  private static final String CONSTANT_CANNOT_BE_NULL_MSG = "constant cannot be null";

  private static final int DEFAULT_MEASUREMENT_INDEX = 0;

  private ValueFilterApi() {
    // forbidden construction
  }
  <#list operators as operator>
  <#switch operator.name>
  <#case "isNull">
  <#case "isNotNull">
  <#--isNull and isNotNull template-->

  public static Value${operator.method} ${operator.name}() {
    return new Value${operator.method}(DEFAULT_MEASUREMENT_INDEX);
  }

  public static Value${operator.method} ${operator.name}(int measurementIndex) {
    return new Value${operator.method}(measurementIndex);
  }

  public static Filter ${operator.name}(int measurementIndex, TSDataType type) {
    // constant cannot be null
    switch (type) {
    <#list filters as filter>
      <#list filter.tsDataType as type>
      case ${type}:
      </#list>
        return new ${filter.javaBoxName}FilterOperators.Value${operator.method}(measurementIndex);
    </#list>
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }
  <#break>
  <#case "between">
  <#case "notBetween">
  <#--between and notBetween template-->

  public static <T extends Comparable<T>> Value${operator.method}<T> ${operator.name}(T value1, T value2) {
    return new Value${operator.method}<>(DEFAULT_MEASUREMENT_INDEX, value1, value2);
  }

  public static <T extends Comparable<T>> Value${operator.method}<T> ${operator.name}(int measurementIndex, T value1, T value2) {
    return new Value${operator.method}<>(measurementIndex, value1, value2);
  }

  public static <T extends Comparable<T>> Filter ${operator.name}(
      int measurementIndex, T value1, T value2, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value1, CONSTANT_CANNOT_BE_NULL_MSG);
    Objects.requireNonNull(value2, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
    <#list filters as filter>
      <#list filter.tsDataType as type>
      case ${type}:
      </#list>
        return new ${filter.javaBoxName}FilterOperators.Value${operator.method}(measurementIndex,(${filter.javaBoxName}) value1,(${filter.javaBoxName}) value2);
    </#list>
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }
  <#break>
  <#case "like">
  <#case "notLike">
  <#case "regexp">
  <#case "notRegexp">
  <#--like, notLike, regexp, notRegexp template-->

  public static Value${operator.method} ${operator.name}(String regex) {
    return new Value${operator.method}(DEFAULT_MEASUREMENT_INDEX, RegexUtils.compileRegex(regex));
  }

  public static Value${operator.method} ${operator.name}(int measurementIndex, String regex) {
    return new Value${operator.method}(measurementIndex, RegexUtils.compileRegex(regex));
  }

  public static Value${operator.method} ${operator.name}(int measurementIndex, Pattern pattern) {
    return new Value${operator.method}(measurementIndex, pattern);
  }

  public static Filter ${operator.name}(int measurementIndex, Pattern pattern, TSDataType type) {
    if (type == TSDataType.TEXT || type == TSDataType.STRING || type == TSDataType.BLOB) {
      return new BinaryFilterOperators.Value${operator.method}(measurementIndex, pattern);
    }else{
      return new Value${operator.method}(measurementIndex, pattern);
    }
  }
  <#break>
  <#case "in">
  <#case "notIn">
  <#--in and notIn template-->

  public static <T extends Comparable<T>> Value${operator.method}<T> ${operator.name}(Set<T> values) {
    return new Value${operator.method}<>(DEFAULT_MEASUREMENT_INDEX, values);
  }

  public static <T extends Comparable<T>> Value${operator.method}<T> ${operator.name}(int measurementIndex, Set<T> values) {
    return new Value${operator.method}<>(measurementIndex, values);
  }

  public static <T extends Comparable<T>> Filter ${operator.name}(
      int measurementIndex, Set<T> values, TSDataType type) {
    // constants cannot be null
    Objects.requireNonNull(values, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
      case BOOLEAN:
        return new BooleanFilterOperators.Value${operator.method}(measurementIndex, (Set<Boolean>) values);
      case INT32:
      case DATE:
        return new IntegerFilterOperators.Value${operator.method}(measurementIndex, (Set<Integer>) values);
      case INT64:
      case TIMESTAMP:
        return new LongFilterOperators.Value${operator.method}(measurementIndex, (Set<Long>) values);
      case FLOAT:
        return new FloatFilterOperators.Value${operator.method}(measurementIndex, (Set<Float>) values);
      case DOUBLE:
        return new DoubleFilterOperators.Value${operator.method}(measurementIndex, (Set<Double>) values);
      case TEXT:
      case BLOB:
      case STRING:
        return new BinaryFilterOperators.Value${operator.method}(measurementIndex, (Set<Binary>) values);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }
  <#break>
  <#default>
  public static <T extends Comparable<T>> Value${operator.method}<T> ${operator.name}(T value) {
    return new Value${operator.method}<>(DEFAULT_MEASUREMENT_INDEX, value);
  }

  public static <T extends Comparable<T>> Value${operator.method}<T> ${operator.name}(int measurementIndex, T value) {
    return new Value${operator.method}<>(measurementIndex, value);
  }

  public static <T extends Comparable<T>> Filter ${operator.name}(
      int measurementIndex, T value, TSDataType type) {
    // constant cannot be null
    Objects.requireNonNull(value, CONSTANT_CANNOT_BE_NULL_MSG);
    switch (type) {
    <#list filters as filter>
      <#list filter.tsDataType as type>
      case ${type}:
      </#list>
        return new ${filter.javaBoxName}FilterOperators.Value${operator.method}(measurementIndex,(${filter.javaBoxName}) value);
    </#list>
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + type);
    }
  }
  <#break>
  </#switch>
  </#list>

}
