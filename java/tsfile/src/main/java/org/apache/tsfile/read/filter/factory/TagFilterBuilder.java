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

import org.apache.tsfile.annotations.TsFileApi;
import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.operator.And;
import org.apache.tsfile.read.filter.operator.Not;
import org.apache.tsfile.read.filter.operator.Or;
import org.apache.tsfile.read.filter.operator.TagFilterOperators;
import org.apache.tsfile.read.filter.operator.TagFilterOperators.ValueEq;
import org.apache.tsfile.read.filter.operator.TagFilterOperators.ValueNotEq;

import java.util.Optional;
import java.util.regex.Pattern;

public class TagFilterBuilder {
  private final TableSchema tableSchema;

  public TagFilterBuilder(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  private int getIdColumnIndex(String columnName) {
    int idColumnOrder = tableSchema.findIdColumnOrder(columnName);
    if (idColumnOrder == -1) {
      throw new IllegalArgumentException("Column '" + columnName + "' is not a tag column");
    }
    return idColumnOrder + 1;
  }

  @TsFileApi
  public Filter eq(String columnName, Object value) {
    return new ValueEq(getIdColumnIndex(columnName), (String) value);
  }

  @TsFileApi
  public Filter neq(String columnName, Object value) {
    return new ValueNotEq(getIdColumnIndex(columnName), (String) value);
  }

  @TsFileApi
  public Filter lt(String columnName, Object value) {
    return new TagFilterOperators.ValueLt(getIdColumnIndex(columnName), (String) value);
  }

  @TsFileApi
  public Filter lteq(String columnName, Object value) {
    return new TagFilterOperators.ValueLtEq(getIdColumnIndex(columnName), (String) value);
  }

  @TsFileApi
  public Filter gt(String columnName, Object value) {
    return new TagFilterOperators.ValueGt(getIdColumnIndex(columnName), (String) value);
  }

  @TsFileApi
  public Filter gteq(String columnName, Object value) {
    return new TagFilterOperators.ValueGtEq(getIdColumnIndex(columnName), (String) value);
  }

  @TsFileApi
  public Filter betweenAnd(String columnName, Object value1, Object value2) {
    return new TagFilterOperators.ValueBetweenAnd(
        getIdColumnIndex(columnName), (String) value1, (String) value2);
  }

  @TsFileApi
  public Filter notBetweenAnd(String columnName, Object value1, Object value2) {
    return new TagFilterOperators.ValueNotBetweenAnd(
        getIdColumnIndex(columnName), (String) value1, (String) value2);
  }

  @TsFileApi
  public Filter regExp(String columnName, String pattern) {
    return new TagFilterOperators.ValueRegexp(
        getIdColumnIndex(columnName), Pattern.compile(pattern));
  }

  @TsFileApi
  public Filter notRegExp(String columnName, String pattern) {
    return new TagFilterOperators.ValueNotRegexp(
        getIdColumnIndex(columnName), Pattern.compile(pattern));
  }

  @TsFileApi
  public Filter like(String columnName, String pattern) {
    return new TagFilterOperators.ValueLike(
        getIdColumnIndex(columnName), LikePattern.compile(pattern, Optional.empty()));
  }

  @TsFileApi
  public Filter notLike(String columnName, String pattern) {
    return new TagFilterOperators.ValueNotLike(
        getIdColumnIndex(columnName), LikePattern.compile(pattern, Optional.empty()));
  }

  @TsFileApi
  public Filter and(Filter left, Filter right) {
    return new And(left, right);
  }

  @TsFileApi
  public Filter or(Filter left, Filter right) {
    return new Or(left, right);
  }

  @TsFileApi
  public Filter not(Filter value) {
    return new Not(value);
  }
}
