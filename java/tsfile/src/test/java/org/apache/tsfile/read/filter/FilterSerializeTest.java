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
package org.apache.tsfile.read.filter;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TimeDuration;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;
import static org.junit.Assert.assertEquals;

public class FilterSerializeTest {

  @Test
  public void testValueFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 1, TSDataType.INT32),
          ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 2L, TSDataType.INT64),
          ValueFilterApi.gtEq(
              DEFAULT_MEASUREMENT_INDEX,
              new Binary("filter", TSFileConfig.STRING_CHARSET),
              TSDataType.TEXT),
          ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 0.1, TSDataType.DOUBLE),
          ValueFilterApi.ltEq(DEFAULT_MEASUREMENT_INDEX, 0.01f, TSDataType.FLOAT),
          FilterFactory.not(ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, true, TSDataType.BOOLEAN)),
          ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, false, TSDataType.BOOLEAN),
          ValueFilterApi.notEq(DEFAULT_MEASUREMENT_INDEX, false, TSDataType.BOOLEAN),
          // TODO: add StringFilter test
          ValueFilterApi.in(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(
                  Arrays.asList(
                      new Binary("a", TSFileConfig.STRING_CHARSET),
                      new Binary("b", TSFileConfig.STRING_CHARSET))),
              TSDataType.STRING),
          ValueFilterApi.notIn(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(
                  Arrays.asList(
                      new Binary("a", TSFileConfig.STRING_CHARSET),
                      new Binary("b", TSFileConfig.STRING_CHARSET))),
              TSDataType.STRING),
          ValueFilterApi.in(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(new Binary("a".getBytes()), new Binary("b".getBytes()))),
              TSDataType.TEXT),
          ValueFilterApi.notIn(
              DEFAULT_MEASUREMENT_INDEX,
              new HashSet<>(Arrays.asList(new Binary("a".getBytes()), new Binary("b".getBytes()))),
              TSDataType.TEXT),
          ValueFilterApi.regexp(DEFAULT_MEASUREMENT_INDEX, "s.*", TSDataType.TEXT),
          ValueFilterApi.like(DEFAULT_MEASUREMENT_INDEX, "s.*", TSDataType.TEXT),
          ValueFilterApi.notRegexp(DEFAULT_MEASUREMENT_INDEX, "s.*", TSDataType.TEXT),
          ValueFilterApi.notLike(DEFAULT_MEASUREMENT_INDEX, "s.*", TSDataType.TEXT),
          ValueFilterApi.between(DEFAULT_MEASUREMENT_INDEX, 1, 100, TSDataType.INT32),
          ValueFilterApi.notBetween(DEFAULT_MEASUREMENT_INDEX, 1, 100, TSDataType.INT32),
          ValueFilterApi.isNull(DEFAULT_MEASUREMENT_INDEX),
          ValueFilterApi.isNotNull(DEFAULT_MEASUREMENT_INDEX)
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testTimeFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          TimeFilterApi.eq(1),
          TimeFilterApi.notEq(7),
          TimeFilterApi.gt(2),
          TimeFilterApi.gtEq(3),
          TimeFilterApi.lt(4),
          TimeFilterApi.ltEq(5),
          FilterFactory.not(TimeFilterApi.eq(6)),
          TimeFilterApi.in(new HashSet<>(Arrays.asList(1L, 2L))),
          TimeFilterApi.notIn(new HashSet<>(Arrays.asList(3L, 4L))),
          TimeFilterApi.between(1, 100),
          TimeFilterApi.notBetween(1, 100)
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testBinaryFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          FilterFactory.and(
              TimeFilterApi.eq(1),
              ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 1, TSDataType.INT32)),
          FilterFactory.or(
              ValueFilterApi.gt(DEFAULT_MEASUREMENT_INDEX, 2L, TSDataType.INT64),
              FilterFactory.not(ValueFilterApi.eq(DEFAULT_MEASUREMENT_INDEX, 6, TSDataType.INT32)))
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void BinaryFilter() throws IOException {
    Filter filter =
        ValueFilterApi.in(
            DEFAULT_MEASUREMENT_INDEX,
            new HashSet<Integer>(Arrays.asList(10, null, 30)),
            TSDataType.INT32);
    validateSerialization(filter);
  }

  @Test
  public void testGroupByFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          TimeFilterApi.groupBy(1, 2, 3, 4), TimeFilterApi.groupBy(4, 3, 2, 1),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  @Test
  public void testGroupByMonthFilter() throws IOException {
    Filter[] filters =
        new Filter[] {
          TimeFilterApi.groupByMonth(
              3,
              4,
              new TimeDuration(0, 1),
              new TimeDuration(0, 2),
              TimeZone.getTimeZone("Asia/Shanghai"),
              TimeUnit.MILLISECONDS),
          TimeFilterApi.groupByMonth(
              2,
              1,
              new TimeDuration(0, 4),
              new TimeDuration(0, 3),
              TimeZone.getTimeZone("Atlantic/Faeroe"),
              TimeUnit.MILLISECONDS),
        };
    for (Filter filter : filters) {
      validateSerialization(filter);
    }
  }

  private void validateSerialization(Filter filter) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
    filter.serialize(dataOutputStream);

    ByteBuffer buffer = ByteBuffer.wrap(outputStream.toByteArray());
    Filter serialized = Filter.deserialize(buffer);
    assertEquals(filter, serialized);
  }
}
