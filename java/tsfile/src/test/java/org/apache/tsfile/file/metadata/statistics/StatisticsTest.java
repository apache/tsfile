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
package org.apache.tsfile.file.metadata.statistics;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;

import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes"})
public class StatisticsTest {

  private static final TypeService<StatisticsChecker> CHECK_STATISTICS_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, INT64, FLOAT, DOUBLE, TIMESTAMP, DATE ->
                (statistics, min, max, sum) -> {
                  assertEquals(min, ((Number) statistics.getMinValue()).intValue());
                  assertEquals(max, ((Number) statistics.getMaxValue()).intValue());
                  assertEquals(min, ((Number) statistics.getFirstValue()).intValue());
                  assertEquals(max, ((Number) statistics.getLastValue()).intValue());
                  assertEquals(sum, statistics.getSumDoubleValue(), 0.001);
                };
            case BOOLEAN ->
                (statistics, min, max, sum) -> {
                  assertEquals(min % 2 == 1, statistics.getFirstValue());
                  assertEquals(max % 2 == 1, statistics.getLastValue());
                  assertEquals(sum, statistics.getSumDoubleValue(), 0.001);
                };
            case TEXT ->
                (statistics, min, max, sum) -> {
                  assertEquals(
                      new Binary(String.valueOf(min), TSFileConfig.STRING_CHARSET),
                      statistics.getFirstValue());
                  assertEquals(
                      new Binary(String.valueOf(max), TSFileConfig.STRING_CHARSET),
                      statistics.getLastValue());
                };
            case STRING ->
                (statistics, min, max, sum) -> {
                  assertEquals(
                      new Binary(String.valueOf(min), TSFileConfig.STRING_CHARSET),
                      statistics.getMinValue());
                  assertEquals(
                      new Binary(String.valueOf(max), TSFileConfig.STRING_CHARSET),
                      statistics.getMaxValue());
                  assertEquals(
                      new Binary(String.valueOf(min), TSFileConfig.STRING_CHARSET),
                      statistics.getFirstValue());
                  assertEquals(
                      new Binary(String.valueOf(max), TSFileConfig.STRING_CHARSET),
                      statistics.getLastValue());
                };
            case BLOB, OBJECT -> (statistics, min, max, sum) -> {};
            case ROW, UNKNOWN, VECTOR ->
                (statistics, min, max, sum) -> {
                  throw new IllegalArgumentException(type.getTypeEnum().toString());
                };
          };

  private static final TypeService<StatisticsGenerator> GENERATE_STATISTICS_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                value -> {
                  IntegerStatistics statistics = new IntegerStatistics();
                  statistics.initializeStats(value, value, value, value, value);
                  return statistics;
                };
            case INT64 ->
                value -> {
                  LongStatistics statistics = new LongStatistics();
                  statistics.initializeStats(value, value, value, value, value);
                  return statistics;
                };
            case FLOAT ->
                value -> {
                  FloatStatistics statistics = new FloatStatistics();
                  statistics.initializeStats(value, value, value, value, value);
                  return statistics;
                };
            case DOUBLE ->
                value -> {
                  DoubleStatistics statistics = new DoubleStatistics();
                  statistics.initializeStats(value, value, value, value, value);
                  return statistics;
                };
            case TEXT ->
                value -> {
                  BinaryStatistics statistics = new BinaryStatistics();
                  statistics.initializeStats(
                      new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET),
                      new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET));
                  return statistics;
                };
            case STRING ->
                value -> {
                  StringStatistics statistics = new StringStatistics();
                  statistics.initializeStats(
                      new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET),
                      new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET),
                      new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET),
                      new Binary(String.valueOf(value), TSFileConfig.STRING_CHARSET));
                  return statistics;
                };
            case BOOLEAN ->
                value -> {
                  BooleanStatistics statistics = new BooleanStatistics();
                  boolean booleanValue = value % 2 == 1;
                  statistics.initializeStats(booleanValue, booleanValue, booleanValue ? 1 : 0);
                  return statistics;
                };
            case BLOB -> value -> new BlobStatistics();
            case OBJECT -> value -> new ObjectStatistics();
            case DATE ->
                value -> {
                  DateStatistics statistics = new DateStatistics();
                  statistics.initializeStats(value, value, value, value, value);
                  return statistics;
                };
            case TIMESTAMP ->
                value -> {
                  TimestampStatistics statistics = new TimestampStatistics();
                  statistics.initializeStats(value, value, value, value, value);
                  return statistics;
                };
            case ROW, UNKNOWN, VECTOR ->
                value -> {
                  throw new IllegalArgumentException(type.getTypeEnum().toString());
                };
          };

  static {
    CHECK_STATISTICS_SERVICE.check();
    GENERATE_STATISTICS_SERVICE.check();
  }

  @Test
  public void testCrossTypeMerge() {
    Set<TSDataType> dataTypes = new HashSet<>();
    Collections.addAll(dataTypes, TSDataType.values());
    dataTypes.remove(TSDataType.VECTOR);
    dataTypes.remove(TSDataType.UNKNOWN);

    for (TSDataType from : dataTypes) {
      for (TSDataType to : dataTypes) {
        Statistics fromStatistics = genStatistics(from, 0);
        Statistics toStatistics = genStatistics(to, 1);
        if (Statistics.canMerge(from, to)) {
          toStatistics.mergeStatistics(fromStatistics);
          checkStatistics(toStatistics, 0, 1, 1.0);
        } else {
          try {
            toStatistics.mergeStatistics(fromStatistics);
            fail("Expected MergeException");
          } catch (Exception e) {
            assertEquals(
                String.format(
                    "Statistics classes mismatched: %s vs. %s",
                    toStatistics.getClass(), fromStatistics.getClass()),
                e.getMessage());
          }
          checkStatistics(toStatistics, 1, 1, 1.0);
        }
      }
    }
  }

  @SuppressWarnings("SameParameterValue")
  private static void checkStatistics(Statistics statistics, int min, int max, double sum) {
    assertEquals(min, statistics.getStartTime());
    assertEquals(max, statistics.getEndTime());
    CHECK_STATISTICS_SERVICE
        .call(Type.fromTsDataType(statistics.getType()))
        .check(statistics, min, max, sum);
  }

  private static Statistics genStatistics(TSDataType dataType, int val) {
    Statistics result =
        GENERATE_STATISTICS_SERVICE.call(Type.fromTsDataType(dataType)).generate(val);
    result.setStartTime(val);
    result.setEndTime(val);
    result.setEmpty(false);
    return result;
  }

  @FunctionalInterface
  private interface StatisticsChecker {

    void check(Statistics statistics, int min, int max, double sum);
  }

  @FunctionalInterface
  private interface StatisticsGenerator {

    Statistics generate(int value);
  }
}
