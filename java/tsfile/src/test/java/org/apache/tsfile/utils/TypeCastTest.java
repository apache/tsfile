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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.IntFunction;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class TypeCastTest {

  private static final TypeService<ArrayCastChecker> CHECK_ARRAY_CAST_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE ->
                (from, to, array) ->
                    assertArrayEquals(
                        (int[]) genValueArray(to), (int[]) to.castFromArray(from, array));
            case INT64, TIMESTAMP ->
                (from, to, array) ->
                    assertArrayEquals(
                        (long[]) genValueArray(to), (long[]) to.castFromArray(from, array));
            case BOOLEAN ->
                (from, to, array) ->
                    assertArrayEquals(
                        (boolean[]) genValueArray(to), (boolean[]) to.castFromArray(from, array));
            case BLOB, STRING, TEXT ->
                (from, to, array) ->
                    assertArrayEquals(
                        TypeCastTest.EXPECTED_BINARY_ARRAY_SERVICE
                            .call(Type.fromTsDataType(from))
                            .generate(array),
                        (Binary[]) to.castFromArray(from, array));
            case FLOAT ->
                (from, to, array) ->
                    assertArrayEquals(
                        (float[]) genValueArray(to), (float[]) to.castFromArray(from, array), 0.1f);
            case DOUBLE ->
                (from, to, array) ->
                    assertArrayEquals(
                        (double[]) genValueArray(to),
                        (double[]) to.castFromArray(from, array),
                        0.1);
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                (from, to, array) -> fail("Unexpected type: " + to);
          };

  private static final TypeService<BinaryArrayGenerator> EXPECTED_BINARY_ARRAY_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32 ->
                array -> {
                  int[] values = (int[]) array;
                  return toBinaryArray(values.length, i -> String.valueOf(values[i]));
                };
            case DATE ->
                array -> {
                  int[] values = (int[]) array;
                  return toBinaryArray(
                      values.length, i -> TSDataType.getDateStringValue(values[i]));
                };
            case INT64, TIMESTAMP ->
                array -> {
                  long[] values = (long[]) array;
                  return toBinaryArray(values.length, i -> String.valueOf(values[i]));
                };
            case FLOAT ->
                array -> {
                  float[] values = (float[]) array;
                  return toBinaryArray(values.length, i -> String.valueOf(values[i]));
                };
            case DOUBLE ->
                array -> {
                  double[] values = (double[]) array;
                  return toBinaryArray(values.length, i -> String.valueOf(values[i]));
                };
            case BOOLEAN ->
                array -> {
                  boolean[] values = (boolean[]) array;
                  return toBinaryArray(values.length, i -> String.valueOf(values[i]));
                };
            case BLOB, STRING, TEXT -> array -> (Binary[]) array;
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                array -> {
                  throw new IllegalArgumentException(
                      "Unsupported data type: " + type.getTypeEnum());
                };
          };

  private static final TypeService<ValueGenerator> GENERATE_VALUE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> () -> 1;
            case INT64, TIMESTAMP -> () -> 1L;
            case BOOLEAN -> () -> false;
            case FLOAT -> () -> 1.0f;
            case DOUBLE -> () -> 1.0;
            case BLOB, OBJECT, STRING, TEXT -> () -> new Binary("1", StandardCharsets.UTF_8);
            case ROW, UNKNOWN, VECTOR ->
                () -> {
                  throw new IllegalArgumentException(
                      "Unsupported data type: " + type.getTypeEnum());
                };
          };

  private static final TypeService<ValueGenerator> GENERATE_VALUE_ARRAY_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> () -> new int[] {1, 2, 3};
            case INT64, TIMESTAMP -> () -> new long[] {1, 2, 3};
            case BOOLEAN -> () -> new boolean[] {true, false};
            case FLOAT -> () -> new float[] {1.0f, 2.0f, 3.0f};
            case DOUBLE -> () -> new double[] {1.0, 2.0, 3.0};
            case BLOB, OBJECT, STRING, TEXT ->
                () ->
                    new Binary[] {
                      new Binary("1", StandardCharsets.UTF_8),
                      new Binary("2", StandardCharsets.UTF_8),
                      new Binary("3", StandardCharsets.UTF_8)
                    };
            case ROW, UNKNOWN, VECTOR ->
                () -> {
                  throw new IllegalArgumentException(
                      "Unsupported data type: " + type.getTypeEnum());
                };
          };

  static {
    CHECK_ARRAY_CAST_SERVICE.check();
    EXPECTED_BINARY_ARRAY_SERVICE.check();
    GENERATE_VALUE_SERVICE.check();
    GENERATE_VALUE_ARRAY_SERVICE.check();
  }

  @Test
  public void testSingleCast() {
    Set<TSDataType> dataTypes = new HashSet<>();
    Collections.addAll(dataTypes, TSDataType.values());
    dataTypes.remove(TSDataType.VECTOR);
    dataTypes.remove(TSDataType.UNKNOWN);
    dataTypes.remove(TSDataType.OBJECT);

    for (TSDataType from : dataTypes) {
      for (TSDataType to : dataTypes) {
        Object src = genValue(from);
        if (to.isCompatible(from)) {
          if (to == TSDataType.STRING || to == TSDataType.TEXT) {
            if (from == TSDataType.DATE) {
              assertEquals(
                  new Binary(LocalDate.ofEpochDay((int) src).toString(), StandardCharsets.UTF_8),
                  new Binary(
                      LocalDate.ofEpochDay(Long.parseLong(genValue(to).toString())).toString(),
                      StandardCharsets.UTF_8));
            } else {
              assertEquals(
                  new Binary(src.toString(), StandardCharsets.UTF_8),
                  to.castFromSingleValue(from, src));
            }
          } else {
            assertEquals(genValue(to), to.castFromSingleValue(from, src));
          }
        } else {
          assertThrows(ClassCastException.class, () -> to.castFromSingleValue(from, src));
        }
      }
    }
  }

  @Test
  public void testArrayCast() {
    Set<TSDataType> dataTypes = new HashSet<>();
    Collections.addAll(dataTypes, TSDataType.values());
    dataTypes.remove(TSDataType.VECTOR);
    dataTypes.remove(TSDataType.UNKNOWN);
    dataTypes.remove(TSDataType.OBJECT);

    for (TSDataType from : dataTypes) {
      for (TSDataType to : dataTypes) {
        Object array = genValueArray(from);
        if (!to.isCompatible(from)) {
          assertThrows(ClassCastException.class, () -> to.castFromArray(from, array));
          continue;
        }
        CHECK_ARRAY_CAST_SERVICE.call(Type.fromTsDataType(to)).check(from, to, array);
      }
    }
  }

  private static Object genValue(TSDataType dataType) {
    return GENERATE_VALUE_SERVICE.call(Type.fromTsDataType(dataType)).generate();
  }

  private static Object genValueArray(TSDataType dataType) {
    return GENERATE_VALUE_ARRAY_SERVICE.call(Type.fromTsDataType(dataType)).generate();
  }

  private static Binary[] toBinaryArray(int length, IntFunction<String> valueProvider) {
    Binary[] result = new Binary[length];
    for (int i = 0; i < length; i++) {
      result[i] = new Binary(valueProvider.apply(i), StandardCharsets.UTF_8);
    }
    return result;
  }

  @FunctionalInterface
  private interface ArrayCastChecker {

    void check(TSDataType from, TSDataType to, Object array);
  }

  @FunctionalInterface
  private interface BinaryArrayGenerator {

    Binary[] generate(Object array);
  }

  @FunctionalInterface
  private interface ValueGenerator {

    Object generate();
  }
}
