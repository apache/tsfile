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

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class TypeCastTest {

  @Test
  public void testSingleCast() {
    Set<TSDataType> dataTypes = new HashSet<>();
    Collections.addAll(dataTypes, TSDataType.values());
    dataTypes.remove(TSDataType.VECTOR);
    dataTypes.remove(TSDataType.UNKNOWN);

    for (TSDataType from : dataTypes) {
      for (TSDataType to : dataTypes) {
        Object src = genValue(from);
        if (to.isCompatible(from)) {
          assertEquals(genValue(to), to.castFromSingleValue(from, src));
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

    for (TSDataType from : dataTypes) {
      for (TSDataType to : dataTypes) {
        Object array = genValueArray(from);
        if (!to.isCompatible(from)) {
          assertThrows(ClassCastException.class, () -> to.castFromArray(from, array));
          return;
        }
        switch (to) {
          case INT32:
          case DATE:
            assertArrayEquals((int[]) genValueArray(to), (int[]) to.castFromArray(from, array));
            break;
          case INT64:
          case TIMESTAMP:
            assertArrayEquals((long[]) genValueArray(to), (long[]) to.castFromArray(from, array));
            break;
          case BOOLEAN:
            assertArrayEquals(
                (boolean[]) genValueArray(to), (boolean[]) to.castFromArray(from, array));
            break;
          case STRING:
          case BLOB:
          case TEXT:
            assertArrayEquals(
                (Binary[]) genValueArray(to), (Binary[]) to.castFromArray(from, array));
            break;
          case FLOAT:
            assertArrayEquals(
                (float[]) genValueArray(to), (float[]) to.castFromArray(from, array), 0.1f);
            break;
          case DOUBLE:
            assertArrayEquals(
                (double[]) genValueArray(to), (double[]) to.castFromArray(from, array), 0.1);
            break;
          case UNKNOWN:
          case VECTOR:
          default:
            fail("Unexpected type: " + to);
        }
      }
    }
  }

  private Object genValue(TSDataType dataType) {
    int i = 1;
    switch (dataType) {
      case INT32:
      case DATE:
        return i;
      case TIMESTAMP:
      case INT64:
        return (long) i;
      case BOOLEAN:
        return false;
      case FLOAT:
        return i * 1.0f;
      case DOUBLE:
        return i * 1.0;
      case STRING:
      case TEXT:
      case BLOB:
        return new Binary(Integer.toString(i), StandardCharsets.UTF_8);
      case UNKNOWN:
      case VECTOR:
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  private Object genValueArray(TSDataType dataType) {
    switch (dataType) {
      case INT32:
      case DATE:
        return new int[] {1, 2, 3};
      case TIMESTAMP:
      case INT64:
        return new long[] {1, 2, 3};
      case BOOLEAN:
        return new boolean[] {true, false};
      case FLOAT:
        return new float[] {1.0f, 2.0f, 3.0f};
      case DOUBLE:
        return new double[] {1.0, 2.0, 3.0};
      case STRING:
      case TEXT:
      case BLOB:
        return new Binary[] {
          new Binary(Integer.toString(1), StandardCharsets.UTF_8),
          new Binary(Integer.toString(2), StandardCharsets.UTF_8),
          new Binary(Integer.toString(3), StandardCharsets.UTF_8)
        };
      case UNKNOWN:
      case VECTOR:
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }
}
