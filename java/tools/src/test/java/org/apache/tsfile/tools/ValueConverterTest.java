/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.tools;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ValueConverterTest {

  // --- null ---

  @Test
  public void testNullReturnsNull() {
    assertNull(ValueConverter.convert(null, TSDataType.INT32, false));
    assertNull(ValueConverter.convert(null, TSDataType.FLOAT, true));
  }

  // --- String → types ---

  @Test
  public void testStringToInt32() {
    Object result = ValueConverter.convert("42", TSDataType.INT32, false);
    assertEquals(42, result);
  }

  @Test
  public void testStringToInt64() {
    Object result = ValueConverter.convert("123456789012", TSDataType.INT64, false);
    assertEquals(123456789012L, result);
  }

  @Test
  public void testStringToFloat() {
    Object result = ValueConverter.convert("3.14", TSDataType.FLOAT, true);
    assertEquals(3.14f, (float) result, 0.001f);
  }

  @Test
  public void testStringToDouble() {
    Object result = ValueConverter.convert("3.14159", TSDataType.DOUBLE, true);
    assertEquals(3.14159, (double) result, 0.00001);
  }

  @Test
  public void testStringToBoolean() {
    assertEquals(true, ValueConverter.convert("true", TSDataType.BOOLEAN, false));
    assertEquals(false, ValueConverter.convert("false", TSDataType.BOOLEAN, false));
  }

  @Test
  public void testStringToTextAsMeasurement() {
    Object result = ValueConverter.convert("hello", TSDataType.TEXT, true);
    assertTrue(result instanceof Binary);
    assertEquals("hello", ((Binary) result).getStringValue(StandardCharsets.UTF_8));
  }

  @Test
  public void testStringToTextAsTag() {
    Object result = ValueConverter.convert("hello", TSDataType.TEXT, false);
    assertTrue(result instanceof String);
    assertEquals("hello", result);
  }

  @Test
  public void testStringToStringAsMeasurement() {
    Object result = ValueConverter.convert("hello", TSDataType.STRING, true);
    assertTrue(result instanceof Binary);
  }

  @Test
  public void testStringToStringAsTag() {
    Object result = ValueConverter.convert("hello", TSDataType.STRING, false);
    assertEquals("hello", result);
  }

  // --- Native type passthrough ---

  @Test
  public void testIntegerPassthroughInt32() {
    Object result = ValueConverter.convert(42, TSDataType.INT32, false);
    assertEquals(42, result);
  }

  @Test
  public void testLongPassthroughInt64() {
    Object result = ValueConverter.convert(100L, TSDataType.INT64, false);
    assertEquals(100L, result);
  }

  @Test
  public void testFloatPassthroughFloat() {
    Object result = ValueConverter.convert(1.5f, TSDataType.FLOAT, true);
    assertEquals(1.5f, result);
  }

  @Test
  public void testDoublePassthroughDouble() {
    Object result = ValueConverter.convert(2.5, TSDataType.DOUBLE, true);
    assertEquals(2.5, result);
  }

  @Test
  public void testBooleanPassthrough() {
    assertEquals(true, ValueConverter.convert(true, TSDataType.BOOLEAN, false));
  }

  // --- Type promotion ---

  @Test
  public void testIntegerToInt64() {
    Object result = ValueConverter.convert(42, TSDataType.INT64, false);
    assertEquals(42L, result);
  }

  @Test
  public void testIntegerToDouble() {
    Object result = ValueConverter.convert(42, TSDataType.DOUBLE, true);
    assertEquals(42.0, result);
  }

  @Test
  public void testFloatToDouble() {
    Object result = ValueConverter.convert(1.5f, TSDataType.DOUBLE, true);
    assertEquals(1.5, (double) result, 0.001);
  }

  @Test
  public void testLongToInt32() {
    Object result = ValueConverter.convert(42L, TSDataType.INT32, false);
    assertEquals(42, result);
  }

  // --- BLOB ---

  @Test
  public void testStringToBlob() {
    Object result = ValueConverter.convert("data", TSDataType.BLOB, true);
    assertTrue(result instanceof Binary);
  }

  @Test
  public void testBytesToBlob() {
    byte[] bytes = new byte[] {1, 2, 3};
    Object result = ValueConverter.convert(bytes, TSDataType.BLOB, true);
    assertTrue(result instanceof Binary);
  }

  @Test
  public void testBinaryPassthrough() {
    Binary binary = new Binary(new byte[] {1, 2});
    Object result = ValueConverter.convert(binary, TSDataType.BLOB, true);
    assertEquals(binary, result);
  }

  // --- Object toString fallback ---

  @Test
  public void testObjectToStringForText() {
    Object result = ValueConverter.convert(12345, TSDataType.TEXT, false);
    assertEquals("12345", result);
  }
}
