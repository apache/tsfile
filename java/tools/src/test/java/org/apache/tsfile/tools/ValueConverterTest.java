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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

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

  // --- DATE ---

  @Test
  public void testStringDashToDate() {
    Object result = ValueConverter.convert("2024-01-15", TSDataType.DATE, true);
    assertEquals(LocalDate.of(2024, 1, 15), result);
  }

  @Test
  public void testStringSlashToDate() {
    Object result = ValueConverter.convert("2024/01/15", TSDataType.DATE, true);
    assertEquals(LocalDate.of(2024, 1, 15), result);
  }

  @Test
  public void testStringDotToDate() {
    Object result = ValueConverter.convert("2024.01.15", TSDataType.DATE, true);
    assertEquals(LocalDate.of(2024, 1, 15), result);
  }

  @Test
  public void testLocalDatePassthrough() {
    LocalDate d = LocalDate.of(2030, 6, 1);
    assertEquals(d, ValueConverter.convert(d, TSDataType.DATE, true));
  }

  @Test
  public void testLocalDateTimeToDate() {
    LocalDateTime ldt = LocalDateTime.of(2024, 3, 10, 5, 30);
    assertEquals(LocalDate.of(2024, 3, 10), ValueConverter.convert(ldt, TSDataType.DATE, true));
  }

  @Test
  public void testEpochDayIntegerToDate() {
    // 1970-01-01 = epoch day 0; 2024-01-15 = day 19737
    Object result = ValueConverter.convert(19737, TSDataType.DATE, true);
    assertEquals(LocalDate.of(2024, 1, 15), result);
  }

  @Test
  public void testEpochDayLongToDate() {
    Object result = ValueConverter.convert(0L, TSDataType.DATE, true);
    assertEquals(LocalDate.ofEpochDay(0), result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDateThrows() {
    ValueConverter.convert("not-a-date", TSDataType.DATE, true);
  }

  // --- TIMESTAMP ---

  @Test
  public void testStringNumericToTimestamp() {
    Object result = ValueConverter.convert("1700000000000", TSDataType.TIMESTAMP, true, "ms");
    assertEquals(1700000000000L, result);
  }

  @Test
  public void testLongPassthroughToTimestamp() {
    Object result = ValueConverter.convert(1700000000000L, TSDataType.TIMESTAMP, true, "ms");
    assertEquals(1700000000000L, result);
  }

  @Test
  public void testIntegerWidenedToTimestamp() {
    Object result = ValueConverter.convert(123, TSDataType.TIMESTAMP, true, "ms");
    assertEquals(123L, result);
  }

  @Test
  public void testInstantToTimestampMs() {
    Instant i = Instant.ofEpochSecond(1700000000L, 500_000_000);
    Object result = ValueConverter.convert(i, TSDataType.TIMESTAMP, true, "ms");
    assertEquals(1700000000_500L, result);
  }

  @Test
  public void testInstantToTimestampUs() {
    Instant i = Instant.ofEpochSecond(1700000000L, 500_000_000);
    Object result = ValueConverter.convert(i, TSDataType.TIMESTAMP, true, "us");
    assertEquals(1700000000_500_000L, result);
  }

  @Test
  public void testInstantToTimestampNs() {
    Instant i = Instant.ofEpochSecond(1700000000L, 123_456_789);
    Object result = ValueConverter.convert(i, TSDataType.TIMESTAMP, true, "ns");
    assertEquals(1700000000_123_456_789L, result);
  }

  @Test
  public void testIsoDateStringToTimestampMs() {
    // 2024-01-15T00:00:00 in UTC = 1705276800000ms
    Object result =
        ValueConverter.convert("2024-01-15T00:00:00+00:00", TSDataType.TIMESTAMP, true, "ms");
    assertEquals(1705276800000L, result);
  }
}
