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

import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class TimeConverterTest {

  @Test
  public void testStringNumericWithSourcePrecisionMs() {
    TimeConverter converter = new TimeConverter("ms");
    long result = converter.convert("1745231234567", "ms");
    assertEquals(1745231234567L, result);
  }

  @Test
  public void testStringNumericWithSourcePrecisionUs() {
    TimeConverter converter = new TimeConverter("ms");
    long result = converter.convert("1745231234567000", "us");
    assertEquals(1745231234567L, result);
  }

  @Test
  public void testStringNumericWithSourcePrecisionNs() {
    TimeConverter converter = new TimeConverter("ms");
    long result = converter.convert("1745231234567000000", "ns");
    assertEquals(1745231234567L, result);
  }

  @Test
  public void testSourcePrecisionMatchesTarget() {
    TimeConverter converter = new TimeConverter("us");
    long result = converter.convert("12345678", "us");
    assertEquals(12345678L, result);
  }

  @Test
  public void testLongWithSourcePrecision() {
    TimeConverter converter = new TimeConverter("ms");
    long result = converter.convert(1745231234567000L, "us");
    assertEquals(1745231234567L, result);
  }

  @Test
  public void testIntegerWithSourcePrecision() {
    TimeConverter converter = new TimeConverter("ms");
    long result = converter.convert(1000, "ms");
    assertEquals(1000L, result);
  }

  @Test
  public void testInferPrecisionNanoseconds() {
    assertEquals("ns", TimeConverter.inferPrecision(1745231234567000000L));
    assertEquals("ns", TimeConverter.inferPrecision(2000000000000000L));
  }

  @Test
  public void testInferPrecisionMicroseconds() {
    // >1e12 and <=1e15 → us
    assertEquals("us", TimeConverter.inferPrecision(1745231234567L));
    assertEquals("us", TimeConverter.inferPrecision(2000000000000L));
  }

  @Test
  public void testInferPrecisionMilliseconds() {
    // >1e11 and <=1e12 → ms
    assertEquals("ms", TimeConverter.inferPrecision(200000000000L));
    assertEquals("ms", TimeConverter.inferPrecision(999999999999L));
  }

  @Test
  public void testInferPrecisionSeconds() {
    assertEquals("s", TimeConverter.inferPrecision(1745231234L));
    assertEquals("s", TimeConverter.inferPrecision(100000000000L));
  }

  @Test
  public void testConvertWithoutSourcePrecisionUsesInference() {
    TimeConverter converter = new TimeConverter("ms");
    // 1745231234567 is >1e12, inferred as "us", should be rescaled to ms
    long result = converter.convert("1745231234567");
    assertEquals(1745231234L, result);
  }

  @Test
  public void testConvertWithSourcePrecisionSkipsInference() {
    TimeConverter converter = new TimeConverter("ms");
    // Same value, but with explicit source precision "ms", should NOT rescale
    long result = converter.convert("1745231234567", "ms");
    assertEquals(1745231234567L, result);
  }

  @Test
  public void testRescaleMsToUs() {
    assertEquals(1000000L, TimeConverter.rescale(1000L, "ms", "us"));
  }

  @Test
  public void testRescaleUsToMs() {
    assertEquals(1L, TimeConverter.rescale(1000L, "us", "ms"));
  }

  @Test
  public void testRescaleNsToMs() {
    assertEquals(1L, TimeConverter.rescale(1000000L, "ns", "ms"));
  }

  @Test
  public void testRescaleSToMs() {
    assertEquals(1000L, TimeConverter.rescale(1L, "s", "ms"));
  }

  @Test
  public void testRescaleSameUnit() {
    assertEquals(42L, TimeConverter.rescale(42L, "ms", "ms"));
  }

  @Test
  public void testInstantToMs() {
    TimeConverter converter = new TimeConverter("ms");
    Instant instant = Instant.ofEpochMilli(1745231234567L);
    long result = converter.convert(instant);
    assertEquals(1745231234567L, result);
  }

  @Test
  public void testInstantToUs() {
    TimeConverter converter = new TimeConverter("us");
    Instant instant = Instant.ofEpochSecond(1000, 500000000L);
    long result = converter.convert(instant);
    assertEquals(1000500000L, result);
  }

  @Test
  public void testInstantToNs() {
    TimeConverter converter = new TimeConverter("ns");
    Instant instant = Instant.ofEpochSecond(1, 123456789L);
    long result = converter.convert(instant);
    assertEquals(1123456789L, result);
  }

  @Test
  public void testDatetimeString() {
    TimeConverter converter = new TimeConverter("ms");
    long result = converter.convert("2025-01-01T00:00:00+00:00");
    assertEquals(1735689600000L, result);
  }

  @Test
  public void testDatetimeStringWithSourcePrecision() {
    TimeConverter converter = new TimeConverter("ms");
    long result = converter.convert("2025-01-01T00:00:00+00:00", "ms");
    assertEquals(1735689600000L, result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullValueThrows() {
    TimeConverter converter = new TimeConverter("ms");
    converter.convert(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullValueWithPrecisionThrows() {
    TimeConverter converter = new TimeConverter("ms");
    converter.convert(null, "ms");
  }
}
