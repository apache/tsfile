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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;

public class DateTimeUtilsTest {

  private static final ZoneId NY = ZoneId.of("America/New_York");

  /**
   * 2024-01-15 (winter) in NY is UTC-5 (EST). Resolving the offset must NOT depend on the current
   * wall-clock time of the JVM, which previously used Instant.now() and would set EDT (UTC-4) when
   * the program ran in summer.
   */
  @Test
  public void testWinterDateGetsStandardOffset() {
    long actual = DateTimeUtils.convertDatetimeStrToLong("2024-01-15T12:00:00", NY, "ms");
    long expected =
        LocalDateTime.of(2024, 1, 15, 12, 0).toInstant(ZoneOffset.ofHours(-5)).toEpochMilli();
    assertEquals(expected, actual);
  }

  /** 2024-07-15 (summer) in NY is UTC-4 (EDT). */
  @Test
  public void testSummerDateGetsDaylightOffset() {
    long actual = DateTimeUtils.convertDatetimeStrToLong("2024-07-15T12:00:00", NY, "ms");
    long expected =
        LocalDateTime.of(2024, 7, 15, 12, 0).toInstant(ZoneOffset.ofHours(-4)).toEpochMilli();
    assertEquals(expected, actual);
  }

  /** A summer and a winter date in NY should be exactly one hour apart at the same wall time. */
  @Test
  public void testWinterVsSummerOffsetDiffers() {
    long winter = DateTimeUtils.convertDatetimeStrToLong("2024-01-15T12:00:00", NY, "ms");
    long summer = DateTimeUtils.convertDatetimeStrToLong("2024-07-15T12:00:00", NY, "ms");
    // Both reference the same date 6 months apart at the same wall-clock time.
    // The summer one is "later" in absolute terms by 6 months minus 1h of DST shift.
    long winterUtc =
        LocalDateTime.of(2024, 1, 15, 12, 0).toInstant(ZoneOffset.ofHours(-5)).toEpochMilli();
    long summerUtc =
        LocalDateTime.of(2024, 7, 15, 12, 0).toInstant(ZoneOffset.ofHours(-4)).toEpochMilli();
    assertEquals(winterUtc, winter);
    assertEquals(summerUtc, summer);
  }

  /** Fixed-offset zones must keep working unchanged. */
  @Test
  public void testFixedOffsetZoneUnchanged() {
    long actual =
        DateTimeUtils.convertDatetimeStrToLong("2024-01-15T12:00:00", ZoneOffset.ofHours(8), "ms");
    long expected =
        LocalDateTime.of(2024, 1, 15, 12, 0).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    assertEquals(expected, actual);
  }

  /** Date-only strings still get expanded to midnight. */
  @Test
  public void testDateOnlyString() {
    long actual = DateTimeUtils.convertDatetimeStrToLong("2024-01-15", NY, "ms");
    long expected =
        LocalDateTime.of(2024, 1, 15, 0, 0).toInstant(ZoneOffset.ofHours(-5)).toEpochMilli();
    assertEquals(expected, actual);
  }

  /** Strings with explicit offset must ignore the supplied ZoneId. */
  @Test
  public void testExplicitOffsetIsRespected() {
    long actual = DateTimeUtils.convertDatetimeStrToLong("2024-01-15T12:00:00+09:00", NY, "ms");
    long expected =
        LocalDateTime.of(2024, 1, 15, 12, 0).toInstant(ZoneOffset.ofHours(9)).toEpochMilli();
    assertEquals(expected, actual);
  }
}
