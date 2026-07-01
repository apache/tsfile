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

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.operator.ExtractTimeFilterOperators.Field;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class ExtractTimeFilterTest {
  // 2025/07/08 09:18:51 00:00:00+8:00
  private final long testTime1 = 1751937531000L;
  // 2025/07/08 10:18:51 00:00:00+8:00
  private final long testTime2 = 1751941131000L;
  private final ZoneId zoneId1 = ZoneId.of("+0000");
  private final ZoneId zoneId2 = ZoneId.of("+0800");

  private final long DAY_INTERVAL = TimeUnit.DAYS.toMillis(1);

  @Test
  public void testEq() {
    // 1751936400000L -> 2025/07/08 09:00:00+8:00
    // 1751940000000L -> 2025/07/08 10:00:00+8:00
    Filter extractTimeEq1 =
        TimeFilterApi.extractTimeEq(1, Field.HOUR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertFalse(extractTimeEq1.satisfy(testTime2, 100));
    Assert.assertTrue(extractTimeEq1.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(extractTimeEq1.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertTrue(extractTimeEq1.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(extractTimeEq1.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertTrue(extractTimeEq1.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertTrue(extractTimeEq1.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertFalse(extractTimeEq1.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertFalse(extractTimeEq1.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(extractTimeEq1.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(extractTimeEq1.containStartEndTime(1751936400000L, 2751936400000L));

    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    Filter extractTimeEq2 =
        TimeFilterApi.extractTimeEq(9, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq2.satisfy(testTime1, 100));
    Assert.assertFalse(extractTimeEq2.satisfy(testTime2, 100));
    Assert.assertTrue(extractTimeEq2.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(extractTimeEq2.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertTrue(extractTimeEq2.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(extractTimeEq2.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertTrue(extractTimeEq2.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertTrue(extractTimeEq2.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertFalse(extractTimeEq2.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertFalse(extractTimeEq2.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(extractTimeEq2.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(extractTimeEq2.containStartEndTime(1751936400000L, 2751936400000L));

    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq2.getTimeRanges());

    // test other extracted results
    extractTimeEq1 = TimeFilterApi.extractTimeEq(2025, Field.YEAR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    // 1735689600000L -> 2025/01/01 00:00:00+00:00
    // 1767225600000L -> 2026/01/01 00:00:00+00:00
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(1735689600000L, 1767225600000L - 1)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(3, Field.QUARTER, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(7, Field.MONTH, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(27, Field.WEEK, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(8, Field.DAY, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 =
        TimeFilterApi.extractTimeEq(8, Field.DAY_OF_MONTH, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 =
        TimeFilterApi.extractTimeEq(2, Field.DAY_OF_WEEK, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 =
        TimeFilterApi.extractTimeEq(189, Field.DAY_OF_YEAR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(18, Field.MINUTE, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(51, Field.SECOND, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(0, Field.MS, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(0, Field.US, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(0, Field.NS, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(testTime1, 100));
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE)),
        extractTimeEq1.getTimeRanges());

    extractTimeEq1 = TimeFilterApi.extractTimeEq(0, Field.MS, zoneId1, TimeUnit.MICROSECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(1751937531000025L, 100));
    extractTimeEq1 = TimeFilterApi.extractTimeEq(25, Field.US, zoneId1, TimeUnit.MICROSECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(1751937531000025L, 100));
    extractTimeEq1 = TimeFilterApi.extractTimeEq(0, Field.NS, zoneId1, TimeUnit.MICROSECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(1751937531000025L, 100));

    extractTimeEq1 = TimeFilterApi.extractTimeEq(0, Field.MS, zoneId1, TimeUnit.NANOSECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(1751937531000025026L, 100));
    extractTimeEq1 = TimeFilterApi.extractTimeEq(25, Field.US, zoneId1, TimeUnit.NANOSECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(1751937531000025026L, 100));
    extractTimeEq1 = TimeFilterApi.extractTimeEq(26, Field.NS, zoneId1, TimeUnit.NANOSECONDS);
    Assert.assertTrue(extractTimeEq1.satisfy(1751937531000025026L, 100));
  }

  @Test
  public void testNotEq() {
    // 1751936400000L -> 2025/07/08 09:00:00+8:00
    // 1751940000000L -> 2025/07/08 10:00:00+8:00
    Filter filter1 = TimeFilterApi.extractTimeNotEq(1, Field.HOUR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertFalse(filter1.satisfy(testTime1, 100));
    Assert.assertTrue(filter1.satisfy(testTime2, 100));
    Assert.assertFalse(filter1.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertFalse(filter1.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertFalse(filter1.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertFalse(filter1.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter1.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L, 2751936400000L));

    // attention: actual contains, but the method returns false
    Assert.assertFalse(
        filter1.containStartEndTime(1751940000000L, 1751936400000L + DAY_INTERVAL - 1));

    // 1735689600000L -> 2025/01/01 00:00:00+00:00
    // 1767225600000L -> 2026/01/01 00:00:00+00:00
    filter1 = TimeFilterApi.extractTimeNotEq(2025, Field.YEAR, zoneId1, TimeUnit.MICROSECONDS);
    Assert.assertEquals(
        Arrays.asList(
            new TimeRange(Long.MIN_VALUE, 1735689600000_000L - 1),
            new TimeRange(1767225600000_000L, Long.MAX_VALUE)),
        filter1.getTimeRanges());

    Filter filter2 = TimeFilterApi.extractTimeNotEq(9, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertFalse(filter2.satisfy(testTime1, 100));
    Assert.assertTrue(filter2.satisfy(testTime2, 100));
    Assert.assertFalse(filter2.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertFalse(filter2.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertFalse(filter2.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter2.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L, 2751936400000L));

    // attention: actual contains, but the method returns false
    Assert.assertFalse(
        filter2.containStartEndTime(1751940000000L, 1751936400000L + DAY_INTERVAL - 1));

    // 1735660800000L -> 2025/01/01 00:00:00+08:00
    // 1767196800000L -> 2026/01/01 00:00:00+08:00
    filter2 = TimeFilterApi.extractTimeNotEq(2025, Field.YEAR, zoneId2, TimeUnit.MICROSECONDS);
    Assert.assertEquals(
        Arrays.asList(
            new TimeRange(Long.MIN_VALUE, 1735660800000_000L - 1),
            new TimeRange(1767196800000_000L, Long.MAX_VALUE)),
        filter2.getTimeRanges());
  }

  @Test
  public void testGt() {
    // 1751936400000L -> 2025/07/08 09:00:00+8:00
    // 1751940000000L -> 2025/07/08 10:00:00+8:00
    Filter filter1 = TimeFilterApi.extractTimeGt(5, Field.HOUR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertFalse(filter1.satisfy(testTime1, 100));
    Assert.assertFalse(filter1.satisfy(testTime2, 100));
    Assert.assertFalse(filter1.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertFalse(filter1.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(filter1.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertFalse(filter1.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertFalse(filter1.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L, 2751936400000L));

    // 1735689600000L -> 2025/01/01 00:00:00+00:00
    // 1767225600000L -> 2026/01/01 00:00:00+00:00
    filter1 = TimeFilterApi.extractTimeGt(2025, Field.YEAR, zoneId1, TimeUnit.MICROSECONDS);
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(1767225600000_000L, Long.MAX_VALUE)),
        filter1.getTimeRanges());

    Filter filter2 = TimeFilterApi.extractTimeGt(5, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertTrue(filter2.satisfy(testTime1, 100));
    Assert.assertTrue(filter2.satisfy(testTime2, 100));
    Assert.assertTrue(filter2.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertTrue(filter2.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertTrue(filter2.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter2.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertTrue(filter2.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter2.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L, 2751936400000L));

    Filter filter3 = TimeFilterApi.extractTimeGt(9, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertFalse(filter3.satisfy(testTime1, 100));
    Assert.assertTrue(filter3.satisfy(testTime2, 100));
    Assert.assertTrue(filter3.satisfyStartEndTime(testTime1, testTime2));
    Assert.assertFalse(filter3.containStartEndTime(testTime1, testTime2));
  }

  @Test
  public void testGtEq() {
    Filter filter1 = TimeFilterApi.extractTimeGtEq(5, Field.HOUR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertFalse(filter1.satisfy(testTime1, 100));
    Assert.assertFalse(filter1.satisfy(testTime2, 100));
    Assert.assertFalse(filter1.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertFalse(filter1.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(filter1.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertFalse(filter1.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertFalse(filter1.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L, 2751936400000L));

    // 1735689600000L -> 2025/01/01 00:00:00+00:00
    // 1767225600000L -> 2026/01/01 00:00:00+00:00
    filter1 = TimeFilterApi.extractTimeGtEq(2025, Field.YEAR, zoneId1, TimeUnit.NANOSECONDS);
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(1735689600000_000_000L, Long.MAX_VALUE)),
        filter1.getTimeRanges());

    Filter filter2 = TimeFilterApi.extractTimeGtEq(5, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertTrue(filter2.satisfy(testTime1, 100));
    Assert.assertTrue(filter2.satisfy(testTime2, 100));
    Assert.assertTrue(filter2.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertTrue(filter2.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertTrue(filter2.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter2.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertTrue(filter2.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter2.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L, 2751936400000L));

    Filter filter3 = TimeFilterApi.extractTimeGtEq(9, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertTrue(filter3.satisfy(testTime1, 100));
    Assert.assertTrue(filter3.satisfy(testTime2, 100));
    Assert.assertTrue(filter3.satisfyStartEndTime(testTime1, testTime2));
    Assert.assertTrue(filter3.containStartEndTime(testTime1, testTime2));
  }

  @Test
  public void testLt() {
    Filter filter1 = TimeFilterApi.extractTimeLt(5, Field.HOUR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(filter1.satisfy(testTime1, 100));
    Assert.assertTrue(filter1.satisfy(testTime2, 100));
    Assert.assertTrue(filter1.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertTrue(filter1.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertTrue(filter1.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter1.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertTrue(filter1.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter1.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L, 2751936400000L));

    // 1735689600000L -> 2025/01/01 00:00:00+00:00
    // 1767225600000L -> 2026/01/01 00:00:00+00:00
    filter1 = TimeFilterApi.extractTimeLt(2025, Field.YEAR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, 1735689600000L - 1)),
        filter1.getTimeRanges());

    Filter filter2 = TimeFilterApi.extractTimeLt(5, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertFalse(filter2.satisfy(testTime1, 100));
    Assert.assertFalse(filter2.satisfy(testTime2, 100));
    Assert.assertFalse(filter2.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertFalse(filter2.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(filter2.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertFalse(filter2.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertFalse(filter2.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L, 2751936400000L));

    Filter filter3 = TimeFilterApi.extractTimeGtEq(9, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertTrue(filter3.satisfy(testTime1, 100));
    Assert.assertTrue(filter3.satisfy(testTime2, 100));
    Assert.assertTrue(filter3.satisfyStartEndTime(testTime1, testTime2));
    Assert.assertTrue(filter3.containStartEndTime(testTime1, testTime2));
  }

  @Test
  public void testLtEq() {
    // 1751936400000L -> 2025/07/08 09:00:00+8:00
    // 1751940000000L -> 2025/07/08 10:00:00+8:00
    Filter filter1 = TimeFilterApi.extractTimeLtEq(5, Field.HOUR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertTrue(filter1.satisfy(testTime1, 100));
    Assert.assertTrue(filter1.satisfy(testTime2, 100));
    Assert.assertTrue(filter1.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter1.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertTrue(filter1.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertTrue(filter1.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter1.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertTrue(filter1.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertTrue(filter1.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter1.containStartEndTime(1751936400000L, 2751936400000L));

    // 1735689600000L -> 2025/01/01 00:00:00+00:00
    // 1767225600000L -> 2026/01/01 00:00:00+00:00
    filter1 = TimeFilterApi.extractTimeLtEq(2025, Field.YEAR, zoneId1, TimeUnit.MILLISECONDS);
    Assert.assertEquals(
        Collections.singletonList(new TimeRange(Long.MIN_VALUE, 1767225600000L - 1)),
        filter1.getTimeRanges());

    Filter filter2 = TimeFilterApi.extractTimeLt(5, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertFalse(filter2.satisfy(testTime1, 100));
    Assert.assertFalse(filter2.satisfy(testTime2, 100));
    Assert.assertFalse(filter2.satisfyStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertTrue(filter2.satisfyStartEndTime(1751936400000L, 2751936400000L));
    Assert.assertFalse(filter2.satisfyStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(filter2.satisfyStartEndTime(1751936400000L - 2, 1751936400000L - 1));

    Assert.assertFalse(filter2.containStartEndTime(1751936400000L, 1751940000000L - 1));
    Assert.assertFalse(filter2.containStartEndTime(testTime1 - 1, testTime1 + 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L - 1, 1751940000000L));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L - 1, testTime1 + 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L - 2, 1751936400000L - 1));
    Assert.assertFalse(filter2.containStartEndTime(1751936400000L, 2751936400000L));

    Filter filter3 = TimeFilterApi.extractTimeGt(9, Field.HOUR, zoneId2, TimeUnit.MILLISECONDS);
    Assert.assertFalse(filter3.satisfy(testTime1, 100));
    Assert.assertTrue(filter3.satisfy(testTime2, 100));
    Assert.assertTrue(filter3.satisfyStartEndTime(testTime1, testTime2));
    Assert.assertFalse(filter3.containStartEndTime(testTime1, testTime2));
  }
}
