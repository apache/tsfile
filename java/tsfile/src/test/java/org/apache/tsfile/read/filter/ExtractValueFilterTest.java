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

import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.tsfile.read.filter.basic.ValueFilter;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.filter.operator.ExtractTimeFilterOperators;
import org.apache.tsfile.read.filter.operator.ExtractValueFilterOperators;

import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

import static org.apache.tsfile.read.filter.FilterTestUtil.newMetadata;

public class ExtractValueFilterTest {
  private final ZoneId zoneId1 = ZoneId.of("+0000");
  private final ZoneId zoneId2 = ZoneId.of("+0800");

  private final LongStatistics statistics = new LongStatistics();
  private final IMetadata metadata = newMetadata(statistics);

  // Test delegate logic is right
  @Test
  public void test() {
    statistics.setEmpty(false);
    // 2025/07/08 09:18:51 00:00:00+8:00
    long testTime1 = 1751937531000L;
    // 2025/07/08 10:18:51 00:00:00+8:00
    long testTime2 = 1751941131000L;

    // 1751936400000L -> 2025/07/08 09:00:00+8:00
    // 1751940000000L -> 2025/07/08 10:00:00+8:00
    ValueFilter extractValueEq1 =
        new ExtractValueFilterOperators.ExtractValueEq(
            ValueFilterApi.DEFAULT_MEASUREMENT_INDEX,
            1,
            ExtractTimeFilterOperators.Field.HOUR,
            zoneId1,
            TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractValueEq1.satisfy(testTime1, testTime1));
    Assert.assertFalse(extractValueEq1.satisfy(testTime2, testTime2));
    statistics.initializeStats(testTime1 - 1, testTime1 + 1, 0, 0, 0);
    Assert.assertFalse(extractValueEq1.canSkip(metadata));
    statistics.initializeStats(1751936400000L, 2751936400000L, 0, 0, 0);
    Assert.assertFalse(extractValueEq1.canSkip(metadata));
    statistics.initializeStats(1751936400000L - 1, testTime1 + 1, 0, 0, 0);
    Assert.assertFalse(extractValueEq1.canSkip(metadata));
    statistics.initializeStats(1751936400000L - 2, 1751936400000L - 1, 0, 0, 0);
    Assert.assertTrue(extractValueEq1.canSkip(metadata));

    statistics.initializeStats(1751936400000L, 1751940000000L - 1, 0, 0, 0);
    Assert.assertTrue(extractValueEq1.allSatisfy(metadata));
    statistics.initializeStats(testTime1 - 1, testTime1 + 1, 0, 0, 0);
    Assert.assertTrue(extractValueEq1.allSatisfy(metadata));
    statistics.initializeStats(1751936400000L - 1, 1751940000000L, 0, 0, 0);
    Assert.assertFalse(extractValueEq1.allSatisfy(metadata));
    statistics.initializeStats(1751936400000L - 1, testTime1 + 1, 0, 0, 0);
    Assert.assertFalse(extractValueEq1.allSatisfy(metadata));
    statistics.initializeStats(1751936400000L - 2, 1751936400000L - 1, 0, 0, 0);
    Assert.assertFalse(extractValueEq1.allSatisfy(metadata));
    statistics.initializeStats(1751936400000L, 2751936400000L, 0, 0, 0);
    Assert.assertFalse(extractValueEq1.allSatisfy(metadata));

    ValueFilter extractValueEq2 =
        new ExtractValueFilterOperators.ExtractValueEq(
            ValueFilterApi.DEFAULT_MEASUREMENT_INDEX,
            9,
            ExtractTimeFilterOperators.Field.HOUR,
            zoneId2,
            TimeUnit.MILLISECONDS);
    Assert.assertTrue(extractValueEq2.satisfy(testTime1, testTime1));
    Assert.assertFalse(extractValueEq2.satisfy(testTime2, testTime2));
    statistics.initializeStats(testTime1 - 1, testTime1 + 1, 0, 0, 0);
    Assert.assertFalse(extractValueEq2.canSkip(metadata));
    statistics.initializeStats(1751936400000L, 2751936400000L, 0, 0, 0);
    Assert.assertFalse(extractValueEq2.canSkip(metadata));
    statistics.initializeStats(1751936400000L - 1, testTime1 + 1, 0, 0, 0);
    Assert.assertFalse(extractValueEq2.canSkip(metadata));
    statistics.initializeStats(1751936400000L - 2, 1751936400000L - 1, 0, 0, 0);
    Assert.assertTrue(extractValueEq2.canSkip(metadata));

    statistics.initializeStats(1751936400000L, 1751940000000L - 1, 0, 0, 0);
    Assert.assertTrue(extractValueEq2.allSatisfy(metadata));
    statistics.initializeStats(testTime1 - 1, testTime1 + 1, 0, 0, 0);
    Assert.assertTrue(extractValueEq2.allSatisfy(metadata));
    statistics.initializeStats(1751936400000L - 1, 1751940000000L, 0, 0, 0);
    Assert.assertFalse(extractValueEq2.allSatisfy(metadata));
    statistics.initializeStats(1751936400000L - 1, testTime1 + 1, 0, 0, 0);
    Assert.assertFalse(extractValueEq2.allSatisfy(metadata));
    statistics.initializeStats(1751936400000L - 2, 1751936400000L - 1, 0, 0, 0);
    Assert.assertFalse(extractValueEq2.allSatisfy(metadata));
    statistics.initializeStats(1751936400000L, 2751936400000L, 0, 0, 0);
    Assert.assertFalse(extractValueEq2.allSatisfy(metadata));
  }
}
