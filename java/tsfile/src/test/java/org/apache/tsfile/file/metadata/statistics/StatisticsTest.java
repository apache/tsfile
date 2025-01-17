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
import org.apache.tsfile.utils.Binary;

import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes"})
public class StatisticsTest {

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
    switch (statistics.getType()) {
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case TIMESTAMP:
      case DATE:
        assertEquals(min, ((Number) statistics.getMinValue()).intValue());
        assertEquals(max, ((Number) statistics.getMaxValue()).intValue());
        assertEquals(min, ((Number) statistics.getFirstValue()).intValue());
        assertEquals(max, ((Number) statistics.getLastValue()).intValue());
        assertEquals(sum, statistics.getSumDoubleValue(), 0.001);
        break;
      case BOOLEAN:
        assertEquals(min % 2 == 1, statistics.getFirstValue());
        assertEquals(max % 2 == 1, statistics.getLastValue());
        assertEquals(sum, statistics.getSumDoubleValue(), 0.001);
        break;
      case TEXT:
        assertEquals(
            new Binary(String.valueOf(min), TSFileConfig.STRING_CHARSET),
            statistics.getFirstValue());
        assertEquals(
            new Binary(String.valueOf(max), TSFileConfig.STRING_CHARSET),
            statistics.getLastValue());
        break;
      case STRING:
        assertEquals(
            new Binary(String.valueOf(min), TSFileConfig.STRING_CHARSET), statistics.getMinValue());
        assertEquals(
            new Binary(String.valueOf(max), TSFileConfig.STRING_CHARSET), statistics.getMaxValue());
        assertEquals(
            new Binary(String.valueOf(min), TSFileConfig.STRING_CHARSET),
            statistics.getFirstValue());
        assertEquals(
            new Binary(String.valueOf(max), TSFileConfig.STRING_CHARSET),
            statistics.getLastValue());
        break;
      case BLOB:
        break;
      default:
        throw new IllegalArgumentException(statistics.getType().toString());
    }
  }

  private static Statistics genStatistics(TSDataType dataType, int val) {
    Statistics result;
    switch (dataType) {
      case INT32:
        IntegerStatistics intStat = new IntegerStatistics();
        intStat.initializeStats(val, val, val, val, val);
        result = intStat;
        break;
      case INT64:
        LongStatistics longStat = new LongStatistics();
        longStat.initializeStats(val, val, val, val, val);
        result = longStat;
        break;
      case FLOAT:
        FloatStatistics floatStat = new FloatStatistics();
        floatStat.initializeStats(val, val, val, val, val);
        result = floatStat;
        break;
      case DOUBLE:
        DoubleStatistics doubleStat = new DoubleStatistics();
        doubleStat.initializeStats(val, val, val, val, val);
        result = doubleStat;
        break;
      case TEXT:
        BinaryStatistics binaryStat = new BinaryStatistics();
        binaryStat.initializeStats(
            new Binary(String.valueOf(val), TSFileConfig.STRING_CHARSET),
            new Binary(String.valueOf(val), TSFileConfig.STRING_CHARSET));
        result = binaryStat;
        break;
      case STRING:
        StringStatistics stringStat = new StringStatistics();
        stringStat.initializeStats(
            new Binary(String.valueOf(val), TSFileConfig.STRING_CHARSET),
            new Binary(String.valueOf(val), TSFileConfig.STRING_CHARSET),
            new Binary(String.valueOf(val), TSFileConfig.STRING_CHARSET),
            new Binary(String.valueOf(val), TSFileConfig.STRING_CHARSET));
        result = stringStat;
        break;
      case BOOLEAN:
        BooleanStatistics boolStat = new BooleanStatistics();
        boolStat.initializeStats(val % 2 == 1, val % 2 == 1, val % 2 == 1 ? 1 : 0);
        result = boolStat;
        break;
      case BLOB:
        BlobStatistics blobStat = new BlobStatistics();
        result = blobStat;
        break;
      case DATE:
        DateStatistics dateStat = new DateStatistics();
        dateStat.initializeStats(val, val, val, val, val);
        result = dateStat;
        break;
      case TIMESTAMP:
        TimestampStatistics timestampStat = new TimestampStatistics();
        timestampStat.initializeStats(val, val, val, val, val);
        result = timestampStat;
        break;
      default:
        throw new IllegalArgumentException(dataType.toString());
    }
    result.setStartTime(val);
    result.setEndTime(val);
    result.setEmpty(false);
    return result;
  }
}
