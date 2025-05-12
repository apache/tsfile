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

package org.apache.tsfile;

import org.apache.tsfile.enums.TSDataType;

import java.util.Arrays;
import java.util.List;

public class BenchMarkConf {
  public static final int TABLET_NUM = 1000;
  public static final int TAG1_NUM = 1;
  public static final int TAG2_NUM = 10;
  public static final int TIMESTAMP_PER_TAG = 1000;
  public static final List<Integer> FIELD_TYPE_VECTOR = Arrays.asList(1, 1, 1, 1, 1);

  public static TSDataType getTsDataType(int index) {
    switch (index) {
      case 0:
        return TSDataType.INT32;
      case 1:
        return TSDataType.INT64;
      case 2:
        return TSDataType.FLOAT;
      case 3:
        return TSDataType.DOUBLE;
      case 4:
        return TSDataType.BOOLEAN;
    }
    return TSDataType.UNKNOWN;
  }

  public static final List<String> DATA_TYPES_NAME =
      Arrays.asList("INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN");

  public static void printConfig() {
    int columnNum = 0;
    for (int count : FIELD_TYPE_VECTOR) {
      columnNum += count;
    }

    System.out.println("TsFile benchmark For Java");
    System.out.println("Schema Configuration:");
    System.out.println("Tag Column num: " + 2);
    System.out.printf(
        "TAG1 num: %d TAG2 num: %d%n%n", BenchMarkConf.TAG1_NUM, BenchMarkConf.TAG2_NUM);

    System.out.println("Field Column and types: ");
    for (int i = 0; i < 5; i++) {
      System.out.printf("%sx%d  ", DATA_TYPES_NAME.get(i), BenchMarkConf.FIELD_TYPE_VECTOR.get(i));
    }

    System.out.printf("%nTablet num: %d%n", BenchMarkConf.TABLET_NUM);
    System.out.printf("Tablet row num per tag: %d%n", BenchMarkConf.TIMESTAMP_PER_TAG);

    long totalPoints =
        (long) BenchMarkConf.TABLET_NUM
            * BenchMarkConf.TAG1_NUM
            * BenchMarkConf.TAG2_NUM
            * BenchMarkConf.TIMESTAMP_PER_TAG
            * columnNum;
    System.out.println("Total points is " + totalPoints);
    System.out.println("======");
  }
}
