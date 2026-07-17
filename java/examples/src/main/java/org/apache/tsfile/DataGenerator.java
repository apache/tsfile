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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;

import java.time.LocalDate;

public class DataGenerator {

  private static final TypeService<ValueGenerator> GENERATE_VALUE_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> index -> index % 2 == 0;
            case INT32 -> index -> index;
            case INT64, TIMESTAMP -> index -> (long) index;
            case FLOAT -> index -> (float) index;
            case DOUBLE -> index -> (double) index;
            case DATE -> index -> LocalDate.of(2024, 1, index % 30 + 1);
            case TEXT, STRING, BLOB ->
                index -> new Binary(String.valueOf(index), TSFileConfig.STRING_CHARSET);
            case OBJECT, ROW, UNKNOWN, VECTOR -> index -> null;
          };

  public static Object generate(TSDataType type, int index) {
    return GENERATE_VALUE_SERVICE.call(Type.fromTsDataType(type)).generate(index);
  }

  @FunctionalInterface
  private interface ValueGenerator {
    Object generate(int index);
  }
}
