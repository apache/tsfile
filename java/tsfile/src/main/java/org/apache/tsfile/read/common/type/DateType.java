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

package org.apache.tsfile.read.common.type;

import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDate;

public class DateType extends AbstractIntType {

  public static final DateType DATE = new DateType();
  private static final LocalDate EMPTY_DATE = LocalDate.of(1000, 1, 1);

  private DateType() {}

  @Override
  public void addValue(int rowIndex, Object value, Object column) {
    checkValueType(value, LocalDate.class, "LocalDate");
    ((LocalDate[]) column)[rowIndex] = value != null ? (LocalDate) value : EMPTY_DATE;
  }

  @Override
  public Object createArray(int capacity) {
    return new LocalDate[capacity];
  }

  @Override
  public void serializeArray(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    LocalDate[] values = (LocalDate[]) array;
    for (int i = 0; i < rowSize; i++) {
      ReadWriteIOUtils.write(
          values[i] == null
              ? DateUtils.EMPTY_DATE_INT
              : DateUtils.parseDateExpressionToInt(values[i]),
          stream);
    }
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.DATE;
  }

  @Override
  public String getDisplayName() {
    return "DATE";
  }

  public static DateType getInstance() {
    return DATE;
  }
}
