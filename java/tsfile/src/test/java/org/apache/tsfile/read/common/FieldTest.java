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
package org.apache.tsfile.read.common;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NullFieldException;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class FieldTest {

  @Test(expected = NullFieldException.class)
  public void construct() {
    Field field = new Field(null);
    field.getIntV();
  }

  @Test
  public void copy() {
    Object[][] values = {
      {TSDataType.BOOLEAN, true},
      {TSDataType.INT32, 1},
      {TSDataType.DATE, 20260716},
      {TSDataType.INT64, 2L},
      {TSDataType.TIMESTAMP, 3L},
      {TSDataType.FLOAT, 1.25F},
      {TSDataType.DOUBLE, 2.5D},
      {TSDataType.TEXT, new Binary("text", StandardCharsets.UTF_8)},
      {TSDataType.STRING, new Binary("string", StandardCharsets.UTF_8)},
      {TSDataType.BLOB, new Binary(new byte[] {1, 2})},
      {TSDataType.OBJECT, new Binary(BytesUtils.longToBytes(2L))}
    };

    for (Object[] value : values) {
      TSDataType dataType = (TSDataType) value[0];
      Field source = Type.fromTsDataType(dataType).getField(value[1]);
      Field copy = Field.copy(source);
      Assert.assertEquals(source.getDataType(), copy.getDataType());
      Assert.assertEquals(source.getObjectValue(dataType), copy.getObjectValue(dataType));
    }
  }
}
