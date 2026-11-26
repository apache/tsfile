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

import org.junit.Assert;
import org.junit.Test;

public class RowRecordTest {

  @Test
  public void testAddNullField() {
    RowRecord record = new RowRecord(1L);
    record.addField(null);
    record.addField(new Field(TSDataType.INT32));

    Assert.assertTrue(record.isNull(0));
    Assert.assertFalse(record.isNull(1));
    Assert.assertNull(record.getField(0));
    Assert.assertNotNull(record.getField(1));
    Assert.assertTrue(record.hasNullField());
    Assert.assertFalse(record.isAllNull());
  }

  @Test
  public void testNullFieldWithNullDataType() {
    RowRecord record = new RowRecord(1L);
    record.addField(new Field(null));

    Assert.assertTrue(record.isNull(0));
    Assert.assertTrue(record.hasNullField());
    Assert.assertTrue(record.isAllNull());
  }

  @Test
  public void testConstructorWithSizeFillsNulls() {
    RowRecord record = new RowRecord(1L, 3);

    Assert.assertTrue(record.isNull(0));
    Assert.assertTrue(record.isNull(1));
    Assert.assertTrue(record.isNull(2));
  }

  @Test
  public void testSetFieldNull() {
    RowRecord record = new RowRecord(1L);
    record.addField(new Field(TSDataType.INT32));
    Assert.assertFalse(record.isNull(0));

    record.setField(0, null);
    Assert.assertTrue(record.isNull(0));
  }
}
