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

package org.apache.tsfile.common.block;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.block.column.ColumnEncoding;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.ColumnEncoder;
import org.apache.tsfile.read.common.block.column.ColumnEncoderFactory;
import org.apache.tsfile.read.common.block.column.DictionaryColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.LongColumnBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DictionaryColumnEncodingTest {
  @Test
  public void testIntColumn() {
    ColumnBuilder columnBuilder = new LongColumnBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        columnBuilder.appendNull();
      } else {
        columnBuilder.writeLong(i % 5);
      }
    }
    Column originalColumn = columnBuilder.build();
    DictionaryColumn input =
        (DictionaryColumn) originalColumn.getPositions(new int[] {1, 3, 5, 8, 9}, 1, 4);

    ColumnEncoder encoder = ColumnEncoderFactory.get(ColumnEncoding.DICTIONARY);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
    try {
      encoder.writeColumn(dos, input);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    Column output = encoder.readColumn(buffer, TSDataType.INT64, input.getPositionCount());
    Assert.assertTrue(output instanceof LongColumn);

    Assert.assertEquals(input.getPositionCount(), output.getPositionCount());
    Assert.assertTrue(output.mayHaveNull());
    Assert.assertEquals(3, output.getLong(0));
    Assert.assertTrue(output.isNull(1));
    Assert.assertEquals(3, output.getLong(2));
    Assert.assertEquals(4, output.getLong(3));
  }
}
