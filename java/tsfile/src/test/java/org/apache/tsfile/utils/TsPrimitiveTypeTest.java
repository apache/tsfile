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
package org.apache.tsfile.utils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.type.RowType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.tsfile.utils.TsPrimitiveType.TsLong;
import org.apache.tsfile.utils.TsPrimitiveType.TsVector;

import org.junit.Assert;
import org.junit.Test;

public class TsPrimitiveTypeTest {

  @Test
  public void testNewAndGet() {
    TsPrimitiveType intValue = Type.fromTsDataType(TSDataType.INT32).getTsPrimitiveType(123);
    Assert.assertEquals(new TsInt(123), intValue);
    Assert.assertEquals(123, intValue.getInt());

    TsPrimitiveType longValue = Type.fromTsDataType(TSDataType.INT64).getTsPrimitiveType(456L);
    Assert.assertEquals(new TsLong(456), longValue);
    Assert.assertEquals(456L, longValue.getLong());

    TsPrimitiveType floatValue = Type.fromTsDataType(TSDataType.FLOAT).getTsPrimitiveType(123f);
    Assert.assertEquals(new TsFloat(123), floatValue);
    Assert.assertEquals(123f, floatValue.getFloat(), 0.01);

    TsPrimitiveType doubleValue = Type.fromTsDataType(TSDataType.DOUBLE).getTsPrimitiveType(456d);
    Assert.assertEquals(new TsDouble(456), doubleValue);
    Assert.assertEquals(456d, doubleValue.getDouble(), 0.01);

    TsPrimitiveType textValue =
        Type.fromTsDataType(TSDataType.TEXT)
            .getTsPrimitiveType(new Binary("123", TSFileConfig.STRING_CHARSET));
    Assert.assertEquals(new TsBinary(new Binary("123", TSFileConfig.STRING_CHARSET)), textValue);
    Assert.assertEquals(new Binary("123", TSFileConfig.STRING_CHARSET), textValue.getBinary());

    TsPrimitiveType booleanValue = Type.fromTsDataType(TSDataType.BOOLEAN).getTsPrimitiveType(true);
    Assert.assertEquals(new TsBoolean(true), booleanValue);
    Assert.assertTrue(booleanValue.getBoolean());
  }

  @Test
  public void testTypeSpecificCreation() {
    TsPrimitiveType emptyDate = Type.fromTsDataType(TSDataType.DATE).getTsPrimitiveType();
    Assert.assertEquals(TSDataType.DATE, emptyDate.getDataType());

    TsPrimitiveType date = Type.fromTsDataType(TSDataType.DATE).getTsPrimitiveType(20260713);
    Assert.assertEquals(20260713, date.getInt());
    Assert.assertEquals(TSDataType.DATE, date.getDataType());

    TsPrimitiveType[] values = {date, null};
    TsPrimitiveType vector = RowType.anonymousRow().getTsPrimitiveType(values);
    Assert.assertSame(values, vector.getVector());
    Assert.assertNull(RowType.anonymousRow().getTsPrimitiveType().getVector());
  }

  @Test
  public void testCompareWithNullValue() {
    TimeValuePair timeValuePair1 =
        new TimeValuePair(
            1,
            new TsPrimitiveType.TsVector(
                new TsPrimitiveType[] {new TsBoolean(true), null, null, new TsInt(1)}));
    TimeValuePair timeValuePair2 =
        new TimeValuePair(
            1,
            new TsPrimitiveType.TsVector(
                new TsPrimitiveType[] {new TsBoolean(true), null, new TsInt(1), null}));
    Assert.assertFalse(timeValuePair1.equals(timeValuePair2));
  }

  @Test
  public void testCopy() {
    TsBoolean booleanValue = new TsBoolean(true);
    TsBoolean booleanCopy = new TsBoolean();
    booleanCopy.copy(booleanValue);
    booleanValue.setBoolean(false);
    Assert.assertTrue(booleanCopy.getBoolean());

    TsInt dateValue = new TsInt(20260722, TSDataType.DATE);
    TsInt dateCopy = new TsInt();
    dateCopy.copy(dateValue);
    dateValue.setInt(20260723);
    Assert.assertEquals(20260722, dateCopy.getInt());
    Assert.assertEquals(TSDataType.DATE, dateCopy.getDataType());

    TsLong longValue = new TsLong(1L);
    TsLong longCopy = new TsLong();
    longCopy.copy(longValue);
    longValue.setLong(2L);
    Assert.assertEquals(1L, longCopy.getLong());

    TsFloat floatValue = new TsFloat(1.0F);
    TsFloat floatCopy = new TsFloat();
    floatCopy.copy(floatValue);
    floatValue.setFloat(2.0F);
    Assert.assertEquals(1.0F, floatCopy.getFloat(), 0.0F);

    TsDouble doubleValue = new TsDouble(1.0D);
    TsDouble doubleCopy = new TsDouble();
    doubleCopy.copy(doubleValue);
    doubleValue.setDouble(2.0D);
    Assert.assertEquals(1.0D, doubleCopy.getDouble(), 0.0D);

    Binary binary = new Binary("original", TSFileConfig.STRING_CHARSET);
    TsBinary binaryValue = new TsBinary(binary);
    TsBinary binaryCopy = new TsBinary();
    binaryCopy.copy(binaryValue);
    binary.getValues()[0] = 'O';
    Assert.assertEquals(
        "original", binaryCopy.getBinary().getStringValue(TSFileConfig.STRING_CHARSET));

    TsVector vectorValue =
        new TsVector(new TsPrimitiveType[] {dateCopy, binaryCopy, null, new TsBoolean(true)});
    TsVector vectorCopy = new TsVector();
    vectorCopy.copy(vectorValue);
    vectorValue.getVector()[0].setInt(0);
    vectorValue.getVector()[1].getBinary().getValues()[0] = 'O';
    vectorValue.getVector()[3].setBoolean(false);

    Assert.assertNotSame(vectorValue.getVector(), vectorCopy.getVector());
    Assert.assertEquals(20260722, vectorCopy.getVector()[0].getInt());
    Assert.assertEquals(TSDataType.DATE, vectorCopy.getVector()[0].getDataType());
    Assert.assertEquals(
        "original",
        vectorCopy.getVector()[1].getBinary().getStringValue(TSFileConfig.STRING_CHARSET));
    Assert.assertNull(vectorCopy.getVector()[2]);
    Assert.assertTrue(vectorCopy.getVector()[3].getBoolean());

    booleanCopy.copy(null);
    Assert.assertFalse(booleanCopy.getBoolean());
  }
}
