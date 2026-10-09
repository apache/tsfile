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

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import org.junit.Assert;
import org.junit.Test;

public class DecoderTest {

  @Test
  public void testGetDecoderByType() {
    Object[][] testCases = {
      {TSEncoding.PLAIN, TSDataType.UNKNOWN, PlainDecoder.class},
      {TSEncoding.DICTIONARY, TSDataType.BLOB, DictionaryDecoder.class},
      {TSEncoding.RLE, TSDataType.BOOLEAN, IntRleDecoder.class},
      {TSEncoding.RLE, TSDataType.INT32, IntRleDecoder.class},
      {TSEncoding.RLE, TSDataType.DATE, IntRleDecoder.class},
      {TSEncoding.RLE, TSDataType.INT64, LongRleDecoder.class},
      {TSEncoding.RLE, TSDataType.VECTOR, LongRleDecoder.class},
      {TSEncoding.RLE, TSDataType.TIMESTAMP, LongRleDecoder.class},
      {TSEncoding.RLE, TSDataType.FLOAT, FloatDecoder.class},
      {TSEncoding.RLE, TSDataType.DOUBLE, FloatDecoder.class},
      {TSEncoding.TS_2DIFF, TSDataType.INT32, DeltaBinaryDecoder.IntDeltaDecoder.class},
      {TSEncoding.TS_2DIFF, TSDataType.DATE, DeltaBinaryDecoder.IntDeltaDecoder.class},
      {TSEncoding.TS_2DIFF, TSDataType.INT64, DeltaBinaryDecoder.LongDeltaDecoder.class},
      {TSEncoding.TS_2DIFF, TSDataType.VECTOR, DeltaBinaryDecoder.LongDeltaDecoder.class},
      {TSEncoding.TS_2DIFF, TSDataType.TIMESTAMP, DeltaBinaryDecoder.LongDeltaDecoder.class},
      {TSEncoding.TS_2DIFF, TSDataType.FLOAT, FloatDecoder.class},
      {TSEncoding.TS_2DIFF, TSDataType.DOUBLE, FloatDecoder.class},
      {TSEncoding.GORILLA_V1, TSDataType.FLOAT, SinglePrecisionDecoderV1.class},
      {TSEncoding.GORILLA_V1, TSDataType.DOUBLE, DoublePrecisionDecoderV1.class},
      {TSEncoding.REGULAR, TSDataType.INT32, RegularDataDecoder.IntRegularDecoder.class},
      {TSEncoding.REGULAR, TSDataType.VECTOR, RegularDataDecoder.LongRegularDecoder.class},
      {TSEncoding.GORILLA, TSDataType.INT32, IntGorillaDecoder.class},
      {TSEncoding.GORILLA, TSDataType.VECTOR, LongGorillaDecoder.class},
      {TSEncoding.GORILLA, TSDataType.FLOAT, SinglePrecisionDecoderV2.class},
      {TSEncoding.GORILLA, TSDataType.DOUBLE, DoublePrecisionDecoderV2.class},
      {TSEncoding.ZIGZAG, TSDataType.INT32, IntZigzagDecoder.class},
      {TSEncoding.ZIGZAG, TSDataType.INT64, LongZigzagDecoder.class},
      {TSEncoding.CHIMP, TSDataType.INT32, IntChimpDecoder.class},
      {TSEncoding.CHIMP, TSDataType.VECTOR, LongChimpDecoder.class},
      {TSEncoding.CHIMP, TSDataType.FLOAT, SinglePrecisionChimpDecoder.class},
      {TSEncoding.CHIMP, TSDataType.DOUBLE, DoublePrecisionChimpDecoder.class},
      {TSEncoding.SPRINTZ, TSDataType.INT32, IntSprintzDecoder.class},
      {TSEncoding.SPRINTZ, TSDataType.INT64, LongSprintzDecoder.class},
      {TSEncoding.SPRINTZ, TSDataType.FLOAT, FloatSprintzDecoder.class},
      {TSEncoding.SPRINTZ, TSDataType.DOUBLE, DoubleSprintzDecoder.class},
      {TSEncoding.RLBE, TSDataType.INT32, IntRLBEDecoder.class},
      {TSEncoding.RLBE, TSDataType.INT64, LongRLBEDecoder.class},
      {TSEncoding.RLBE, TSDataType.FLOAT, FloatRLBEDecoder.class},
      {TSEncoding.RLBE, TSDataType.DOUBLE, DoubleRLBEDecoder.class},
      {TSEncoding.CAMEL, TSDataType.DOUBLE, CamelDecoder.class}
    };

    for (Object[] testCase : testCases) {
      Assert.assertEquals(
          testCase[2],
          Decoder.getDecoderByType((TSEncoding) testCase[0], (TSDataType) testCase[1]).getClass());
    }
  }

  @Test
  public void testUnsupportedDecoder() {
    Object[][] testCases = {
      {TSEncoding.RLE, TSDataType.TEXT},
      {TSEncoding.TS_2DIFF, TSDataType.BOOLEAN},
      {TSEncoding.GORILLA_V1, TSDataType.INT32},
      {TSEncoding.REGULAR, TSDataType.FLOAT},
      {TSEncoding.GORILLA, TSDataType.BOOLEAN},
      {TSEncoding.ZIGZAG, TSDataType.VECTOR},
      {TSEncoding.CHIMP, TSDataType.TEXT},
      {TSEncoding.SPRINTZ, TSDataType.VECTOR},
      {TSEncoding.RLBE, TSDataType.VECTOR},
      {TSEncoding.CAMEL, TSDataType.FLOAT}
    };

    for (Object[] testCase : testCases) {
      try {
        Decoder.getDecoderByType((TSEncoding) testCase[0], (TSDataType) testCase[1]);
        Assert.fail("Expected TsFileDecodingException");
      } catch (TsFileDecodingException ignored) {
        // Expected.
      }
    }
  }
}
