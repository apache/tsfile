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

import org.apache.tsfile.encoding.encoder.DeltaBinaryEncoder;
import org.apache.tsfile.encoding.encoder.DoublePrecisionChimpEncoder;
import org.apache.tsfile.encoding.encoder.DoublePrecisionEncoderV1;
import org.apache.tsfile.encoding.encoder.DoublePrecisionEncoderV2;
import org.apache.tsfile.encoding.encoder.DoubleRLBE;
import org.apache.tsfile.encoding.encoder.DoubleSprintzEncoder;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.FloatEncoder;
import org.apache.tsfile.encoding.encoder.FloatRLBE;
import org.apache.tsfile.encoding.encoder.FloatSprintzEncoder;
import org.apache.tsfile.encoding.encoder.IntChimpEncoder;
import org.apache.tsfile.encoding.encoder.IntGorillaEncoder;
import org.apache.tsfile.encoding.encoder.IntRLBE;
import org.apache.tsfile.encoding.encoder.IntRleEncoder;
import org.apache.tsfile.encoding.encoder.IntSprintzEncoder;
import org.apache.tsfile.encoding.encoder.IntZigzagEncoder;
import org.apache.tsfile.encoding.encoder.LongChimpEncoder;
import org.apache.tsfile.encoding.encoder.LongGorillaEncoder;
import org.apache.tsfile.encoding.encoder.LongRLBE;
import org.apache.tsfile.encoding.encoder.LongRleEncoder;
import org.apache.tsfile.encoding.encoder.LongSprintzEncoder;
import org.apache.tsfile.encoding.encoder.LongZigzagEncoder;
import org.apache.tsfile.encoding.encoder.RegularDataEncoder;
import org.apache.tsfile.encoding.encoder.SinglePrecisionChimpEncoder;
import org.apache.tsfile.encoding.encoder.SinglePrecisionEncoderV1;
import org.apache.tsfile.encoding.encoder.SinglePrecisionEncoderV2;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public final class EncodingTypeServices {

  private static final String ERROR_MSG_KEY = "error.encoding.ts_encoding_builder_unsupported_type";

  public static final TypeService<EncoderBuilder> RLE_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE, BOOLEAN -> maxPointNumber -> new IntRleEncoder();
            case INT64, TIMESTAMP -> maxPointNumber -> new LongRleEncoder();
            case FLOAT ->
                maxPointNumber ->
                    new FloatEncoder(TSEncoding.RLE, TSDataType.FLOAT, maxPointNumber);
            case DOUBLE ->
                maxPointNumber ->
                    new FloatEncoder(TSEncoding.RLE, TSDataType.DOUBLE, maxPointNumber);
            case TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                maxPointNumber -> {
                  throw unsupported(TSEncoding.RLE, type.getTypeEnum());
                };
          };

  public static final TypeService<EncoderBuilder> TS_2DIFF_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> maxPointNumber -> new DeltaBinaryEncoder.IntDeltaEncoder();
            case INT64, TIMESTAMP -> maxPointNumber -> new DeltaBinaryEncoder.LongDeltaEncoder();
            case FLOAT ->
                maxPointNumber ->
                    new FloatEncoder(TSEncoding.TS_2DIFF, TSDataType.FLOAT, maxPointNumber);
            case DOUBLE ->
                maxPointNumber ->
                    new FloatEncoder(TSEncoding.TS_2DIFF, TSDataType.DOUBLE, maxPointNumber);
            case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                maxPointNumber -> {
                  throw unsupported(TSEncoding.TS_2DIFF, type.getTypeEnum());
                };
          };

  public static final TypeService<Encoder> GORILLA_V1_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case FLOAT -> new SinglePrecisionEncoderV1();
            case DOUBLE -> new DoublePrecisionEncoderV1();
            case BOOLEAN,
                INT32,
                DATE,
                INT64,
                TIMESTAMP,
                TEXT,
                BLOB,
                STRING,
                OBJECT,
                ROW,
                UNKNOWN,
                VECTOR ->
                throw unsupported(TSEncoding.GORILLA_V1, type.getTypeEnum());
          };

  public static final TypeService<Encoder> REGULAR_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> new RegularDataEncoder.IntRegularEncoder();
            case INT64, TIMESTAMP -> new RegularDataEncoder.LongRegularEncoder();
            case BOOLEAN, FLOAT, DOUBLE, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                throw unsupported(TSEncoding.REGULAR, type.getTypeEnum());
          };

  public static final TypeService<Encoder> GORILLA_V2_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case FLOAT -> new SinglePrecisionEncoderV2();
            case DOUBLE -> new DoublePrecisionEncoderV2();
            case INT32, DATE -> new IntGorillaEncoder();
            case INT64, TIMESTAMP -> new LongGorillaEncoder();
            case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                throw unsupported(TSEncoding.GORILLA, type.getTypeEnum());
          };

  public static final TypeService<Encoder> SPRINTZ_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> new IntSprintzEncoder();
            case INT64, TIMESTAMP -> new LongSprintzEncoder();
            case FLOAT -> new FloatSprintzEncoder();
            case DOUBLE -> new DoubleSprintzEncoder();
            case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                throw unsupported(TSEncoding.SPRINTZ, type.getTypeEnum());
          };

  public static final TypeService<Encoder> RLBE_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> new IntRLBE();
            case INT64, TIMESTAMP -> new LongRLBE();
            case FLOAT -> new FloatRLBE();
            case DOUBLE -> new DoubleRLBE();
            case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                throw unsupported(TSEncoding.RLBE, type.getTypeEnum());
          };

  public static final TypeService<Encoder> ZIGZAG_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case INT32, DATE -> new IntZigzagEncoder();
            case INT64, TIMESTAMP -> new LongZigzagEncoder();
            case BOOLEAN, FLOAT, DOUBLE, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                throw unsupported(TSEncoding.ZIGZAG, type.getTypeEnum());
          };

  public static final TypeService<Encoder> CHIMP_ENCODER_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case FLOAT -> new SinglePrecisionChimpEncoder();
            case DOUBLE -> new DoublePrecisionChimpEncoder();
            case INT32, DATE -> new IntChimpEncoder();
            case INT64, TIMESTAMP -> new LongChimpEncoder();
            case BOOLEAN, TEXT, BLOB, STRING, OBJECT, ROW, UNKNOWN, VECTOR ->
                throw unsupported(TSEncoding.CHIMP, type.getTypeEnum());
          };

  static {
    RLE_ENCODER_SERVICE.check();
    TS_2DIFF_ENCODER_SERVICE.check();
    GORILLA_V1_ENCODER_SERVICE.check();
    REGULAR_ENCODER_SERVICE.check();
    GORILLA_V2_ENCODER_SERVICE.check();
    SPRINTZ_ENCODER_SERVICE.check();
    RLBE_ENCODER_SERVICE.check();
    ZIGZAG_ENCODER_SERVICE.check();
    CHIMP_ENCODER_SERVICE.check();
  }

  private EncodingTypeServices() {}

  private static UnSupportedDataTypeException unsupported(TSEncoding encoding, Object type) {
    return new UnSupportedDataTypeException(Messages.format(ERROR_MSG_KEY, encoding, type))
        .setChecked(true);
  }

  @FunctionalInterface
  public interface EncoderBuilder {

    Encoder build(int maxPointNumber);
  }
}
