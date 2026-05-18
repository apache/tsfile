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
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

public abstract class Decoder {

  private static final String ERROR_MSG_KEY = "error.encoding.decoder_not_found";

  private TSEncoding type;

  public Decoder(TSEncoding type) {
    this.type = type;
  }

  public void setType(TSEncoding type) {
    this.type = type;
  }

  public TSEncoding getType() {
    return type;
  }

  public static Decoder getDecoderByType(TSEncoding encoding, TSDataType dataType) {
    switch (encoding) {
      case PLAIN:
        return new PlainDecoder();
      case RLE:
        switch (dataType) {
          case BOOLEAN:
          case INT32:
          case DATE:
            return new IntRleDecoder();
          case INT64:
          case VECTOR:
          case TIMESTAMP:
            return new LongRleDecoder();
          case FLOAT:
          case DOUBLE:
            return new FloatDecoder(TSEncoding.valueOf(encoding.toString()), dataType);
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      case TS_2DIFF:
        switch (dataType) {
          case INT32:
          case DATE:
            return new DeltaBinaryDecoder.IntDeltaDecoder();
          case INT64:
          case VECTOR:
          case TIMESTAMP:
            return new DeltaBinaryDecoder.LongDeltaDecoder();
          case FLOAT:
          case DOUBLE:
            return new FloatDecoder(TSEncoding.valueOf(encoding.toString()), dataType);
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      case GORILLA_V1:
        switch (dataType) {
          case FLOAT:
            return new SinglePrecisionDecoderV1();
          case DOUBLE:
            return new DoublePrecisionDecoderV1();
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      case REGULAR:
        switch (dataType) {
          case INT32:
          case DATE:
            return new RegularDataDecoder.IntRegularDecoder();
          case INT64:
          case VECTOR:
          case TIMESTAMP:
            return new RegularDataDecoder.LongRegularDecoder();
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      case GORILLA:
        switch (dataType) {
          case FLOAT:
            return new SinglePrecisionDecoderV2();
          case DOUBLE:
            return new DoublePrecisionDecoderV2();
          case INT32:
          case DATE:
            return new IntGorillaDecoder();
          case INT64:
          case VECTOR:
          case TIMESTAMP:
            return new LongGorillaDecoder();
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      case DICTIONARY:
        return new DictionaryDecoder();
      case ZIGZAG:
        switch (dataType) {
          case INT32:
          case DATE:
            return new IntZigzagDecoder();
          case INT64:
          case TIMESTAMP:
            return new LongZigzagDecoder();
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      case CHIMP:
        switch (dataType) {
          case FLOAT:
            return new SinglePrecisionChimpDecoder();
          case DOUBLE:
            return new DoublePrecisionChimpDecoder();
          case INT32:
          case DATE:
            return new IntChimpDecoder();
          case INT64:
          case VECTOR:
          case TIMESTAMP:
            return new LongChimpDecoder();
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      case SPRINTZ:
        switch (dataType) {
          case INT32:
          case DATE:
            return new IntSprintzDecoder();
          case INT64:
          case TIMESTAMP:
            return new LongSprintzDecoder();
          case FLOAT:
            return new FloatSprintzDecoder();
          case DOUBLE:
            return new DoubleSprintzDecoder();
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      case RLBE:
        switch (dataType) {
          case INT32:
          case DATE:
            return new IntRLBEDecoder();
          case INT64:
          case TIMESTAMP:
            return new LongRLBEDecoder();
          case FLOAT:
            return new FloatRLBEDecoder();
          case DOUBLE:
            return new DoubleRLBEDecoder();
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      case CAMEL:
        switch (dataType) {
          case DOUBLE:
            return new CamelDecoder();
          default:
            throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
        }
      default:
        throw new TsFileDecodingException(Messages.format(ERROR_MSG_KEY, encoding, dataType));
    }
  }

  public int readInt(ByteBuffer buffer) {
    throw new TsFileDecodingException(
        Messages.format("error.encoding.decoder_method_not_supported", "readInt"));
  }

  public boolean readBoolean(ByteBuffer buffer) {
    throw new TsFileDecodingException(
        Messages.format("error.encoding.decoder_method_not_supported", "readBoolean"));
  }

  public short readShort(ByteBuffer buffer) {
    throw new TsFileDecodingException(
        Messages.format("error.encoding.decoder_method_not_supported", "readShort"));
  }

  public long readLong(ByteBuffer buffer) {
    throw new TsFileDecodingException(
        Messages.format("error.encoding.decoder_method_not_supported", "readLong"));
  }

  public float readFloat(ByteBuffer buffer) {
    throw new TsFileDecodingException(
        Messages.format("error.encoding.decoder_method_not_supported", "readFloat"));
  }

  public double readDouble(ByteBuffer buffer) {
    throw new TsFileDecodingException(
        Messages.format("error.encoding.decoder_method_not_supported", "readDouble"));
  }

  public Binary readBinary(ByteBuffer buffer) {
    throw new TsFileDecodingException(
        Messages.format("error.encoding.decoder_method_not_supported", "readBinary"));
  }

  public BigDecimal readBigDecimal(ByteBuffer buffer) {
    throw new TsFileDecodingException(
        Messages.format("error.encoding.decoder_method_not_supported", "readBigDecimal"));
  }

  public abstract boolean hasNext(ByteBuffer buffer) throws IOException;

  public abstract void reset();
}
