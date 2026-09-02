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
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

public abstract class Decoder {

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
    return Type.fromTsDataType(dataType).getDecoder(encoding);
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
