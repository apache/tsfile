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

import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

public abstract class DecoderWrapper extends Decoder {

  protected final Decoder originDecoder;

  public DecoderWrapper(Decoder originDecoder) {
    super(originDecoder.getType());
    this.originDecoder = originDecoder;
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    return originDecoder.hasNext(buffer);
  }

  @Override
  public void reset() {
    originDecoder.reset();
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    return originDecoder.readInt(buffer);
  }

  @Override
  public boolean readBoolean(ByteBuffer buffer) {
    return originDecoder.readBoolean(buffer);
  }

  @Override
  public short readShort(ByteBuffer buffer) {
    return originDecoder.readShort(buffer);
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    return originDecoder.readLong(buffer);
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    return originDecoder.readFloat(buffer);
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    return originDecoder.readDouble(buffer);
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    return originDecoder.readBinary(buffer);
  }

  @Override
  public BigDecimal readBigDecimal(ByteBuffer buffer) {
    return originDecoder.readBigDecimal(buffer);
  }
}
