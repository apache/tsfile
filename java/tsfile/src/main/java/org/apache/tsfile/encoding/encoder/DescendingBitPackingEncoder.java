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

package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DescendingBitPackingEncoder extends Encoder {
  private boolean isSigned;
  private List<Long> buffer = new ArrayList<>();

  private int getValueWidth(long value) {
    return 64 - Long.numberOfLeadingZeros(value);
  }

  private static long zigzagEncode(long value) {
    return (value << 1) ^ (value >> 63 & 1);
  }

  public DescendingBitPackingEncoder(boolean isSigned) {
    super(TSEncoding.DESCENDING_BIT_PACKING);
    this.isSigned = isSigned;
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    if (isSigned) {
      value = zigzagEncode(value);
    }
    buffer.add(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    // TODO Auto-generated method stub
    throw new TsFileEncodingException("Not implemented yet");
  }

  public static class IntDescendingBitPackingEncoder extends DescendingBitPackingEncoder {
    public IntDescendingBitPackingEncoder(boolean isSigned) {
      super(isSigned);
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
      // TODO Auto-generated method stub
      super.encode(Long.valueOf(value), out);
    }
  }
}
