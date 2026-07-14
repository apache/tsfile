/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

package org.apache.tsfile.read.common.type;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encoding.decoder.DictionaryDecoder;
import org.apache.tsfile.encoding.decoder.PlainDecoder;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class AbstractType implements Type {

  private static final String DECODER_NOT_FOUND_ERROR_KEY = "error.encoding.decoder_not_found";

  @Override
  public Decoder getDecoder(TSEncoding encoding) {
    return switch (encoding) {
      case PLAIN -> new PlainDecoder();
      case DICTIONARY -> new DictionaryDecoder();
      default -> throw decoderNotFound(encoding);
    };
  }

  protected TsFileDecodingException decoderNotFound(TSEncoding encoding) {
    return new TsFileDecodingException(
        Messages.format(DECODER_NOT_FOUND_ERROR_KEY, encoding, getTypeEnum()));
  }

  protected void binaryToBytes(TsPrimitiveType value, byte[] valueBytes, int offset) {
    Binary binary = value.getBinary();
    BytesUtils.intToBytes(binary.getLength(), valueBytes, offset);
    System.arraycopy(binary.getValues(), 0, valueBytes, offset + Integer.BYTES, binary.getLength());
  }

  protected int binaryTypeSize(TsPrimitiveType value) {
    return Integer.BYTES + value.getBinary().getLength();
  }

  protected int binaryTypeSize(Object value) {
    return Integer.BYTES + ((Binary) value).getLength();
  }

  protected void checkValueType(Object value, Class<?> expectedClass, String expectedType) {
    if (value != null && !expectedClass.isInstance(value)) {
      throw new IllegalArgumentException(
          Messages.format(
              "error.write.tablet_expected_type",
              expectedType,
              getDisplayName(),
              value.getClass().getName()));
    }
  }

  protected int serializedSizeOfBinaryValues(Object column, int rowSize) {
    Binary[] binaryValues = (Binary[]) column;
    int size = 0;
    for (int i = 0; i < rowSize; i++) {
      size = Math.addExact(size, Byte.BYTES);
      if (binaryValues[i] != null) {
        size = Math.addExact(size, ReadWriteIOUtils.sizeToWrite(binaryValues[i]));
      }
    }
    return size;
  }

  protected void serializeBinaryValues(Object array, int rowSize, DataOutputStream stream)
      throws IOException {
    Binary[] binaryValues = (Binary[]) array;
    for (int i = 0; i < rowSize; i++) {
      ReadWriteIOUtils.write(BytesUtils.boolToByte(binaryValues[i] != null), stream);
      if (binaryValues[i] != null) {
        ReadWriteIOUtils.write(binaryValues[i], stream);
      }
    }
  }

  protected Object deserializeBinaryValues(ByteBuffer buffer, int rowSize) {
    Binary[] values = new Binary[rowSize];
    for (int i = 0; i < rowSize; i++) {
      boolean isNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(buffer));
      values[i] = isNotNull ? ReadWriteIOUtils.readBinary(buffer) : Binary.EMPTY_VALUE;
    }
    return values;
  }

  protected boolean hasEnoughLength(Object left, Object right, int rowSize) {
    return Array.getLength(left) >= rowSize && Array.getLength(right) >= rowSize;
  }

  protected boolean binaryArrayEquals(Object left, Object right, int rowSize) {
    if (!hasEnoughLength(left, right, rowSize)) {
      return false;
    }
    Binary[] leftValues = (Binary[]) left;
    Binary[] rightValues = (Binary[]) right;
    for (int i = 0; i < rowSize; i++) {
      if (!Objects.equals(leftValues[i], rightValues[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Object getValue(Object column, int rowIndex) {
    return Array.get(column, rowIndex);
  }

  @Override
  public String toString() {
    return getDisplayName();
  }
}
