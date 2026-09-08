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
package org.apache.tsfile.file.header;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Versioned file encryption metadata stored before the first TsFile data marker.
 *
 * <p>The header contains only public identifiers and a wrapped data key. Plaintext key material and
 * runtime implementation class names are never serialized.
 */
public final class FileEncryptionHeader {

  public static final byte VERSION = 1;
  public static final int MAX_HEADER_SIZE = 1024 * 1024;

  private FileEncryptionHeader() {}

  public static int serialize(EncryptParameter parameter, OutputStream outputStream)
      throws IOException {
    validate(parameter);

    PublicBAOS payload = new PublicBAOS();
    int payloadSize = 0;
    payloadSize += ReadWriteIOUtils.write(VERSION, payload);
    payloadSize += ReadWriteIOUtils.writeVar(parameter.getProviderId(), payload);
    payloadSize += ReadWriteIOUtils.writeVar(parameter.getProfileId(), payload);
    payloadSize += ReadWriteIOUtils.writeVar(parameter.getKeyId(), payload);
    payloadSize += ReadWriteIOUtils.writeVar(parameter.getKeyVersion(), payload);
    payloadSize += writeBytes(parameter.getFileCryptoId(), payload);
    payloadSize += writeBytes(parameter.getWrappedDataKey(), payload);

    if (payloadSize != payload.size() || payloadSize > MAX_HEADER_SIZE) {
      throw new EncryptException(
          Messages.format("error.file.encryption_header_invalid_size", payloadSize));
    }

    int size = ReadWriteIOUtils.write(MetaMarker.ENCRYPTION_HEADER, outputStream);
    size += ReadWriteForEncodingUtils.writeUnsignedVarInt(payloadSize, outputStream);
    payload.writeTo(outputStream);
    return size + payloadSize;
  }

  /** Deserializes a header after its {@link MetaMarker#ENCRYPTION_HEADER} marker was consumed. */
  public static EncryptParameter deserialize(InputStream inputStream) throws IOException {
    int payloadSize = ReadWriteForEncodingUtils.readUnsignedVarInt(inputStream);
    if (payloadSize <= 0 || payloadSize > MAX_HEADER_SIZE) {
      throw new EncryptException(
          Messages.format("error.file.encryption_header_invalid_size", payloadSize));
    }

    byte[] payload = readExactly(inputStream, payloadSize);
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    byte version = buffer.get();
    if (version != VERSION) {
      throw new EncryptException(
          Messages.format("error.file.encryption_header_unsupported_version", version));
    }

    EncryptParameter parameter;
    try {
      parameter =
          EncryptParameter.pageAeadBuilder()
              .providerId(ReadWriteIOUtils.readVarIntString(buffer))
              .profileId(ReadWriteIOUtils.readVarIntString(buffer))
              .keyId(ReadWriteIOUtils.readVarIntString(buffer))
              .keyVersion(ReadWriteIOUtils.readVarIntString(buffer))
              .fileCryptoId(readBytes(buffer))
              .wrappedDataKey(readBytes(buffer))
              .build();
    } catch (RuntimeException e) {
      throw new EncryptException(Messages.get("error.file.encryption_header_malformed"), e);
    }
    if (buffer.hasRemaining()) {
      throw new EncryptException(Messages.get("error.file.encryption_header_trailing_bytes"));
    }
    validate(parameter);
    return parameter;
  }

  private static int writeBytes(byte[] bytes, OutputStream outputStream) throws IOException {
    int size = ReadWriteForEncodingUtils.writeUnsignedVarInt(bytes.length, outputStream);
    outputStream.write(bytes);
    return size + bytes.length;
  }

  private static byte[] readBytes(ByteBuffer buffer) {
    int length = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    if (length < 0 || length > MAX_HEADER_SIZE || length > buffer.remaining()) {
      throw new EncryptException(
          Messages.format("error.file.encryption_header_invalid_component_size", length));
    }
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return bytes;
  }

  private static byte[] readExactly(InputStream inputStream, int size) throws IOException {
    byte[] bytes = new byte[size];
    int offset = 0;
    while (offset < size) {
      int read = inputStream.read(bytes, offset, size - offset);
      if (read < 0) {
        throw new EncryptException(
            Messages.format("error.file.encryption_header_truncated", size, offset));
      }
      if (read == 0) {
        int value = inputStream.read();
        if (value < 0) {
          throw new EncryptException(
              Messages.format("error.file.encryption_header_truncated", size, offset));
        }
        bytes[offset++] = (byte) value;
      } else {
        offset += read;
      }
    }
    return bytes;
  }

  private static void validate(EncryptParameter parameter) {
    if (parameter == null || !parameter.isTdePageAead()) {
      invalidField("pageAead");
    }
    requireText(parameter.getProviderId(), "providerId");
    requireText(parameter.getProfileId(), "profileId");
    requireText(parameter.getKeyId(), "keyId");
    requireText(parameter.getKeyVersion(), "keyVersion");

    byte[] fileCryptoId = parameter.getFileCryptoId();
    if (fileCryptoId == null || fileCryptoId.length != EncryptParameter.FILE_CRYPTO_ID_LENGTH) {
      invalidField("fileCryptoId");
    }
    byte[] wrappedDataKey = parameter.getWrappedDataKey();
    if (wrappedDataKey == null || wrappedDataKey.length == 0) {
      invalidField("wrappedDataKey");
    }
  }

  private static void requireText(String value, String field) {
    if (value == null || value.trim().isEmpty()) {
      invalidField(field);
    }
  }

  private static void invalidField(String field) {
    throw new EncryptException(
        Messages.format("error.file.encryption_header_invalid_field", field));
  }
}
