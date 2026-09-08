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
package org.apache.tsfile.encrypt;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.i18n.Messages;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Canonical context for page-level authenticated encryption.
 *
 * <p>Encryption and decryption construct the same associated data from persisted file encryption
 * metadata and stable page fields. Providers should pass {@link #getAssociatedData()} to their AEAD
 * implementation unchanged.
 */
public final class PageCryptoContext {

  private static final int ASSOCIATED_DATA_VERSION = 1;

  private final int uncompressedSize;
  private final int compressedPlaintextSize;
  private final int encryptedPageBodySize;
  private final int pageIndex;
  private final byte[] associatedData;

  private PageCryptoContext(
      EncryptParameter parameter,
      int uncompressedSize,
      int compressedPlaintextSize,
      int encryptedPageBodySize,
      int pageIndex) {
    validate(parameter, uncompressedSize, compressedPlaintextSize, pageIndex);
    this.uncompressedSize = uncompressedSize;
    this.compressedPlaintextSize = compressedPlaintextSize;
    this.encryptedPageBodySize = encryptedPageBodySize;
    this.pageIndex = pageIndex;
    this.associatedData =
        buildAssociatedData(parameter, uncompressedSize, compressedPlaintextSize, pageIndex);
  }

  public static PageCryptoContext forEncryption(
      EncryptParameter parameter,
      int uncompressedSize,
      int compressedPlaintextSize,
      int pageIndex) {
    validate(parameter, uncompressedSize, compressedPlaintextSize, pageIndex);
    int encryptedPageBodySize =
        Math.addExact(compressedPlaintextSize, parameter.getPageBodyOverhead());
    return new PageCryptoContext(
        parameter, uncompressedSize, compressedPlaintextSize, encryptedPageBodySize, pageIndex);
  }

  public static PageCryptoContext forDecryption(
      EncryptParameter parameter, int uncompressedSize, int encryptedPageBodySize, int pageIndex) {
    validate(parameter, uncompressedSize, 0, pageIndex);
    int compressedPlaintextSize = encryptedPageBodySize - parameter.getPageBodyOverhead();
    if (compressedPlaintextSize < 0) {
      throw new EncryptException(
          Messages.format(
              "error.encrypt.page_body_too_short",
              encryptedPageBodySize,
              parameter.getPageBodyOverhead()));
    }
    return new PageCryptoContext(
        parameter, uncompressedSize, compressedPlaintextSize, encryptedPageBodySize, pageIndex);
  }

  public int getUncompressedSize() {
    return uncompressedSize;
  }

  /** Returns the compressed plaintext size on both encryption and decryption paths. */
  public int getInputSize() {
    return compressedPlaintextSize;
  }

  public int getCompressedPlaintextSize() {
    return compressedPlaintextSize;
  }

  public int getEncryptedPageBodySize() {
    return encryptedPageBodySize;
  }

  public int getPageIndex() {
    return pageIndex;
  }

  public boolean isFirstPage() {
    return pageIndex == 0;
  }

  public byte[] getAssociatedData() {
    return Arrays.copyOf(associatedData, associatedData.length);
  }

  private static void validate(
      EncryptParameter parameter,
      int uncompressedSize,
      int compressedPlaintextSize,
      int pageIndex) {
    if (parameter == null || !parameter.isTdePageAead()) {
      throw new EncryptException(Messages.get("error.encrypt.page_context_invalid_parameter"));
    }
    if (parameter.getFileCryptoId() == null
        || parameter.getFileCryptoId().length != EncryptParameter.FILE_CRYPTO_ID_LENGTH) {
      throw new EncryptException(Messages.get("error.encrypt.page_context_invalid_file_id"));
    }
    if (uncompressedSize < 0 || compressedPlaintextSize < 0 || pageIndex < 0) {
      throw new EncryptException(
          Messages.format(
              "error.encrypt.page_context_invalid_sizes",
              uncompressedSize,
              compressedPlaintextSize,
              pageIndex));
    }
  }

  private static byte[] buildAssociatedData(
      EncryptParameter parameter,
      int uncompressedSize,
      int compressedPlaintextSize,
      int pageIndex) {
    byte[][] components =
        new byte[][] {
          bytes(parameter.getProviderId()),
          bytes(parameter.getProfileId()),
          bytes(parameter.getKeyId()),
          bytes(parameter.getKeyVersion()),
          parameter.getFileCryptoId()
        };
    int size = Integer.BYTES * (4 + components.length);
    for (byte[] component : components) {
      size = Math.addExact(size, component.length);
    }

    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.putInt(ASSOCIATED_DATA_VERSION);
    for (byte[] component : components) {
      buffer.putInt(component.length);
      buffer.put(component);
    }
    buffer.putInt(pageIndex);
    buffer.putInt(uncompressedSize);
    buffer.putInt(compressedPlaintextSize);
    return buffer.array();
  }

  private static byte[] bytes(String value) {
    return value == null ? new byte[0] : value.getBytes(TSFileConfig.STRING_CHARSET);
  }
}
