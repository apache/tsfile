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
package org.apache.tsfile.write.page;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IEncryptor;
import org.apache.tsfile.encrypt.PageCryptoContext;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.i18n.Messages;

import java.io.IOException;
import java.nio.ByteBuffer;

class PageBodyEncoder {

  private PageBodyEncoder() {}

  static EncodedPageBody encode(
      ByteBuffer pageData,
      int uncompressedSize,
      ICompressor compressor,
      EncryptParameter encryptParameter,
      int pageIndex)
      throws IOException {
    byte[] plaintext;
    int plaintextOffset = 0;
    int compressedSize;

    if (compressor.getType().equals(CompressionType.UNCOMPRESSED)) {
      plaintext = pageData.array();
      plaintextOffset = pageData.position();
      compressedSize = uncompressedSize;
    } else if (compressor.getType().equals(CompressionType.GZIP)) {
      plaintext = compressor.compress(pageData.array(), pageData.position(), uncompressedSize);
      compressedSize = plaintext.length;
    } else {
      plaintext = new byte[compressor.getMaxBytesForCompression(uncompressedSize)];
      compressedSize =
          compressor.compress(pageData.array(), pageData.position(), uncompressedSize, plaintext);
    }

    IEncryptor encryptor = IEncryptor.getEncryptor(encryptParameter);
    byte[] encryptedPageBody;
    if (encryptParameter != null && encryptParameter.isTdePageAead()) {
      PageCryptoContext pageCryptoContext =
          PageCryptoContext.forEncryption(
              encryptParameter, uncompressedSize, compressedSize, pageIndex);
      encryptedPageBody =
          encryptor.encryptPage(plaintext, plaintextOffset, compressedSize, pageCryptoContext);
      if (encryptedPageBody.length != pageCryptoContext.getEncryptedPageBodySize()) {
        throw new EncryptException(
            Messages.format(
                "error.encrypt.page_output_size_mismatch",
                pageCryptoContext.getEncryptedPageBodySize(),
                encryptedPageBody.length));
      }
    } else if (encryptor.getEncryptionType() == EncryptionType.UNENCRYPTED) {
      return new EncodedPageBody(plaintext, plaintextOffset, compressedSize);
    } else {
      encryptedPageBody = encryptor.encrypt(plaintext, plaintextOffset, compressedSize);
    }
    return new EncodedPageBody(encryptedPageBody);
  }
}
