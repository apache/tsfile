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

package org.apache.tsfile.read.reader.page;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.encrypt.PageCryptoContext;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.i18n.Messages;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LazyLoadPageData {
  /** Reference to the data of original chunkDataBuffer. * */
  private final byte[] chunkData;

  private final int pageDataOffset;

  private final IUnCompressor unCompressor;

  private final EncryptParameter encryptParam;

  private final int pageIndex;

  public LazyLoadPageData(byte[] data, int offset, IUnCompressor unCompressor) {
    this.chunkData = data;
    this.pageDataOffset = offset;
    this.unCompressor = unCompressor;
    this.encryptParam = EncryptUtils.getEncryptParameter();
    this.pageIndex = -1;
  }

  public LazyLoadPageData(
      byte[] data, int offset, IUnCompressor unCompressor, EncryptParameter encryptParam) {
    this.chunkData = data;
    this.pageDataOffset = offset;
    this.unCompressor = unCompressor;
    this.encryptParam = encryptParam;
    this.pageIndex = -1;
  }

  public LazyLoadPageData(
      byte[] data,
      int offset,
      IUnCompressor unCompressor,
      EncryptParameter encryptParam,
      int pageIndex) {
    this.chunkData = data;
    this.pageDataOffset = offset;
    this.unCompressor = unCompressor;
    this.encryptParam = encryptParam;
    this.pageIndex = pageIndex;
  }

  public ByteBuffer uncompressPageData(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    byte[] decryptedPageData;
    if (encryptParam != null && encryptParam.isTdePageAead()) {
      PageCryptoContext pageCryptoContext =
          PageCryptoContext.forDecryption(
              encryptParam, pageHeader.getUncompressedSize(), compressedPageBodyLength, pageIndex);
      decryptedPageData =
          decryptor.decryptPage(
              chunkData, pageDataOffset, compressedPageBodyLength, pageCryptoContext);
      if (decryptedPageData.length != pageCryptoContext.getCompressedPlaintextSize()) {
        throw new IOException(
            Messages.format(
                "error.encrypt.page_plaintext_size_mismatch",
                pageCryptoContext.getCompressedPlaintextSize(),
                decryptedPageData.length));
      }
    } else if (decryptor.getEncryptionType() == EncryptionType.UNENCRYPTED) {
      decryptedPageData =
          java.util.Arrays.copyOfRange(
              chunkData, pageDataOffset, pageDataOffset + compressedPageBodyLength);
    } else {
      decryptedPageData = decryptor.decrypt(chunkData, pageDataOffset, compressedPageBodyLength);
    }
    try {
      unCompressor.uncompress(
          decryptedPageData, 0, decryptedPageData.length, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          Messages.format(
              "error.read.uncompress_error_with_header",
              pageHeader.getUncompressedSize(),
              pageHeader.getCompressedSize(),
              pageHeader,
              e.getMessage()));
    }
    return ByteBuffer.wrap(uncompressedPageData);
  }

  public IUnCompressor getUnCompressor() {
    return unCompressor;
  }
}
