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

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.header.PageHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

public class LazyLoadPageData {
  /** Reference to the data of original chunkDataBuffer. * */
  private final byte[] chunkData;

  private final int pageDataOffset;

  private final IUnCompressor unCompressor;

  private final IDecryptor decryptor;

  public LazyLoadPageData(byte[] data, int offset, IUnCompressor unCompressor) {
    this.chunkData = data;
    this.pageDataOffset = offset;
    this.unCompressor = unCompressor;
    if (TSFileDescriptor.getInstance().getConfig().getEncryptFlag()) {
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update("IoTDB is the best".getBytes());
        md.update(TSFileDescriptor.getInstance().getConfig().getEncryptKey().getBytes());
        byte[] tem = md.digest();
        this.decryptor =
            IDecryptor.getDecryptor(
                TSFileDescriptor.getInstance().getConfig().getEncryptType(), tem);
      } catch (Exception e) {
        throw new EncryptException("md5 function not found while use md5 to generate data key");
      }
    } else {
      this.decryptor = IDecryptor.getDecryptor("UNENCRYPTED", null);
    }
  }

  public LazyLoadPageData(
      byte[] data, int offset, IUnCompressor unCompressor, IDecryptor decryptor) {
    this.chunkData = data;
    this.pageDataOffset = offset;
    this.unCompressor = unCompressor;
    this.decryptor = decryptor;
  }

  public ByteBuffer uncompressPageData(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    try {
      byte[] decryptedPageData =
          decryptor.decrypt(chunkData, pageDataOffset, compressedPageBodyLength);
      unCompressor.uncompress(
          decryptedPageData, 0, compressedPageBodyLength, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage());
    }
    return ByteBuffer.wrap(uncompressedPageData);
  }

  public IUnCompressor getUnCompressor() {
    return unCompressor;
  }
}
