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

/** Binds a chunk's page identity to the absolute position of its on-disk marker. */
public final class ChunkCryptoContext {
  /** Format bound for opaque proofs; the immutable provider profile defines their exact size. */
  public static final int MAX_AUTHENTICATION_SIZE = 1024;

  private static final int ASSOCIATED_DATA_VERSION = 1;
  private static final byte[] DOMAIN = "TsFile/ChunkPosition".getBytes(TSFileConfig.STRING_CHARSET);
  private final byte[] associatedData;

  private ChunkCryptoContext(byte[] fileCryptoId, long chunkOffset, long chunkOrdinal) {
    associatedData =
        ByteBuffer.allocate(
                Integer.BYTES * 2 + DOMAIN.length + fileCryptoId.length + Long.BYTES * 2)
            .putInt(DOMAIN.length)
            .put(DOMAIN)
            .putInt(ASSOCIATED_DATA_VERSION)
            .put(fileCryptoId)
            .putLong(chunkOffset)
            .putLong(chunkOrdinal)
            .array();
  }

  public static ChunkCryptoContext forChunk(
      EncryptParameter parameter, long chunkOffset, long chunkOrdinal) {
    if (parameter == null || !parameter.isTdePageAead() || chunkOffset < 0 || chunkOrdinal < 0) {
      throw new EncryptException(Messages.get("error.encrypt.chunk_context_invalid"));
    }
    byte[] fileId = parameter.getFileCryptoId();
    if (fileId == null || fileId.length != EncryptParameter.FILE_CRYPTO_ID_LENGTH) {
      throw new EncryptException(Messages.get("error.encrypt.chunk_context_invalid"));
    }
    return new ChunkCryptoContext(fileId, chunkOffset, chunkOrdinal);
  }

  public byte[] getAssociatedData() {
    return Arrays.copyOf(associatedData, associatedData.length);
  }
}
