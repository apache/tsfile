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

import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.i18n.Messages;

import java.util.concurrent.ConcurrentHashMap;

public interface IEncrypt extends AutoCloseable {

  static ConcurrentHashMap<String, java.lang.reflect.Constructor<?>> encryptMap =
      new ConcurrentHashMap<>();

  static ConcurrentHashMap<String, String> encryptTypeToClassMap = new ConcurrentHashMap<>();

  IDecryptor getDecryptor();

  IEncryptor getEncryptor();

  /**
   * Returns the fixed number of bytes added to every encrypted page body by this profile.
   *
   * <p>Page AEAD providers must override this method. The value is part of the profile contract and
   * must not change while files using that profile exist.
   */
  default int getPageBodyOverhead() {
    return -1;
  }

  /**
   * Returns this immutable profile's chunk-position proof size. Page-AEAD providers must override
   * this method; unsupported providers fail closed. The size must be positive and no larger than
   * {@link ChunkCryptoContext#MAX_AUTHENTICATION_SIZE}.
   */
  default int getChunkAuthenticationSize() {
    return -1;
  }

  /**
   * Returns a fixed-size opaque proof authenticating a chunk's physical position. The proof is
   * exactly {@link #getChunkAuthenticationSize()} bytes. Providers using AEAD must use a fresh
   * nonce under the file DEK, independently of page encryption calls.
   */
  default byte[] authenticateChunk(ChunkCryptoContext context) {
    throw new EncryptException(Messages.get("error.encrypt.chunk_authentication_unsupported"));
  }

  /** Verifies the proof before a chunk identity is used; unsupported providers fail closed. */
  default void verifyChunkAuthentication(byte[] authentication, ChunkCryptoContext context) {
    throw new EncryptException(Messages.get("error.encrypt.chunk_authentication_unsupported"));
  }

  @Override
  default void close() {}
}
