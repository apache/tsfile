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
import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.i18n.Messages;

import java.util.concurrent.atomic.AtomicBoolean;

/** Enforces page-only, fail-closed semantics independently of a provider's legacy methods. */
final class PageAeadEncryption implements IEncrypt {
  private final IEncrypt delegate;
  private final IEncryptor encryptor;
  private final IDecryptor decryptor;
  private final AtomicBoolean closed = new AtomicBoolean();

  PageAeadEncryption(IEncrypt delegate) {
    this.delegate = delegate;
    IEncryptor providerEncryptor = delegate.getEncryptor();
    IDecryptor providerDecryptor = delegate.getDecryptor();
    if (providerEncryptor == null || providerDecryptor == null) {
      throw new EncryptException(Messages.get("error.encrypt.page_aead_operations_missing"));
    }
    encryptor =
        new IEncryptor() {
          @Override
          public byte[] encrypt(byte[] data) {
            throw legacyOperation();
          }

          @Override
          public byte[] encrypt(byte[] data, int offset, int size) {
            throw legacyOperation();
          }

          @Override
          public byte[] encryptPage(byte[] data, int offset, int size, PageCryptoContext context) {
            ensureOpen();
            return providerEncryptor.encryptPage(data, offset, size, context);
          }

          @Override
          public EncryptionType getEncryptionType() {
            return EncryptionType.NewWay;
          }
        };
    decryptor =
        new IDecryptor() {
          @Override
          public byte[] decrypt(byte[] data) {
            throw legacyOperation();
          }

          @Override
          public byte[] decrypt(byte[] data, int offset, int size) {
            throw legacyOperation();
          }

          @Override
          public byte[] decryptPage(byte[] data, int offset, int size, PageCryptoContext context) {
            ensureOpen();
            try {
              return providerDecryptor.decryptPage(data, offset, size, context);
            } catch (EncryptException e) {
              throw e;
            } catch (RuntimeException e) {
              throw new EncryptException(
                  Messages.get("error.encrypt.page_aead_decryption_failed"), e);
            }
          }

          @Override
          public EncryptionType getEncryptionType() {
            return EncryptionType.NewWay;
          }
        };
  }

  private static EncryptException legacyOperation() {
    return new EncryptException(Messages.get("error.encrypt.page_aead_requires_page_context"));
  }

  private void ensureOpen() {
    if (closed.get()) {
      throw new EncryptException(Messages.get("error.encrypt.parameter_destroyed"));
    }
  }

  @Override
  public IDecryptor getDecryptor() {
    ensureOpen();
    return decryptor;
  }

  @Override
  public IEncryptor getEncryptor() {
    ensureOpen();
    return encryptor;
  }

  @Override
  public int getPageBodyOverhead() {
    ensureOpen();
    return delegate.getPageBodyOverhead();
  }

  @Override
  public byte[] authenticateChunk(ChunkCryptoContext context) {
    ensureOpen();
    byte[] authentication = delegate.authenticateChunk(context);
    validateChunkAuthentication(authentication);
    return authentication;
  }

  @Override
  public void verifyChunkAuthentication(byte[] authentication, ChunkCryptoContext context) {
    ensureOpen();
    validateChunkAuthentication(authentication);
    try {
      delegate.verifyChunkAuthentication(authentication, context);
    } catch (EncryptException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new EncryptException(Messages.get("error.encrypt.chunk_authentication_failed"), e);
    }
  }

  private static void validateChunkAuthentication(byte[] authentication) {
    if (authentication == null || authentication.length != ChunkCryptoContext.AUTHENTICATION_SIZE) {
      throw new EncryptException(Messages.get("error.encrypt.invalid_chunk_authentication"));
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      delegate.close();
    }
  }
}
