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

import javax.security.auth.Destroyable;

import java.util.Arrays;

/**
 * Parameters required to encrypt or decrypt one TsFile.
 *
 * <p>The two-argument constructor preserves the legacy encryption API. New file-level encryption
 * schemes should use {@link #pageAeadBuilder()} and identify an {@link IEncryptProvider} and one of
 * its immutable encryption profiles by stable ids. Runtime implementation class names and algorithm
 * implementation details are deliberately not part of the persisted parameters.
 */
public class EncryptParameter implements AutoCloseable, Destroyable {

  public static final int FILE_CRYPTO_ID_LENGTH = 16;

  private final String type;
  private final byte[] key;
  private final boolean pageAead;
  private final String providerId;
  private final String profileId;
  private final String keyId;
  private final String keyVersion;
  private final byte[] wrappedDataKey;
  private final byte[] fileCryptoId;

  private transient volatile IEncrypt fileEncrypt;
  private transient volatile boolean destroyed;

  public EncryptParameter(String type, byte[] key) {
    this.type = type;
    this.key = key;
    this.pageAead = false;
    this.providerId = null;
    this.profileId = null;
    this.keyId = null;
    this.keyVersion = null;
    this.wrappedDataKey = null;
    this.fileCryptoId = null;
  }

  private EncryptParameter(Builder builder) {
    this.type = null;
    this.key = copy(builder.key);
    this.pageAead = true;
    this.providerId = builder.providerId;
    this.profileId = builder.profileId;
    this.keyId = builder.keyId;
    this.keyVersion = builder.keyVersion;
    this.wrappedDataKey = copy(builder.wrappedDataKey);
    this.fileCryptoId = copy(builder.fileCryptoId);
  }

  public static Builder pageAeadBuilder() {
    return new Builder();
  }

  /** Returns an independent parameter object with no initialized runtime provider state. */
  public EncryptParameter copy() {
    if (destroyed) {
      throw new EncryptException(Messages.get("error.encrypt.parameter_destroyed"));
    }
    if (!isTdePageAead()) {
      return new EncryptParameter(type, key);
    }
    return pageAeadBuilder()
        .key(key)
        .providerId(providerId)
        .profileId(profileId)
        .keyId(keyId)
        .keyVersion(keyVersion)
        .wrappedDataKey(wrappedDataKey)
        .fileCryptoId(fileCryptoId)
        .build();
  }

  public byte[] getKey() {
    return key;
  }

  public String getType() {
    return type;
  }

  public String getProviderId() {
    return providerId;
  }

  public String getProfileId() {
    return profileId;
  }

  public String getKeyId() {
    return keyId;
  }

  public String getKeyVersion() {
    return keyVersion;
  }

  public byte[] getWrappedDataKey() {
    return copy(wrappedDataKey);
  }

  public byte[] getFileCryptoId() {
    return copy(fileCryptoId);
  }

  public int getPageBodyOverhead() {
    int overhead = getOrCreateFileEncrypt().getPageBodyOverhead();
    if (overhead <= 0) {
      throw new EncryptException(
          Messages.format("error.encrypt.invalid_page_body_overhead", overhead, profileId));
    }
    return overhead;
  }

  public boolean isTdePageAead() {
    return pageAead;
  }

  IEncrypt getOrCreateFileEncrypt() {
    if (destroyed) {
      throw new EncryptException(Messages.get("error.encrypt.parameter_destroyed"));
    }
    IEncrypt current = fileEncrypt;
    if (current == null) {
      synchronized (this) {
        if (destroyed) {
          throw new EncryptException(Messages.get("error.encrypt.parameter_destroyed"));
        }
        current = fileEncrypt;
        if (current == null) {
          current = EncryptionProviderRegistry.create(this);
          fileEncrypt = current;
        }
      }
    }
    return current;
  }

  @Override
  public boolean isDestroyed() {
    return destroyed;
  }

  @Override
  public void destroy() {
    IEncrypt current;
    synchronized (this) {
      if (destroyed) {
        return;
      }
      destroyed = true;
      current = fileEncrypt;
      fileEncrypt = null;
      if (key != null && isTdePageAead()) {
        Arrays.fill(key, (byte) 0);
      }
    }
    if (current != null) {
      current.close();
    }
  }

  @Override
  public void close() {
    destroy();
  }

  private static byte[] copy(byte[] value) {
    return value == null ? null : Arrays.copyOf(value, value.length);
  }

  public static final class Builder {

    private byte[] key;
    private String providerId;
    private String profileId;
    private String keyId;
    private String keyVersion;
    private byte[] wrappedDataKey;
    private byte[] fileCryptoId;

    private Builder() {}

    public Builder key(byte[] key) {
      this.key = copy(key);
      return this;
    }

    public Builder providerId(String providerId) {
      this.providerId = providerId;
      return this;
    }

    public Builder profileId(String profileId) {
      this.profileId = profileId;
      return this;
    }

    public Builder keyId(String keyId) {
      this.keyId = keyId;
      return this;
    }

    public Builder keyVersion(String keyVersion) {
      this.keyVersion = keyVersion;
      return this;
    }

    public Builder wrappedDataKey(byte[] wrappedDataKey) {
      this.wrappedDataKey = copy(wrappedDataKey);
      return this;
    }

    public Builder fileCryptoId(byte[] fileCryptoId) {
      this.fileCryptoId = copy(fileCryptoId);
      return this;
    }

    public EncryptParameter build() {
      return new EncryptParameter(this);
    }
  }
}
