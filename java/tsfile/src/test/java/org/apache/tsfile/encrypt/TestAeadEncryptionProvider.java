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

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/** JDK-only AEAD provider used to exercise the generic TsFile encryption SPI. */
public final class TestAeadEncryptionProvider implements IEncryptProvider {

  public static final TestAeadEncryptionProvider INSTANCE = new TestAeadEncryptionProvider();
  public static final String PROVIDER_ID = "test-aead-provider";
  public static final String PROFILE_ID = "AES_GCM_128_AES_WRAP_128_V1";

  private static final int IV_LENGTH = 12;
  private static final int TAG_LENGTH = 16;

  private static final byte[] TEST_KEY_ENCRYPTION_KEY =
      new byte[] {
        0x10, 0x32, 0x54, 0x76, 0x01, 0x23, 0x45, 0x67, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
        0x00
      };
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final AtomicInteger CREATE_COUNT = new AtomicInteger();

  private TestAeadEncryptionProvider() {}

  @Override
  public String getProviderId() {
    return PROVIDER_ID;
  }

  @Override
  public IEncrypt create(EncryptParameter encryptParameter) {
    if (!PROFILE_ID.equals(encryptParameter.getProfileId())) {
      throw new EncryptException("unsupported test encryption profile");
    }
    CREATE_COUNT.incrementAndGet();
    return new TestAeadEncrypt(resolveDataKey(encryptParameter));
  }

  public static EncryptParameter createParameter(byte[] dataKey, byte[] fileCryptoId) {
    return EncryptParameter.pageAeadBuilder()
        .key(dataKey)
        .providerId(PROVIDER_ID)
        .profileId(PROFILE_ID)
        .keyId("test-key")
        .keyVersion("1")
        .wrappedDataKey(wrapDataKey(dataKey))
        .fileCryptoId(fileCryptoId)
        .build();
  }

  public static int getCreateCount() {
    return CREATE_COUNT.get();
  }

  public static void resetCreateCount() {
    CREATE_COUNT.set(0);
  }

  private static byte[] resolveDataKey(EncryptParameter parameter) {
    byte[] dataKey = parameter.getKey();
    if (dataKey != null) {
      return Arrays.copyOf(dataKey, dataKey.length);
    }
    try {
      Cipher cipher = Cipher.getInstance("AESWrap");
      cipher.init(Cipher.UNWRAP_MODE, new SecretKeySpec(TEST_KEY_ENCRYPTION_KEY, "AES"));
      SecretKey unwrapped =
          (SecretKey) cipher.unwrap(parameter.getWrappedDataKey(), "AES", Cipher.SECRET_KEY);
      return unwrapped.getEncoded();
    } catch (GeneralSecurityException e) {
      throw new EncryptException("test data key unwrap failed", e);
    }
  }

  private static byte[] wrapDataKey(byte[] dataKey) {
    try {
      Cipher cipher = Cipher.getInstance("AESWrap");
      cipher.init(Cipher.WRAP_MODE, new SecretKeySpec(TEST_KEY_ENCRYPTION_KEY, "AES"));
      return cipher.wrap(new SecretKeySpec(dataKey, "AES"));
    } catch (GeneralSecurityException e) {
      throw new EncryptException("test data key wrap failed", e);
    }
  }

  private static final class TestAeadEncrypt implements IEncrypt {

    private final byte[] dataKey;

    private TestAeadEncrypt(byte[] dataKey) {
      this.dataKey = dataKey;
    }

    @Override
    public IDecryptor getDecryptor() {
      return new AeadDecryptor(dataKey);
    }

    @Override
    public IEncryptor getEncryptor() {
      return new AeadEncryptor(dataKey);
    }

    @Override
    public int getPageBodyOverhead() {
      return IV_LENGTH + TAG_LENGTH;
    }

    @Override
    public void close() {
      Arrays.fill(dataKey, (byte) 0);
    }
  }

  private static final class AeadEncryptor implements IEncryptor {

    private final byte[] dataKey;

    private AeadEncryptor(byte[] dataKey) {
      this.dataKey = dataKey;
    }

    @Override
    public byte[] encrypt(byte[] data) {
      return encrypt(data, 0, data.length);
    }

    @Override
    public byte[] encrypt(byte[] data, int offset, int size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] encryptPage(
        byte[] data, int offset, int size, PageCryptoContext pageCryptoContext) {
      byte[] iv = new byte[IV_LENGTH];
      SECURE_RANDOM.nextBytes(iv);
      try {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(
            Cipher.ENCRYPT_MODE,
            new SecretKeySpec(dataKey, "AES"),
            new GCMParameterSpec(TAG_LENGTH * Byte.SIZE, iv));
        cipher.updateAAD(pageCryptoContext.getAssociatedData());
        byte[] ciphertext = cipher.doFinal(data, offset, size);
        byte[] pageBody = new byte[iv.length + ciphertext.length];
        System.arraycopy(iv, 0, pageBody, 0, iv.length);
        System.arraycopy(ciphertext, 0, pageBody, iv.length, ciphertext.length);
        return pageBody;
      } catch (GeneralSecurityException e) {
        throw new EncryptException("test page encryption failed", e);
      }
    }

    @Override
    public EncryptionType getEncryptionType() {
      return EncryptionType.NewWay;
    }
  }

  private static final class AeadDecryptor implements IDecryptor {

    private final byte[] dataKey;

    private AeadDecryptor(byte[] dataKey) {
      this.dataKey = dataKey;
    }

    @Override
    public byte[] decrypt(byte[] data) {
      return decrypt(data, 0, data.length);
    }

    @Override
    public byte[] decrypt(byte[] data, int offset, int size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] decryptPage(
        byte[] data, int offset, int size, PageCryptoContext pageCryptoContext) {
      try {
        GCMParameterSpec spec =
            new GCMParameterSpec(TAG_LENGTH * Byte.SIZE, data, offset, IV_LENGTH);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dataKey, "AES"), spec);
        cipher.updateAAD(pageCryptoContext.getAssociatedData());
        return cipher.doFinal(data, offset + IV_LENGTH, size - IV_LENGTH);
      } catch (GeneralSecurityException e) {
        throw new EncryptException("test page decryption failed", e);
      }
    }

    @Override
    public EncryptionType getEncryptionType() {
      return EncryptionType.NewWay;
    }
  }
}
