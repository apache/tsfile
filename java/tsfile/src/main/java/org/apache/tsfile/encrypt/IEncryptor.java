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
import org.apache.tsfile.exception.encrypt.EncryptKeyLengthNotMatchException;
import org.apache.tsfile.file.metadata.enums.EncryptionType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** encrypt data according to tsfileconfig. */
public interface IEncryptor {

  static final Logger logger = LoggerFactory.getLogger(IEncryptor.class);

  static IEncryptor getEncryptor(String name, byte[] key) {
    return getEncryptor(EncryptionType.valueOf(name), key);
  }

  static IEncryptor getEncryptor(EncryptionType name, byte[] key) {
    if (name == null) {
      return new NoEncryptor();
    }
    switch (name) {
      case SM4128:
        return new SM4128Encryptor(key);
      case AES128:
        return new AES128Encryptor(key);
      default:
        // log a warning
        logger.warn("Unknown encryption type: {}", name);
        return new NoEncryptor();
    }
  }

  byte[] encrypt(byte[] data);

  byte[] encrypt(byte[] data, int offset, int size);

  EncryptionType getEncryptionType();

  class NoEncryptor implements IEncryptor {

    @Override
    public byte[] encrypt(byte[] data) {
      return data;
    }

    @Override
    public byte[] encrypt(byte[] data, int offset, int size) {
      return Arrays.copyOfRange(data, offset, offset + size);
    }

    @Override
    public EncryptionType getEncryptionType() {
      return EncryptionType.UNENCRYPTED;
    }
  }

  class SM4128Encryptor implements IEncryptor {

    private final SM4Utils sm4;

    SM4128Encryptor(byte[] key) {
      if (key.length != 16) {
        throw new EncryptKeyLengthNotMatchException(16, key.length);
      }
      this.sm4 = new SM4Utils(key, key);
    }

    @Override
    public byte[] encrypt(byte[] data) {
      return sm4.cryptData_CTR(data);
    }

    @Override
    public byte[] encrypt(byte[] data, int offset, int size) {
      return encrypt(Arrays.copyOfRange(data, offset, offset + size));
    }

    @Override
    public EncryptionType getEncryptionType() {
      return EncryptionType.SM4128;
    }
  }

  class AES128Encryptor implements IEncryptor {
    private final Cipher AES;

    AES128Encryptor(byte[] key) {
      if (key.length != 16) {
        throw new EncryptKeyLengthNotMatchException(16, key.length);
      }
      SecretKeySpec secretKeySpec = new SecretKeySpec(key, "AES");
      // Create IV parameter
      IvParameterSpec ivParameterSpec = new IvParameterSpec(key);
      try {
        // Create Cipher instance and initialize it for encryption in CTR mode without padding
        this.AES = Cipher.getInstance("AES/CTR/NoPadding");
        AES.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivParameterSpec);
      } catch (InvalidAlgorithmParameterException
          | NoSuchPaddingException
          | NoSuchAlgorithmException
          | InvalidKeyException e) {
        throw new EncryptException("AES128Encryptor init failed ", e);
      }
    }

    @Override
    public byte[] encrypt(byte[] data) {
      try {
        return AES.doFinal(data);
      } catch (IllegalBlockSizeException | BadPaddingException e) {
        throw new EncryptException("AES128Encryptor encrypt failed ", e);
      }
    }

    @Override
    public byte[] encrypt(byte[] data, int offset, int size) {
      try {
        return AES.doFinal(data, offset, size);
      } catch (IllegalBlockSizeException | BadPaddingException e) {
        throw new EncryptException("AES128Encryptor encrypt failed ", e);
      }
    }

    @Override
    public EncryptionType getEncryptionType() {
      return EncryptionType.AES128;
    }
  }
}
