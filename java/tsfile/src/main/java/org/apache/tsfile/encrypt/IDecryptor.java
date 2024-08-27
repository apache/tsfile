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

import java.io.Serializable;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** encrypt data according to tsfileconfig. */
public interface IDecryptor extends Serializable {

  Logger logger = LoggerFactory.getLogger(IDecryptor.class);

  static IDecryptor getDecryptor(String name, byte[] key) {
    return getDecryptor(EncryptionType.valueOf(name), key);
  }

  static IDecryptor getDecryptor(EncryptionType name, byte[] key) {
    if (name == null) {
      return new NoDecryptor();
    }
    switch (name) {
      case UNENCRYPTED:
        return new NoDecryptor();
      case SM4128:
        return new SM4128Decryptor(key);
      case AES128:
        return new AES128Decryptor(key);
      default:
        logger.warn("Unknown encryption type: {}", name);
        return new NoDecryptor();
    }
  }

  byte[] decrypt(byte[] data);

  byte[] decrypt(byte[] data, int offset, int size);

  EncryptionType getEncryptionType();

  class NoDecryptor implements IDecryptor {

    @Override
    public byte[] decrypt(byte[] data) {
      return data;
    }

    @Override
    public byte[] decrypt(byte[] data, int offset, int size) {
      return Arrays.copyOfRange(data, offset, offset + size);
    }

    @Override
    public EncryptionType getEncryptionType() {
      return EncryptionType.UNENCRYPTED;
    }
  }

  class SM4128Decryptor implements IDecryptor {

    private final SM4Utils sm4;

    SM4128Decryptor(byte[] key) {
      if (key.length != 16) {
        throw new EncryptKeyLengthNotMatchException(16, key.length);
      }
      this.sm4 = new SM4Utils(key, key);
    }

    @Override
    public byte[] decrypt(byte[] data) {
      return sm4.cryptData_CTR(data);
    }

    @Override
    public byte[] decrypt(byte[] data, int offset, int size) {
      return decrypt(Arrays.copyOfRange(data, offset, offset + size));
    }

    @Override
    public EncryptionType getEncryptionType() {
      return EncryptionType.SM4128;
    }
  }

  class AES128Decryptor implements IDecryptor {
    private final Cipher AES;

    AES128Decryptor(byte[] key) {
      if (key.length != 16) {
        throw new EncryptKeyLengthNotMatchException(16, key.length);
      }
      SecretKeySpec secretKeySpec = new SecretKeySpec(key, "AES");
      // Create IV parameter
      IvParameterSpec ivParameterSpec = new IvParameterSpec(key);
      try {
        // Create Cipher instance and initialize it for encryption in CTR mode without padding
        this.AES = Cipher.getInstance("AES/CTR/NoPadding");
        AES.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec);
      } catch (InvalidAlgorithmParameterException
          | NoSuchPaddingException
          | NoSuchAlgorithmException
          | InvalidKeyException e) {
        throw new EncryptException("AES128Decryptor init failed " + e.getMessage());
      }
    }

    @Override
    public byte[] decrypt(byte[] data) {
      try {
        return AES.doFinal(data);
      } catch (IllegalBlockSizeException | BadPaddingException e) {
        throw new EncryptException("AES128Decryptor decrypt failed " + e.getMessage());
      }
    }

    @Override
    public byte[] decrypt(byte[] data, int offset, int size) {
      try {
        return AES.doFinal(data, offset, size);
      } catch (IllegalBlockSizeException | BadPaddingException e) {
        throw new EncryptException("AES128Decryptor decrypt failed " + e.getMessage());
      }
    }

    @Override
    public EncryptionType getEncryptionType() {
      return EncryptionType.AES128;
    }
  }
}
