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
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.exception.encrypt.EncryptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.lang.reflect.InvocationTargetException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

public class EncryptUtils {

  private static final Logger logger = LoggerFactory.getLogger(EncryptUtils.class);

  private static final String encryptClassPrefix = "org.apache.tsfile.encrypt.";

  private static volatile String normalKeyStr;

  private static volatile EncryptParameter encryptParam;

  private static final String HMAC_ALGORITHM = "HmacSHA256";
  private static final int ITERATION_COUNT = 1024;
  private static final int SALT_LENGTH = 16;
  private static final int INT_SIZE = 4;
  private static final int dkLen = 16;

  public static String getNormalKeyStr() {
    if (normalKeyStr == null) {
      synchronized (EncryptUtils.class) {
        if (normalKeyStr == null) {
          normalKeyStr = getNormalKeyStr(TSFileDescriptor.getInstance().getConfig());
        }
      }
    }
    return normalKeyStr;
  }

  public static String getEncryptClass(String encryptType) {
    String classNameRegex = "^(\\p{Alpha}\\w*)(\\.\\p{Alpha}\\w+)+$";
    if (IEncrypt.encryptTypeToClassMap.containsKey(encryptType)) {
      return IEncrypt.encryptTypeToClassMap.get(encryptType);
    } else if (encryptType.matches(classNameRegex)) {
      IEncrypt.encryptTypeToClassMap.put(encryptType, encryptType);
      return encryptType;
    } else {
      IEncrypt.encryptTypeToClassMap.put(encryptType, encryptClassPrefix + encryptType);
      return encryptClassPrefix + encryptType;
    }
  }

  public static byte[] getEncryptKeyFromToken(String token, byte[] salt) {
    if (token == null || token.trim().isEmpty()) {
      return generateSalt();
    }
    try {
      return deriveKeyInternal(token.getBytes(), salt, ITERATION_COUNT, dkLen);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new EncryptException("Error deriving key from token", e);
    }
  }

  private static byte[] deriveKeyInternal(byte[] password, byte[] salt, int c, int dkLen)
      throws NoSuchAlgorithmException, InvalidKeyException {

    int hLen = getPRFLength();

    if (dkLen < 1) {
      throw new EncryptException("main key's dkLen must be positive integer: " + dkLen);
    }
    if ((long) dkLen > (long) (Math.pow(2, 32) - 1) * hLen) {
      throw new EncryptException("main key's dkLen is too long: " + dkLen);
    }

    int n = (int) Math.ceil((double) dkLen / hLen);
    int r = dkLen - (n - 1) * hLen;

    byte[] blocks = new byte[n * hLen];

    for (int i = 1; i <= n; i++) {
      byte[] block = F(password, salt, c, i);
      System.arraycopy(block, 0, blocks, (i - 1) * hLen, hLen);
    }

    return Arrays.copyOf(blocks, dkLen);
  }

  /** main function F */
  private static byte[] F(byte[] password, byte[] salt, int c, int i)
      throws NoSuchAlgorithmException, InvalidKeyException {

    // U1 = PRF(P, S || INT(i))
    byte[] input = concatenate(salt, intToBigEndian(i));
    byte[] U = prf(password, input);
    byte[] result = U.clone();

    // U2 to Uc
    for (int j = 2; j <= c; j++) {
      U = prf(password, U);
      xorBytes(result, U);
    }

    return result;
  }

  /** PRF implementation (HMAC-SHA256) */
  private static byte[] prf(byte[] key, byte[] data)
      throws NoSuchAlgorithmException, InvalidKeyException {
    Mac hmac = Mac.getInstance(HMAC_ALGORITHM);
    hmac.init(new SecretKeySpec(key, HMAC_ALGORITHM));
    return hmac.doFinal(data);
  }

  private static int getPRFLength() throws NoSuchAlgorithmException {
    return Mac.getInstance(HMAC_ALGORITHM).getMacLength();
  }

  public static byte[] generateSalt() {
    byte[] salt = new byte[SALT_LENGTH];
    new SecureRandom().nextBytes(salt);
    return salt;
  }

  private static byte[] intToBigEndian(int i) {
    return new byte[] {(byte) (i >>> 24), (byte) (i >>> 16), (byte) (i >>> 8), (byte) i};
  }

  private static void xorBytes(byte[] result, byte[] input) {
    for (int i = 0; i < result.length; i++) {
      result[i] ^= input[i];
    }
  }

  private static byte[] concatenate(byte[] a, byte[] b) {
    byte[] output = new byte[a.length + b.length];
    System.arraycopy(a, 0, output, 0, a.length);
    System.arraycopy(b, 0, output, a.length, b.length);
    return output;
  }

  public static byte[] hexStringToByteArray(String hexString) {
    int len = hexString.length();
    byte[] byteArray = new byte[len / 2];

    for (int i = 0; i < len; i += 2) {
      byteArray[i / 2] =
          (byte)
              ((Character.digit(hexString.charAt(i), 16) << 4)
                  + Character.digit(hexString.charAt(i + 1), 16));
    }

    return byteArray;
  }

  public static String byteArrayToHexString(byte[] bytes) {
    StringBuilder sb = new StringBuilder();

    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }

    return sb.toString();
  }

  public static String getNormalKeyStr(TSFileConfig conf) {
    final MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new EncryptException(
          "SHA-256 algorithm not found while using SHA-256 to generate data key", e);
    }
    md.update("IoTDB is the best".getBytes());
    md.update(conf.getEncryptKey());
    byte[] data_key = Arrays.copyOfRange(md.digest(), 0, 16);
    data_key =
        IEncryptor.getEncryptor(conf.getEncryptType(), conf.getEncryptKey()).encrypt(data_key);

    StringBuilder valueStr = new StringBuilder();

    for (byte b : data_key) {
      valueStr.append(b).append(",");
    }

    valueStr.deleteCharAt(valueStr.length() - 1);
    return valueStr.toString();
  }

  public static EncryptParameter getEncryptParameter() {
    if (encryptParam == null) {
      synchronized (EncryptUtils.class) {
        if (encryptParam == null) {
          encryptParam = getEncryptParameter(TSFileDescriptor.getInstance().getConfig());
        }
      }
    }
    return encryptParam;
  }

  public static EncryptParameter getEncryptParameter(TSFileConfig conf) {
    String encryptType;
    byte[] dataEncryptKey;
    if (!Objects.equals(conf.getEncryptType(), "UNENCRYPTED")
        && !Objects.equals(conf.getEncryptType(), "org.apache.tsfile.encrypt.UNENCRYPTED")) {
      encryptType = conf.getEncryptType();
      final MessageDigest md;
      try {
        md = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
        throw new EncryptException(
            "SHA-256 algorithm not found while using SHA-256 to generate data key", e);
      }
      md.update("IoTDB is the best".getBytes());
      md.update(conf.getEncryptKey());
      dataEncryptKey = Arrays.copyOfRange(md.digest(), 0, 16);
    } else {
      encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      dataEncryptKey = null;
    }
    return new EncryptParameter(encryptType, dataEncryptKey);
  }

  public static IEncrypt getEncrypt() {
    return getEncrypt(TSFileDescriptor.getInstance().getConfig());
  }

  public static IEncrypt getEncrypt(String encryptType, byte[] dataEncryptKey) {
    try {
      String className = getEncryptClass(encryptType);
      if (IEncrypt.encryptMap.containsKey(className)) {
        return ((IEncrypt) IEncrypt.encryptMap.get(className).newInstance(dataEncryptKey));
      }
      Class<?> encryptTypeClass = Class.forName(className);
      java.lang.reflect.Constructor<?> constructor =
          encryptTypeClass.getDeclaredConstructor(byte[].class);
      IEncrypt.encryptMap.put(className, constructor);
      return ((IEncrypt) constructor.newInstance(dataEncryptKey));
    } catch (ClassNotFoundException e) {
      throw new EncryptException("Get encryptor class failed: " + encryptType, e);
    } catch (NoSuchMethodException e) {
      throw new EncryptException("Get constructor for encryptor failed: " + encryptType, e);
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new EncryptException("New encryptor instance failed: " + encryptType, e);
    }
  }

  public static IEncrypt getEncrypt(TSFileConfig conf) {
    String encryptType;
    byte[] dataEncryptKey;
    if (!Objects.equals(conf.getEncryptType(), "UNENCRYPTED")
        && !Objects.equals(conf.getEncryptType(), "org.apache.tsfile.encrypt.UNENCRYPTED")) {
      encryptType = conf.getEncryptType();
      final MessageDigest md;
      try {
        md = MessageDigest.getInstance("SHA-256");
      } catch (NoSuchAlgorithmException e) {
        throw new EncryptException(
            "SHA-256 algorithm not found while using SHA-256 to generate data key", e);
      }
      md.update("IoTDB is the best".getBytes());
      md.update(conf.getEncryptKey());
      dataEncryptKey = Arrays.copyOfRange(md.digest(), 0, 16);
    } else {
      encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      dataEncryptKey = null;
    }
    return getEncrypt(encryptType, dataEncryptKey);
  }

  public static byte[] getSecondKeyFromStr(String str) {
    String[] strArray = str.split(",");
    byte[] key = new byte[strArray.length];
    for (int i = 0; i < strArray.length; i++) {
      key[i] = Byte.parseByte(strArray[i]);
    }
    return key;
  }
}
