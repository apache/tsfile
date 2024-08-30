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

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.metadata.enums.EncryptionType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;

public class EncryptUtils {

  public static String normalKeyStr = getNormalKeyStr();

  public static IEncryptor encryptor = getDefaultEncryptor();

  public static IDecryptor decryptor = getDefaultDecryptor();

  public static String getEncryptKeyFromPath(String path) {
    try (BufferedReader br = new BufferedReader(new FileReader(path))) {
      StringBuilder sb = new StringBuilder();
      String line;
      boolean first = true;
      while ((line = br.readLine()) != null) {
        if (first) {
          sb.append(line);
          first = false;
        } else {
          sb.append("\n").append(line);
        }
      }
      return sb.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  public static String getNormalKeyStr() {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update("IoTDB is the best".getBytes());
      md.update(TSFileDescriptor.getInstance().getConfig().getEncryptKey().getBytes());
      byte[] data_key = md.digest();
      StringBuilder valueStr = new StringBuilder();

      for (byte b : data_key) {
        valueStr.append(b).append(",");
      }

      valueStr.deleteCharAt(valueStr.length() - 1);
      String str = valueStr.toString();

      return str;
    } catch (Exception e) {
      throw new EncryptException("md5 function not found while using md5 to generate data key");
    }
  }

  public static IEncryptor getDefaultEncryptor() {
    EncryptionType encryptType;
    byte[] dataEncryptKey;
    if (TSFileDescriptor.getInstance().getConfig().getEncryptFlag()) {
      encryptType = TSFileDescriptor.getInstance().getConfig().getEncryptType();
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update("IoTDB is the best".getBytes());
        md.update(TSFileDescriptor.getInstance().getConfig().getEncryptKey().getBytes());
        dataEncryptKey = md.digest();
      } catch (Exception e1) {
        throw new EncryptException("md5 function not found while using md5 to generate data key");
      }
    } else {
      encryptType = EncryptionType.UNENCRYPTED;
      dataEncryptKey = null;
    }
    return IEncryptor.getEncryptor(encryptType, dataEncryptKey);
  }

  public static IDecryptor getDefaultDecryptor() {
    EncryptionType encryptType;
    byte[] dataEncryptKey;
    if (TSFileDescriptor.getInstance().getConfig().getEncryptFlag()) {
      encryptType = TSFileDescriptor.getInstance().getConfig().getEncryptType();
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update("IoTDB is the best".getBytes());
        md.update(TSFileDescriptor.getInstance().getConfig().getEncryptKey().getBytes());
        dataEncryptKey = md.digest();
      } catch (Exception e1) {
        throw new EncryptException("md5 function not found while using md5 to generate data key");
      }
    } else {
      encryptType = EncryptionType.UNENCRYPTED;
      dataEncryptKey = null;
    }
    return IDecryptor.getDecryptor(encryptType, dataEncryptKey);
  }

  public static byte[] getKeyFromStr(String str) {
    String[] strArray = str.split(",");
    byte[] key = new byte[strArray.length];
    for (int i = 0; i < strArray.length; i++) {
      key[i] = Byte.parseByte(strArray[i]);
    }
    return key;
  }
}
