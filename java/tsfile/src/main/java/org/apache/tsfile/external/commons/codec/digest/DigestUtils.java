/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tsfile.external.commons.codec.digest;

import org.apache.tsfile.external.commons.codec.binary.Hex;
import org.apache.tsfile.external.commons.codec.binary.MessageDigestAlgorithms;
import org.apache.tsfile.external.commons.codec.binary.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/////////////////////////////////////////////////////////////////////////////////////////////////
// IoTDB
/////////////////////////////////////////////////////////////////////////////////////////////////

public class DigestUtils {

  /**
   * Calculates the MD2 digest and returns the value as a 32 character hexadecimal string.
   *
   * @param data Data to digest
   * @return MD2 digest as a hexadecimal string
   * @since 1.7
   */
  public static String md2Hex(final String data) {
    return Hex.encodeHexString(md2(data));
  }

  /**
   * Calculates the MD2 digest and returns the value as a 16 element {@code byte[]}.
   *
   * @param data Data to digest; converted to bytes using {@link StringUtils#getBytesUtf8(String)}
   * @return MD2 digest
   * @since 1.7
   */
  public static byte[] md2(final String data) {
    return md2(StringUtils.getBytesUtf8(data));
  }

  /**
   * Calculates the MD2 digest and returns the value as a 16 element {@code byte[]}.
   *
   * @param data Data to digest
   * @return MD2 digest
   * @since 1.7
   */
  public static byte[] md2(final byte[] data) {
    return getMd2Digest().digest(data);
  }

  /**
   * Gets an MD2 MessageDigest.
   *
   * @return An MD2 digest instance.
   * @throws IllegalArgumentException when a {@link NoSuchAlgorithmException} is caught, which
   *     should never happen because MD2 is a built-in algorithm
   * @see MessageDigestAlgorithms#MD2
   * @since 1.7
   */
  public static MessageDigest getMd2Digest() {
    return getDigest(MessageDigestAlgorithms.MD2);
  }

  public static String md5Hex(InputStream data) throws IOException {
    return Hex.encodeHexString(md5(data));
  }

  public static byte[] md5(InputStream data) throws IOException {
    return digest(getMd5Digest(), data);
  }

  /**
   * Calculates the MD5 digest and returns the value as a 32 character hexadecimal string.
   *
   * @param data Data to digest
   * @return MD5 digest as a hexadecimal string
   */
  public static String md5Hex(final byte[] data) {
    return Hex.encodeHexString(md5(data));
  }

  /**
   * Calculates the MD5 digest and returns the value as a 16 element {@code byte[]}.
   *
   * @param data Data to digest
   * @return MD5 digest
   */
  public static byte[] md5(final byte[] data) {
    return getMd5Digest().digest(data);
  }

  public static byte[] digest(MessageDigest messageDigest, InputStream data) throws IOException {
    return updateDigest(messageDigest, data).digest();
  }

  public static MessageDigest updateDigest(MessageDigest digest, InputStream inputStream)
      throws IOException {
    byte[] buffer = new byte[1024];

    for (int read = inputStream.read(buffer, 0, 1024);
        read > -1;
        read = inputStream.read(buffer, 0, 1024)) {
      digest.update(buffer, 0, read);
    }

    return digest;
  }

  public static MessageDigest getMd5Digest() {
    return getDigest("MD5");
  }

  public static MessageDigest getDigest(String algorithm) {
    try {
      return getMessageDigest(algorithm);
    } catch (NoSuchAlgorithmException var2) {
      throw new IllegalArgumentException(var2);
    }
  }

  private static MessageDigest getMessageDigest(String algorithm) throws NoSuchAlgorithmException {
    return MessageDigest.getInstance(algorithm);
  }
}
