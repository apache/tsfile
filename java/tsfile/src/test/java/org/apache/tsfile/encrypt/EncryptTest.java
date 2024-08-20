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

import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.utils.PublicBAOS;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class EncryptTest {
  private final String inputString =
      "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
          + "AES, a fast encryptor/decryptor.";
  private final String key = "mkmkmkmkmkmkmkmk";

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void NoEncryptorTest() throws IOException {
    IEncryptor encryptor = new IEncryptor.NoEncryptor();
    IDecryptor decryptor = new IDecryptor.NoDecryptor();
    byte[] encrypted = encryptor.encrypt(inputString.getBytes(StandardCharsets.UTF_8));
    byte[] decrypted = decryptor.decrypt(encrypted);

    String result = new String(decrypted, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void SM4128Test() throws IOException {
    IEncryptor encryptor = new IEncryptor.SM4128Encryptor(key.getBytes(StandardCharsets.UTF_8));
    IDecryptor decryptor = new IDecryptor.SM4128Decryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(inputString.getBytes(StandardCharsets.UTF_8));
    byte[] decrypted = decryptor.decrypt(encrypted);

    String result = new String(decrypted, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void SM4128Test1() throws IOException {
    PublicBAOS out = new PublicBAOS();
    out.write(inputString.getBytes(StandardCharsets.UTF_8));
    IEncryptor encryptor = new IEncryptor.SM4128Encryptor(key.getBytes(StandardCharsets.UTF_8));
    IDecryptor decryptor = new IDecryptor.SM4128Decryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(out.getBuf(), 0, out.size());
    byte[] decrypted = decryptor.decrypt(encrypted);

    String result = new String(decrypted, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void AES128Test() throws IOException {
    IEncryptor encryptor = new IEncryptor.AES128Encryptor(key.getBytes(StandardCharsets.UTF_8));
    IDecryptor decryptor = new IDecryptor.AES128Decryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(inputString.getBytes(StandardCharsets.UTF_8));
    byte[] decrypted = decryptor.decrypt(encrypted);

    String result = new String(decrypted, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void AES128Test1() throws IOException {
    PublicBAOS out = new PublicBAOS();
    out.write(inputString.getBytes(StandardCharsets.UTF_8));
    IEncryptor encryptor = new IEncryptor.AES128Encryptor(key.getBytes(StandardCharsets.UTF_8));
    IDecryptor decryptor = new IDecryptor.AES128Decryptor(key.getBytes(StandardCharsets.UTF_8));
    byte[] encrypted = encryptor.encrypt(out.getBuf(), 0, out.size());
    byte[] decrypted = decryptor.decrypt(encrypted);

    String result = new String(decrypted, StandardCharsets.UTF_8);
    assertEquals(inputString, result);
  }

  @Test
  public void GetEncryptorTest() {
    IEncryptor encryptor = IEncryptor.getEncryptor("AES128", key.getBytes(StandardCharsets.UTF_8));
    assertEquals(encryptor.getEncryptionType(), EncryptionType.AES128);
    IEncryptor encryptor2 =
        IEncryptor.getEncryptor(EncryptionType.AES128, key.getBytes(StandardCharsets.UTF_8));
    assertEquals(encryptor2.getEncryptionType(), EncryptionType.AES128);
  }

  @Test
  public void GetEncryptorTest2() {
    IEncryptor encryptor = IEncryptor.getEncryptor("SM4128", key.getBytes(StandardCharsets.UTF_8));
    assertEquals(encryptor.getEncryptionType(), EncryptionType.SM4128);
    IEncryptor encryptor2 =
        IEncryptor.getEncryptor(EncryptionType.SM4128, key.getBytes(StandardCharsets.UTF_8));
    assertEquals(encryptor2.getEncryptionType(), EncryptionType.SM4128);
  }
}
