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
package org.apache.tsfile.custom.encrypt;

import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.encrypt.IEncrypt;
import org.apache.tsfile.encrypt.IEncryptor;
import org.apache.tsfile.encrypt.UNENCRYPTED;
import org.apache.tsfile.exception.encrypt.EncryptException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class ExternalEncryptTest {
  private static final String ALIAS = "EXTERNAL_TEST_ENCRYPTION";
  private static boolean nonEncryptInitialized;
  private static boolean nonEncryptConstructed;
  private final byte[] key = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);
  private final byte[] data = "external encryption implementation".getBytes(StandardCharsets.UTF_8);

  @Before
  public void setUp() {
    clearMappings();
    nonEncryptInitialized = false;
    nonEncryptConstructed = false;
  }

  @After
  public void tearDown() {
    clearMappings();
  }

  private void clearMappings() {
    IEncrypt.encryptTypeToClassMap.remove(ALIAS);
    IEncrypt.encryptTypeToClassMap.remove(ExternalEncrypt.class.getName());
    IEncrypt.encryptTypeToClassMap.remove(NonEncrypt.class.getName());
    IEncrypt.encryptMap.remove(ExternalEncrypt.class.getName());
    IEncrypt.encryptMap.remove(NonEncrypt.class.getName());
  }

  @Test
  public void testFullyQualifiedExternalImplementation() {
    assertFactoriesAccept(ExternalEncrypt.class.getName());
  }

  @Test
  public void testRegisteredExternalImplementation() {
    IEncrypt.encryptTypeToClassMap.put(ALIAS, ExternalEncrypt.class.getName());
    assertFactoriesAccept(ALIAS);
    assertEquals(ExternalEncrypt.class.getName(), IEncrypt.encryptTypeToClassMap.get(ALIAS));
  }

  private void assertFactoriesAccept(String type) {
    assertEquals(ExternalEncrypt.class.getName(), EncryptUtils.getEncryptClass(type));
    IEncrypt encrypt = EncryptUtils.getEncrypt(type, key);
    assertEquals(ExternalEncrypt.class, encrypt.getClass());
    assertArrayEquals(data, encrypt.getDecryptor().decrypt(encrypt.getEncryptor().encrypt(data)));

    // Exercise each factory's validation path without a cached constructor.
    IEncrypt.encryptMap.remove(ExternalEncrypt.class.getName());
    assertArrayEquals(data, IEncryptor.getEncryptor(type, key).encrypt(data));
    IEncrypt.encryptMap.remove(ExternalEncrypt.class.getName());
    assertArrayEquals(data, IDecryptor.getDecryptor(type, key).decrypt(data));
  }

  @Test
  public void testRejectsExternalNonEncryptBeforeInitialization() {
    assertFactoriesReject(NonEncrypt.class.getName());
  }

  @Test
  public void testRejectsRegisteredNonEncryptBeforeInitialization() {
    IEncrypt.encryptTypeToClassMap.put(ALIAS, NonEncrypt.class.getName());
    assertFactoriesReject(ALIAS);
  }

  private void assertFactoriesReject(String type) {
    assertThrows(EncryptException.class, () -> EncryptUtils.getEncrypt(type, key));
    assertThrows(EncryptException.class, () -> IEncryptor.getEncryptor(type, key));
    assertThrows(EncryptException.class, () -> IDecryptor.getDecryptor(type, key));
    assertFalse(nonEncryptInitialized);
    assertFalse(nonEncryptConstructed);
    assertFalse(IEncrypt.encryptMap.containsKey(NonEncrypt.class.getName()));
  }

  public static class ExternalEncrypt extends UNENCRYPTED {
    public ExternalEncrypt(byte[] key) {
      super(key);
    }
  }

  public static class NonEncrypt {
    static {
      nonEncryptInitialized = true;
    }

    public NonEncrypt(byte[] key) {
      nonEncryptConstructed = true;
    }
  }
}
