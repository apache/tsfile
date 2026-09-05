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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class PageCryptoContextTest {

  @BeforeClass
  public static void setUpProvider() {
    EncryptionProviderRegistry.registerProvider(TestAeadEncryptionProvider.INSTANCE);
  }

  @AfterClass
  public static void tearDownProvider() {
    EncryptionProviderRegistry.unregisterProvider(TestAeadEncryptionProvider.PROVIDER_ID);
  }

  @Before
  public void resetProviderCount() {
    TestAeadEncryptionProvider.resetCreateCount();
  }

  @Test
  public void testAeadRejectsTamperingAndPageSwap() {
    byte[] dataKey = new byte[16];
    Arrays.fill(dataKey, (byte) 0x5A);
    byte[] fileCryptoId = new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH];
    Arrays.fill(fileCryptoId, (byte) 0x2C);
    EncryptParameter parameter = TestAeadEncryptionProvider.createParameter(dataKey, fileCryptoId);
    byte[] plaintext = new byte[] {1, 3, 5, 7, 9, 11, 13, 15};

    try {
      IEncryptor encryptor = IEncryptor.getEncryptor(parameter);
      IDecryptor decryptor = IDecryptor.getDecryptor(parameter);
      PageCryptoContext page0 =
          PageCryptoContext.forEncryption(parameter, plaintext.length, plaintext.length, 0);
      PageCryptoContext page1 =
          PageCryptoContext.forEncryption(parameter, plaintext.length, plaintext.length, 1);

      byte[] encryptedPage0 = encryptor.encryptPage(plaintext, 0, plaintext.length, page0);
      byte[] encryptedPage1 = encryptor.encryptPage(plaintext, 0, plaintext.length, page1);
      assertArrayEquals(
          plaintext, decryptor.decryptPage(encryptedPage0, 0, encryptedPage0.length, page0));
      assertArrayEquals(
          plaintext, decryptor.decryptPage(encryptedPage1, 0, encryptedPage1.length, page1));

      assertThrows(
          EncryptException.class,
          () -> decryptor.decryptPage(encryptedPage0, 0, encryptedPage0.length, page1));

      encryptedPage0[encryptedPage0.length - 1] ^= 1;
      assertThrows(
          EncryptException.class,
          () -> decryptor.decryptPage(encryptedPage0, 0, encryptedPage0.length, page0));

      assertEquals(1, TestAeadEncryptionProvider.getCreateCount());
    } finally {
      parameter.close();
    }
  }
}
