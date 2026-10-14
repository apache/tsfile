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
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.metadata.enums.EncryptionType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
          PageCryptoContext.forEncryption(parameter, plaintext.length, plaintext.length, 0, 0);
      PageCryptoContext page1 =
          PageCryptoContext.forEncryption(parameter, plaintext.length, plaintext.length, 1, 0);
      PageCryptoContext anotherChunkPage0 =
          PageCryptoContext.forEncryption(parameter, plaintext.length, plaintext.length, 0, 1);

      byte[] encryptedPage0 = encryptor.encryptPage(plaintext, 0, plaintext.length, page0);
      byte[] encryptedPage1 = encryptor.encryptPage(plaintext, 0, plaintext.length, page1);
      assertArrayEquals(
          plaintext, decryptor.decryptPage(encryptedPage0, 0, encryptedPage0.length, page0));
      assertArrayEquals(
          plaintext, decryptor.decryptPage(encryptedPage1, 0, encryptedPage1.length, page1));

      assertThrows(
          EncryptException.class,
          () -> decryptor.decryptPage(encryptedPage0, 0, encryptedPage0.length, page1));
      assertThrows(
          EncryptException.class,
          () -> decryptor.decryptPage(encryptedPage0, 0, encryptedPage0.length, anotherChunkPage0));

      encryptedPage0[encryptedPage0.length - 1] ^= 1;
      assertThrows(
          EncryptException.class,
          () -> decryptor.decryptPage(encryptedPage0, 0, encryptedPage0.length, page0));

      assertEquals(1, TestAeadEncryptionProvider.getCreateCount());
    } finally {
      parameter.close();
    }
  }

  @Test
  public void testChunkOrdinalAllocationSurvivesCopyAndRecovery() {
    EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    try {
      assertEquals(0, parameter.nextChunkOrdinal());
      try (EncryptParameter copy = parameter.copy()) {
        assertEquals(1, copy.nextChunkOrdinal());
        parameter.resumeAfterChunkOrdinal(10);
        assertEquals(11, copy.nextChunkOrdinal());
      }
      assertEquals(12, parameter.nextChunkOrdinal());
    } finally {
      parameter.close();
    }
  }

  @Test
  public void testRetainedReferencesShareProviderUntilLastClose() {
    EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    int previousCloseCount = TestAeadEncryptionProvider.getCloseCount();
    try (EncryptParameter first = parameter.retain();
        EncryptParameter second = parameter.retain()) {
      assertSame(first.getOrCreateFileEncrypt(), second.getOrCreateFileEncrypt());
      assertEquals(1, TestAeadEncryptionProvider.getCreateCount());
      parameter.close();
      first.close();
      assertEquals(previousCloseCount, TestAeadEncryptionProvider.getCloseCount());
      assertEquals(28, second.getPageBodyOverhead());
      assertThrows(EncryptException.class, parameter::retain);
      assertThrows(EncryptException.class, first::getOrCreateFileEncrypt);
    } finally {
      parameter.close();
    }
    assertEquals(previousCloseCount + 1, TestAeadEncryptionProvider.getCloseCount());
  }

  @Test
  public void testRetainedReferenceCanInitializeAfterOriginalCloses() {
    EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    try (EncryptParameter retained = parameter.retain()) {
      parameter.close();
      byte[] plaintext = {1, 2, 3};
      PageCryptoContext context = PageCryptoContext.forEncryption(retained, 3, 3, 0, 0);
      byte[] encrypted = IEncryptor.getEncryptor(retained).encryptPage(plaintext, 0, 3, context);
      assertArrayEquals(
          plaintext,
          IDecryptor.getDecryptor(retained).decryptPage(encrypted, 0, encrypted.length, context));
      assertEquals(1, TestAeadEncryptionProvider.getCreateCount());
    } finally {
      parameter.close();
    }
  }

  @Test
  public void testConcurrentRetainedOwnersCreateAndCloseProviderOnce() throws Exception {
    EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    List<EncryptParameter> owners = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(4);
    CountDownLatch start = new CountDownLatch(1);
    int previousCloseCount = TestAeadEncryptionProvider.getCloseCount();
    try {
      for (int i = 0; i < 4; i++) {
        owners.add(parameter.retain());
      }
      parameter.close();
      List<Future<IEncrypt>> results = new ArrayList<>();
      for (EncryptParameter owner : owners) {
        results.add(
            executor.submit(
                () -> {
                  try (EncryptParameter reference = owner) {
                    start.await();
                    return reference.getOrCreateFileEncrypt();
                  }
                }));
      }
      start.countDown();
      IEncrypt provider = results.get(0).get(10, TimeUnit.SECONDS);
      for (Future<IEncrypt> result : results) {
        assertSame(provider, result.get(10, TimeUnit.SECONDS));
      }
      assertEquals(1, TestAeadEncryptionProvider.getCreateCount());
      assertEquals(previousCloseCount + 1, TestAeadEncryptionProvider.getCloseCount());
    } finally {
      start.countDown();
      executor.shutdownNow();
      for (EncryptParameter owner : owners) {
        owner.close();
      }
      parameter.close();
    }
  }

  @Test
  public void testCopyStillOwnsAnIndependentProvider() {
    try (EncryptParameter parameter =
            TestAeadEncryptionProvider.createParameter(
                new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
        EncryptParameter copy = parameter.copy()) {
      parameter.getPageBodyOverhead();
      copy.getPageBodyOverhead();
      assertEquals(2, TestAeadEncryptionProvider.getCreateCount());
      parameter.close();
      assertEquals(28, copy.getPageBodyOverhead());
    }
  }

  @Test
  public void testProviderLookupNormalizesIdLikeRegistration() {
    try (EncryptParameter parameter =
            TestAeadEncryptionProvider.createParameter(
                new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
        EncryptParameter nonCanonical =
            EncryptParameter.pageAeadBuilder()
                .key(parameter.getKey())
                .providerId(" " + TestAeadEncryptionProvider.PROVIDER_ID + " ")
                .profileId(parameter.getProfileId())
                .keyId(parameter.getKeyId())
                .keyVersion(parameter.getKeyVersion())
                .wrappedDataKey(parameter.getWrappedDataKey())
                .fileCryptoId(parameter.getFileCryptoId())
                .build()) {
      assertEquals(28, nonCanonical.getPageBodyOverhead());
    }
  }

  @Test
  public void testPageAeadFileContextRequiresSharedAllocatorAndKey() {
    byte[] fileId = new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH];
    EncryptParameter parameter = TestAeadEncryptionProvider.createParameter(new byte[16], fileId);
    EncryptParameter independent = TestAeadEncryptionProvider.createParameter(new byte[16], fileId);
    try (EncryptParameter copy = parameter.copy()) {
      assertTrue(parameter.sharesPageAeadFileContext(copy));
      assertFalse(parameter.sharesPageAeadFileContext(independent));
      copy.getKey()[0] ^= 1;
      assertFalse(parameter.sharesPageAeadFileContext(copy));
    } finally {
      parameter.close();
      independent.close();
    }
  }

  @Test
  public void testAssociatedDataCannotBeModifiedByProvider() {
    EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    try {
      PageCryptoContext context = PageCryptoContext.forEncryption(parameter, 8, 8, 0, 0);
      byte[] expected = context.getAssociatedData();
      byte[] providerCopy = context.getAssociatedData();
      providerCopy[0] ^= 1;
      assertArrayEquals(expected, context.getAssociatedData());
    } finally {
      parameter.close();
    }
  }

  @Test
  public void testCachedAssociatedDataPreservesEncodingAndReferenceLifetime() throws Exception {
    try (EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(new byte[16], new byte[16])) {
      PageCryptoContext.forEncryption(parameter, 100, 80, 0, 0);
      try (EncryptParameter retained = parameter.retain()) {
        parameter.close();
        for (int page = 0; page < 100; page++) {
          ByteArrayOutputStream expected = new ByteArrayOutputStream();
          DataOutputStream output = new DataOutputStream(expected);
          output.writeInt(1);
          for (String field :
              new String[] {
                retained.getProviderId(), retained.getProfileId(),
                retained.getKeyId(), retained.getKeyVersion()
              }) {
            byte[] bytes = field.getBytes(TSFileConfig.STRING_CHARSET);
            output.writeInt(bytes.length);
            output.write(bytes);
          }
          output.writeInt(EncryptParameter.FILE_CRYPTO_ID_LENGTH);
          output.write(retained.getFileCryptoId());
          output.writeLong(7);
          output.writeInt(page);
          output.writeInt(100);
          output.writeInt(80);
          PageCryptoContext context = PageCryptoContext.forEncryption(retained, 100, 80, page, 7);
          assertArrayEquals(expected.toByteArray(), context.getAssociatedData());
          context.getAssociatedData()[0] ^= 1;
          assertArrayEquals(expected.toByteArray(), context.getAssociatedData());
          assertArrayEquals(
              expected.toByteArray(),
              PageCryptoContext.forDecryption(
                      retained, 100, context.getEncryptedPageBodySize(), page, 7)
                  .getAssociatedData());
        }
      }
    }
  }

  @Test
  public void testLegacyCipherCannotSilentlyHandleAuthenticatedPages() {
    EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    try {
      PageCryptoContext context = PageCryptoContext.forEncryption(parameter, 1, 1, 0, 0);
      IEncryptor legacyEncryptor =
          new IEncryptor() {
            @Override
            public byte[] encrypt(byte[] data) {
              return data;
            }

            @Override
            public byte[] encrypt(byte[] data, int offset, int size) {
              throw new AssertionError("Legacy encryption must not be called for AEAD pages");
            }

            @Override
            public EncryptionType getEncryptionType() {
              return EncryptionType.NewWay;
            }
          };
      IDecryptor legacyDecryptor =
          new IDecryptor() {
            @Override
            public byte[] decrypt(byte[] data) {
              return data;
            }

            @Override
            public byte[] decrypt(byte[] data, int offset, int size) {
              throw new AssertionError("Legacy decryption must not be called for AEAD pages");
            }

            @Override
            public EncryptionType getEncryptionType() {
              return EncryptionType.NewWay;
            }
          };
      byte[] page = {1};
      assertTrue(
          assertThrows(
                  EncryptException.class, () -> legacyEncryptor.encryptPage(page, 0, 1, context))
              .getMessage()
              .contains(legacyEncryptor.getClass().getName()));
      assertTrue(
          assertThrows(
                  EncryptException.class, () -> legacyDecryptor.decryptPage(page, 0, 1, context))
              .getMessage()
              .contains(legacyDecryptor.getClass().getName()));
    } finally {
      parameter.close();
    }
  }

  @Test
  public void testUnregisterProviderTrimsId() {
    EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(
            new byte[16], new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH]);
    try {
      EncryptionProviderRegistry.create(parameter);
      EncryptionProviderRegistry.unregisterProvider(
          " " + TestAeadEncryptionProvider.PROVIDER_ID + " ");
      assertThrows(EncryptException.class, () -> EncryptionProviderRegistry.create(parameter));
    } finally {
      EncryptionProviderRegistry.registerProvider(TestAeadEncryptionProvider.INSTANCE);
      parameter.close();
    }
  }
}
