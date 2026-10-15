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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class EncryptionProviderRegistryTest {
  private static final String SLOW_ID = "registry-slow-provider";
  private static final String GOOD_ID = "registry-good-provider";
  private static final String BAD_ID = "registry-retry-provider";
  private static volatile CountDownLatch entered;
  private static volatile CountDownLatch release;
  private static volatile boolean failConstruction;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    resetDiscovery();
    entered = new CountDownLatch(1);
    release = new CountDownLatch(1);
    failConstruction = false;
  }

  @After
  public void tearDown() throws Exception {
    release.countDown();
    for (String id : new String[] {SLOW_ID, GOOD_ID, BAD_ID}) {
      EncryptionProviderRegistry.unregisterProvider(id);
    }
    resetDiscovery();
  }

  private static void resetDiscovery() throws Exception {
    Field field = EncryptionProviderRegistry.class.getDeclaredField("servicesLoaded");
    field.setAccessible(true);
    field.setBoolean(null, false);
  }

  @Test
  public void testConcurrentFirstUseWaitsForServiceRegistration() throws Exception {
    try (URLClassLoader loader = serviceLoader(SlowProvider.class)) {
      ExecutorService pool = Executors.newFixedThreadPool(2);
      CountDownLatch secondStarted = new CountDownLatch(1);
      try {
        Future<?> first = pool.submit(() -> createThrough(loader, SLOW_ID));
        assertTrue(entered.await(10, TimeUnit.SECONDS));
        Future<?> second =
            pool.submit(
                () -> {
                  secondStarted.countDown();
                  createThrough(loader, SLOW_ID);
                });
        assertTrue(secondStarted.await(10, TimeUnit.SECONDS));
        Thread.sleep(100);
        assertFalse(second.isDone());
        release.countDown();
        first.get(10, TimeUnit.SECONDS);
        second.get(10, TimeUnit.SECONDS);
      } finally {
        release.countDown();
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  public void testFailedDiscoveryIsRetryableAndContinuesRegistration() throws Exception {
    failConstruction = true;
    try (URLClassLoader loader = serviceLoader(RetryProvider.class, GoodProvider.class)) {
      assertThrows(EncryptException.class, () -> createThrough(loader, GOOD_ID));
      Field field = EncryptionProviderRegistry.class.getDeclaredField("PROVIDERS");
      field.setAccessible(true);
      assertTrue(((java.util.Map<?, ?>) field.get(null)).containsKey(GOOD_ID));
      failConstruction = false;
      createThrough(loader, GOOD_ID);
      createThrough(loader, BAD_ID);
    }
  }

  @Test
  public void testUnsupportedChunkAuthenticationNamesImplementation() {
    IEncrypt legacy = new UNENCRYPTED(null);
    try (legacy) {
      assertTrue(
          assertThrows(EncryptException.class, () -> legacy.authenticateChunk(null))
              .getMessage()
              .contains(legacy.getClass().getName()));
      assertTrue(
          assertThrows(
                  EncryptException.class, () -> legacy.verifyChunkAuthentication(new byte[0], null))
              .getMessage()
              .contains(legacy.getClass().getName()));
    }
  }

  @Test
  public void testLegacyOperationsAreRejectedEvenForPermissiveProvider() {
    IEncrypt permissive = new UNENCRYPTED(null);
    try (IEncrypt guarded = new PageAeadEncryption(permissive)) {
      assertThrows(EncryptException.class, () -> guarded.getEncryptor().encrypt(new byte[1]));
      assertThrows(EncryptException.class, () -> guarded.getEncryptor().encrypt(new byte[1], 0, 1));
      assertThrows(EncryptException.class, () -> guarded.getDecryptor().decrypt(new byte[1]));
      assertThrows(EncryptException.class, () -> guarded.getDecryptor().decrypt(new byte[1], 0, 1));
    }
  }

  private URLClassLoader serviceLoader(Class<?>... providers) throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Path descriptor = root.resolve("META-INF/services/" + IEncryptProvider.class.getName());
    Files.createDirectories(descriptor.getParent());
    StringBuilder names = new StringBuilder();
    for (Class<?> provider : providers) {
      names.append(provider.getName()).append('\n');
    }
    Files.write(descriptor, names.toString().getBytes(StandardCharsets.UTF_8));
    return new URLClassLoader(
        new java.net.URL[] {root.toUri().toURL()}, getClass().getClassLoader());
  }

  private static void createThrough(ClassLoader loader, String providerId) {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    try (EncryptParameter parameter =
            EncryptParameter.pageAeadBuilder()
                .key(new byte[16])
                .providerId(providerId)
                .profileId(TestAeadEncryptionProvider.PROFILE_ID)
                .keyId("test-key")
                .keyVersion("1")
                .wrappedDataKey(new byte[24])
                .fileCryptoId(new byte[16])
                .build();
        IEncrypt encrypt = EncryptionProviderRegistry.create(parameter)) {
      assertEquals(28, encrypt.getPageBodyOverhead());
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  public static class SlowProvider implements IEncryptProvider {
    public SlowProvider() {
      entered.countDown();
      try {
        if (!release.await(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Service registration probe timed out");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }

    @Override
    public String getProviderId() {
      return SLOW_ID;
    }

    @Override
    public IEncrypt create(EncryptParameter parameter) {
      return TestAeadEncryptionProvider.INSTANCE.create(parameter);
    }
  }

  public static class GoodProvider implements IEncryptProvider {
    @Override
    public String getProviderId() {
      return GOOD_ID;
    }

    @Override
    public IEncrypt create(EncryptParameter parameter) {
      return TestAeadEncryptionProvider.INSTANCE.create(parameter);
    }
  }

  public static class RetryProvider extends GoodProvider {
    public RetryProvider() {
      if (failConstruction) {
        throw new IllegalStateException("Injected service discovery failure");
      }
    }

    @Override
    public String getProviderId() {
      return BAD_ID;
    }
  }
}
