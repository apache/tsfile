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
import org.apache.tsfile.i18n.Messages;

import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Registry for file-level encryption providers identified by stable provider ids. */
public final class EncryptionProviderRegistry {

  private static final ConcurrentHashMap<String, IEncryptProvider> PROVIDERS =
      new ConcurrentHashMap<>();
  private static final AtomicBoolean SERVICES_LOADED = new AtomicBoolean();

  private EncryptionProviderRegistry() {}

  public static void registerProvider(IEncryptProvider provider) {
    String providerId = provider == null ? null : provider.getProviderId();
    if (providerId == null || providerId.trim().isEmpty()) {
      throw new EncryptException(Messages.get("error.encrypt.invalid_provider_id"));
    }
    providerId = providerId.trim();
    IEncryptProvider existing = PROVIDERS.putIfAbsent(providerId, provider);
    if (existing != null && !existing.getClass().equals(provider.getClass())) {
      throw new EncryptException(Messages.format("error.encrypt.duplicate_provider", providerId));
    }
  }

  public static void unregisterProvider(String providerId) {
    if (providerId != null) {
      PROVIDERS.remove(providerId);
    }
  }

  static IEncrypt create(EncryptParameter encryptParameter) {
    loadServices();
    String providerId = encryptParameter.getProviderId();
    IEncryptProvider provider = providerId == null ? null : PROVIDERS.get(providerId);
    if (provider == null) {
      throw new EncryptException(Messages.format("error.encrypt.provider_not_found", providerId));
    }
    IEncrypt encrypt = provider.create(encryptParameter);
    if (encrypt == null) {
      throw new EncryptException(
          Messages.format("error.encrypt.provider_returned_null", providerId));
    }
    return encrypt;
  }

  private static void loadServices() {
    if (SERVICES_LOADED.compareAndSet(false, true)) {
      for (IEncryptProvider provider : ServiceLoader.load(IEncryptProvider.class)) {
        registerProvider(provider);
      }
    }
  }
}
