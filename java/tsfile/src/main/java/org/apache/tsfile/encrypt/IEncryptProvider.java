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

/**
 * Service provider interface for file-level encryption implementations.
 *
 * <p>Provider ids are persisted in a TsFile and therefore must remain stable across releases. A
 * provider implementation may be registered explicitly through {@link EncryptionProviderRegistry}
 * or discovered through {@link java.util.ServiceLoader}. Providers own the encryption profiles
 * referenced by {@link EncryptParameter#getProfileId()}; a profile's algorithms, page layout, key
 * envelope format, and page body overhead must remain immutable while files using it exist.
 *
 * <p>The caller must generate an independent DEK and fileCryptoId for every new file. An
 * EncryptParameter, including its copies, describes one file, not a reusable encryption template.
 * Providers must ensure that nonces are not reused under the same DEK, including after append,
 * recovery, or recreation of a runtime context. PageCryptoContext is authenticated context, not a
 * guaranteed-unique nonce source. Random-nonce profiles must define per-key usage limits and reject
 * use beyond those limits, counting both page encryption and chunk position authentication.
 * Version-1 encrypted chunk headers also require the fixed-size opaque position proof defined by
 * {@link ChunkCryptoContext}; unsupported providers must reject these operations.
 */
public interface IEncryptProvider {

  String getProviderId();

  /**
   * Creates a file context that may be shared by concurrent chunk readers. Implementations must own
   * their runtime key resources independently of the supplied parameter's lifetime. TsFile closes
   * the context after its last retained reference is released.
   */
  IEncrypt create(EncryptParameter encryptParameter);
}
