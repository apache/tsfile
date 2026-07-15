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
 */
public interface IEncryptProvider {

  String getProviderId();

  IEncrypt create(EncryptParameter encryptParameter);
}
