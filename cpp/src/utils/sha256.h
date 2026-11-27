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

#ifndef UTILS_SHA256_H
#define UTILS_SHA256_H

#include <stddef.h>
#include <stdint.h>

#include <string>

namespace storage {
namespace crypto {

/** Compute the SHA-256 digest of @p data into @p digest (32 bytes). */
void sha256(const void* data, size_t length, unsigned char digest[32]);

/** Compute HMAC-SHA256 of @p data under @p key into @p digest (32 bytes). */
void hmac_sha256(const unsigned char* key, size_t key_length, const void* data,
                 size_t length, unsigned char digest[32]);

/** Lower-case hex encoding of @p length bytes. */
std::string to_hex(const unsigned char* data, size_t length);

/** Convenience: hex SHA-256 of a string. */
std::string sha256_hex(const std::string& data);

}  // namespace crypto
}  // namespace storage

#endif  // UTILS_SHA256_H
