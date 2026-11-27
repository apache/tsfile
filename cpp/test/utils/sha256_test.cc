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

#include "utils/sha256.h"

#include <gtest/gtest.h>

#include <string>

namespace {

using storage::crypto::hmac_sha256;
using storage::crypto::sha256_hex;
using storage::crypto::to_hex;

TEST(Sha256Test, KnownDigests) {
    EXPECT_EQ(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        sha256_hex(""));
    EXPECT_EQ(
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        sha256_hex("abc"));
    EXPECT_EQ(
        "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592",
        sha256_hex("The quick brown fox jumps over the lazy dog"));
}

TEST(Sha256Test, LongInputCrossesBlockBoundaries) {
    // 1 MiB of 'a' exercises multi-block updates and the length padding.
    std::string data(1024 * 1024, 'a');
    unsigned char digest[32];
    storage::crypto::sha256(data.data(), data.size(), digest);
    // Independently confirmed (sha256sum of the same input).
    EXPECT_EQ(
        "9bc1b2a288b26af7257a36277ae3816a7d4f16e89c1e7e77d0a5c48bad62b360",
        to_hex(digest, sizeof(digest)));
}

TEST(Sha256Test, HmacRfc4231TestCase2) {
    const std::string key = "Jefe";
    const std::string data = "what do ya want for nothing?";
    unsigned char digest[32];
    hmac_sha256(reinterpret_cast<const unsigned char*>(key.data()), key.size(),
                data.data(), data.size(), digest);
    EXPECT_EQ(
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
        to_hex(digest, sizeof(digest)));
}

TEST(Sha256Test, HmacLongKeyIsHashed) {
    // RFC 4231 Test Case 6: 131-byte key.
    const std::string key(131, 'a');
    const std::string data =
        "Test Using Larger Than Block-Size Key - Hash Key First";
    unsigned char digest[32];
    hmac_sha256(reinterpret_cast<const unsigned char*>(key.data()), key.size(),
                data.data(), data.size(), digest);
    EXPECT_EQ(
        "d021303c2f34bf3ae541b4ec9c672691694c6f71c5c0c55781545a66115b14a7",
        to_hex(digest, sizeof(digest)));
}

}  // namespace
