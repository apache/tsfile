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

// SHA-256 and HMAC-SHA256 (FIPS 180-4 / RFC 2104), used by the S3 request
// signer. Kept in-tree so object-storage support does not add a crypto
// dependency on top of libcurl.

#include "utils/sha256.h"

#include <cstring>

namespace storage {
namespace crypto {

namespace {

const uint32_t K[64] = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu,
    0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u, 0xd807aa98u, 0x12835b01u,
    0x243185beu, 0x550c7dc3u, 0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u,
    0xc19bf174u, 0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu,
    0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau, 0x983e5152u,
    0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u,
    0x06ca6351u, 0x14292967u, 0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu,
    0x53380d13u, 0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
    0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u, 0xd192e819u,
    0xd6990624u, 0xf40e3585u, 0x106aa070u, 0x19a4c116u, 0x1e376c08u,
    0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu,
    0x682e6ff3u, 0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
    0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u};

inline uint32_t rotr(uint32_t value, int bits) {
    return (value >> bits) | (value << (32 - bits));
}

class Sha256Ctx {
   public:
    void reset() {
        state_[0] = 0x6a09e667u;
        state_[1] = 0xbb67ae85u;
        state_[2] = 0x3c6ef372u;
        state_[3] = 0xa54ff53au;
        state_[4] = 0x510e527fu;
        state_[5] = 0x9b05688cu;
        state_[6] = 0x1f83d9abu;
        state_[7] = 0x5be0cd19u;
        total_bytes_ = 0;
        buffer_size_ = 0;
    }

    void update(const unsigned char* data, size_t length) {
        total_bytes_ += length;
        while (length > 0) {
            const size_t room = 64 - buffer_size_;
            const size_t take = length < room ? length : room;
            std::memcpy(buffer_ + buffer_size_, data, take);
            buffer_size_ += take;
            data += take;
            length -= take;
            if (buffer_size_ == 64) {
                transform(buffer_);
                buffer_size_ = 0;
            }
        }
    }

    void finish(unsigned char digest[32]) {
        const uint64_t bit_length = total_bytes_ * 8;
        unsigned char pad = 0x80;
        update(&pad, 1);
        unsigned char zero = 0;
        while (buffer_size_ != 56) {
            update(&zero, 1);
        }
        unsigned char length_bytes[8];
        for (int i = 0; i < 8; i++) {
            length_bytes[i] =
                static_cast<unsigned char>(bit_length >> (56 - 8 * i));
        }
        update(length_bytes, 8);
        for (int i = 0; i < 8; i++) {
            digest[4 * i] = static_cast<unsigned char>(state_[i] >> 24);
            digest[4 * i + 1] = static_cast<unsigned char>(state_[i] >> 16);
            digest[4 * i + 2] = static_cast<unsigned char>(state_[i] >> 8);
            digest[4 * i + 3] = static_cast<unsigned char>(state_[i]);
        }
    }

   private:
    void transform(const unsigned char block[64]) {
        uint32_t w[64];
        for (int i = 0; i < 16; i++) {
            w[i] = (static_cast<uint32_t>(block[4 * i]) << 24) |
                   (static_cast<uint32_t>(block[4 * i + 1]) << 16) |
                   (static_cast<uint32_t>(block[4 * i + 2]) << 8) |
                   static_cast<uint32_t>(block[4 * i + 3]);
        }
        for (int i = 16; i < 64; i++) {
            const uint32_t s0 =
                rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
            const uint32_t s1 =
                rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16] + s0 + w[i - 7] + s1;
        }
        uint32_t a = state_[0];
        uint32_t b = state_[1];
        uint32_t c = state_[2];
        uint32_t d = state_[3];
        uint32_t e = state_[4];
        uint32_t f = state_[5];
        uint32_t g = state_[6];
        uint32_t h = state_[7];
        for (int i = 0; i < 64; i++) {
            const uint32_t s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            const uint32_t ch = (e & f) ^ ((~e) & g);
            const uint32_t temp1 = h + s1 + ch + K[i] + w[i];
            const uint32_t s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            const uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
            const uint32_t temp2 = s0 + maj;
            h = g;
            g = f;
            f = e;
            e = d + temp1;
            d = c;
            c = b;
            b = a;
            a = temp1 + temp2;
        }
        state_[0] += a;
        state_[1] += b;
        state_[2] += c;
        state_[3] += d;
        state_[4] += e;
        state_[5] += f;
        state_[6] += g;
        state_[7] += h;
    }

    uint32_t state_[8];
    uint64_t total_bytes_;
    unsigned char buffer_[64];
    size_t buffer_size_;
};

}  // namespace

void sha256(const void* data, size_t length, unsigned char digest[32]) {
    Sha256Ctx ctx;
    ctx.reset();
    if (length > 0) {
        ctx.update(static_cast<const unsigned char*>(data), length);
    }
    ctx.finish(digest);
}

void hmac_sha256(const unsigned char* key, size_t key_length, const void* data,
                 size_t length, unsigned char digest[32]) {
    unsigned char key_block[64];
    std::memset(key_block, 0, sizeof(key_block));
    if (key_length > sizeof(key_block)) {
        sha256(key, key_length, key_block);
    } else {
        std::memcpy(key_block, key, key_length);
    }
    unsigned char inner_pad[64];
    unsigned char outer_pad[64];
    for (size_t i = 0; i < sizeof(key_block); i++) {
        inner_pad[i] = static_cast<unsigned char>(key_block[i] ^ 0x36);
        outer_pad[i] = static_cast<unsigned char>(key_block[i] ^ 0x5c);
    }
    Sha256Ctx inner;
    inner.reset();
    inner.update(inner_pad, sizeof(inner_pad));
    if (length > 0) {
        inner.update(static_cast<const unsigned char*>(data), length);
    }
    unsigned char inner_digest[32];
    inner.finish(inner_digest);

    Sha256Ctx outer;
    outer.reset();
    outer.update(outer_pad, sizeof(outer_pad));
    outer.update(inner_digest, sizeof(inner_digest));
    outer.finish(digest);
}

std::string to_hex(const unsigned char* data, size_t length) {
    static const char kDigits[] = "0123456789abcdef";
    std::string out;
    out.resize(length * 2);
    for (size_t i = 0; i < length; i++) {
        out[2 * i] = kDigits[data[i] >> 4];
        out[2 * i + 1] = kDigits[data[i] & 0x0F];
    }
    return out;
}

std::string sha256_hex(const std::string& data) {
    unsigned char digest[32];
    sha256(data.data(), data.size(), digest);
    return to_hex(digest, sizeof(digest));
}

}  // namespace crypto
}  // namespace storage
