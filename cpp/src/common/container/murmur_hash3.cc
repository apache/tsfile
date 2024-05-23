/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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
#include "murmur_hash3.h"

namespace common {

/* ================ Murmur128Hash ================ */
// follow Java IoTDB exactly.
int64_t Murmur128Hash::inner_hash(const char *buf, int32_t len, int64_t seed) {
    const int block_count = len >> 4;  // as 128-bit blocks
    int64_t h1 = seed;
    int64_t h2 = seed;
    int64_t c1 = 0x87c37b91114253d5L;
    int64_t c2 = 0x4cf5ad432745937fL;

    for (int i = 0; i < block_count; i++) {
        int64_t k1 = get_block(buf, i * 2);
        int64_t k2 = get_block(buf, i * 2 + 1);
        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
        h1 = rotl64(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;
        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;
        h2 = rotl64(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5;
    }

    int offset = block_count * 16;
    int64_t k1 = 0;
    int64_t k2 = 0;
    switch (len & 15) {
        case 15:
            k2 ^= ((int64_t)(buf[offset + 14])) << 48;
            // fallthrough
        case 14:
            k2 ^= ((int64_t)(buf[offset + 13])) << 40;
            // fallthrough
        case 13:
            k2 ^= ((int64_t)(buf[offset + 12])) << 32;
            // fallthrough
        case 12:
            k2 ^= ((int64_t)(buf[offset + 11])) << 24;
            // fallthrough
        case 11:
            k2 ^= ((int64_t)(buf[offset + 10])) << 16;
            // fallthrough
        case 10:
            k2 ^= ((int64_t)(buf[offset + 9])) << 8;
            // fallthrough
        case 9:
            k2 ^= buf[offset + 8];
            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
            // fallthrough
        case 8:
            k1 ^= ((int64_t)buf[offset + 7]) << 56;
            // fallthrough
        case 7:
            k1 ^= ((int64_t)buf[offset + 6]) << 48;
            // fallthrough
        case 6:
            k1 ^= ((int64_t)buf[offset + 5]) << 40;
            // fallthrough
        case 5:
            k1 ^= ((int64_t)buf[offset + 4]) << 32;
            // fallthrough
        case 4:
            k1 ^= ((int64_t)buf[offset + 3]) << 24;
            // fallthrough
        case 3:
            k1 ^= ((int64_t)buf[offset + 2]) << 16;
            // fallthrough
        case 2:
            k1 ^= ((int64_t)buf[offset + 1]) << 8;
            // fallthrough
        case 1:
            k1 ^= buf[offset];
            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
            break;
        default:  // 0
            // do nothing
            break;
    }

    h1 ^= len;
    h2 ^= len;
    h1 += h2;
    h2 += h1;
    h1 = fmix(h1);
    h2 = fmix(h2);
    h1 += h2;
    h2 += h1;
    return h1 + h2;
}

int64_t Murmur128Hash::get_block(const char *buf, int32_t index) {
    int block_offset = index << 3;
    int64_t res = 0;
    res += ((int64_t)(buf[block_offset + 0] & 0xFF));
    res += (((int64_t)(buf[block_offset + 1] & 0xFF)) << 8);
    res += (((int64_t)(buf[block_offset + 2] & 0xFF)) << 16);
    res += (((int64_t)(buf[block_offset + 3] & 0xFF)) << 24);
    res += (((int64_t)(buf[block_offset + 4] & 0xFF)) << 32);
    res += (((int64_t)(buf[block_offset + 5] & 0xFF)) << 40);
    res += (((int64_t)(buf[block_offset + 6] & 0xFF)) << 48);
    res += (((int64_t)(buf[block_offset + 7] & 0xFF)) << 56);
    return res;
}

}  // end namespace common