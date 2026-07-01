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
#ifndef ENCODING_ENCODE_UTILS_H
#define ENCODING_ENCODE_UTILS_H

#include "utils/util_define.h"

namespace storage {

FORCE_INLINE int32_t number_of_leading_zeros(int32_t i) {
    if (i == 0) {
        return 32;
    }
    int32_t n = 1;
    if (((uint32_t)i) >> 16 == 0) {
        n += 16;
        i <<= 16;
    }
    if (((uint32_t)i) >> 24 == 0) {
        n += 8;
        i <<= 8;
    }
    if (((uint32_t)i) >> 28 == 0) {
        n += 4;
        i <<= 4;
    }
    if (((uint32_t)i) >> 30 == 0) {
        n += 2;
        i <<= 2;
    }
    n -= ((uint32_t)i) >> 31;
    return n;
}

FORCE_INLINE int32_t number_of_trailing_zeros(int32_t i) {
    if (i == 0) {
        return 32;
    }
    uint32_t x = static_cast<uint32_t>(i);
    int32_t n = 31;
    uint32_t y;
    y = x << 16;
    if (y != 0) {
        n -= 16;
        x = y;
    }

    y = x << 8;
    if (y != 0) {
        n -= 8;
        x = y;
    }

    y = x << 4;
    if (y != 0) {
        n -= 4;
        x = y;
    }

    y = x << 2;
    if (y != 0) {
        n -= 2;
        x = y;
    }
    return n - static_cast<int32_t>((x << 1) >> 31);
}

FORCE_INLINE int get_int32_max_bit_width(const std::vector<int32_t>& nums) {
    int ret = 1;
    for (auto num : nums) {
        int bit_width = 32 - number_of_leading_zeros(num);
        ret = std::max(ret, bit_width);
    }
    return ret;
}

FORCE_INLINE int32_t number_of_leading_zeros(int64_t i) {
    if (i == 0) {
        return 64;
    }
    int32_t n = 1;
    int32_t x = (int32_t)(((uint64_t)i) >> 32);
    if (x == 0) {
        n += 32;
        x = (int32_t)i;
    }
    if (((uint32_t)x) >> 16 == 0) {
        n += 16;
        x <<= 16;
    }
    if (((uint32_t)x) >> 24 == 0) {
        n += 8;
        x <<= 8;
    }
    if (((uint32_t)x) >> 28 == 0) {
        n += 4;
        x <<= 4;
    }
    if (((uint32_t)x) >> 30 == 0) {
        n += 2;
        x <<= 2;
    }
    n -= ((uint32_t)x) >> 31;
    return n;
}

FORCE_INLINE int32_t number_of_trailing_zeros(int64_t i) {
    if (i == 0) return 64;
    uint32_t x, y;
    int32_t n = 63;
    y = static_cast<uint32_t>(i);
    if (y != 0) {
        n -= 32;
        x = y;
    } else {
        x = static_cast<uint32_t>(static_cast<uint64_t>(i) >> 32);
    }
    y = x << 16;
    if (y) {
        n -= 16;
        x = y;
    }
    y = x << 8;
    if (y) {
        n -= 8;
        x = y;
    }
    y = x << 4;
    if (y) {
        n -= 4;
        x = y;
    }
    y = x << 2;
    if (y) {
        n -= 2;
        x = y;
    }
    return n - static_cast<int32_t>((x << 1) >> 31);
}

FORCE_INLINE int get_int64_max_bit_width(const std::vector<int64_t>& nums) {
    int ret = 1;
    for (auto num : nums) {
        int bit_width = 64 - number_of_leading_zeros(num);
        ret = std::max(ret, bit_width);
    }
    return ret;
}

}  // end namespace storage
#endif  // ENCODING_ENCODE_UTILS_H
