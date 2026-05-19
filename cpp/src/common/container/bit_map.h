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
#ifndef COMMON_CONTAINER_BIT_MAP_H
#define COMMON_CONTAINER_BIT_MAP_H

#include <string.h>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace common {

// Cross-platform bit-twiddling helpers. GCC/Clang use their builtins; MSVC
// uses the equivalent intrinsics from <intrin.h>; any other compiler falls
// back to a portable loop.
namespace bitops {
// Population count of an 8-bit value.
FORCE_INLINE int popcount8(uint8_t v) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_popcount(v);
#elif defined(_MSC_VER)
    return static_cast<int>(__popcnt(static_cast<unsigned int>(v)));
#else
    int c = 0;
    while (v) {
        v = static_cast<uint8_t>(v & (v - 1));
        ++c;
    }
    return c;
#endif
}
// Count trailing zero bits. The argument must be non-zero.
FORCE_INLINE int ctz_nonzero(uint32_t v) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ctz(v);
#elif defined(_MSC_VER)
    unsigned long idx;
    _BitScanForward(&idx, v);
    return static_cast<int>(idx);
#else
    int c = 0;
    while (!(v & 1u)) {
        v >>= 1;
        ++c;
    }
    return c;
#endif
}
// Count trailing zero bits of a 64-bit value. The argument must be non-zero.
FORCE_INLINE int ctz64_nonzero(uint64_t v) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ctzll(v);
#elif defined(_MSC_VER)
    unsigned long idx;
#if defined(_M_X64) || defined(_M_ARM64)
    _BitScanForward64(&idx, v);
#else
    // 32-bit MSVC has no _BitScanForward64.
    if (static_cast<uint32_t>(v) != 0) {
        _BitScanForward(&idx, static_cast<uint32_t>(v));
    } else {
        _BitScanForward(&idx, static_cast<uint32_t>(v >> 32));
        idx += 32;
    }
#endif
    return static_cast<int>(idx);
#else
    int c = 0;
    while (!(v & 1ull)) {
        v >>= 1;
        ++c;
    }
    return c;
#endif
}
}  // namespace bitops

class BitMap {
   public:
    BitMap() : bitmap_(nullptr), size_(0), init_as_zero_(true) {}
    ~BitMap();
    int init(uint32_t item_size, bool init_as_zero = true);

    FORCE_INLINE void reset() {
        const char initial_char = init_as_zero_ ? 0x00 : 0xFF;
        memset(bitmap_, initial_char, size_);
    }

    FORCE_INLINE void set(uint32_t index) {
        uint32_t offset = index >> 3;
        ASSERT(offset < size_);

        char* start_addr = bitmap_ + offset;
        uint8_t bit_mask = get_bit_mask(index);
        *start_addr = (*start_addr) | (bit_mask);
    }

    FORCE_INLINE void clear(uint32_t index) {
        uint32_t offset = index >> 3;
        ASSERT(offset < size_);

        char* start_addr = bitmap_ + offset;
        uint8_t bit_mask = get_bit_mask(index);
        *start_addr = (*start_addr) & (~bit_mask);
    }

    FORCE_INLINE void clear_all() { memset(bitmap_, 0x00, size_); }

    FORCE_INLINE bool test(uint32_t index) {
        uint32_t offset = index >> 3;
        ASSERT(offset < size_);

        char* start_addr = bitmap_ + offset;
        uint8_t bit_mask = get_bit_mask(index);
        return (*start_addr & bit_mask);
    }

    // Count the number of bits set to 1 (i.e., number of null entries).
    FORCE_INLINE uint32_t count_set_bits() const {
        uint32_t count = 0;
        const uint8_t* p = reinterpret_cast<const uint8_t*>(bitmap_);
        for (uint32_t i = 0; i < size_; i++) {
            count += bitops::popcount8(p[i]);
        }
        return count;
    }

    // Find the next set bit (null position) at or after @from,
    // within [0, total_bits). Returns total_bits if none found.
    // Skips zero bytes in bulk so cost is proportional to the number
    // of null bytes, not total rows.
    FORCE_INLINE uint32_t next_set_bit(uint32_t from,
                                       uint32_t total_bits) const {
        if (from >= total_bits) return total_bits;
        const uint8_t* p = reinterpret_cast<const uint8_t*>(bitmap_);
        uint32_t byte_idx = from >> 3;
        // Check remaining bits in the first (partial) byte
        uint8_t byte_val = p[byte_idx] >> (from & 7);
        if (byte_val) {
            return from + bitops::ctz_nonzero(byte_val);
        }
        // Scan subsequent full bytes, skipping zeros
        const uint32_t byte_end = (total_bits + 7) >> 3;
        for (++byte_idx; byte_idx < byte_end; ++byte_idx) {
            if (p[byte_idx]) {
                uint32_t pos =
                    (byte_idx << 3) + bitops::ctz_nonzero(p[byte_idx]);
                return pos < total_bits ? pos : total_bits;
            }
        }
        return total_bits;
    }

    FORCE_INLINE uint32_t get_size() { return size_; }

    FORCE_INLINE char* get_bitmap() { return bitmap_; }

   private:
    FORCE_INLINE uint8_t get_bit_mask(uint32_t index) {
        return 1 << (index & 7);
    }

   private:
    char* bitmap_;
    uint32_t size_;
    bool init_as_zero_;
};
}  // namespace common

#endif  // COMMON_CONTAINER_BIT_MAP_H
