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

#include "common/allocator/alloc_base.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace common {

namespace bitops {
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

FORCE_INLINE int ctz_nonzero(uint64_t v) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ctzll(v);
#elif defined(_MSC_VER)
    unsigned long idx;
    _BitScanForward64(&idx, v);
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
    BitMap()
        : bitmap_(nullptr),
          size_(0),
          init_as_zero_(true),
          has_set_bits_(false) {}
    ~BitMap();
    int init(uint32_t item_size, bool init_as_zero = true,
             AllocModID mod_id = MOD_TSBLOCK);

    FORCE_INLINE void reset() {
        const char initial_char = init_as_zero_ ? 0x00 : 0xFF;
        memset(bitmap_, initial_char, size_);
        has_set_bits_ = !init_as_zero_;
    }

    FORCE_INLINE void set(uint32_t index) {
        uint32_t offset = index >> 3;
        ASSERT(offset < size_);

        char* start_addr = bitmap_ + offset;
        uint8_t bit_mask = get_bit_mask(index);
        *start_addr = (*start_addr) | (bit_mask);
        has_set_bits_ = true;
    }

    FORCE_INLINE void clear(uint32_t index) {
        uint32_t offset = index >> 3;
        ASSERT(offset < size_);

        char* start_addr = bitmap_ + offset;
        uint8_t bit_mask = get_bit_mask(index);
        *start_addr = (*start_addr) & (~bit_mask);
    }

    FORCE_INLINE void clear_all() {
        memset(bitmap_, 0x00, size_);
        has_set_bits_ = false;
    }

    // Copy `bytes` of externally-owned bitmap data into this BitMap's buffer
    // and keep has_set_bits_ in sync. Without this, callers that memcpy
    // directly into get_bitmap() can leave the has_set_bits_ shortcut stale
    // and downstream readers (may_have_set_bits()) will falsely treat the
    // bitmap as empty.
    FORCE_INLINE void copy_from(const char* src, uint32_t bytes) {
        ASSERT(bytes <= size_);
        memcpy(bitmap_, src, bytes);
        // Conservative: assume the caller-provided bitmap can have set bits.
        // We could scan to be precise, but the false-positive only costs a
        // bit of per-cell testing in writers — never silent data loss.
        if (bytes > 0) {
            has_set_bits_ = true;
        }
    }

    FORCE_INLINE bool test(uint32_t index) {
        uint32_t offset = index >> 3;
        ASSERT(offset < size_);

        char* start_addr = bitmap_ + offset;
        uint8_t bit_mask = get_bit_mask(index);
        return (*start_addr & bit_mask);
    }

    FORCE_INLINE uint32_t count_set_bits() const {
        uint32_t count = 0;
        const uint8_t* p = reinterpret_cast<const uint8_t*>(bitmap_);
        for (uint32_t i = 0; i < size_; i++) {
            count += bitops::popcount8(p[i]);
        }
        return count;
    }

    FORCE_INLINE uint32_t next_set_bit(uint32_t from,
                                       uint32_t total_bits) const {
        if (from >= total_bits) return total_bits;
        const uint8_t* p = reinterpret_cast<const uint8_t*>(bitmap_);
        uint32_t byte_idx = from >> 3;
        uint8_t byte_val = p[byte_idx] >> (from & 7);
        if (byte_val) {
            return from + bitops::ctz_nonzero(static_cast<uint32_t>(byte_val));
        }
        const uint32_t byte_end = (total_bits + 7) >> 3;
        for (++byte_idx; byte_idx < byte_end; ++byte_idx) {
            if (p[byte_idx]) {
                uint32_t pos =
                    (byte_idx << 3) +
                    bitops::ctz_nonzero(static_cast<uint32_t>(p[byte_idx]));
                return pos < total_bits ? pos : total_bits;
            }
        }
        return total_bits;
    }

    FORCE_INLINE uint32_t get_size() { return size_; }

    FORCE_INLINE char* get_bitmap() { return bitmap_; }

    // Fast check: returns false only when guaranteed no bits are set.
    // May return true even when no bits are actually set (conservative).
    FORCE_INLINE bool may_have_set_bits() const { return has_set_bits_; }

   private:
    FORCE_INLINE uint8_t get_bit_mask(uint32_t index) {
        return 1 << (index & 7);
    }

   private:
    char* bitmap_;
    uint32_t size_;
    bool init_as_zero_;
    bool has_set_bits_;
};
}  // namespace common

#endif  // COMMON_CONTAINER_BIT_MAP_H
