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

#ifndef ENCODING_PLAIN_DECODER_H
#define ENCODING_PLAIN_DECODER_H

#include <algorithm>
#include <cstdint>
#include <cstring>

#if defined(_MSC_VER)
#include <intrin.h>
#include <stdlib.h>
#endif

#include "encoding/decoder.h"

namespace storage {

FORCE_INLINE uint32_t plain_bswap32(uint32_t v) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap32(v);
#elif defined(_MSC_VER)
    return _byteswap_ulong(v);
#else
    return ((v & 0x000000FFu) << 24) | ((v & 0x0000FF00u) << 8) |
           ((v & 0x00FF0000u) >> 8) | ((v & 0xFF000000u) >> 24);
#endif
}

FORCE_INLINE uint64_t plain_bswap64(uint64_t v) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap64(v);
#elif defined(_MSC_VER)
    return _byteswap_uint64(v);
#else
    return ((v & 0x00000000000000FFull) << 56) |
           ((v & 0x000000000000FF00ull) << 40) |
           ((v & 0x0000000000FF0000ull) << 24) |
           ((v & 0x00000000FF000000ull) << 8) |
           ((v & 0x000000FF00000000ull) >> 8) |
           ((v & 0x0000FF0000000000ull) >> 24) |
           ((v & 0x00FF000000000000ull) >> 40) |
           ((v & 0xFF00000000000000ull) >> 56);
#endif
}

class PlainDecoder : public Decoder {
   public:
    ~PlainDecoder() override = default;
    FORCE_INLINE void reset() override { /* do nothing */
    }
    FORCE_INLINE bool has_remaining(const common::ByteStream& buffer) override {
        return buffer.has_remaining();
    }
    FORCE_INLINE int read_boolean(bool& ret_bool,
                                  common::ByteStream& in) override {
        return common::SerializationUtil::read_ui8((uint8_t&)ret_bool, in);
    }

    FORCE_INLINE int read_int32(int32_t& ret_int32,
                                common::ByteStream& in) override {
        return common::SerializationUtil::read_var_int(ret_int32, in);
    }

    FORCE_INLINE int read_int64(int64_t& ret_int64,
                                common::ByteStream& in) override {
        return common::SerializationUtil::read_i64(ret_int64, in);
    }

    FORCE_INLINE int read_float(float& ret_float,
                                common::ByteStream& in) override {
        return common::SerializationUtil::read_float(ret_float, in);
    }

    FORCE_INLINE int read_double(double& ret_double,
                                 common::ByteStream& in) override {
        return common::SerializationUtil::read_double(ret_double, in);
    }

    FORCE_INLINE int read_String(common::String& ret_String,
                                 common::PageArena& pa,
                                 common::ByteStream& in) override {
        return common::SerializationUtil::read_mystring(ret_String, &pa, in);
    }

    // ── Batch overrides ──────────────────────────────────────────────────────
    //
    // INT32: PLAIN encoding uses varint (variable stride).  Override to avoid
    // virtual dispatch per element; actual decode is still per-value.
    int read_batch_int32(int32_t* out, int capacity, int& actual,
                         common::ByteStream& in) override {
        actual = 0;
        while (actual < capacity && in.has_remaining()) {
            int ret = common::SerializationUtil::read_var_int(out[actual], in);
            if (ret != common::E_OK) return ret;
            ++actual;
        }
        return common::E_OK;
    }

    int skip_int32(int count, int& skipped, common::ByteStream& in) override {
        skipped = 0;
        int32_t dummy;
        while (skipped < count && in.has_remaining()) {
            int ret = common::SerializationUtil::read_var_int(dummy, in);
            if (ret != common::E_OK) {
                return ret;
            }
            ++skipped;
        }
        return common::E_OK;
    }

    // Fixed-stride INT64 / FLOAT / DOUBLE share the same shape: when the
    // ByteStream is wrapped (contiguous buf), advance the read pointer in one
    // step and byte-swap in place; otherwise fall back to per-value reads.
    // The macros below expand into one override per type.
#define PLAIN_SKIP_FIXED(NAME, T, STRIDE, READ_ONE)                         \
    int NAME(int count, int& skipped, common::ByteStream& in) override {    \
        skipped = 0;                                                        \
        if (!in.is_wrapped()) {                                             \
            T dummy;                                                        \
            while (skipped < count && in.has_remaining()) {                 \
                int ret = READ_ONE(dummy, in);                              \
                if (ret != common::E_OK) {                                  \
                    return ret;                                             \
                }                                                           \
                ++skipped;                                                  \
            }                                                               \
            return common::E_OK;                                            \
        }                                                                   \
        skipped = static_cast<int>(std::min<uint32_t>(                      \
            in.remaining_size() / (STRIDE), static_cast<uint32_t>(count))); \
        if (skipped <= 0) {                                                 \
            skipped = 0;                                                    \
            return common::E_OK;                                            \
        }                                                                   \
        in.wrapped_buf_advance_read_pos(static_cast<uint32_t>(skipped) *    \
                                        (STRIDE));                          \
        return common::E_OK;                                                \
    }

#define PLAIN_READ_BATCH_FIXED(NAME, T, U, STRIDE, READ_ONE, BSWAP)            \
    int NAME(T* out, int capacity, int& actual, common::ByteStream& in)        \
        override {                                                             \
        actual = 0;                                                            \
        if (!in.is_wrapped()) {                                                \
            while (actual < capacity && in.has_remaining()) {                  \
                int ret = READ_ONE(out[actual], in);                           \
                if (ret != common::E_OK) {                                     \
                    return ret;                                                \
                }                                                              \
                ++actual;                                                      \
            }                                                                  \
            return common::E_OK;                                               \
        }                                                                      \
        int n = static_cast<int>(std::min<uint32_t>(                           \
            in.remaining_size() / (STRIDE), static_cast<uint32_t>(capacity))); \
        if (n <= 0) {                                                          \
            return common::E_OK;                                               \
        }                                                                      \
        const uint8_t* src =                                                   \
            (const uint8_t*)in.get_wrapped_buf() + in.read_pos();              \
        in.wrapped_buf_advance_read_pos(static_cast<uint32_t>(n) * (STRIDE));  \
        actual = n;                                                            \
        for (int i = 0; i < n; ++i) {                                          \
            U v;                                                               \
            memcpy(&v, src + i * (STRIDE), (STRIDE));                          \
            v = BSWAP(v);                                                      \
            memcpy(&out[i], &v, (STRIDE));                                     \
        }                                                                      \
        return common::E_OK;                                                   \
    }

    PLAIN_SKIP_FIXED(skip_int64, int64_t, 8,
                     common::SerializationUtil::read_i64)
    PLAIN_SKIP_FIXED(skip_float, float, 4,
                     common::SerializationUtil::read_float)
    PLAIN_SKIP_FIXED(skip_double, double, 8,
                     common::SerializationUtil::read_double)

    PLAIN_READ_BATCH_FIXED(read_batch_int64, int64_t, uint64_t, 8,
                           common::SerializationUtil::read_i64, plain_bswap64)
    PLAIN_READ_BATCH_FIXED(read_batch_float, float, uint32_t, 4,
                           common::SerializationUtil::read_float, plain_bswap32)
    PLAIN_READ_BATCH_FIXED(read_batch_double, double, uint64_t, 8,
                           common::SerializationUtil::read_double,
                           plain_bswap64)

#undef PLAIN_SKIP_FIXED
#undef PLAIN_READ_BATCH_FIXED
};

}  // end namespace storage
#endif  // ENCODING_PLAIN_DECODER_H
