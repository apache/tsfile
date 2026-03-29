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

#ifndef ENCODING_TS2DIFF_DECODER_H
#define ENCODING_TS2DIFF_DECODER_H

#include <sys/types.h>

#include <cstddef>
#include <cstring>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "utils/util_define.h"

#ifdef ENABLE_SIMD
#include "simde/x86/avx2.h"
#endif

namespace storage {

// ============================================================================
// SIMD batch decode helpers (INT32)
// ============================================================================
#ifdef ENABLE_SIMD

// Decode 4 INT32 values from bit-packed data using SIMD gather + shift.
// @in:        pointer to the start of packed bit data for the block
// @bit_width: bits per delta value
// @delta_min: minimum delta offset for this block
// @index:     current position within the block (0-based, among write_index_
//             deltas)
// @base:      the previous reconstructed value (for prefix-sum)
// @out:       output array (4 values written)
// Returns:    the last reconstructed value (new base for next group)
static inline int32_t simd_decode_4_i32(const uint8_t* in, int32_t bit_width,
                                        int32_t delta_min, int32_t index,
                                        int32_t base, int32_t out[4]) {
    static const simde__m128i SHUF_REV4 = simde_mm_setr_epi8(
        3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12);

    const simde__m128i VMIN4 = simde_mm_set1_epi32(delta_min);

    int32_t pos0 = index * bit_width;
    int32_t pos[4] = {pos0, pos0 + bit_width, pos0 + 2 * bit_width,
                      pos0 + 3 * bit_width};
    int32_t bidx[4] = {pos[0] >> 3, pos[1] >> 3, pos[2] >> 3, pos[3] >> 3};
    int32_t off[4] = {pos[0] & 7, pos[1] & 7, pos[2] & 7, pos[3] & 7};

    simde__m128i IDX = simde_mm_setr_epi32(bidx[0], bidx[1], bidx[2], bidx[3]);
    simde__m128i OFF = simde_mm_setr_epi32(off[0], off[1], off[2], off[3]);

    simde__m128i V4;

    if (bit_width <= 16) {
        int rshift = 32 - bit_width;
        simde__m128i w32_le =
            simde_mm_i32gather_epi32((const int*)in, IDX, 1);
        simde__m128i w32_be = simde_mm_shuffle_epi8(w32_le, SHUF_REV4);
        simde__m128i U32 = simde_mm_sllv_epi32(w32_be, OFF);
        simde__m128i RS32 = simde_mm_set1_epi32(rshift);
        V4 = simde_mm_srlv_epi32(U32, RS32);
    } else {
        static const simde__m256i SHUF_REV8 = simde_mm256_setr_epi8(
            7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4,
            3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
        int rshift = 64 - bit_width;
        simde__m256i w64_le =
            simde_mm256_i32gather_epi64((const long long*)in, IDX, 1);
        simde__m256i w64_be = simde_mm256_shuffle_epi8(w64_le, SHUF_REV8);
        simde__m256i OFF64 = simde_mm256_cvtepu32_epi64(OFF);
        simde__m256i U64 = simde_mm256_sllv_epi64(w64_be, OFF64);
        simde__m256i V64 =
            simde_mm256_srl_epi64(U64, simde_mm_cvtsi32_si128(rshift));
        simde__m256i perm =
            simde_mm256_setr_epi32(0, 2, 4, 6, 0, 0, 0, 0);
        simde__m256i comp = simde_mm256_permutevar8x32_epi32(V64, perm);
        V4 = simde_mm256_castsi256_si128(comp);
    }

    // Add delta_min
    V4 = simde_mm_add_epi32(V4, VMIN4);

    // Prefix sum to reconstruct absolute values
    simde__m128i t;
    t = simde_mm_slli_si128(V4, 4);
    V4 = simde_mm_add_epi32(V4, t);
    t = simde_mm_slli_si128(V4, 8);
    V4 = simde_mm_add_epi32(V4, t);

    // Add base
    simde__m128i C4 = simde_mm_set1_epi32(base);
    V4 = simde_mm_add_epi32(V4, C4);

    simde_mm_storeu_si128((simde__m128i*)out, V4);
    return out[3];
}

// Decode 4 INT64 values from bit-packed data using SIMD.
static inline int64_t simd_decode_4_i64(const uint8_t* in, int32_t bit_width,
                                        int64_t delta_min, int32_t index,
                                        int64_t base, int64_t out[4]) {
    static const simde__m256i SHUF_REV8 = simde_mm256_setr_epi8(
        7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3,
        2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);

    const simde__m256i VMIN4 = simde_mm256_set1_epi64x(delta_min);

    int32_t pos0 = index * bit_width;
    int32_t pos[4] = {pos0, pos0 + bit_width, pos0 + 2 * bit_width,
                      pos0 + 3 * bit_width};
    int32_t bidx[4] = {pos[0] >> 3, pos[1] >> 3, pos[2] >> 3, pos[3] >> 3};
    int32_t off[4] = {pos[0] & 7, pos[1] & 7, pos[2] & 7, pos[3] & 7};

    simde__m128i IDX = simde_mm_setr_epi32(bidx[0], bidx[1], bidx[2], bidx[3]);

    int rshift = 64 - bit_width;
    simde__m256i w64_le =
        simde_mm256_i32gather_epi64((const long long*)in, IDX, 1);
    simde__m256i w64_be = simde_mm256_shuffle_epi8(w64_le, SHUF_REV8);
    simde__m256i OFF64 = simde_mm256_cvtepu32_epi64(
        simde_mm_setr_epi32(off[0], off[1], off[2], off[3]));
    simde__m256i U64 = simde_mm256_sllv_epi64(w64_be, OFF64);
    simde__m256i V64 =
        simde_mm256_srl_epi64(U64, simde_mm_cvtsi32_si128(rshift));

    // Add delta_min
    V64 = simde_mm256_add_epi64(V64, VMIN4);

    // Prefix sum (64-bit, 4 lanes)
    simde__m256i t;
    // shift by 8 bytes = 1 lane
    t = simde_mm256_slli_si256(V64, 8);
    V64 = simde_mm256_add_epi64(V64, t);
    // cross-lane: add lane[1] to lane[2] and lane[3]
    // Extract high 128 bits, add broadcast of element[1] to both elements
    int64_t tmp_buf[4];
    simde_mm256_storeu_si256((simde__m256i*)tmp_buf, V64);
    tmp_buf[2] += tmp_buf[1];
    tmp_buf[3] += tmp_buf[1];
    V64 = simde_mm256_loadu_si256((const simde__m256i*)tmp_buf);

    // Add base
    simde__m256i C4 = simde_mm256_set1_epi64x(base);
    V64 = simde_mm256_add_epi64(V64, C4);

    simde_mm256_storeu_si256((simde__m256i*)out, V64);
    return out[3];
}

#endif  // ENABLE_SIMD

// ============================================================================
// Scalar batch decode helpers
// ============================================================================

// Scalar: extract one value from bit-packed data.
// @data:      pointer to packed bits (NOT advanced; caller handles position)
// @bit_pos:   bit offset from start of data
// @bit_width: bits per value
static inline int64_t scalar_read_bits(const uint8_t* data, int32_t bit_pos,
                                       int32_t bit_width) {
    int64_t value = 0;
    int bits = bit_width;
    int byte_idx = bit_pos >> 3;
    int bit_offset = bit_pos & 7;
    int bits_avail = 8 - bit_offset;

    while (bits > 0) {
        if (bits >= bits_avail) {
            uint8_t d = data[byte_idx] & ((1 << bits_avail) - 1);
            value = (value << bits_avail) | d;
            bits -= bits_avail;
            byte_idx++;
            bits_avail = 8;
        } else {
            uint8_t d = (data[byte_idx] >> (bits_avail - bits)) &
                        ((1 << bits) - 1);
            value = (value << bits) | d;
            bits = 0;
        }
    }
    return value;
}

// ============================================================================
// TS2DIFFDecoder template
// ============================================================================

template <typename T>
class TS2DIFFDecoder : public Decoder {
   public:
    TS2DIFFDecoder() { reset(); }
    ~TS2DIFFDecoder() override {}

    void reset() override {
        write_index_ = -1;
        bits_left_ = 0;
        stored_value_ = 0;
        buffer_ = 0;
        delta_min_ = 0;
        first_value_ = 0;
        previous_value_ = 0;
        bit_width_ = 0;
        current_index_ = 0;
        header_peeked_ = false;
    }

    FORCE_INLINE bool has_remaining(const common::ByteStream& buffer) override {
        if (buffer.has_remaining()) return true;
        return header_peeked_ || bits_left_ != 0 ||
               (current_index_ <= write_index_ &&
                write_index_ != -1 && current_index_ != 0);
    }

    void read_header(common::ByteStream& in) {
        common::SerializationUtil::read_i32(write_index_, in);
        common::SerializationUtil::read_i32(bit_width_, in);
    }

    // If empty, cache 8 bits from in_stream to 'buffer_'.
    void read_byte_if_empty(common::ByteStream& in) {
        if (bits_left_ == 0) {
            uint32_t read_len = 0;
            in.read_buf(&buffer_, 1, read_len);
            if (read_len != 0) {
                bits_left_ = 8;
            }
        }
    }

    int64_t read_long(int bits, common::ByteStream& in) {
        int64_t value = 0;
        while (bits > 0) {
            read_byte_if_empty(in);
            if (bits > bits_left_ || bits == 8) {
                // Take only the bits_left_ "least significant" bits.
                uint8_t d = (uint8_t)(buffer_ & ((1 << bits_left_) - 1));
                value = (value << bits_left_) + (d & 0xFF);
                bits -= bits_left_;
                bits_left_ = 0;
            } else {
                // Shift to correct position and take only least significant
                // bits.
                uint8_t d =
                    (uint8_t)((((uint8_t)buffer_) >> (bits_left_ - bits)) &
                              ((1 << bits) - 1));
                value = (value << bits) + (d & 0xFF);
                bits_left_ -= bits;
                bits = 0;
            }
            if (bits <= 0 && current_index_ == 0) {
                break;
            }
        }
        return value;
    }

    T decode(common::ByteStream& in);
    int read_boolean(bool& ret_value, common::ByteStream& in) override;
    int read_int32(int32_t& ret_value, common::ByteStream& in) override;
    int read_int64(int64_t& ret_value, common::ByteStream& in) override;
    int read_float(float& ret_value, common::ByteStream& in) override;
    int read_double(double& ret_value, common::ByteStream& in) override;
    int read_String(common::String& ret_value, common::PageArena& pa,
                    common::ByteStream& in) override;

    int read_batch_int32(int32_t* out, int capacity, int& actual,
                         common::ByteStream& in) override;
    int read_batch_int64(int64_t* out, int capacity, int& actual,
                         common::ByteStream& in) override;
    int skip_int32(int count, int& skipped, common::ByteStream& in) override;
    int skip_int64(int count, int& skipped, common::ByteStream& in) override;

    bool peek_next_block_range_int64(common::ByteStream& in,
                                      int64_t& block_min,
                                      int64_t& block_max,
                                      int& block_count) override;
    int skip_peeked_block_int64(common::ByteStream& in,
                                 int& skipped) override;

   public:
    T first_value_;
    T previous_value_;
    T stored_value_;
    T delta_min_;
    uint8_t buffer_;
    int bits_left_;
    int bit_width_;
    int write_index_;
    int current_index_;
    bool header_peeked_;
};

// ============================================================================
// Per-value decode (unchanged)
// ============================================================================

template <>
inline int32_t TS2DIFFDecoder<int32_t>::decode(common::ByteStream& in) {
    int32_t ret_value = stored_value_;
    if (UNLIKELY(current_index_ == 0)) {
        read_header(in);
        common::SerializationUtil::read_i32(delta_min_, in);
        common::SerializationUtil::read_i32(first_value_, in);
        ret_value = first_value_;
        bits_left_ = 0;
        buffer_ = 0;
        if (write_index_ == 0) {
            current_index_ = 0;
        } else {
            current_index_ = 1;
        }
        return ret_value;
    }
    // although it seems we are reading an int64, bit_width_ guarantees
    // that it does not overflow int32
    stored_value_ = read_long(bit_width_, in);
    ret_value = stored_value_ + first_value_ + delta_min_;
    if (current_index_++ >= write_index_) {
        current_index_ = 0;
        bits_left_ = 0;
    }
    first_value_ = ret_value;
    return ret_value;
}

template <>
inline int64_t TS2DIFFDecoder<int64_t>::decode(common::ByteStream& in) {
    int64_t ret_value = stored_value_;
    if (UNLIKELY(current_index_ == 0)) {
        read_header(in);
        common::SerializationUtil::read_i64(delta_min_, in);
        common::SerializationUtil::read_i64(first_value_, in);
        ret_value = first_value_;
        if (write_index_ == 0) {
            current_index_ = 0;
        } else {
            current_index_ = 1;
        }
        return ret_value;
    }
    stored_value_ = (int64_t)read_long(bit_width_, in);
    ret_value = stored_value_ + first_value_ + delta_min_;
    first_value_ = ret_value;
    if (current_index_++ >= write_index_) {
        current_index_ = 0;
        bits_left_ = 0;
    }
    return ret_value;
}

// ============================================================================
// Batch decode: INT32
// Decodes one full block (up to 129 values) per call using SIMD when enabled.
// ============================================================================

template <>
inline int TS2DIFFDecoder<int32_t>::read_batch_int32(
    int32_t* out, int capacity, int& actual, common::ByteStream& in) {
    actual = 0;

    while (actual < capacity && has_remaining(in)) {
        // If we are mid-block (current_index_ != 0), finish it per-value.
        if (current_index_ != 0) {
            while (actual < capacity && current_index_ != 0 &&
                   has_remaining(in)) {
                out[actual++] = decode(in);
            }
            continue;
        }

        // Start of a new block — read header
        read_header(in);
        common::SerializationUtil::read_i32(delta_min_, in);
        common::SerializationUtil::read_i32(first_value_, in);
        bits_left_ = 0;
        buffer_ = 0;

        // Output first_value
        if (actual >= capacity) {
            // Must consume first_value next time; set state for per-value path
            current_index_ = 0;
            // We already consumed the header; push first_value as stored
            // and let the next call to decode() handle it.
            // Actually, we need to handle this: rewind is not possible.
            // So we output first_value and accept going 1 over capacity.
        }
        out[actual++] = first_value_;

        if (write_index_ == 0) {
            // Block has only first_value, no deltas
            current_index_ = 0;
            continue;
        }

        // Direct pointer into the wrapped ByteStream buffer.
        int32_t block_bytes = (write_index_ * bit_width_ + 7) / 8;
        const uint8_t* blk_ptr =
            (const uint8_t*)in.get_wrapped_buf() + in.read_pos();
        in.wrapped_buf_advance_read_pos(static_cast<uint32_t>(block_bytes));

        int32_t remaining = write_index_;
        if (actual + remaining > capacity) {
            int32_t prev = first_value_;
            int32_t bit_pos = 0;
            for (int32_t i = 0; i < remaining && actual < capacity; ++i) {
                int64_t delta = scalar_read_bits(blk_ptr, bit_pos, bit_width_);
                bit_pos += bit_width_;
                int32_t val =
                    (int32_t)delta + prev + delta_min_;
                prev = val;
                out[actual++] = val;
            }
            current_index_ = 0;
            continue;
        }

        // Full block decode
        int32_t prev = first_value_;
        int32_t i = 0;

#ifdef ENABLE_SIMD
        // SIMD path: decode 8 values at a time (2 groups of 4)
        for (; i + 7 < remaining; i += 8) {
            int32_t need_bytes =
                ((i + 7) * bit_width_ + bit_width_ + 7) / 8 +
                (bit_width_ > 16 ? 8 : 4);
            if (need_bytes > block_bytes) break;

            int32_t grp_out[8];
            prev = simd_decode_4_i32(blk_ptr, bit_width_, delta_min_, i, prev,
                                     grp_out);
            prev = simd_decode_4_i32(blk_ptr, bit_width_, delta_min_, i + 4,
                                     prev, grp_out + 4);

            memcpy(out + actual, grp_out, 8 * sizeof(int32_t));
            actual += 8;
        }
#endif

        // Scalar tail
        int32_t bit_pos = i * bit_width_;
        for (; i < remaining; ++i) {
            int64_t delta = scalar_read_bits(blk_ptr, bit_pos, bit_width_);
            bit_pos += bit_width_;
            int32_t val = (int32_t)delta + prev + delta_min_;
            prev = val;
            out[actual++] = val;
        }

        // Block done, reset state
        first_value_ = prev;
        current_index_ = 0;
    }

    return common::E_OK;
}

// ============================================================================
// Batch decode: INT64
// ============================================================================

template <>
inline int TS2DIFFDecoder<int64_t>::read_batch_int64(
    int64_t* out, int capacity, int& actual, common::ByteStream& in) {
    actual = 0;

    while (actual < capacity && has_remaining(in)) {
        // If mid-block, finish per-value
        if (current_index_ != 0) {
            while (actual < capacity && current_index_ != 0 &&
                   has_remaining(in)) {
                out[actual++] = decode(in);
            }
            continue;
        }

        // Start of a new block
        if (!header_peeked_) {
            read_header(in);
            common::SerializationUtil::read_i64(delta_min_, in);
            common::SerializationUtil::read_i64(first_value_, in);
            bits_left_ = 0;
            buffer_ = 0;
        }
        header_peeked_ = false;

        out[actual++] = first_value_;

        if (write_index_ == 0) {
            current_index_ = 0;
            continue;
        }

        int32_t block_bytes = (write_index_ * bit_width_ + 7) / 8;
        // Direct pointer into the wrapped ByteStream buffer.
        const uint8_t* blk_ptr =
            (const uint8_t*)in.get_wrapped_buf() + in.read_pos();
        in.wrapped_buf_advance_read_pos(static_cast<uint32_t>(block_bytes));

        int32_t remaining = write_index_;
        if (actual + remaining > capacity) {
            int64_t prev = first_value_;
            int32_t bit_pos = 0;
            for (int32_t j = 0; j < remaining && actual < capacity; ++j) {
                int64_t delta = scalar_read_bits(blk_ptr, bit_pos, bit_width_);
                bit_pos += bit_width_;
                int64_t val = delta + prev + delta_min_;
                prev = val;
                out[actual++] = val;
            }
            current_index_ = 0;
            continue;
        }

        int64_t prev = first_value_;
        int32_t i = 0;

#ifdef ENABLE_SIMD
        // SIMD path: decode 4 INT64 values at a time
        for (; i + 3 < remaining; i += 4) {
            int32_t need_bytes =
                ((i + 3) * bit_width_ + bit_width_ + 7) / 8 + 8;
            if (need_bytes > block_bytes) break;

            int64_t grp_out[4];
            prev = simd_decode_4_i64(blk_ptr, bit_width_, delta_min_, i, prev,
                                     grp_out);
            memcpy(out + actual, grp_out, 4 * sizeof(int64_t));
            actual += 4;
        }
#endif

        // Scalar tail
        int32_t bit_pos = i * bit_width_;
        for (; i < remaining; ++i) {
            int64_t delta = scalar_read_bits(blk_ptr, bit_pos, bit_width_);
            bit_pos += bit_width_;
            int64_t val = delta + prev + delta_min_;
            prev = val;
            out[actual++] = val;
        }

        first_value_ = prev;
        current_index_ = 0;
    }

    return common::E_OK;
}

// ============================================================================
// Skip: INT32 — read header only, jump over packed data
// ============================================================================

template <>
inline int TS2DIFFDecoder<int32_t>::skip_int32(int count, int& skipped,
                                                common::ByteStream& in) {
    skipped = 0;

    // If mid-block, finish current block per-value
    while (skipped < count && current_index_ != 0 && has_remaining(in)) {
        decode(in);
        ++skipped;
    }

    // Skip whole blocks
    while (skipped < count && has_remaining(in)) {
        int32_t wi, bw, dm, fv;
        common::SerializationUtil::read_i32(wi, in);
        common::SerializationUtil::read_i32(bw, in);
        common::SerializationUtil::read_i32(dm, in);
        common::SerializationUtil::read_i32(fv, in);

        int32_t block_vals = wi + 1;
        int32_t skip_bytes = (wi * bw + 7) / 8;
        in.wrapped_buf_advance_read_pos(skip_bytes);

        skipped += block_vals;
        // Reset decoder state
        bits_left_ = 0;
        buffer_ = 0;
        current_index_ = 0;
        write_index_ = -1;
    }

    return common::E_OK;
}

// ============================================================================
// Skip: INT64
// ============================================================================

template <>
inline int TS2DIFFDecoder<int64_t>::skip_int64(int count, int& skipped,
                                                common::ByteStream& in) {
    skipped = 0;

    while (skipped < count && current_index_ != 0 && has_remaining(in)) {
        decode(in);
        ++skipped;
    }

    while (skipped < count && has_remaining(in)) {
        int32_t wi, bw;
        int64_t dm, fv;
        common::SerializationUtil::read_i32(wi, in);
        common::SerializationUtil::read_i32(bw, in);
        common::SerializationUtil::read_i64(dm, in);
        common::SerializationUtil::read_i64(fv, in);

        int32_t block_vals = wi + 1;
        int32_t skip_bytes = (wi * bw + 7) / 8;
        in.wrapped_buf_advance_read_pos(skip_bytes);

        skipped += block_vals;
        bits_left_ = 0;
        buffer_ = 0;
        current_index_ = 0;
        write_index_ = -1;
    }

    return common::E_OK;
}

// ============================================================================
// Block-level filter check: peek header and compute value range
// ============================================================================

template <>
inline bool TS2DIFFDecoder<int64_t>::peek_next_block_range_int64(
    common::ByteStream& in, int64_t& block_min, int64_t& block_max,
    int& block_count) {
    if (current_index_ != 0 || !has_remaining(in)) return false;

    read_header(in);
    common::SerializationUtil::read_i64(delta_min_, in);
    common::SerializationUtil::read_i64(first_value_, in);
    bits_left_ = 0;
    buffer_ = 0;

    block_min = first_value_;
    block_count = write_index_ + 1;

    if (write_index_ == 0 || bit_width_ == 0) {
        block_max = first_value_ + (int64_t)write_index_ * delta_min_;
    } else if (bit_width_ >= 63) {
        block_max = INT64_MAX;
    } else {
        int64_t max_delta = delta_min_ + ((1LL << bit_width_) - 1);
        block_max = first_value_ + (int64_t)write_index_ * max_delta;
    }

    header_peeked_ = true;
    return true;
}

template <>
inline int TS2DIFFDecoder<int64_t>::skip_peeked_block_int64(
    common::ByteStream& in, int& skipped) {
    skipped = write_index_ + 1;
    int32_t skip_bytes = (write_index_ * bit_width_ + 7) / 8;
    in.wrapped_buf_advance_read_pos(skip_bytes);
    header_peeked_ = false;
    bits_left_ = 0;
    buffer_ = 0;
    current_index_ = 0;
    write_index_ = -1;
    return common::E_OK;
}

// INT32 specialization: not applicable (timestamps are always INT64)
template <>
inline bool TS2DIFFDecoder<int32_t>::peek_next_block_range_int64(
    common::ByteStream& in, int64_t& block_min, int64_t& block_max,
    int& block_count) {
    return false;
}

template <>
inline int TS2DIFFDecoder<int32_t>::skip_peeked_block_int64(
    common::ByteStream& in, int& skipped) {
    return common::E_NOT_SUPPORT;
}

// ============================================================================
// Default (unsupported type) batch/skip — fall back to base class
// ============================================================================

template <>
inline int TS2DIFFDecoder<int32_t>::read_batch_int64(
    int64_t* out, int capacity, int& actual, common::ByteStream& in) {
    return Decoder::read_batch_int64(out, capacity, actual, in);
}

template <>
inline int TS2DIFFDecoder<int32_t>::skip_int64(
    int count, int& skipped, common::ByteStream& in) {
    return Decoder::skip_int64(count, skipped, in);
}

template <>
inline int TS2DIFFDecoder<int64_t>::read_batch_int32(
    int32_t* out, int capacity, int& actual, common::ByteStream& in) {
    return Decoder::read_batch_int32(out, capacity, actual, in);
}

template <>
inline int TS2DIFFDecoder<int64_t>::skip_int32(
    int count, int& skipped, common::ByteStream& in) {
    return Decoder::skip_int32(count, skipped, in);
}

// ============================================================================
// Float / Double wrapper decoders (unchanged)
// ============================================================================

class FloatTS2DIFFDecoder : public TS2DIFFDecoder<int32_t> {
   public:
    float decode(common::ByteStream& in) {
        int32_t value_int = TS2DIFFDecoder<int32_t>::decode(in);
        return common::int_to_float(value_int);
    }

    int read_boolean(bool& ret_value, common::ByteStream& in);
    int read_int32(int32_t& ret_value, common::ByteStream& in);
    int read_int64(int64_t& ret_value, common::ByteStream& in);
    int read_float(float& ret_value, common::ByteStream& in);
    int read_double(double& ret_value, common::ByteStream& in);

    int read_batch_float(float* out, int capacity, int& actual,
                         common::ByteStream& in) override {
        // Reuse SIMD batch decode for int32, then bit-cast to float
        int32_t* buf = reinterpret_cast<int32_t*>(out);
        int ret = TS2DIFFDecoder<int32_t>::read_batch_int32(
            buf, capacity, actual, in);
        if (ret != common::E_OK) return ret;
        for (int i = 0; i < actual; ++i) {
            out[i] = common::int_to_float(buf[i]);
        }
        return common::E_OK;
    }
};

class DoubleTS2DIFFDecoder : public TS2DIFFDecoder<int64_t> {
   public:
    double decode(common::ByteStream& in) {
        int64_t value_long = TS2DIFFDecoder<int64_t>::decode(in);
        return common::long_to_double(value_long);
    }

    int read_boolean(bool& ret_value, common::ByteStream& in);
    int read_int32(int32_t& ret_value, common::ByteStream& in);
    int read_int64(int64_t& ret_value, common::ByteStream& in);
    int read_float(float& ret_value, common::ByteStream& in);
    int read_double(double& ret_value, common::ByteStream& in);

    int read_batch_double(double* out, int capacity, int& actual,
                          common::ByteStream& in) override {
        // Reuse SIMD batch decode for int64, then bit-cast to double
        int64_t* buf = reinterpret_cast<int64_t*>(out);
        int ret = TS2DIFFDecoder<int64_t>::read_batch_int64(
            buf, capacity, actual, in);
        if (ret != common::E_OK) return ret;
        for (int i = 0; i < actual; ++i) {
            out[i] = common::long_to_double(buf[i]);
        }
        return common::E_OK;
    }
};

typedef TS2DIFFDecoder<int32_t> IntTS2DIFFDecoder;
typedef TS2DIFFDecoder<int64_t> LongTS2DIFFDecoder;

// wrap as Decoder interface
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_boolean(bool& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_int32(int32_t& ret_value,
                                               common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_int64(int64_t& ret_value,
                                               common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_float(float& ret_value,
                                               common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_double(double& ret_value,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_String(common::String& ret_value,
                                                common::PageArena& pa,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_boolean(bool& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_int32(int32_t& ret_value,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_int64(int64_t& ret_value,
                                                common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_float(float& ret_value,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_double(double& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_String(common::String& ret_value,
                                                 common::PageArena& pa,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_boolean(bool& ret_value,
                                                   common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_int32(int32_t& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_int64(int64_t& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_float(float& ret_value,
                                                 common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_double(double& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_boolean(bool& ret_value,
                                                    common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_int32(int32_t& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_int64(int64_t& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_float(float& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_double(double& ret_value,
                                                   common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}

}  // end namespace storage
#endif  // ENCODING_TS2DIFF_DECODER_H
