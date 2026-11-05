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

#ifndef ENCODING_INT64PACKER_ENCODER_H
#define ENCODING_INT64PACKER_ENCODER_H

#define NUM_OF_INTS 8

#include "encoder.h"

#ifdef ENABLE_SIMD
#include "common/simd_dispatch.h"
#include "simde/x86/avx2.h"
#ifdef TSFILE_X86_AVX2_DISPATCH
#include <immintrin.h>
#endif
#endif

namespace storage {

#ifdef ENABLE_SIMD
// SIMD decode of 4 big-endian bit-packed INT64 values. Mirrors the TS_2DIFF
// helper but skips delta reconstruction: RLE packs raw values, so the gathered
// and shifted bits are already the final value (negative values are always
// stored with the full 64-bit width, so the logical shift is lossless).
// `in` points at the start of the packed group; `index` is the first value's
// position within the group (0 or 4 for an 8-value group).
static inline void rle_simd_unpack_4_i64(const uint8_t* in, int32_t width,
                                         int32_t index, int64_t out[4]) {
    static const simde__m256i SHUF_REV8 = simde_mm256_setr_epi8(
        7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2,
        1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
    const int32_t pos0 = index * width;
    const int32_t pos[4] = {pos0, pos0 + width, pos0 + 2 * width,
                            pos0 + 3 * width};
    const int32_t bidx[4] = {pos[0] >> 3, pos[1] >> 3, pos[2] >> 3,
                             pos[3] >> 3};
    const int32_t off[4] = {pos[0] & 7, pos[1] & 7, pos[2] & 7, pos[3] & 7};
    const simde__m128i IDX =
        simde_mm_setr_epi32(bidx[0], bidx[1], bidx[2], bidx[3]);
    const int32_t rshift = 64 - width;
    simde__m256i w64_le =
        simde_mm256_i32gather_epi64((const int64_t*)in, IDX, 1);
    simde__m256i w64_be = simde_mm256_shuffle_epi8(w64_le, SHUF_REV8);
    simde__m256i OFF64 = simde_mm256_cvtepu32_epi64(
        simde_mm_setr_epi32(off[0], off[1], off[2], off[3]));
    simde__m256i U64 = simde_mm256_sllv_epi64(w64_be, OFF64);
    simde__m256i V64 =
        simde_mm256_srl_epi64(U64, simde_mm_cvtsi32_si128(rshift));
    simde_mm256_storeu_si256((simde__m256i*)out, V64);
}

#ifdef TSFILE_X86_AVX2_DISPATCH
// Same kernel with real AVX2 intrinsics. simde picks its native path only when
// the whole translation unit is compiled with -mavx2, which the library is not
// (portable baseline), so a target-attributed copy is needed to actually emit
// vpgatherqq/vpshufb. Selected once per packer via common::cpu_has_avx2().
TSFILE_TARGET_AVX2
static inline void rle_avx2_unpack_4_i64(const uint8_t* in, int32_t width,
                                         int32_t index, int64_t out[4]) {
    const __m256i SHUF_REV8 =
        _mm256_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8,
                         7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
    const int32_t pos0 = index * width;
    const int32_t pos[4] = {pos0, pos0 + width, pos0 + 2 * width,
                            pos0 + 3 * width};
    const __m128i IDX =
        _mm_setr_epi32(pos[0] >> 3, pos[1] >> 3, pos[2] >> 3, pos[3] >> 3);
    const int32_t rshift = 64 - width;
    __m256i w64_le = _mm256_i32gather_epi64((const long long*)in, IDX, 1);
    __m256i w64_be = _mm256_shuffle_epi8(w64_le, SHUF_REV8);
    __m256i OFF64 = _mm256_cvtepu32_epi64(
        _mm_setr_epi32(pos[0] & 7, pos[1] & 7, pos[2] & 7, pos[3] & 7));
    __m256i U64 = _mm256_sllv_epi64(w64_be, OFF64);
    __m256i V64 = _mm256_srl_epi64(U64, _mm_cvtsi32_si128(rshift));
    _mm256_storeu_si256((__m256i*)out, V64);
}
#endif  // TSFILE_X86_AVX2_DISPATCH
#endif

class Int64Packer {
   private:
    int width_;
#ifdef TSFILE_X86_AVX2_DISPATCH
    // Resolved once per packer (one per bit-width pattern) so the hot
    // unpack loop pays no thread-safe-static guard on every group.
    bool use_avx2_;
#endif

   public:
#ifdef TSFILE_X86_AVX2_DISPATCH
    Int64Packer(int width_)
        : width_(width_), use_avx2_(common::cpu_has_avx2()) {}
#else
    Int64Packer(int width_) : width_(width_) {}
#endif
    ~Int64Packer() { destroy(); }

    void destroy() { /* do nothing for IntPacker */
    }
    void reset() { /* do thing for IntPacker */
    }

    void pack_8values(const int64_t values[], int offset, unsigned char buf[]) {
        int buf_idx = 0;
        int value_idx = offset;
        // remaining bits for the current unfinished Integer
        int left_bit = 0;

        while (value_idx < NUM_OF_INTS + offset) {
            // buffer is used for saving 64 bits as a part of result
            int64_t buffer = 0;
            // remaining size of bits in the 'buffer'
            int left_size = 64;

            // encode the left bits of current Integer to 'buffer'
            if (left_bit > 0) {
                buffer |= (static_cast<uint64_t>(values[value_idx])
                           << (64 - left_bit));
                left_size -= left_bit;
                left_bit = 0;
                value_idx++;
            }

            while (left_size >= width_ && value_idx < NUM_OF_INTS + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (static_cast<uint64_t>(values[value_idx])
                           << (left_size - width_));
                left_size -= width_;
                value_idx++;
            }
            // If the remaining space of the buffer can not save the bits for
            // one Integer,
            if (left_size > 0 && value_idx < NUM_OF_INTS + offset) {
                // put the first 'left_size' bits of the Integer into remaining
                // space of the buffer
                buffer |= ((static_cast<uint64_t>(values[value_idx])) >>
                           (width_ - left_size));
                left_bit = width_ - left_size;
            }

            // put the buffer into the final result
            for (int j = 0; j < 8; j++) {
                buf[buf_idx] =
                    (unsigned char)(((uint64_t)buffer >> ((8 - j - 1) * 8)) &
                                    0xFF);
                buf_idx++;
                // width_ is the bit num of each value, but here is means the
                // max byte num
                if (buf_idx >= width_ * 8 / 8) {
                    return;
                }
            }
        }
    }

    /**
     * decode Integers from byte array.
     *
     * @param buf - array where bytes are in.
     * @param offset - offset of first byte to be decoded in buf
     * @param values - decoded result , the length of 'values' should be @{link
     * IntPacker#NUM_OF_INTS}
     */
    void unpack_8values(const unsigned char buf[], int offset,
                        int64_t values[]) {
#ifdef ENABLE_SIMD
        // Widths 59/61/62/63 can start at a bit offset that needs a ninth
        // byte for the gather; decode those with the scalar path below.
        if (width_ <= 58 || width_ == 60 || width_ == 64) {
#ifdef TSFILE_X86_AVX2_DISPATCH
            if (use_avx2_) {
                rle_avx2_unpack_4_i64(buf + offset, width_, 0, values);
                rle_avx2_unpack_4_i64(buf + offset, width_, 4, values + 4);
                return;
            }
#endif
            rle_simd_unpack_4_i64(buf + offset, width_, 0, values);
            rle_simd_unpack_4_i64(buf + offset, width_, 4, values + 4);
            return;
        }
#endif
        // Scalar fallback for widths that can cross a ninth byte (59/61/62/63).
        // Decode whole bytes into a 128-bit buffer — ~8x fewer iterations than
        // the old bit-by-bit loop — then shift out one width-wide value at a
        // time. Widths up to 63 need up to 63 + 7 = 70 buffered bits, which the
        // 64-bit int32-style buffer cannot hold; hence 128 bits.
        int byte_idx = offset;
        unsigned __int128 buffer = 0;
        int total_bits = 0;
        int value_idx = 0;
        while (value_idx < 8) {
            while (total_bits < width_) {
                buffer = (buffer << 8) | (buf[byte_idx] & 0xFF);
                byte_idx++;
                total_bits += 8;
            }
            while (total_bits >= width_ && value_idx < 8) {
                values[value_idx] =
                    static_cast<int64_t>(buffer >> (total_bits - width_));
                value_idx++;
                total_bits -= width_;
                buffer &= (((unsigned __int128)1 << total_bits) - 1);
            }
        }
    }

    /**
     * decode all values from 'buf' with specified offset and length decoded
     * result will be saved in the array named 'values'.
     *
     * @param buf array where all bytes are in.
     * @param length length of bytes to be decoded in buf.
     * @param values decoded result.
     */
    void unpack_all_values(const unsigned char buf[], int length,
                           int64_t values[]) {
        int idx = 0;
        int k = 0;
        while (idx < length) {
            int64_t tv[8];
            // decode 8 values one time, current result will be saved in the
            // array named 'tv'
            unpack_8values(buf, idx, tv);
            // System.arraycopy(tv, 0, values, k, 8);
            std::memmove(values + k, tv, 8 * sizeof(int64_t));
            idx += width_;
            k += 8;
        }
    }

    void set_width(int width_) { this->width_ = width_; }
};

}  // end namespace storage
#endif  // ENCODING_IntPacker_ENCODER_H
