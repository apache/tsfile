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

#ifndef ENCODING_INT32PACKER_ENCODER_H
#define ENCODING_INT32PACKER_ENCODER_H

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
// SIMD decode of 4 big-endian bit-packed INT32 values. Uses the 64-bit gather
// so all widths 1..32 fit without a width guard; the trailing permute packs
// the low 32 bits of each 64-bit lane into four contiguous INT32 lanes.
static inline void rle_simd_unpack_4_i32(const uint8_t* in, int32_t width,
                                         int32_t index, int32_t out[4]) {
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
    const simde__m256i perm = simde_mm256_setr_epi32(0, 2, 4, 6, 0, 0, 0, 0);
    simde__m256i comp = simde_mm256_permutevar8x32_epi32(V64, perm);
    simde_mm_storeu_si128((simde__m128i*)out,
                          simde_mm256_castsi256_si128(comp));
}

#ifdef TSFILE_X86_AVX2_DISPATCH
// Native-AVX2 twin of rle_simd_unpack_4_i32; see int64_packer.h for why the
// target attribute is required (simde otherwise emits its portable fallback).
TSFILE_TARGET_AVX2
static inline void rle_avx2_unpack_4_i32(const uint8_t* in, int32_t width,
                                         int32_t index, int32_t out[4]) {
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
    const __m256i perm = _mm256_setr_epi32(0, 2, 4, 6, 0, 0, 0, 0);
    __m256i comp = _mm256_permutevar8x32_epi32(V64, perm);
    _mm_storeu_si128((__m128i*)out, _mm256_castsi256_si128(comp));
}
#endif  // TSFILE_X86_AVX2_DISPATCH
#endif

class Int32Packer {
   private:
    int width_;
#ifdef TSFILE_X86_AVX2_DISPATCH
    // Resolved once per packer (one per bit-width pattern) so the hot
    // unpack loop pays no thread-safe-static guard on every group.
    bool use_avx2_;
#endif

   public:
#ifdef TSFILE_X86_AVX2_DISPATCH
    Int32Packer(int width_)
        : width_(width_), use_avx2_(common::cpu_has_avx2()) {}
#else
    Int32Packer(int width_) : width_(width_) {}
#endif
    ~Int32Packer() { destroy(); }

    void destroy() { /* do nothing for IntPacker */
    }
    void reset() { /* do thing for IntPacker */
    }

    void pack_8values(const int32_t values[], int offset, unsigned char buf[]) {
        int buf_idx = 0;
        int value_idx = offset;
        // remaining bits for the current unfinished Integer
        int left_bit = 0;

        while (value_idx < NUM_OF_INTS + offset) {
            // buffer is used for saving 32 bits as a part of result
            int32_t buffer = 0;
            // remaining size of bits in the 'buffer'
            int left_size = 32;

            // encode the left bits of current Integer to 'buffer'
            if (left_bit > 0) {
                buffer |= (static_cast<uint32_t>(values[value_idx])
                           << (32 - left_bit));
                left_size -= left_bit;
                left_bit = 0;
                value_idx++;
            }

            while (left_size >= width_ && value_idx < NUM_OF_INTS + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (static_cast<uint32_t>(values[value_idx])
                           << (left_size - width_));
                left_size -= width_;
                value_idx++;
            }
            // If the remaining space of the buffer can not save the bits for
            // one Integer,
            if (left_size > 0 && value_idx < NUM_OF_INTS + offset) {
                // put the first 'left_size' bits of the Integer into remaining
                // space of the buffer
                buffer |= (static_cast<uint32_t>(values[value_idx]) >>
                           (width_ - left_size));
                left_bit = width_ - left_size;
            }

            // put the buffer into the final result
            for (int j = 0; j < 4; j++) {
                buf[buf_idx] =
                    (unsigned char)((buffer >> ((3 - j) * 8)) & 0xFF);
                buf_idx++;
                // width_ is the bit num of each value, but here is means the
                // max byte num
                if (buf_idx >= width_) {
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
     *
     * The SIMD paths gather 8 bytes per value, so the buffer must hold at
     * least 8 readable bytes past the packed group; callers allocate that
     * padding (the RLE decoders zero it, the Sprintz decoders size their
     * block buffer with +8).
     */
    void unpack_8values(const unsigned char buf[], int offset,
                        int32_t values[]) {
#ifdef ENABLE_SIMD
        // Every width 1..32 fits within the 8-byte gather, so no width guard
        // is needed here (unlike the 64-bit packer).
#ifdef TSFILE_X86_AVX2_DISPATCH
        if (use_avx2_) {
            rle_avx2_unpack_4_i32(buf + offset, width_, 0, values);
            rle_avx2_unpack_4_i32(buf + offset, width_, 4, values + 4);
            return;
        }
#endif
        rle_simd_unpack_4_i32(buf + offset, width_, 0, values);
        rle_simd_unpack_4_i32(buf + offset, width_, 4, values + 4);
        return;
#endif
        int byte_idx = offset;
        uint64_t buffer = 0;
        // total bits which have reader from 'buf' to 'buffer'. i.e.,
        // number of available bits to be decoded.
        int total_bits = 0;
        int value_idx = 0;

        while (value_idx < NUM_OF_INTS) {
            // If current available bits are not enough to decode one Integer,
            // then add next byte from buf to 'buffer' until total_bits >= width
            while (total_bits < width_) {
                buffer = (buffer << 8) | (buf[byte_idx] & 0xFF);
                byte_idx++;
                total_bits += 8;
            }

            // If current available bits are enough to decode one Integer,
            // then decode one Integer one by one until left bits in 'buffer' is
            // not enough to decode one Integer.
            while (total_bits >= width_ && value_idx < 8) {
                values[value_idx] = (int)(buffer >> (total_bits - width_));
                value_idx++;
                total_bits -= width_;
                buffer = buffer & ((1 << total_bits) - 1);
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
                           int32_t values[]) {
        int idx = 0;
        int k = 0;
        while (idx < length) {
            int32_t tv[8];
            // decode 8 values one time, current result will be saved in the
            // array named 'tv'
            unpack_8values(buf, idx, tv);
            // System.arraycopy(tv, 0, values, k, 8);
            std::memmove(values + k, tv, 8 * sizeof(int32_t));
            idx += width_;
            k += 8;
        }
    }

    void set_width(int width_) { this->width_ = width_; }
};

}  // end namespace storage
#endif  // ENCODING_IntPacker_ENCODER_H
