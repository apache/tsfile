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

#ifndef ENCODING_BITPACKING_INT64_PACKER_N_H
#define ENCODING_BITPACKING_INT64_PACKER_N_H

#include <algorithm>
#include <cstdint>

#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace storage {
namespace bitpacking {

// Bit-packer that supports variable N (1..32). Ported from Java LongPacker.packNValues/unpackNValues.
class Int64PackerN {
   public:
    explicit Int64PackerN(int width_bits, bool enable_simd = true)
        : width_(width_bits), enable_simd_(enable_simd) {}

    void set_width(int width_bits) { width_ = width_bits; }
    void set_enable_simd(bool enable) { enable_simd_ = enable; }

    void pack_n_values(const int64_t *values, int offset, int n, uint8_t *buf) const {
        // Width-dispatched specialized kernels (reduces branches; allows compiler to unroll).
        // Fallback to generic for uncommon widths.
        switch (width_) {
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
            case 15:
            case 16:
            case 17:
            case 18:
            case 19:
            case 20:
            case 21:
            case 22:
            case 23:
            case 24:
            case 25:
            case 26:
            case 27:
            case 28:
            case 29:
            case 30:
            case 31:
            case 32:
            case 33:
                pack_n_values_w(values + offset, n, buf, width_, enable_simd_);
                return;
            default:
                pack_n_values_generic(values, offset, n, buf);
                return;
        }
    }

    void unpack_n_values(const uint8_t *buf, int offset, int n, int64_t *values) const {
        switch (width_) {
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
            case 15:
            case 16:
            case 17:
            case 18:
            case 19:
            case 20:
            case 21:
            case 22:
            case 23:
            case 24:
            case 25:
            case 26:
            case 27:
            case 28:
            case 29:
            case 30:
            case 31:
            case 32:
            case 33:
                unpack_n_values_w(buf + offset, n, values, width_, enable_simd_);
                return;
            default:
                unpack_n_values_generic(buf, offset, n, values);
                return;
        }
    }

   private:
    static FORCE_INLINE uint64_t mask_w_u64(int w) {
        return (w >= 64) ? ~0ULL : ((1ULL << w) - 1ULL);
    }

    static FORCE_INLINE void pack_n_values_w(const int64_t *values, int n, uint8_t *buf, int w, bool enable_simd) {
        // Fast paths for byte-aligned widths. These are also the most common widths after bit-width selection.
        // Encoding is big-endian within each value (matches the bit-buffer emission order).
        if (enable_simd && w == 6) {
            pack_n_values_6(values, n, buf);
            return;
        }
        if (enable_simd && w == 7) {
            pack_n_values_7(values, n, buf);
            return;
        }
        if (enable_simd && w == 8) {
            pack_n_values_8(values, n, buf);
            return;
        }
        if (enable_simd && w == 9) {
            pack_n_values_9(values, n, buf);
            return;
        }
        if (enable_simd && w == 10) {
            pack_n_values_10(values, n, buf);
            return;
        }
        if (enable_simd && w == 11) {
            pack_n_values_11(values, n, buf);
            return;
        }
        if (enable_simd && w == 12) {
            pack_n_values_12(values, n, buf);
            return;
        }
        if (enable_simd && w == 16) {
            pack_n_values_16(values, n, buf);
            return;
        }
        if (enable_simd && w == 24) {
            pack_n_values_24(values, n, buf);
            return;
        }
        if (enable_simd && w == 32) {
            pack_n_values_32(values, n, buf);
            return;
        }
        if (enable_simd && w == 33) {
            pack_n_values_33(values, n, buf);
            return;
        }
#if defined(__ARM_NEON)
        if (enable_simd && w > 0 && w < 32) {
            pack_n_values_neon_generic(values, n, buf, w);
            return;
        }
#endif
        // Pack using a bit-buffer; emits big-endian bytes for each 64-bit chunk (matches Java logic).
        // This is still scalar but specialized by w, and has a tighter loop than the generic.
        const int byte_limit = (n * w + 7) / 8;
        int out = 0;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (int i = 0; i < n; i++) {
            uint64_t v = (uint64_t)values[i];
            bitbuf = (bitbuf << w) | (v & ((w == 64) ? ~0ULL : ((1ULL << w) - 1ULL)));
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) {
                    buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
        }
        if (bits > 0 && out < byte_limit) {
            buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
        }
        // Caller guarantees buffer has enough space; no need to pad further.
    }

    static FORCE_INLINE void unpack_n_values_w(const uint8_t *buf, int n, int64_t *values, int w, bool enable_simd) {
        if (enable_simd && w == 8) {
            unpack_n_values_8(buf, n, values);
            return;
        }
        if (enable_simd && w == 16) {
            unpack_n_values_16(buf, n, values);
            return;
        }
        if (enable_simd && w == 24) {
            unpack_n_values_24(buf, n, values);
            return;
        }
        if (enable_simd && w == 32) {
            unpack_n_values_32(buf, n, values);
            return;
        }
#if defined(__ARM_NEON)
        if (enable_simd && w > 0 && w < 32) {
            unpack_n_values_neon_generic(buf, n, values, w);
            return;
        }
#endif
        int byte_idx = 0;
        uint64_t bitbuf = 0;
        int bits = 0;
        const uint64_t mask = mask_w_u64(w);
        for (int i = 0; i < n; i++) {
            while (bits < w) {
                bitbuf = (bitbuf << 8) | (uint64_t)(buf[byte_idx++] & 0xFF);
                bits += 8;
            }
            bits -= w;
            values[i] = (int64_t)((bitbuf >> bits) & mask);
        }
    }

    static FORCE_INLINE void pack_n_values_8(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        // Process 16 values per iteration. We still need to narrow from int64 -> u8.
        for (; i + 15 < n; i += 16) {
            // Load 16x64-bit as 8x uint64x2_t
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint64x2_t v2 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 4));
            uint64x2_t v3 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 6));
            uint64x2_t v4 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 8));
            uint64x2_t v5 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 10));
            uint64x2_t v6 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 12));
            uint64x2_t v7 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 14));

            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x2_t n2 = vmovn_u64(v2);
            uint32x2_t n3 = vmovn_u64(v3);
            uint32x2_t n4 = vmovn_u64(v4);
            uint32x2_t n5 = vmovn_u64(v5);
            uint32x2_t n6 = vmovn_u64(v6);
            uint32x2_t n7 = vmovn_u64(v7);

            uint16x4_t m0 = vmovn_u32(vcombine_u32(n0, n1));
            uint16x4_t m1 = vmovn_u32(vcombine_u32(n2, n3));
            uint16x4_t m2 = vmovn_u32(vcombine_u32(n4, n5));
            uint16x4_t m3 = vmovn_u32(vcombine_u32(n6, n7));

            uint8x8_t b01 = vmovn_u16(vcombine_u16(m0, m1));  // 8 bytes
            uint8x8_t b23 = vmovn_u16(vcombine_u16(m2, m3));  // 8 bytes

            vst1_u8(buf + i, b01);
            vst1_u8(buf + i + 8, b23);
        }
        for (; i < n; i++) {
            buf[i] = (uint8_t)(values[i] & 0xFF);
        }
#else
        for (int i = 0; i < n; i++) {
            buf[i] = (uint8_t)(values[i] & 0xFF);
        }
#endif
    }

    static FORCE_INLINE void unpack_n_values_8(const uint8_t *buf, int n, int64_t *values) {
        for (int i = 0; i < n; i++) {
            values[i] = (int64_t)(buf[i] & 0xFF);
        }
    }

    static FORCE_INLINE void pack_n_values_16(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        // 8 values -> 16 bytes
        for (; i + 7 < n; i += 8) {
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint64x2_t v2 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 4));
            uint64x2_t v3 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 6));
            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x2_t n2 = vmovn_u64(v2);
            uint32x2_t n3 = vmovn_u64(v3);
            uint16x4_t u0 = vmovn_u32(vcombine_u32(n0, n1));
            uint16x4_t u1 = vmovn_u32(vcombine_u32(n2, n3));
            uint16x8_t u = vcombine_u16(u0, u1);

            // big-endian bytes per u16: hi then lo
            uint8x16_t lo = vreinterpretq_u8_u16(u);
            // swap bytes within each 16-bit lane
            uint8x16_t be = vrev16q_u8(lo);
            vst1q_u8(buf + (i * 2), be);
        }
        for (; i < n; i++) {
            uint16_t v = (uint16_t)(values[i] & 0xFFFF);
            buf[i * 2] = (uint8_t)(v >> 8);
            buf[i * 2 + 1] = (uint8_t)(v & 0xFF);
        }
#else
        for (int i = 0; i < n; i++) {
            uint16_t v = (uint16_t)(values[i] & 0xFFFF);
            buf[i * 2] = (uint8_t)(v >> 8);
            buf[i * 2 + 1] = (uint8_t)(v & 0xFF);
        }
#endif
    }

    static FORCE_INLINE void unpack_n_values_16(const uint8_t *buf, int n, int64_t *values) {
        for (int i = 0; i < n; i++) {
            uint16_t v = ((uint16_t)(buf[i * 2] & 0xFF) << 8) | (uint16_t)(buf[i * 2 + 1] & 0xFF);
            values[i] = (int64_t)v;
        }
    }

    static FORCE_INLINE void pack_n_values_12(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        int out = 0;
        const uint16x8_t vmask = vdupq_n_u16((uint16_t)0x0FFF);
        uint16_t tmp16[8];
        for (; i + 7 < n; i += 8) {  // 8 values -> 12 bytes
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint64x2_t v2 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 4));
            uint64x2_t v3 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 6));
            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x2_t n2 = vmovn_u64(v2);
            uint32x2_t n3 = vmovn_u64(v3);
            uint16x4_t u0 = vmovn_u32(vcombine_u32(n0, n1));
            uint16x4_t u1 = vmovn_u32(vcombine_u32(n2, n3));
            uint16x8_t u = vcombine_u16(u0, u1);
            u = vandq_u16(u, vmask);
            vst1q_u16(tmp16, u);
            // Big-endian 12-bit stream: per pair -> 3 bytes.
            for (int k = 0; k < 8; k += 2) {
                uint16_t a = tmp16[k];
                uint16_t b = tmp16[k + 1];
                buf[out++] = (uint8_t)(a >> 4);
                buf[out++] = (uint8_t)(((a & 0x0F) << 4) | (uint8_t)(b >> 8));
                buf[out++] = (uint8_t)(b & 0xFF);
            }
        }
        // tail: scalar bit-buffer (same semantics)
        const int w = 12;
        const int byte_limit = (n * w + 7) / 8;
        uint64_t bitbuf = 0;
        int bits = 0;
        // If we already emitted full chunks, keep out aligned (should be <= byte_limit).
        // out already equals (i/8)*12 bytes here.
        for (; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x0FFFULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) {
                    buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
        }
        if (bits > 0 && out < byte_limit) {
            buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
        }
#else
        // Scalar (same as generic, but unrolled by pairs for w=12)
        int out = 0;
        int i = 0;
        for (; i + 1 < n; i += 2) {
            uint16_t a = (uint16_t)((uint64_t)values[i] & 0x0FFFULL);
            uint16_t b = (uint16_t)((uint64_t)values[i + 1] & 0x0FFFULL);
            buf[out++] = (uint8_t)(a >> 4);
            buf[out++] = (uint8_t)(((a & 0x0F) << 4) | (uint8_t)(b >> 8));
            buf[out++] = (uint8_t)(b & 0xFF);
        }
        if (i < n) {
            uint16_t a = (uint16_t)((uint64_t)values[i] & 0x0FFFULL);
            buf[out++] = (uint8_t)(a >> 4);
            buf[out++] = (uint8_t)((a & 0x0F) << 4);
        }
#endif
    }

    static FORCE_INLINE void pack_n_values_6(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        int out = 0;
        const uint8x16_t vmask = vdupq_n_u8((uint8_t)0x3F);
        uint8_t tmp8[16];
        for (; i + 15 < n; i += 16) {  // 16 values -> 12 bytes
            // Load 16x64 -> narrow to 16x8 (low bits), then pack 6-bit stream.
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint64x2_t v2 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 4));
            uint64x2_t v3 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 6));
            uint64x2_t v4 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 8));
            uint64x2_t v5 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 10));
            uint64x2_t v6 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 12));
            uint64x2_t v7 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 14));

            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x2_t n2 = vmovn_u64(v2);
            uint32x2_t n3 = vmovn_u64(v3);
            uint32x2_t n4 = vmovn_u64(v4);
            uint32x2_t n5 = vmovn_u64(v5);
            uint32x2_t n6 = vmovn_u64(v6);
            uint32x2_t n7 = vmovn_u64(v7);

            uint16x4_t m0 = vmovn_u32(vcombine_u32(n0, n1));
            uint16x4_t m1 = vmovn_u32(vcombine_u32(n2, n3));
            uint16x4_t m2 = vmovn_u32(vcombine_u32(n4, n5));
            uint16x4_t m3 = vmovn_u32(vcombine_u32(n6, n7));

            uint8x8_t b01 = vmovn_u16(vcombine_u16(m0, m1));
            uint8x8_t b23 = vmovn_u16(vcombine_u16(m2, m3));
            uint8x16_t b = vcombine_u8(b01, b23);
            b = vandq_u8(b, vmask);
            vst1q_u8(tmp8, b);

            // Pack 16x6 bits => 12 bytes (big-endian bitstream):
            // For each group of 4 values (24 bits) -> 3 bytes.
            for (int k = 0; k < 16; k += 4) {
                uint32_t a = (uint32_t)tmp8[k + 0];
                uint32_t b2 = (uint32_t)tmp8[k + 1];
                uint32_t c = (uint32_t)tmp8[k + 2];
                uint32_t d = (uint32_t)tmp8[k + 3];
                uint32_t bits24 = (a << 18) | (b2 << 12) | (c << 6) | d;
                buf[out++] = (uint8_t)(bits24 >> 16);
                buf[out++] = (uint8_t)(bits24 >> 8);
                buf[out++] = (uint8_t)(bits24);
            }
        }
        // tail: scalar bit-buffer
        const int w = 6;
        const int byte_limit = (n * w + 7) / 8;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x3FULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) {
                    buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
        }
        if (bits > 0 && out < byte_limit) {
            buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
        }
#else
        // Scalar grouped packing: 4 values -> 3 bytes
        int out = 0;
        int i = 0;
        for (; i + 3 < n; i += 4) {
            uint32_t a = (uint32_t)((uint64_t)values[i + 0] & 0x3FULL);
            uint32_t b = (uint32_t)((uint64_t)values[i + 1] & 0x3FULL);
            uint32_t c = (uint32_t)((uint64_t)values[i + 2] & 0x3FULL);
            uint32_t d = (uint32_t)((uint64_t)values[i + 3] & 0x3FULL);
            uint32_t bits24 = (a << 18) | (b << 12) | (c << 6) | d;
            buf[out++] = (uint8_t)(bits24 >> 16);
            buf[out++] = (uint8_t)(bits24 >> 8);
            buf[out++] = (uint8_t)(bits24);
        }
        if (i < n) {
            const int w = 6;
            const int byte_limit = (n * w + 7) / 8;
            uint64_t bitbuf = 0;
            int bits = 0;
            for (; i < n; i++) {
                uint64_t v = (uint64_t)values[i] & 0x3FULL;
                bitbuf = (bitbuf << w) | v;
                bits += w;
                while (bits >= 8) {
                    bits -= 8;
                    if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
            if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
        }
#endif
    }

    static FORCE_INLINE void pack_n_values_7(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        int out = 0;
        const uint8x16_t vmask = vdupq_n_u8((uint8_t)0x7F);
        uint8_t tmp8[16];
        for (; i + 15 < n; i += 16) {  // 16 values -> 14 bytes
            // Load 16x64 -> narrow to 16x8 (low bits), then pack 7-bit stream.
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint64x2_t v2 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 4));
            uint64x2_t v3 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 6));
            uint64x2_t v4 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 8));
            uint64x2_t v5 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 10));
            uint64x2_t v6 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 12));
            uint64x2_t v7 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 14));

            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x2_t n2 = vmovn_u64(v2);
            uint32x2_t n3 = vmovn_u64(v3);
            uint32x2_t n4 = vmovn_u64(v4);
            uint32x2_t n5 = vmovn_u64(v5);
            uint32x2_t n6 = vmovn_u64(v6);
            uint32x2_t n7 = vmovn_u64(v7);

            uint16x4_t m0 = vmovn_u32(vcombine_u32(n0, n1));
            uint16x4_t m1 = vmovn_u32(vcombine_u32(n2, n3));
            uint16x4_t m2 = vmovn_u32(vcombine_u32(n4, n5));
            uint16x4_t m3 = vmovn_u32(vcombine_u32(n6, n7));

            uint8x8_t b01 = vmovn_u16(vcombine_u16(m0, m1));
            uint8x8_t b23 = vmovn_u16(vcombine_u16(m2, m3));
            uint8x16_t b = vcombine_u8(b01, b23);
            b = vandq_u8(b, vmask);
            vst1q_u8(tmp8, b);

            // Pack 8x7 bits => 7 bytes, twice per 16 values.
            for (int base = 0; base < 16; base += 8) {
                uint64_t bitbuf = 0;
                int bits = 0;
                for (int k = 0; k < 8; k++) {
                    bitbuf = (bitbuf << 7) | (uint64_t)tmp8[base + k];
                    bits += 7;
                    while (bits >= 8) {
                        bits -= 8;
                        buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                    }
                }
                // bits should be 0 here (56 bits -> 7 bytes)
            }
        }
        // tail: scalar bit-buffer
        const int w = 7;
        const int byte_limit = (n * w + 7) / 8;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x7FULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) {
                    buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
        }
        if (bits > 0 && out < byte_limit) {
            buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
        }
#else
        // Scalar bit-buffer
        const int w = 7;
        const int byte_limit = (n * w + 7) / 8;
        int out = 0;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (int i = 0; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x7FULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
            }
        }
        if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
#endif
    }

    static FORCE_INLINE void pack_n_values_9(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        int out = 0;
        const uint16x8_t vmask = vdupq_n_u16((uint16_t)0x01FF);
        uint16_t tmp16[8];
        for (; i + 7 < n; i += 8) {  // 8 values -> 9 bytes
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint64x2_t v2 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 4));
            uint64x2_t v3 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 6));
            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x2_t n2 = vmovn_u64(v2);
            uint32x2_t n3 = vmovn_u64(v3);
            uint16x4_t u0 = vmovn_u32(vcombine_u32(n0, n1));
            uint16x4_t u1 = vmovn_u32(vcombine_u32(n2, n3));
            uint16x8_t u = vcombine_u16(u0, u1);
            u = vandq_u16(u, vmask);
            vst1q_u16(tmp16, u);

            uint64_t bitbuf = 0;
            int bits = 0;
            for (int k = 0; k < 8; k++) {
                bitbuf = (bitbuf << 9) | (uint64_t)tmp16[k];
                bits += 9;
                while (bits >= 8) {
                    bits -= 8;
                    buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
            // 72 bits -> 9 bytes, bits ends at 0.
        }
        // tail: scalar bit-buffer
        const int w = 9;
        const int byte_limit = (n * w + 7) / 8;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x01FFULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
            }
        }
        if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
#else
        // Scalar bit-buffer
        const int w = 9;
        const int byte_limit = (n * w + 7) / 8;
        int out = 0;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (int i = 0; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x01FFULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
            }
        }
        if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
#endif
    }

    static FORCE_INLINE void pack_n_values_10(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        int out = 0;
        const uint16x8_t vmask = vdupq_n_u16((uint16_t)0x03FF);
        uint16_t tmp16[8];
        for (; i + 7 < n; i += 8) {  // 8 values -> 10 bytes
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint64x2_t v2 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 4));
            uint64x2_t v3 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 6));
            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x2_t n2 = vmovn_u64(v2);
            uint32x2_t n3 = vmovn_u64(v3);
            uint16x4_t u0 = vmovn_u32(vcombine_u32(n0, n1));
            uint16x4_t u1 = vmovn_u32(vcombine_u32(n2, n3));
            uint16x8_t u = vcombine_u16(u0, u1);
            u = vandq_u16(u, vmask);
            vst1q_u16(tmp16, u);

            uint64_t bitbuf = 0;
            int bits = 0;
            for (int k = 0; k < 8; k++) {
                bitbuf = (bitbuf << 10) | (uint64_t)tmp16[k];
                bits += 10;
                while (bits >= 8) {
                    bits -= 8;
                    buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
            // 80 bits -> 10 bytes
        }
        // tail: scalar bit-buffer
        const int w = 10;
        const int byte_limit = (n * w + 7) / 8;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x03FFULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
            }
        }
        if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
#else
        const int w = 10;
        const int byte_limit = (n * w + 7) / 8;
        int out = 0;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (int i = 0; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x03FFULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
            }
        }
        if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
#endif
    }

    static FORCE_INLINE void pack_n_values_11(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        int out = 0;
        const uint16x8_t vmask = vdupq_n_u16((uint16_t)0x07FF);
        uint16_t tmp16[8];
        for (; i + 7 < n; i += 8) {  // 8 values -> 11 bytes
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint64x2_t v2 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 4));
            uint64x2_t v3 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 6));
            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x2_t n2 = vmovn_u64(v2);
            uint32x2_t n3 = vmovn_u64(v3);
            uint16x4_t u0 = vmovn_u32(vcombine_u32(n0, n1));
            uint16x4_t u1 = vmovn_u32(vcombine_u32(n2, n3));
            uint16x8_t u = vcombine_u16(u0, u1);
            u = vandq_u16(u, vmask);
            vst1q_u16(tmp16, u);

            uint64_t bitbuf = 0;
            int bits = 0;
            for (int k = 0; k < 8; k++) {
                bitbuf = (bitbuf << 11) | (uint64_t)tmp16[k];
                bits += 11;
                while (bits >= 8) {
                    bits -= 8;
                    buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
            // 88 bits -> 11 bytes
        }
        // tail: scalar bit-buffer
        const int w = 11;
        const int byte_limit = (n * w + 7) / 8;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x07FFULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
            }
        }
        if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
#else
        const int w = 11;
        const int byte_limit = (n * w + 7) / 8;
        int out = 0;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (int i = 0; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x07FFULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
            }
        }
        if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
#endif
    }

    static FORCE_INLINE void pack_n_values_33(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        const int w = 33;
        const int byte_limit = (n * w + 7) / 8;
        int out = 0;
        uint64_t bitbuf = 0;
        int bits = 0;
        const uint64x2_t vmask = vdupq_n_u64(0x1FFFFFFFFULL);
        uint64_t tmp64[2];
        int i = 0;
        for (; i + 1 < n; i += 2) {
            uint64x2_t v = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i));
            v = vandq_u64(v, vmask);
            vst1q_u64(tmp64, v);
            for (int k = 0; k < 2; k++) {
                bitbuf = (bitbuf << w) | (tmp64[k] & 0x1FFFFFFFFULL);
                bits += w;
                while (bits >= 8) {
                    bits -= 8;
                    if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
        }
        for (; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x1FFFFFFFFULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
            }
        }
        if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
#else
        const int w = 33;
        const int byte_limit = (n * w + 7) / 8;
        int out = 0;
        uint64_t bitbuf = 0;
        int bits = 0;
        for (int i = 0; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & 0x1FFFFFFFFULL;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
            }
        }
        if (bits > 0 && out < byte_limit) buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
#endif
    }

    static FORCE_INLINE void pack_n_values_24(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        for (; i + 3 < n; i += 4) {  // 4 values -> 12 bytes
            // Load 4x64-bit (two vectors), narrow to 4x32-bit, then write 3 bytes/value big-endian.
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x4_t x = vcombine_u32(n0, n1);  // [x0 x1 x2 x3] as u32 lanes

            // Convert to bytes in big-endian order per u32 lane, then drop the top byte.
            uint8x16_t b = vreinterpretq_u8_u32(x);
            uint8x16_t be = vrev32q_u8(b);  // for each u32: [b3 b2 b1 b0]

            uint8_t tmp[16];
            vst1q_u8(tmp, be);
            // tmp layout after vrev32: x0(4B),x1(4B),x2(4B),x3(4B) each big-endian.
            // Emit low 24 bits => drop tmp[k*4 + 0], keep [1..3].
            for (int k = 0; k < 4; k++) {
                const int o = (i + k) * 3;
                buf[o + 0] = tmp[k * 4 + 1];
                buf[o + 1] = tmp[k * 4 + 2];
                buf[o + 2] = tmp[k * 4 + 3];
            }
        }
        for (; i < n; i++) {
            uint32_t v = (uint32_t)(values[i] & 0xFFFFFFu);
            const int o = i * 3;
            buf[o + 0] = (uint8_t)(v >> 16);
            buf[o + 1] = (uint8_t)(v >> 8);
            buf[o + 2] = (uint8_t)(v & 0xFF);
        }
#else
        for (int i = 0; i < n; i++) {
            uint32_t v = (uint32_t)(values[i] & 0xFFFFFFu);
            const int o = i * 3;
            buf[o + 0] = (uint8_t)(v >> 16);
            buf[o + 1] = (uint8_t)(v >> 8);
            buf[o + 2] = (uint8_t)(v & 0xFF);
        }
#endif
    }

    static FORCE_INLINE void unpack_n_values_24(const uint8_t *buf, int n, int64_t *values) {
        for (int i = 0; i < n; i++) {
            const int o = i * 3;
            uint32_t v = ((uint32_t)(buf[o + 0] & 0xFF) << 16) | ((uint32_t)(buf[o + 1] & 0xFF) << 8) |
                         (uint32_t)(buf[o + 2] & 0xFF);
            values[i] = (int64_t)v;
        }
    }

    static FORCE_INLINE void pack_n_values_32(const int64_t *values, int n, uint8_t *buf) {
#if defined(__ARM_NEON)
        int i = 0;
        for (; i + 3 < n; i += 4) {  // 4 values -> 16 bytes
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x4_t x = vcombine_u32(n0, n1);
            // Store big-endian per u32.
            uint8x16_t b = vreinterpretq_u8_u32(x);
            uint8x16_t be = vrev32q_u8(b);
            vst1q_u8(buf + (i * 4), be);
        }
        for (; i < n; i++) {
            uint32_t v = (uint32_t)(values[i] & 0xFFFFFFFFu);
            const int o = i * 4;
            buf[o + 0] = (uint8_t)(v >> 24);
            buf[o + 1] = (uint8_t)(v >> 16);
            buf[o + 2] = (uint8_t)(v >> 8);
            buf[o + 3] = (uint8_t)(v & 0xFF);
        }
#else
        for (int i = 0; i < n; i++) {
            uint32_t v = (uint32_t)(values[i] & 0xFFFFFFFFu);
            const int o = i * 4;
            buf[o + 0] = (uint8_t)(v >> 24);
            buf[o + 1] = (uint8_t)(v >> 16);
            buf[o + 2] = (uint8_t)(v >> 8);
            buf[o + 3] = (uint8_t)(v & 0xFF);
        }
#endif
    }

    static FORCE_INLINE void unpack_n_values_32(const uint8_t *buf, int n, int64_t *values) {
        for (int i = 0; i < n; i++) {
            const int o = i * 4;
            uint32_t v = ((uint32_t)(buf[o + 0] & 0xFF) << 24) | ((uint32_t)(buf[o + 1] & 0xFF) << 16) |
                         ((uint32_t)(buf[o + 2] & 0xFF) << 8) | (uint32_t)(buf[o + 3] & 0xFF);
            values[i] = (int64_t)v;
        }
    }

#if defined(__ARM_NEON)
    // Generic NEON-assisted bit-buffer pack/unpack for widths 1..31.
    // Semantics MUST match Java LongPacker.packNValues/unpackNValues (big-endian bitstream).
    //
    // Note: This is not a byte-aligned memcpy; it uses NEON to speed up value ingest/masking
    // and (for unpack) batch stores. The bitstream assembly still uses a scalar bit-buffer,
    // because big-endian arbitrary-bit packing is inherently a cross-lane operation.
    static FORCE_INLINE void pack_n_values_neon_generic(const int64_t *values, int n, uint8_t *buf, int w) {
        const int byte_limit = (n * w + 7) / 8;
        int out = 0;
        uint64_t bitbuf = 0;
        int bits = 0;

        const uint32_t mask32 = (w == 32) ? 0xFFFFFFFFu : ((1u << w) - 1u);
        const uint32x4_t vmask = vdupq_n_u32(mask32);

        int i = 0;
        uint32_t tmp[4];
        for (; i + 3 < n; i += 4) {
            uint64x2_t v0 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 0));
            uint64x2_t v1 = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i + 2));
            uint32x2_t n0 = vmovn_u64(v0);
            uint32x2_t n1 = vmovn_u64(v1);
            uint32x4_t x = vcombine_u32(n0, n1);
            x = vandq_u32(x, vmask);
            vst1q_u32(tmp, x);

            // Append 4 masked values to big-endian bitstream.
            for (int k = 0; k < 4; k++) {
                bitbuf = (bitbuf << w) | (uint64_t)(tmp[k] & mask32);
                bits += w;
                while (bits >= 8) {
                    bits -= 8;
                    if (out < byte_limit) {
                        buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                    }
                }
            }
        }
        for (; i < n; i++) {
            uint64_t v = (uint64_t)values[i] & (uint64_t)mask32;
            bitbuf = (bitbuf << w) | v;
            bits += w;
            while (bits >= 8) {
                bits -= 8;
                if (out < byte_limit) {
                    buf[out++] = (uint8_t)((bitbuf >> bits) & 0xFF);
                }
            }
        }
        if (bits > 0 && out < byte_limit) {
            buf[out++] = (uint8_t)((bitbuf << (8 - bits)) & 0xFF);
        }
    }

    static FORCE_INLINE void unpack_n_values_neon_generic(const uint8_t *buf, int n, int64_t *values, int w) {
        int byte_idx = 0;
        uint64_t bitbuf = 0;
        int bits = 0;
        const uint64_t mask = mask_w_u64(w);

        int i = 0;
        for (; i + 3 < n; i += 4) {
            uint32_t out4[4];
            for (int k = 0; k < 4; k++) {
                while (bits < w) {
                    bitbuf = (bitbuf << 8) | (uint64_t)(buf[byte_idx++] & 0xFF);
                    bits += 8;
                }
                bits -= w;
                out4[k] = (uint32_t)((bitbuf >> bits) & mask);
            }
            uint32x4_t v = vld1q_u32(out4);
            // Widen to u64 lanes then store as int64_t scalars.
            uint32x2_t lo = vget_low_u32(v);
            uint32x2_t hi = vget_high_u32(v);
            uint64x2_t vlo = vmovl_u32(lo);
            uint64x2_t vhi = vmovl_u32(hi);
            uint64_t tmp64[4];
            vst1q_u64(tmp64 + 0, vlo);
            vst1q_u64(tmp64 + 2, vhi);
            values[i + 0] = (int64_t)tmp64[0];
            values[i + 1] = (int64_t)tmp64[1];
            values[i + 2] = (int64_t)tmp64[2];
            values[i + 3] = (int64_t)tmp64[3];
        }
        for (; i < n; i++) {
            while (bits < w) {
                bitbuf = (bitbuf << 8) | (uint64_t)(buf[byte_idx++] & 0xFF);
                bits += 8;
            }
            bits -= w;
            values[i] = (int64_t)((bitbuf >> bits) & mask);
        }
    }
#endif

    void pack_n_values_generic(const int64_t *values, int offset, int n, uint8_t *buf) const {
        int buf_idx = 0;
        int value_idx = offset;
        int left_bit = 0;
        int byte_limit = (n * width_ + 7) / 8;

        while (value_idx < n + offset && buf_idx < byte_limit) {
            uint64_t buffer = 0;
            int left_size = 64;

            if (left_bit > 0) {
                buffer |= ((uint64_t)values[value_idx] << (64 - left_bit));
                left_size -= left_bit;
                left_bit = 0;
                value_idx++;
            }

            while (left_size >= width_ && value_idx < n + offset) {
                buffer |= ((uint64_t)values[value_idx] << (left_size - width_));
                left_size -= width_;
                value_idx++;
            }
            if (left_size > 0 && value_idx < n + offset) {
                buffer |= ((uint64_t)values[value_idx] >> (width_ - left_size));
                left_bit = width_ - left_size;
            }

            for (int j = 0; j < 8 && buf_idx < byte_limit; j++) {
                buf[buf_idx] = (uint8_t)((buffer >> ((8 - j - 1) * 8)) & 0xFF);
                buf_idx++;
            }
        }
    }

    void unpack_n_values_generic(const uint8_t *buf, int offset, int n, int64_t *values) const {
        int byte_idx = offset;
        int value_idx = 0;
        int left_bits = 8;
        int total_bits = 0;

        while (value_idx < n) {
            uint64_t v = 0;
            total_bits = 0;
            while (total_bits < width_) {
                if (width_ - total_bits >= left_bits) {
                    v <<= left_bits;
                    v |= ((uint64_t)((1ULL << left_bits) - 1ULL) &
                          (uint64_t)(buf[byte_idx] & 0xFF));
                    total_bits += left_bits;
                    byte_idx++;
                    left_bits = 8;
                } else {
                    int t = width_ - total_bits;
                    v <<= t;
                    v |= ((uint64_t)((((1ULL << left_bits) - 1ULL) &
                                      (uint64_t)(buf[byte_idx] & 0xFF)) >>
                                     (left_bits - t)));
                    left_bits -= t;
                    total_bits += t;
                }
            }
            values[value_idx] = (int64_t)v;
            value_idx++;
        }
    }

    int width_;
    bool enable_simd_;
};

}  // namespace bitpacking
}  // namespace storage

#endif  // ENCODING_BITPACKING_INT64_PACKER_N_H

