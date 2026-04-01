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

#ifndef ENCODING_OPTIMAL_SPRINTZ_OPTIMAL_PACK_SIZE_H
#define ENCODING_OPTIMAL_SPRINTZ_OPTIMAL_PACK_SIZE_H

#include <algorithm>
#include <cstdint>
#include <vector>

#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace storage {
namespace optimal {

// Ported from Java: org.apache.tsfile.encoding.optimal.SprintzOptimalPackSize
// Cost model: sum(pack_len * max_bitwidth_in_pack) + num_packs * bitsPerBlockOverhead.
class SprintzOptimalPackSize {
   public:
    static int find_optimal_pack_size(const int64_t *values, int n,
                                      std::vector<int> *bw_scratch = nullptr,
                                      bool enable_simd = false) {
        if (n <= 0) {
            return 1;
        }
        if (n < 8) {
            return std::max(1, n);
        }

        std::vector<int> local_bw;
        std::vector<int> *bw = bw_scratch;
        if (bw == nullptr) {
            local_bw.resize(n);
            bw = &local_bw;
        } else if ((int)bw->size() < n) {
            bw->resize(n);
        }

        // Compute per-value bitwidths; NEON best-effort on arm64.
        if (enable_simd) {
#if defined(__ARM_NEON)
            int i = 0;
            // clamp to >=1 so bitwidth>=1
            const uint64x2_t vone = vdupq_n_u64(1);
            for (; i + 1 < n; i += 2) {
                uint64x2_t v = vld1q_u64(reinterpret_cast<const uint64_t *>(values + i));
                // vmax(v,1) without relying on vmaxq_u64
                uint64x2_t mask = vcgtq_u64(v, vone);
                v = vbslq_u64(mask, v, vone);
                // clz: some toolchains don't expose vclzq_u64; use 32-bit lanes (values here are <= 2^64-1).
                uint32x4_t v32 = vreinterpretq_u32_u64(v);
                uint32x4_t clz32 = vclzq_u32(v32);
                // Each original 64-bit lane uses two 32-bit lanes.
                uint64_t lanes[2];
                uint32_t c32[4];
                vst1q_u32(c32, clz32);
                // Reconstruct clz64 for each lane:
                // If high32 != 0 => clz64 = clz(high32)
                // else clz64 = 32 + clz(low32)
                auto clz64_from_parts = [](uint32_t high32, uint32_t low32) -> uint64_t {
                    if (high32 != 0) {
                        return (uint64_t)__builtin_clz(high32);
                    }
                    return 32ULL + (uint64_t)__builtin_clz(low32);
                };
                // c32 holds clz of each 32-bit lane, but we need clz of the value parts.
                // Compute high/low words.
                uint64_t x0 = ((uint64_t)vgetq_lane_u32(v32, 1) << 32) | (uint64_t)vgetq_lane_u32(v32, 0);
                uint64_t x1 = ((uint64_t)vgetq_lane_u32(v32, 3) << 32) | (uint64_t)vgetq_lane_u32(v32, 2);
                uint32_t x0_hi = (uint32_t)(x0 >> 32), x0_lo = (uint32_t)(x0 & 0xffffffffu);
                uint32_t x1_hi = (uint32_t)(x1 >> 32), x1_lo = (uint32_t)(x1 & 0xffffffffu);
                lanes[0] = clz64_from_parts(x0_hi, x0_lo);
                lanes[1] = clz64_from_parts(x1_hi, x1_lo);
                (*bw)[i] = (int)(64 - lanes[0]);
                (*bw)[i + 1] = (int)(64 - lanes[1]);
            }
            for (; i < n; i++) {
                uint64_t v = (uint64_t)std::max<int64_t>(1, values[i]);
                (*bw)[i] = 64 - __builtin_clzll(v);
            }
#else
            for (int i = 0; i < n; i++) {
                uint64_t v = (uint64_t)std::max<int64_t>(1, values[i]);
                (*bw)[i] = 64 - __builtin_clzll(v);
            }
#endif
        } else {
            for (int i = 0; i < n; i++) {
                uint64_t v = (uint64_t)std::max<int64_t>(1, values[i]);
                (*bw)[i] = 64 - __builtin_clzll(v);
            }
        }

        const int bits_per_block_overhead = 80;  // 1B packSize + 1B bitWidth + 8B preValue

        int best_pack_size = 1;
        int max_pack_size = std::min(32, n);
        int64_t best_cost = INT64_MAX;

#if defined(__ARM_NEON)
        // If SIMD enabled, also keep an 8-bit copy of bitwidths for fast vmax reductions.
        std::vector<uint8_t> bw8_local;
        const uint8_t *bw8 = nullptr;
        if (enable_simd) {
            bw8_local.resize((size_t)n);
            for (int i = 0; i < n; i++) {
                int v = (*bw)[i];
                if (v < 1) v = 1;
                if (v > 255) v = 255;
                bw8_local[(size_t)i] = (uint8_t)v;
            }
            bw8 = bw8_local.data();
        }
        auto max_u8_block = [](const uint8_t *p, int len) -> int {
            if (len <= 0) return 1;
            int i = 0;
            uint8x16_t vmax16 = vdupq_n_u8(1);
            for (; i + 15 < len; i += 16) {
                uint8x16_t v = vld1q_u8(p + i);
                vmax16 = vmaxq_u8(vmax16, v);
            }
            uint8_t m = vmaxvq_u8(vmax16);
            for (; i < len; i++) {
                if (p[i] > m) m = p[i];
            }
            return (int)m;
        };
#endif

        for (int p = 1; p <= max_pack_size; p++) {
            int m = (n + p - 1) / p;
            int64_t cost = 0;
            for (int i = 0; i < m; i++) {
                int start = i * p;
                int end = std::min(start + p, n);
                int max_bw = 1;
#if defined(__ARM_NEON)
                if (enable_simd && bw8 != nullptr) {
                    max_bw = max_u8_block(bw8 + start, end - start);
                } else {
                    for (int j = start; j < end; j++) {
                        max_bw = std::max(max_bw, (*bw)[j]);
                    }
                }
#else
                for (int j = start; j < end; j++) {
                    max_bw = std::max(max_bw, (*bw)[j]);
                }
#endif
                cost += (int64_t)(end - start) * (int64_t)max_bw;
            }
            cost += (int64_t)m * (int64_t)bits_per_block_overhead;
            if (cost < best_cost) {
                best_cost = cost;
                best_pack_size = p;
            }
        }

        return best_pack_size;
    }
};

}  // namespace optimal
}  // namespace storage

#endif  // ENCODING_OPTIMAL_SPRINTZ_OPTIMAL_PACK_SIZE_H

