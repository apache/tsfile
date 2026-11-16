/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#ifndef ENCODING_ALP_AVX2_H
#define ENCODING_ALP_AVX2_H

// Native AVX2 copies of the ALP SIMD kernels. The library is built for the
// portable x86-64 baseline, so the SIMDe kernels in alp_simde.h compile into
// SIMDe's portable fallback (in particular the element-wise emulated gathers
// in the bit-unpack). These copies are compiled with
// __attribute__((target("avx2"))) and selected at runtime by
// common::cpu_has_avx2(); see common/simd_dispatch.h. The portable SIMDe
// kernels remain the fallback for non-x86 targets, MSVC, and CPUs without
// AVX2, and both paths must produce identical bytes and values.

#include <stdint.h>

#include "alp_traits.h"
#include "common/simd_dispatch.h"

#ifdef TSFILE_X86_AVX2_DISPATCH

#include <immintrin.h>

namespace storage {
namespace alp {
namespace avx2_detail {

// AVX2 has no packed double -> int64 conversion (that arrives with AVX-512DQ),
// so each lane uses the scalar SSE2 conversion instruction. It truncates like
// _mm256_cvttps_epi32 and yields the "integer indefinite" value for NaN and
// out-of-range inputs; the callers range-check the result before using it.
TSFILE_TARGET_AVX2
static inline __m256i AlpAvx2CvttpdEpi64(__m256d v) {
#if defined(__x86_64__)
    const __m128d lo = _mm256_castpd256_pd128(v);
    const __m128d hi = _mm256_extractf128_pd(v, 1);
    const long long l0 = _mm_cvttsd_si64(lo);
    const long long l1 = _mm_cvttsd_si64(_mm_unpackhi_pd(lo, lo));
    const long long h0 = _mm_cvttsd_si64(hi);
    const long long h1 = _mm_cvttsd_si64(_mm_unpackhi_pd(hi, hi));
    return _mm256_setr_epi64x(l0, l1, h0, h1);
#else
    // 32-bit x86 has no scalar 64-bit conversion; SIMDe's own fallback uses a
    // C cast here too.
    alignas(32) double lanes[4];
    _mm256_storeu_pd(lanes, v);
    return _mm256_setr_epi64x(
        static_cast<int64_t>(lanes[0]), static_cast<int64_t>(lanes[1]),
        static_cast<int64_t>(lanes[2]), static_cast<int64_t>(lanes[3]));
#endif
}

// Same story for int64 -> double: no packed AVX2 conversion. The scalar
// conversion is the one the scalar reference and the SIMDe fallback use, so
// the rounding is identical.
TSFILE_TARGET_AVX2
static inline __m256d AlpAvx2Cvtepi64Pd(__m256i v) {
    alignas(32) int64_t lanes[4];
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(lanes), v);
    return _mm256_setr_pd(
        static_cast<double>(lanes[0]), static_cast<double>(lanes[1]),
        static_cast<double>(lanes[2]), static_cast<double>(lanes[3]));
}

template <typename T>
struct AlpAvx2Kernel;

template <>
struct AlpAvx2Kernel<float> {
    TSFILE_TARGET_AVX2
    static AlpStatus EncodeValues(const float* values, uint32_t count,
                                  uint8_t factor, uint8_t exponent,
                                  int32_t* encoded, uint8_t* bitmap,
                                  float* exceptions,
                                  uint32_t* exception_count) {
        typedef AlpTypeTraits<float> Traits;
        const float factor_value = static_cast<float>(ALP_FACT[factor]);
        const float frac_value = ALP_FLOAT_FRAC[exponent];

        // Keep the scalar reference's multiplication order -- (value *
        // 10^exponent) * 10^-factor -- so both paths emit identical bytes.
        const __m256 vexp = _mm256_set1_ps(ALP_FLOAT_EXP[exponent]);
        const __m256 vfrac_factor = _mm256_set1_ps(ALP_FLOAT_FRAC[factor]);
        const __m256 vmagic = _mm256_set1_ps(ALP_FLOAT_MAGIC);
        const __m256 vfactor = _mm256_set1_ps(factor_value);
        const __m256 vfrac = _mm256_set1_ps(frac_value);
        const __m256 vlower = _mm256_set1_ps(ALP_FLOAT_ENCODING_LOWER_LIMIT);
        const __m256 vupper = _mm256_set1_ps(ALP_FLOAT_ENCODING_UPPER_LIMIT);
        const __m256i vnegzero =
            _mm256_set1_epi32(static_cast<int32_t>(0x80000000u));
        const __m256i vall = _mm256_set1_epi32(-1);

        uint32_t ex_count = 0;
        uint32_t i = 0;
        for (; i + 7 < count; i += 8) {
            const __m256 v = _mm256_loadu_ps(values + i);
            const __m256 scaled =
                _mm256_mul_ps(_mm256_mul_ps(v, vexp), vfrac_factor);
            const __m256 rounded =
                _mm256_sub_ps(_mm256_add_ps(scaled, vmagic), vmagic);
            const __m256i vi = _mm256_cvttps_epi32(rounded);
            const __m256 back = _mm256_mul_ps(
                _mm256_mul_ps(_mm256_cvtepi32_ps(vi), vfactor), vfrac);

            const __m256 in_range =
                _mm256_and_ps(_mm256_cmp_ps(scaled, vlower, _CMP_GE_OQ),
                              _mm256_cmp_ps(scaled, vupper, _CMP_LE_OQ));
            const __m256 rounded_in_range =
                _mm256_and_ps(_mm256_cmp_ps(rounded, vlower, _CMP_GE_OQ),
                              _mm256_cmp_ps(rounded, vupper, _CMP_LE_OQ));
            const __m256 ordered = _mm256_cmp_ps(v, v, _CMP_ORD_Q);
            const __m256 equal = _mm256_cmp_ps(back, v, _CMP_EQ_OQ);
            const __m256i is_negzero =
                _mm256_cmpeq_epi32(_mm256_castps_si256(v), vnegzero);
            const __m256 not_negzero =
                _mm256_castsi256_ps(_mm256_xor_si256(is_negzero, vall));
            const __m256 ok = _mm256_and_ps(
                _mm256_and_ps(in_range, rounded_in_range),
                _mm256_and_ps(ordered, _mm256_and_ps(equal, not_negzero)));
            const int mask = _mm256_movemask_ps(ok);

            _mm256_storeu_si256(reinterpret_cast<__m256i*>(encoded + i), vi);
            for (int lane = 0; lane < 8; ++lane) {
                if ((mask & (1 << lane)) == 0) {
                    const uint32_t index = i + static_cast<uint32_t>(lane);
                    bitmap[index >> 3] |=
                        static_cast<uint8_t>(1u << (index & 7u));
                    exceptions[ex_count++] = values[index];
                    encoded[index] = 0;
                }
            }
        }

        for (; i < count; ++i) {
            int32_t value = 0;
            if (!Traits::EncodeValue(values[i], factor, exponent, &value) ||
                !Traits::BitwiseEqual(
                    Traits::DecodeValue(value, factor, exponent), values[i])) {
                bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
                exceptions[ex_count++] = values[i];
            } else {
                encoded[i] = value;
            }
        }
        *exception_count = ex_count;
        return ALP_OK;
    }

    TSFILE_TARGET_AVX2
    static AlpStatus DecodeValues(const int32_t* encoded, uint32_t count,
                                  uint8_t factor, uint8_t exponent,
                                  const uint8_t* bitmap,
                                  const float* exceptions,
                                  uint32_t exception_count, float* out) {
        typedef AlpTypeTraits<float> Traits;
        const __m256 vfactor =
            _mm256_set1_ps(static_cast<float>(ALP_FACT[factor]));
        const __m256 vfrac = _mm256_set1_ps(ALP_FLOAT_FRAC[exponent]);

        uint32_t i = 0;
        for (; i + 7 < count; i += 8) {
            const __m256i vi = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(encoded + i));
            const __m256 value = _mm256_mul_ps(
                _mm256_mul_ps(_mm256_cvtepi32_ps(vi), vfactor), vfrac);
            _mm256_storeu_ps(out + i, value);
        }
        for (; i < count; ++i) {
            out[i] = Traits::DecodeValue(encoded[i], factor, exponent);
        }

        uint32_t exception_index = 0;
        for (uint32_t index = 0; index < count; ++index) {
            if (bitmap != nullptr &&
                (bitmap[index >> 3] &
                 static_cast<uint8_t>(1u << (index & 7u))) != 0) {
                if (exception_index >= exception_count) {
                    return ALP_MALFORMED;
                }
                out[index] = exceptions[exception_index++];
            }
        }
        return exception_index == exception_count ? ALP_OK : ALP_MALFORMED;
    }
};

template <>
struct AlpAvx2Kernel<double> {
    TSFILE_TARGET_AVX2
    static AlpStatus EncodeValues(const double* values, uint32_t count,
                                  uint8_t factor, uint8_t exponent,
                                  int64_t* encoded, uint8_t* bitmap,
                                  double* exceptions,
                                  uint32_t* exception_count) {
        typedef AlpTypeTraits<double> Traits;
        const double factor_value = static_cast<double>(ALP_FACT[factor]);
        const double frac_value = ALP_DOUBLE_FRAC[exponent];

        // Same multiplication order as the scalar reference.
        const __m256d vexp = _mm256_set1_pd(ALP_DOUBLE_EXP[exponent]);
        const __m256d vfrac_factor = _mm256_set1_pd(ALP_DOUBLE_FRAC[factor]);
        const __m256d vmagic = _mm256_set1_pd(ALP_DOUBLE_MAGIC);
        const __m256d vfactor = _mm256_set1_pd(factor_value);
        const __m256d vfrac = _mm256_set1_pd(frac_value);
        const __m256d vlower = _mm256_set1_pd(ALP_DOUBLE_ENCODING_LOWER_LIMIT);
        const __m256d vupper = _mm256_set1_pd(ALP_DOUBLE_ENCODING_UPPER_LIMIT);
        const __m256i vnegzero =
            _mm256_set1_epi64x(static_cast<int64_t>(0x8000000000000000ULL));
        const __m256i vall = _mm256_set1_epi32(-1);

        uint32_t ex_count = 0;
        uint32_t i = 0;
        for (; i + 3 < count; i += 4) {
            const __m256d v = _mm256_loadu_pd(values + i);
            const __m256d scaled =
                _mm256_mul_pd(_mm256_mul_pd(v, vexp), vfrac_factor);
            const __m256d rounded =
                _mm256_sub_pd(_mm256_add_pd(scaled, vmagic), vmagic);
            const __m256i vi = AlpAvx2CvttpdEpi64(rounded);
            const __m256d back = _mm256_mul_pd(
                _mm256_mul_pd(AlpAvx2Cvtepi64Pd(vi), vfactor), vfrac);

            const __m256d in_range =
                _mm256_and_pd(_mm256_cmp_pd(scaled, vlower, _CMP_GE_OQ),
                              _mm256_cmp_pd(scaled, vupper, _CMP_LE_OQ));
            const __m256d rounded_in_range =
                _mm256_and_pd(_mm256_cmp_pd(rounded, vlower, _CMP_GE_OQ),
                              _mm256_cmp_pd(rounded, vupper, _CMP_LE_OQ));
            const __m256d ordered = _mm256_cmp_pd(v, v, _CMP_ORD_Q);
            const __m256d equal = _mm256_cmp_pd(back, v, _CMP_EQ_OQ);
            const __m256i is_negzero =
                _mm256_cmpeq_epi64(_mm256_castpd_si256(v), vnegzero);
            const __m256d not_negzero =
                _mm256_castsi256_pd(_mm256_xor_si256(is_negzero, vall));
            const __m256d ok = _mm256_and_pd(
                _mm256_and_pd(in_range, rounded_in_range),
                _mm256_and_pd(ordered, _mm256_and_pd(equal, not_negzero)));
            const int mask = _mm256_movemask_pd(ok);

            _mm256_storeu_si256(reinterpret_cast<__m256i*>(encoded + i), vi);
            for (int lane = 0; lane < 4; ++lane) {
                if ((mask & (1 << lane)) == 0) {
                    const uint32_t index = i + static_cast<uint32_t>(lane);
                    bitmap[index >> 3] |=
                        static_cast<uint8_t>(1u << (index & 7u));
                    exceptions[ex_count++] = values[index];
                    encoded[index] = 0;
                }
            }
        }

        for (; i < count; ++i) {
            int64_t value = 0;
            if (!Traits::EncodeValue(values[i], factor, exponent, &value) ||
                !Traits::BitwiseEqual(
                    Traits::DecodeValue(value, factor, exponent), values[i])) {
                bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
                exceptions[ex_count++] = values[i];
            } else {
                encoded[i] = value;
            }
        }
        *exception_count = ex_count;
        return ALP_OK;
    }

    TSFILE_TARGET_AVX2
    static AlpStatus DecodeValues(const int64_t* encoded, uint32_t count,
                                  uint8_t factor, uint8_t exponent,
                                  const uint8_t* bitmap,
                                  const double* exceptions,
                                  uint32_t exception_count, double* out) {
        typedef AlpTypeTraits<double> Traits;
        const __m256d vfactor =
            _mm256_set1_pd(static_cast<double>(ALP_FACT[factor]));
        const __m256d vfrac = _mm256_set1_pd(ALP_DOUBLE_FRAC[exponent]);

        uint32_t i = 0;
        for (; i + 3 < count; i += 4) {
            const __m256i vi = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(encoded + i));
            const __m256d value = _mm256_mul_pd(
                _mm256_mul_pd(AlpAvx2Cvtepi64Pd(vi), vfactor), vfrac);
            _mm256_storeu_pd(out + i, value);
        }
        for (; i < count; ++i) {
            out[i] = Traits::DecodeValue(encoded[i], factor, exponent);
        }

        uint32_t exception_index = 0;
        for (uint32_t index = 0; index < count; ++index) {
            if (bitmap != nullptr &&
                (bitmap[index >> 3] &
                 static_cast<uint8_t>(1u << (index & 7u))) != 0) {
                if (exception_index >= exception_count) {
                    return ALP_MALFORMED;
                }
                out[index] = exceptions[exception_index++];
            }
        }
        return exception_index == exception_count ? ALP_OK : ALP_MALFORMED;
    }
};

// Both unpack kernels gather one 64-bit window per lane, so a lane can read up
// to 8 bytes starting at that value's byte offset. Callers must pass the padded
// ALP body produced by AlpPackBits, which reserves at least 16 zero guard bytes
// past the logical stream; AlpDecodeBlock validates body_bytes against that
// padded size before unpacking. The kernels process whole groups of four only:
// AlpUnpackBits passes count & ~3 and decodes the remaining values itself.
template <typename Unsigned>
struct AlpAvx2UnpackKernel;

template <>
struct AlpAvx2UnpackKernel<uint32_t> {
    TSFILE_TARGET_AVX2
    static bool Unpack(const uint8_t* body, uint32_t count, uint8_t bit_width,
                       uint32_t* out) {
        if (bit_width == 0) {
            for (uint32_t i = 0; i < count; ++i) {
                out[i] = 0;
            }
            return true;
        }
        const uint64_t mask64 =
            bit_width >= 32 ? static_cast<uint64_t>(0xFFFFFFFFu)
                            : ((static_cast<uint64_t>(1) << bit_width) - 1);
        const __m256i mask = _mm256_set1_epi64x(static_cast<int64_t>(mask64));
        const __m256i perm = _mm256_setr_epi32(0, 2, 4, 6, 0, 0, 0, 0);
        for (uint32_t i = 0; i + 3 < count; i += 4) {
            int32_t byte_offsets[4];
            int64_t bit_offsets[4];
            for (int lane = 0; lane < 4; ++lane) {
                const uint64_t bit_pos =
                    static_cast<uint64_t>(i + lane) * bit_width;
                byte_offsets[lane] = static_cast<int32_t>(bit_pos >> 3);
                bit_offsets[lane] = static_cast<int64_t>(bit_pos & 7u);
            }
            const __m128i indices =
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(byte_offsets));
            const __m256i windows = _mm256_i32gather_epi64(
                reinterpret_cast<const long long*>(body), indices, 1);
            const __m256i shifts = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(bit_offsets));
            const __m256i shifted = _mm256_srlv_epi64(windows, shifts);
            const __m256i masked = _mm256_and_si256(shifted, mask);
            const __m256i compact = _mm256_permutevar8x32_epi32(masked, perm);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(out + i),
                             _mm256_castsi256_si128(compact));
        }
        return true;
    }
};

template <>
struct AlpAvx2UnpackKernel<uint64_t> {
    TSFILE_TARGET_AVX2
    static bool Unpack(const uint8_t* body, uint32_t count, uint8_t bit_width,
                       uint64_t* out) {
        if (bit_width == 0) {
            for (uint32_t i = 0; i < count; ++i) {
                out[i] = 0;
            }
            return true;
        }
        const uint64_t mask64 =
            bit_width >= 64 ? ~static_cast<uint64_t>(0)
                            : ((static_cast<uint64_t>(1) << bit_width) - 1);
        const __m256i mask = _mm256_set1_epi64x(static_cast<int64_t>(mask64));
        for (uint32_t i = 0; i + 3 < count; i += 4) {
            int32_t byte_offsets[4];
            int64_t bit_offsets[4];
            for (int lane = 0; lane < 4; ++lane) {
                const uint64_t bit_pos =
                    static_cast<uint64_t>(i + lane) * bit_width;
                byte_offsets[lane] = static_cast<int32_t>(bit_pos >> 3);
                bit_offsets[lane] = static_cast<int64_t>(bit_pos & 7u);
            }
            const __m128i indices =
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(byte_offsets));
            const __m256i windows = _mm256_i32gather_epi64(
                reinterpret_cast<const long long*>(body), indices, 1);
            const __m256i shifts = _mm256_loadu_si256(
                reinterpret_cast<const __m256i*>(bit_offsets));
            const __m256i shifted = _mm256_srlv_epi64(windows, shifts);
            const __m256i masked = _mm256_and_si256(shifted, mask);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(out + i), masked);
        }
        return true;
    }
};

}  // namespace avx2_detail
}  // namespace alp
}  // namespace storage

#endif  // TSFILE_X86_AVX2_DISPATCH

#endif  // ENCODING_ALP_AVX2_H
