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

#ifndef ENCODING_ALP_SIMDE_H
#define ENCODING_ALP_SIMDE_H

#include <stdint.h>

#include "alp_traits.h"
#include "simde/x86/avx2.h"
#include "simde/x86/sse4.1.h"
#include "simde/x86/avx512/cvtt.h"
#include "simde/x86/avx512/cvt.h"

namespace storage {
namespace alp {

template <typename T>
struct AlpSimdKernel;

template <>
struct AlpSimdKernel<float> {
    static AlpStatus EncodeValues(const float* values, uint32_t count,
                                  uint8_t factor, uint8_t exponent,
                                  int32_t* encoded, uint8_t* bitmap,
                                  float* exceptions,
                                  uint32_t* exception_count) {
        typedef AlpTypeTraits<float> Traits;
        const float scale = ALP_FLOAT_EXP[exponent] * ALP_FLOAT_FRAC[factor];
        const float factor_value = static_cast<float>(ALP_FACT[factor]);
        const float frac_value = ALP_FLOAT_FRAC[exponent];

        const simde__m256 vscale = simde_mm256_set1_ps(scale);
        const simde__m256 vmagic = simde_mm256_set1_ps(ALP_FLOAT_MAGIC);
        const simde__m256 vfactor = simde_mm256_set1_ps(factor_value);
        const simde__m256 vfrac = simde_mm256_set1_ps(frac_value);
        const simde__m256 vlower =
            simde_mm256_set1_ps(ALP_FLOAT_ENCODING_LOWER_LIMIT);
        const simde__m256 vupper =
            simde_mm256_set1_ps(ALP_FLOAT_ENCODING_UPPER_LIMIT);
        const simde__m256i vnegzero = simde_mm256_set1_epi32(
            static_cast<int32_t>(0x80000000u));
        const simde__m256i vall = simde_mm256_set1_epi32(-1);

        uint32_t ex_count = 0;
        uint32_t i = 0;
        for (; i + 7 < count; i += 8) {
            const simde__m256 v = simde_mm256_loadu_ps(values + i);
            const simde__m256 scaled = simde_mm256_mul_ps(v, vscale);
            const simde__m256 rounded = simde_mm256_sub_ps(
                simde_mm256_add_ps(scaled, vmagic), vmagic);
            const simde__m256i vi = simde_mm256_cvttps_epi32(rounded);
            const simde__m256 back = simde_mm256_mul_ps(
                simde_mm256_mul_ps(simde_mm256_cvtepi32_ps(vi), vfactor),
                vfrac);

            const simde__m256 in_range = simde_mm256_and_ps(
                simde_mm256_cmp_ps(scaled, vlower, SIMDE_CMP_GE_OQ),
                simde_mm256_cmp_ps(scaled, vupper, SIMDE_CMP_LE_OQ));
            const simde__m256 rounded_in_range = simde_mm256_and_ps(
                simde_mm256_cmp_ps(rounded, vlower, SIMDE_CMP_GE_OQ),
                simde_mm256_cmp_ps(rounded, vupper, SIMDE_CMP_LE_OQ));
            const simde__m256 ordered =
                simde_mm256_cmp_ps(v, v, SIMDE_CMP_ORD_Q);
            const simde__m256 equal =
                simde_mm256_cmp_ps(back, v, SIMDE_CMP_EQ_OQ);
            const simde__m256i is_negzero = simde_mm256_cmpeq_epi32(
                simde_mm256_castps_si256(v), vnegzero);
            const simde__m256 not_negzero = simde_mm256_castsi256_ps(
                simde_mm256_xor_si256(is_negzero, vall));
            const simde__m256 ok = simde_mm256_and_ps(
                simde_mm256_and_ps(in_range, rounded_in_range),
                simde_mm256_and_ps(ordered,
                                   simde_mm256_and_ps(equal, not_negzero)));
            const int mask = simde_mm256_movemask_ps(ok);

            simde_mm256_storeu_si256(
                reinterpret_cast<simde__m256i*>(encoded + i), vi);
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

    static AlpStatus DecodeValues(const int32_t* encoded, uint32_t count,
                                  uint8_t factor, uint8_t exponent,
                                  const uint8_t* bitmap,
                                  const float* exceptions,
                                  uint32_t exception_count, float* out) {
        typedef AlpTypeTraits<float> Traits;
        const simde__m256 vfactor = simde_mm256_set1_ps(
            static_cast<float>(ALP_FACT[factor]));
        const simde__m256 vfrac =
            simde_mm256_set1_ps(ALP_FLOAT_FRAC[exponent]);

        uint32_t i = 0;
        for (; i + 7 < count; i += 8) {
            const simde__m256i vi = simde_mm256_loadu_si256(
                reinterpret_cast<const simde__m256i*>(encoded + i));
            const simde__m256 value = simde_mm256_mul_ps(
                simde_mm256_mul_ps(simde_mm256_cvtepi32_ps(vi), vfactor),
                vfrac);
            simde_mm256_storeu_ps(out + i, value);
        }
        for (; i < count; ++i) {
            out[i] = Traits::DecodeValue(encoded[i], factor, exponent);
        }

        uint32_t exception_index = 0;
        for (uint32_t index = 0; index < count; ++index) {
            if (bitmap != NULL &&
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
struct AlpSimdKernel<double> {
    static AlpStatus EncodeValues(const double* values, uint32_t count,
                                  uint8_t factor, uint8_t exponent,
                                  int64_t* encoded, uint8_t* bitmap,
                                  double* exceptions,
                                  uint32_t* exception_count) {
        typedef AlpTypeTraits<double> Traits;
        const double scale =
            ALP_DOUBLE_EXP[exponent] * ALP_DOUBLE_FRAC[factor];
        const double factor_value = static_cast<double>(ALP_FACT[factor]);
        const double frac_value = ALP_DOUBLE_FRAC[exponent];

        const simde__m128d vscale = simde_mm_set1_pd(scale);
        const simde__m128d vmagic = simde_mm_set1_pd(ALP_DOUBLE_MAGIC);
        const simde__m128d vfactor = simde_mm_set1_pd(factor_value);
        const simde__m128d vfrac = simde_mm_set1_pd(frac_value);
        const simde__m128d vlower =
            simde_mm_set1_pd(ALP_DOUBLE_ENCODING_LOWER_LIMIT);
        const simde__m128d vupper =
            simde_mm_set1_pd(ALP_DOUBLE_ENCODING_UPPER_LIMIT);
        const simde__m128i vnegzero = simde_mm_set1_epi64x(
            static_cast<int64_t>(0x8000000000000000ULL));
        const simde__m128i vall = simde_mm_set1_epi32(-1);

        uint32_t ex_count = 0;
        uint32_t i = 0;
        for (; i + 1 < count; i += 2) {
            const simde__m128d v = simde_mm_loadu_pd(values + i);
            const simde__m128d scaled = simde_mm_mul_pd(v, vscale);
            const simde__m128d rounded =
                simde_mm_sub_pd(simde_mm_add_pd(scaled, vmagic), vmagic);
            const simde__m128i vi = simde_mm_cvttpd_epi64(rounded);
            const simde__m128d back = simde_mm_mul_pd(
                simde_mm_mul_pd(simde_mm_cvtepi64_pd(vi), vfactor), vfrac);

            const simde__m128d in_range = simde_mm_and_pd(
                simde_mm_cmp_pd(scaled, vlower, SIMDE_CMP_GE_OQ),
                simde_mm_cmp_pd(scaled, vupper, SIMDE_CMP_LE_OQ));
            const simde__m128d rounded_in_range = simde_mm_and_pd(
                simde_mm_cmp_pd(rounded, vlower, SIMDE_CMP_GE_OQ),
                simde_mm_cmp_pd(rounded, vupper, SIMDE_CMP_LE_OQ));
            const simde__m128d ordered = simde_mm_cmp_pd(v, v, SIMDE_CMP_ORD_Q);
            const simde__m128d equal = simde_mm_cmp_pd(back, v, SIMDE_CMP_EQ_OQ);
            const simde__m128i is_negzero = simde_mm_cmpeq_epi64(
                simde_mm_castpd_si128(v), vnegzero);
            const simde__m128d not_negzero = simde_mm_castsi128_pd(
                simde_mm_xor_si128(is_negzero, vall));
            const simde__m128d ok = simde_mm_and_pd(
                simde_mm_and_pd(in_range, rounded_in_range),
                simde_mm_and_pd(ordered,
                                simde_mm_and_pd(equal, not_negzero)));
            const int mask = simde_mm_movemask_pd(ok);

            simde_mm_storeu_si128(
                reinterpret_cast<simde__m128i*>(encoded + i), vi);
            for (int lane = 0; lane < 2; ++lane) {
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

    static AlpStatus DecodeValues(const int64_t* encoded, uint32_t count,
                                  uint8_t factor, uint8_t exponent,
                                  const uint8_t* bitmap,
                                  const double* exceptions,
                                  uint32_t exception_count, double* out) {
        typedef AlpTypeTraits<double> Traits;
        const simde__m128d vfactor = simde_mm_set1_pd(
            static_cast<double>(ALP_FACT[factor]));
        const simde__m128d vfrac =
            simde_mm_set1_pd(ALP_DOUBLE_FRAC[exponent]);

        uint32_t i = 0;
        for (; i + 1 < count; i += 2) {
            const simde__m128i vi = simde_mm_loadu_si128(
                reinterpret_cast<const simde__m128i*>(encoded + i));
            const simde__m128d value = simde_mm_mul_pd(
                simde_mm_mul_pd(simde_mm_cvtepi64_pd(vi), vfactor), vfrac);
            simde_mm_storeu_pd(out + i, value);
        }
        for (; i < count; ++i) {
            out[i] = Traits::DecodeValue(encoded[i], factor, exponent);
        }

        uint32_t exception_index = 0;
        for (uint32_t index = 0; index < count; ++index) {
            if (bitmap != NULL &&
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

template <typename T>
inline AlpStatus AlpSimdEncodeValues(
    const T* values, uint32_t count, uint8_t factor, uint8_t exponent,
    typename AlpTypeTraits<T>::Encoded* encoded, uint8_t* bitmap, T* exceptions,
    uint32_t* exception_count) {
    return AlpSimdKernel<T>::EncodeValues(values, count, factor, exponent,
                                          encoded, bitmap, exceptions,
                                          exception_count);
}

template <typename T>
inline AlpStatus AlpSimdDecodeValues(
    const typename AlpTypeTraits<T>::Encoded* encoded, uint32_t count,
    uint8_t factor, uint8_t exponent, const uint8_t* bitmap,
    const T* exceptions, uint32_t exception_count, T* out) {
    return AlpSimdKernel<T>::DecodeValues(encoded, count, factor, exponent,
                                          bitmap, exceptions, exception_count,
                                          out);
}

}  // namespace alp
}  // namespace storage

#endif  // ENCODING_ALP_SIMDE_H
