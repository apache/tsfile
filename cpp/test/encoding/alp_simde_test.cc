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

#include <cstring>
#include <limits>
#include <vector>

#include "encoding/alp/alp_scalar.h"
#include "gtest/gtest.h"

#ifdef ENABLE_SIMD
#include "encoding/alp/alp_simde.h"
#endif

namespace storage {
namespace alp {

#ifdef ENABLE_SIMD

template <typename T>
static void ScalarEncodeValues(const T* values, uint32_t count, uint8_t factor,
                               uint8_t exponent,
                               typename AlpTypeTraits<T>::Encoded* encoded,
                               uint8_t* bitmap, T* exceptions,
                               uint32_t* exception_count) {
    typedef AlpTypeTraits<T> Traits;
    uint32_t ex_count = 0;
    for (uint32_t i = 0; i < count; ++i) {
        typename Traits::Encoded value = 0;
        if (!Traits::EncodeValue(values[i], factor, exponent, &value) ||
            !Traits::BitwiseEqual(Traits::DecodeValue(value, factor, exponent),
                                  values[i])) {
            bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            exceptions[ex_count++] = values[i];
        } else {
            encoded[i] = value;
        }
    }
    *exception_count = ex_count;
}

template <typename T>
static void ExpectSimdMatchesScalarForPair(const std::vector<T>& values,
                                           uint8_t factor, uint8_t exponent) {
    typedef AlpTypeTraits<T> Traits;
    ASSERT_FALSE(values.empty());

    const uint32_t count = static_cast<uint32_t>(values.size());
    std::vector<typename Traits::Encoded> scalar_encoded(count, 0);
    std::vector<typename Traits::Encoded> simd_encoded(count, 0);
    std::vector<uint8_t> scalar_bitmap((count + 7) / 8, 0);
    std::vector<uint8_t> simd_bitmap((count + 7) / 8, 0);
    std::vector<T> scalar_exceptions(count);
    std::vector<T> simd_exceptions(count);
    uint32_t scalar_exception_count = 0;
    uint32_t simd_exception_count = 0;

    ScalarEncodeValues(&values[0], count, factor, exponent, &scalar_encoded[0],
                       &scalar_bitmap[0], &scalar_exceptions[0],
                       &scalar_exception_count);
    ASSERT_EQ(ALP_OK,
              AlpSimdEncodeValues(&values[0], count, factor, exponent,
                                  &simd_encoded[0], &simd_bitmap[0],
                                  &simd_exceptions[0], &simd_exception_count));

    EXPECT_EQ(scalar_exception_count, simd_exception_count);
    EXPECT_EQ(scalar_bitmap, simd_bitmap);
    EXPECT_EQ(scalar_encoded, simd_encoded);
    ASSERT_EQ(scalar_exception_count, simd_exception_count);
    for (uint32_t i = 0; i < scalar_exception_count; ++i) {
        EXPECT_TRUE(
            Traits::BitwiseEqual(scalar_exceptions[i], simd_exceptions[i]));
    }

    std::vector<T> simd_decoded(count);
    ASSERT_EQ(ALP_OK,
              AlpSimdDecodeValues(
                  &scalar_encoded[0], count, factor, exponent,
                  scalar_exception_count == 0 ? NULL : &scalar_bitmap[0],
                  scalar_exception_count == 0 ? NULL : &scalar_exceptions[0],
                  scalar_exception_count, &simd_decoded[0]));
    for (uint32_t i = 0; i < count; ++i) {
        EXPECT_TRUE(Traits::BitwiseEqual(values[i], simd_decoded[i]))
            << "index " << i;
    }
}

template <typename T>
static void ExpectSimdMatchesScalar(const std::vector<T>& values) {
    uint8_t factor = 0;
    uint8_t exponent = 0;
    ASSERT_TRUE(AlpChooseFactorExponent(
        &values[0], static_cast<uint32_t>(values.size()), factor, exponent));
    ExpectSimdMatchesScalarForPair(values, factor, exponent);
}

TEST(AlpSimdeTest, FloatValuesMatchScalar) {
    std::vector<float> values;
    for (int i = 0; i < 1024; ++i) {
        values.push_back(static_cast<float>(i % 251) * 0.01f - 1.25f);
    }
    ExpectSimdMatchesScalar(values);
}

TEST(AlpSimdeTest, DoubleValuesMatchScalar) {
    std::vector<double> values;
    for (int i = 0; i < 1024; ++i) {
        values.push_back(static_cast<double>(i % 509) * 0.001 - 0.25);
    }
    ExpectSimdMatchesScalar(values);
}

TEST(AlpSimdeTest, SpecialValuesMatchScalar) {
    const float nan_f = std::numeric_limits<float>::quiet_NaN();
    const double nan_d = std::numeric_limits<double>::quiet_NaN();
    std::vector<float> floats = {0.0f,
                                 -0.0f,
                                 1.0f,
                                 -1.0f,
                                 nan_f,
                                 std::numeric_limits<float>::infinity(),
                                 -std::numeric_limits<float>::infinity(),
                                 std::numeric_limits<float>::denorm_min(),
                                 std::numeric_limits<float>::max()};
    ExpectSimdMatchesScalar(floats);

    std::vector<double> doubles = {0.0,
                                   -0.0,
                                   1.0,
                                   -1.0,
                                   nan_d,
                                   std::numeric_limits<double>::infinity(),
                                   -std::numeric_limits<double>::infinity(),
                                   std::numeric_limits<double>::denorm_min(),
                                   std::numeric_limits<double>::max()};
    ExpectSimdMatchesScalar(doubles);
}

// The scale factors are applied in the same order by both implementations, so
// a SIMD build and a scalar build must emit identical bytes for every
// (factor, exponent) pair -- including the pairs that only reject values.
template <typename T>
static void ExpectSimdMatchesScalarAcrossPairs(double magnitude) {
    std::vector<T> values(64);
    uint64_t state = 0x9e3779b97f4a7c15ULL;
    for (size_t i = 0; i < values.size(); ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        const double fraction = static_cast<double>(state % 100000) / 100000.0;
        values[i] = static_cast<T>(fraction * magnitude);
    }
    typedef AlpTypeTraits<T> Traits;
    for (uint8_t exponent = 0; exponent <= Traits::MaxExponent(); ++exponent) {
        for (uint8_t factor = 0; factor <= exponent; ++factor) {
            ExpectSimdMatchesScalarForPair(values, factor, exponent);
        }
    }
}

TEST(AlpSimdeTest, DoubleEncodeMatchesScalarAcrossPairs) {
    ExpectSimdMatchesScalarAcrossPairs<double>(1.0);
    ExpectSimdMatchesScalarAcrossPairs<double>(1.0e-6);
    ExpectSimdMatchesScalarAcrossPairs<double>(1.0e6);
    ExpectSimdMatchesScalarAcrossPairs<double>(1.0e12);
}

TEST(AlpSimdeTest, FloatEncodeMatchesScalarAcrossPairs) {
    ExpectSimdMatchesScalarAcrossPairs<float>(1.0);
    ExpectSimdMatchesScalarAcrossPairs<float>(1.0e-5);
    ExpectSimdMatchesScalarAcrossPairs<float>(1.0e5);
}

// A packed field wider than 57 bits cannot be reconstructed from a single
// 64-bit window, so the SIMD unpack must leave it to the scalar tail. Compare
// the production entry point against a bit-by-bit reference for every width.
template <typename Unsigned>
static Unsigned ReferenceUnpack(const uint8_t* body, uint32_t index,
                                uint8_t bit_width) {
    Unsigned value = 0;
    for (uint8_t b = 0; b < bit_width; ++b) {
        const uint64_t bit_pos = static_cast<uint64_t>(index) * bit_width + b;
        const uint8_t bit =
            static_cast<uint8_t>((body[bit_pos >> 3] >> (bit_pos & 7u)) & 1u);
        value |= static_cast<Unsigned>(bit) << b;
    }
    return value;
}

template <typename Unsigned>
static void ExpectUnpackMatchesReference(uint8_t bit_width) {
    const uint32_t count = 64;
    const Unsigned mask =
        bit_width >= sizeof(Unsigned) * 8
            ? static_cast<Unsigned>(~static_cast<Unsigned>(0))
            : static_cast<Unsigned>((static_cast<uint64_t>(1) << bit_width) -
                                    1);
    std::vector<Unsigned> values(count);
    uint64_t state = 0x243f6a8885a308d3ULL;
    for (size_t i = 0; i < values.size(); ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        values[i] = static_cast<Unsigned>(state) & mask;
    }
    std::vector<uint8_t> body;
    AlpPackBits(&values[0], count, bit_width, body);
    const uint8_t dummy = 0;
    const uint8_t* body_ptr = body.empty() ? &dummy : &body[0];

    std::vector<Unsigned> unpacked(count, 0);
    ASSERT_TRUE(AlpUnpackBits(body_ptr, static_cast<uint32_t>(body.size()),
                              count, bit_width, unpacked));
    for (uint32_t i = 0; i < count; ++i) {
        EXPECT_EQ(ReferenceUnpack<Unsigned>(body_ptr, i, bit_width),
                  unpacked[i])
            << "bit_width=" << static_cast<int>(bit_width) << " index=" << i;
    }
}

TEST(AlpSimdeTest, UnpackMatchesReferenceAcrossBitWidths) {
    for (uint8_t bit_width = 1; bit_width <= 64; ++bit_width) {
        ExpectUnpackMatchesReference<uint64_t>(bit_width);
    }
    for (uint8_t bit_width = 1; bit_width <= 32; ++bit_width) {
        ExpectUnpackMatchesReference<uint32_t>(bit_width);
    }
}

#ifdef ALP_AVX2_DISPATCH

// The AlpSimdeTest cases above run through the dispatched entry points, so on
// an AVX2 CPU they exercise the native kernels. These tests compare the AVX2
// kernels against the SIMDe fallback directly, so a mismatch cannot be hidden
// by the scalar reference agreeing with both.

template <typename T>
static uint32_t EncodeWithSimde(
    const std::vector<T>& values, uint8_t factor, uint8_t exponent,
    std::vector<typename AlpTypeTraits<T>::Encoded>& encoded,
    std::vector<uint8_t>& bitmap, std::vector<T>& exceptions) {
    const uint32_t count = static_cast<uint32_t>(values.size());
    encoded.assign(count, 0);
    bitmap.assign((count + 7) / 8, 0);
    exceptions.assign(count, T());
    uint32_t exception_count = 0;
    EXPECT_EQ(ALP_OK, AlpSimdeKernel<T>::EncodeValues(
                          values.empty() ? nullptr : &values[0], count, factor,
                          exponent, encoded.empty() ? nullptr : &encoded[0],
                          bitmap.empty() ? nullptr : &bitmap[0],
                          exceptions.empty() ? nullptr : &exceptions[0],
                          &exception_count));
    return exception_count;
}

template <typename T>
static uint32_t EncodeWithAvx2(
    const std::vector<T>& values, uint8_t factor, uint8_t exponent,
    std::vector<typename AlpTypeTraits<T>::Encoded>& encoded,
    std::vector<uint8_t>& bitmap, std::vector<T>& exceptions) {
    const uint32_t count = static_cast<uint32_t>(values.size());
    encoded.assign(count, 0);
    bitmap.assign((count + 7) / 8, 0);
    exceptions.assign(count, T());
    uint32_t exception_count = 0;
    EXPECT_EQ(ALP_OK, avx2_detail::AlpAvx2Kernel<T>::EncodeValues(
                          values.empty() ? nullptr : &values[0], count, factor,
                          exponent, encoded.empty() ? nullptr : &encoded[0],
                          bitmap.empty() ? nullptr : &bitmap[0],
                          exceptions.empty() ? nullptr : &exceptions[0],
                          &exception_count));
    return exception_count;
}

template <typename T>
static void ExpectAvx2MatchesSimdeForPair(const std::vector<T>& values,
                                          uint8_t factor, uint8_t exponent) {
    typedef AlpTypeTraits<T> Traits;
    typedef typename Traits::Encoded Encoded;

    std::vector<Encoded> simde_encoded;
    std::vector<uint8_t> simde_bitmap;
    std::vector<T> simde_exceptions;
    std::vector<Encoded> avx2_encoded;
    std::vector<uint8_t> avx2_bitmap;
    std::vector<T> avx2_exceptions;

    const uint32_t simde_count =
        EncodeWithSimde(values, factor, exponent, simde_encoded, simde_bitmap,
                        simde_exceptions);
    const uint32_t avx2_count = EncodeWithAvx2(
        values, factor, exponent, avx2_encoded, avx2_bitmap, avx2_exceptions);
    ASSERT_EQ(simde_count, avx2_count);
    EXPECT_EQ(simde_bitmap, avx2_bitmap);
    EXPECT_EQ(simde_encoded, avx2_encoded);
    for (uint32_t i = 0; i < simde_count; ++i) {
        EXPECT_TRUE(
            Traits::BitwiseEqual(simde_exceptions[i], avx2_exceptions[i]))
            << "exception index " << i;
    }

    const uint32_t count = static_cast<uint32_t>(values.size());
    std::vector<T> simde_decoded(count);
    std::vector<T> avx2_decoded(count);
    const uint8_t* bitmap_ptr =
        simde_bitmap.empty() ? nullptr : &simde_bitmap[0];
    const T* exception_ptr = simde_count == 0 ? nullptr : &simde_exceptions[0];
    ASSERT_EQ(ALP_OK,
              AlpSimdeKernel<T>::DecodeValues(
                  simde_encoded.empty() ? nullptr : &simde_encoded[0], count,
                  factor, exponent, bitmap_ptr, exception_ptr, simde_count,
                  simde_decoded.empty() ? nullptr : &simde_decoded[0]));
    const uint32_t avx2_status = avx2_detail::AlpAvx2Kernel<T>::DecodeValues(
        simde_encoded.empty() ? nullptr : &simde_encoded[0], count, factor,
        exponent, bitmap_ptr, exception_ptr, simde_count,
        avx2_decoded.empty() ? nullptr : &avx2_decoded[0]);
    ASSERT_EQ(ALP_OK, avx2_status);
    for (uint32_t i = 0; i < count; ++i) {
        EXPECT_TRUE(Traits::BitwiseEqual(simde_decoded[i], avx2_decoded[i]))
            << "decoded index " << i;
    }
}

template <typename T>
static void ExpectAvx2MatchesSimde(const std::vector<T>& values) {
    if (!common::cpu_has_avx2()) {
        GTEST_SKIP() << "CPU does not support AVX2";
        return;
    }
    typedef AlpTypeTraits<T> Traits;
    for (uint8_t exponent = 0; exponent <= Traits::MaxExponent(); ++exponent) {
        for (uint8_t factor = 0; factor <= exponent; ++factor) {
            ExpectAvx2MatchesSimdeForPair(values, factor, exponent);
        }
    }
}

TEST(AlpAvx2Test, FloatValuesMatchSimde) {
    std::vector<float> values;
    for (int i = 0; i < 1024; ++i) {
        values.push_back(static_cast<float>(i % 251) * 0.01f - 1.25f);
    }
    ExpectAvx2MatchesSimde(values);
}

TEST(AlpAvx2Test, DoubleValuesMatchSimde) {
    std::vector<double> values;
    for (int i = 0; i < 1024; ++i) {
        values.push_back(static_cast<double>(i % 509) * 0.001 - 0.25);
    }
    ExpectAvx2MatchesSimde(values);
}

TEST(AlpAvx2Test, SpecialValuesMatchSimde) {
    const float nan_f = std::numeric_limits<float>::quiet_NaN();
    const double nan_d = std::numeric_limits<double>::quiet_NaN();
    std::vector<float> floats = {0.0f,
                                 -0.0f,
                                 1.0f,
                                 -1.0f,
                                 nan_f,
                                 std::numeric_limits<float>::infinity(),
                                 -std::numeric_limits<float>::infinity(),
                                 std::numeric_limits<float>::denorm_min(),
                                 std::numeric_limits<float>::max()};
    ExpectAvx2MatchesSimde(floats);

    std::vector<double> doubles = {0.0,
                                   -0.0,
                                   1.0,
                                   -1.0,
                                   nan_d,
                                   std::numeric_limits<double>::infinity(),
                                   -std::numeric_limits<double>::infinity(),
                                   std::numeric_limits<double>::denorm_min(),
                                   std::numeric_limits<double>::max()};
    ExpectAvx2MatchesSimde(doubles);
}

template <typename T>
static void ExpectAvx2MatchesSimdeAroundVectorBoundaries() {
    if (!common::cpu_has_avx2()) {
        GTEST_SKIP() << "CPU does not support AVX2";
        return;
    }
    const uint32_t counts[] = {0,  1,  2,  3,  4,   5,   6,   7,   8,
                               9,  15, 16, 17, 23,  24,  25,  31,  32,
                               33, 63, 64, 65, 127, 128, 129, 1024};
    uint64_t state = 0x9e3779b97f4a7c15ULL;
    std::vector<T> maximum(counts[sizeof(counts) / sizeof(counts[0]) - 1]);
    for (size_t i = 0; i < maximum.size(); ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        const double fraction = static_cast<double>(state % 100000) / 1000.0;
        maximum[i] = static_cast<T>(fraction - 50.0);
    }
    for (size_t c = 0; c < sizeof(counts) / sizeof(counts[0]); ++c) {
        const std::vector<T> values(maximum.begin(),
                                    maximum.begin() + counts[c]);
        // A pair that encodes the decimal data plus one that rejects most of
        // it, so both the vector body and the exception path are covered.
        ExpectAvx2MatchesSimdeForPair(values, 0, 0);
        ExpectAvx2MatchesSimdeForPair(values, 3, 3);
        ExpectAvx2MatchesSimdeForPair(values, 2, 2);
    }
}

TEST(AlpAvx2Test, FloatAroundVectorBoundaries) {
    ExpectAvx2MatchesSimdeAroundVectorBoundaries<float>();
}

TEST(AlpAvx2Test, DoubleAroundVectorBoundaries) {
    ExpectAvx2MatchesSimdeAroundVectorBoundaries<double>();
}

template <typename Unsigned>
static void ExpectAvx2UnpackMatchesSimde(uint8_t bit_width, uint32_t count) {
    const Unsigned mask =
        bit_width >= sizeof(Unsigned) * 8
            ? static_cast<Unsigned>(~static_cast<Unsigned>(0))
            : static_cast<Unsigned>((static_cast<uint64_t>(1) << bit_width) -
                                    1);
    std::vector<Unsigned> values(count);
    uint64_t state = 0x243f6a8885a308d3ULL;
    for (size_t i = 0; i < values.size(); ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        values[i] = static_cast<Unsigned>(state) & mask;
    }
    std::vector<uint8_t> body;
    AlpPackBits(values.empty() ? nullptr : &values[0], count, bit_width, body);
    const uint8_t dummy = 0;
    const uint8_t* body_ptr = body.empty() ? &dummy : &body[0];

    std::vector<Unsigned> simde_out(count, 0);
    std::vector<Unsigned> avx2_out(count, 0);
    ASSERT_TRUE(AlpSimdeUnpackKernel<Unsigned>::Unpack(
        body_ptr, count, bit_width, simde_out.data()));
    ASSERT_TRUE(avx2_detail::AlpAvx2UnpackKernel<Unsigned>::Unpack(
        body_ptr, count, bit_width, avx2_out.data()));
    EXPECT_EQ(simde_out, avx2_out)
        << "bit_width=" << static_cast<int>(bit_width) << " count=" << count;
    // The kernels only cover whole groups of four; compare every value they
    // did process against the input that was packed, so a mistake shared by
    // both implementations cannot hide behind their agreement.
    const uint32_t processed = count & ~static_cast<uint32_t>(3);
    for (uint32_t i = 0; i < processed; ++i) {
        EXPECT_EQ(values[i], avx2_out[i])
            << "bit_width=" << static_cast<int>(bit_width) << " count=" << count
            << " index=" << i;
    }
}

template <typename Unsigned>
static void ExpectAvx2UnpackMatchesSimdeAcrossBitWidths() {
    if (!common::cpu_has_avx2()) {
        GTEST_SKIP() << "CPU does not support AVX2";
        return;
    }
    const uint32_t counts[] = {0, 1, 2, 3, 4, 5, 7, 8, 9, 31, 64, 65, 1024};
    const uint8_t max_width = AlpSimdeUnpackKernel<Unsigned>::SafeBitWidth();
    for (uint8_t bit_width = 0; bit_width <= max_width; ++bit_width) {
        for (size_t c = 0; c < sizeof(counts) / sizeof(counts[0]); ++c) {
            ExpectAvx2UnpackMatchesSimde<Unsigned>(bit_width, counts[c]);
        }
    }
}

TEST(AlpAvx2Test, UnpackUint32MatchesSimde) {
    ExpectAvx2UnpackMatchesSimdeAcrossBitWidths<uint32_t>();
}

TEST(AlpAvx2Test, UnpackUint64MatchesSimde) {
    ExpectAvx2UnpackMatchesSimdeAcrossBitWidths<uint64_t>();
}

TEST(AlpAvx2Test, DecodeMalformedStatusMatchesSimde) {
    if (!common::cpu_has_avx2()) {
        GTEST_SKIP() << "CPU does not support AVX2";
        return;
    }
    // Bitmap position 1 is marked as an exception but no exception value is
    // supplied: both paths must reject the block rather than read past the
    // exception array.
    const int32_t encoded[4] = {1, 2, 3, 4};
    const uint8_t bitmap[1] = {0x02};
    float out[4] = {0, 0, 0, 0};
    EXPECT_EQ(ALP_MALFORMED, AlpSimdeKernel<float>::DecodeValues(
                                 encoded, 4, 0, 0, bitmap, nullptr, 0, out));
    EXPECT_EQ(ALP_MALFORMED, avx2_detail::AlpAvx2Kernel<float>::DecodeValues(
                                 encoded, 4, 0, 0, bitmap, nullptr, 0, out));

    // One exception value but no bitmap position to consume it.
    const float exceptions[1] = {5.0f};
    EXPECT_EQ(ALP_MALFORMED, AlpSimdeKernel<float>::DecodeValues(
                                 encoded, 4, 0, 0, bitmap, exceptions, 2, out));
    EXPECT_EQ(ALP_MALFORMED, avx2_detail::AlpAvx2Kernel<float>::DecodeValues(
                                 encoded, 4, 0, 0, bitmap, exceptions, 2, out));
}

#endif  // ALP_AVX2_DISPATCH

#endif  // ENABLE_SIMD

}  // namespace alp
}  // namespace storage
