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

#endif  // ENABLE_SIMD

}  // namespace alp
}  // namespace storage
