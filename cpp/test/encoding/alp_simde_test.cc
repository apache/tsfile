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
static void ExpectSimdMatchesScalar(const std::vector<T>& values) {
    typedef AlpTypeTraits<T> Traits;
    ASSERT_FALSE(values.empty());
    uint8_t factor = 0;
    uint8_t exponent = 0;
    ASSERT_TRUE(AlpChooseFactorExponent(
        &values[0], static_cast<uint32_t>(values.size()), factor, exponent));

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

#endif  // ENABLE_SIMD

}  // namespace alp
}  // namespace storage
