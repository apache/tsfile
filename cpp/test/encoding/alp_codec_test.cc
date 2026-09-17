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

#include <cmath>
#include <cstring>
#include <limits>
#include <vector>

#include "encoding/alp/alp_scalar.h"
#include "gtest/gtest.h"

namespace storage {
namespace alp {

template <typename T>
static bool BitEqual(T lhs, T rhs) {
    return std::memcmp(&lhs, &rhs, sizeof(T)) == 0;
}

template <typename T>
static void ExpectRoundTrip(const std::vector<T>& values) {
    std::vector<uint8_t> encoded;
    ASSERT_EQ(ALP_OK, AlpEncodePage(values.empty() ? NULL : &values[0],
                                    static_cast<uint32_t>(values.size()),
                                    encoded));
    std::vector<T> decoded;
    ASSERT_EQ(ALP_OK, AlpDecodePage(encoded.empty() ? NULL : &encoded[0],
                                    static_cast<uint32_t>(encoded.size()),
                                    decoded));
    ASSERT_EQ(values.size(), decoded.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_TRUE(BitEqual(values[i], decoded[i])) << "index " << i;
    }

    // Encoding must be deterministic.
    std::vector<uint8_t> encoded_again;
    ASSERT_EQ(ALP_OK, AlpEncodePage(values.empty() ? NULL : &values[0],
                                    static_cast<uint32_t>(values.size()),
                                    encoded_again));
    EXPECT_EQ(encoded, encoded_again);
}

TEST(AlpCodecTest, FloatDecimalRoundTrip) {
    std::vector<float> values;
    for (int i = 0; i < 2048; ++i) {
        values.push_back(static_cast<float>(i % 100) / 100.0f - 0.5f);
    }
    ExpectRoundTrip(values);
}

TEST(AlpCodecTest, DoubleDecimalRoundTrip) {
    std::vector<double> values;
    for (int i = 0; i < 2048; ++i) {
        values.push_back(static_cast<double>(i % 1000) / 1000.0 - 0.5);
    }
    ExpectRoundTrip(values);
}

TEST(AlpCodecTest, SpecialValuesRoundTrip) {
    const float nan_f = std::numeric_limits<float>::quiet_NaN();
    const float inf_f = std::numeric_limits<float>::infinity();
    const double nan_d = std::numeric_limits<double>::quiet_NaN();
    const double inf_d = std::numeric_limits<double>::infinity();

    std::vector<float> floats = {0.0f, -0.0f, 1.0f, -1.0f,
                                 nan_f, inf_f, -inf_f,
                                 std::numeric_limits<float>::denorm_min(),
                                 std::numeric_limits<float>::max(),
                                 std::numeric_limits<float>::lowest()};
    ExpectRoundTrip(floats);

    std::vector<double> doubles = {0.0, -0.0, 1.0, -1.0,
                                   nan_d, inf_d, -inf_d,
                                   std::numeric_limits<double>::denorm_min(),
                                   std::numeric_limits<double>::max(),
                                   std::numeric_limits<double>::lowest()};
    ExpectRoundTrip(doubles);
}

TEST(AlpCodecTest, PartialAndEmptyBlocks) {
    ExpectRoundTrip(std::vector<float>());
    ExpectRoundTrip(std::vector<float>(1, 1.25f));
    ExpectRoundTrip(std::vector<float>(ALP_BLOCK_SIZE - 1, 2.5f));
    ExpectRoundTrip(std::vector<float>(ALP_BLOCK_SIZE, 3.75f));
    ExpectRoundTrip(std::vector<float>(ALP_BLOCK_SIZE + 1, 4.125f));
}

TEST(AlpCodecTest, HighEntropyFallsBackButRoundTrips) {
    std::vector<double> values;
    uint64_t state = 0x123456789abcdef0ULL;
    for (int i = 0; i < 4096; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        uint64_t bits = state;
        double value = 0.0;
        std::memcpy(&value, &bits, sizeof(value));
        if (std::isfinite(value)) {
            values.push_back(value);
        }
    }
    ExpectRoundTrip(values);
}

TEST(AlpCodecTest, TruncatedPayloadReturnsError) {
    std::vector<double> values;
    for (int i = 0; i < 128; ++i) {
        values.push_back(static_cast<double>(i) * 0.25);
    }
    std::vector<uint8_t> encoded;
    ASSERT_EQ(ALP_OK, AlpEncodePage(&values[0],
                                    static_cast<uint32_t>(values.size()),
                                    encoded));
    std::vector<double> decoded;
    EXPECT_NE(ALP_OK, AlpDecodePage(&encoded[0],
                                    static_cast<uint32_t>(encoded.size() / 2),
                                    decoded));
}

template <typename T>
static AlpBlockHeader FirstBlockHeader(const std::vector<uint8_t>& encoded) {
    AlpBlockHeader header;
    std::memset(&header, 0, sizeof(header));
    uint32_t pos = ALP_PAGE_HEADER_SIZE;
    EXPECT_TRUE(AlpParseBlockHeader(encoded.empty() ? NULL : &encoded[0],
                                    static_cast<uint32_t>(encoded.size()), pos,
                                    header));
    return header;
}

TEST(AlpCodecTest, DecimalDataUsesAlpScheme) {
    std::vector<float> values(ALP_BLOCK_SIZE);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<float>(i % 128) * 0.01f;
    }
    std::vector<uint8_t> encoded;
    ASSERT_EQ(ALP_OK, AlpEncodePage(&values[0],
                                    static_cast<uint32_t>(values.size()),
                                    encoded));
    const AlpBlockHeader header = FirstBlockHeader<float>(encoded);
    EXPECT_EQ(ALP_SCHEME_ALP, header.scheme);
    EXPECT_LT(header.body_bytes, values.size() * sizeof(float));
}

TEST(AlpCodecTest, HighEntropyUsesPlainScheme) {
    std::vector<double> values(ALP_BLOCK_SIZE);
    uint64_t state = 0x9e3779b97f4a7c15ULL;
    for (size_t i = 0; i < values.size(); ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        uint64_t bits = state ^ (state >> 29);
        std::memcpy(&values[i], &bits, sizeof(double));
        if (!std::isfinite(values[i])) {
            values[i] = static_cast<double>(i);
        }
    }
    std::vector<uint8_t> encoded;
    ASSERT_EQ(ALP_OK, AlpEncodePage(&values[0],
                                    static_cast<uint32_t>(values.size()),
                                    encoded));
    const AlpBlockHeader header = FirstBlockHeader<double>(encoded);
    EXPECT_EQ(ALP_SCHEME_PLAIN, header.scheme);
}

TEST(AlpCodecTest, SmallIntegerBitWidths) {
    std::vector<double> values(ALP_BLOCK_SIZE);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = static_cast<double>(i % 256);
    }
    std::vector<uint8_t> encoded;
    ASSERT_EQ(ALP_OK, AlpEncodePage(&values[0],
                                    static_cast<uint32_t>(values.size()),
                                    encoded));
    const AlpBlockHeader header = FirstBlockHeader<double>(encoded);
    EXPECT_EQ(ALP_SCHEME_ALP, header.scheme);
    EXPECT_EQ(8, header.bit_width);
    EXPECT_EQ(0, header.for_base);

    std::vector<double> signed_values(ALP_BLOCK_SIZE);
    for (size_t i = 0; i < signed_values.size(); ++i) {
        signed_values[i] = static_cast<double>(static_cast<int>(i % 256) - 128);
    }
    std::vector<uint8_t> signed_encoded;
    ASSERT_EQ(ALP_OK, AlpEncodePage(&signed_values[0],
                                    static_cast<uint32_t>(signed_values.size()),
                                    signed_encoded));
    const AlpBlockHeader signed_header =
        FirstBlockHeader<double>(signed_encoded);
    EXPECT_EQ(ALP_SCHEME_ALP, signed_header.scheme);
    EXPECT_EQ(8, signed_header.bit_width);
    EXPECT_EQ(-128, signed_header.for_base);
}

}  // namespace alp
}  // namespace storage
