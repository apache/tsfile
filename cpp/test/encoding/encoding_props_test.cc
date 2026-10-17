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

#include <gtest/gtest.h>

#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"

namespace {
using namespace common;
using namespace storage;

TEST(EncodingProps, MatchesJavaIntegerParsingAndFallbacks) {
    const std::vector<std::pair<std::string, int>> cases{
        {"0", 0},
        {"2", 2},
        {"4", 4},
        {"+4", 4},
        {"0004", 4},
        {"-0", 0},
        {"-4", 2},
        {"-2147483648", 2},
        {"2147483647", 2147483647},
        {"2147483648", 0},
        {"-2147483649", 0},
        {"", 0},
        {"+", 0},
        {"-", 0},
        {" 4", 0},
        {"4 ", 0},
        {"4x", 0},
        {"4.0", 0},
        {"++4", 0},
        {std::string("4\0", 2), 0},
        {"\xD9\xA4", 4},
        {"\xEF\xBC\x94", 4},
        {"+\xD9\xA4", 4},
        {"-\xD9\xA4", 2},
        {"\xE0\xA5\xAA", 4},
        {"\xE1\x81\x84", 4},
        {"\xD9", 0},
        {"\xE0\xA5", 0},
        {"\xE0\x41\x80", 0},
        {"\xE0\x80\xB4", 0},
        {"\xED\xA0\x80", 0},
        {"\xC0\xB4", 0},
        {"\xF0\x9D\x9F\x9C", 0},
        {"\xC2\xA0", 0},
        {"\xE2\x82\xAC", 0}};
    for (const auto& item : cases) {
        SCOPED_TRACE(item.first);
        EXPECT_EQ(detail::max_point_number({{"max_point_number", item.first}}),
                  item.second);
    }
    EXPECT_EQ(detail::max_point_number({}), 2);
    EXPECT_EQ(detail::max_point_number({{"unit", "celsius"}}), 2);
}

TEST(EncodingProps, FactorySetsPagePrecisionAndPreservesItOnReset) {
    for (auto type : {FLOAT, DOUBLE}) {
        for (int precision : {0, 2, 4}) {
            auto* encoder = EncoderFactory::alloc_value_encoder(
                TS_2DIFF, type,
                {{"max_point_number", std::to_string(precision)}});
            ASSERT_NE(encoder, nullptr);
            for (int page = 0; page < 2; ++page) {
                ByteStream bytes(64, MOD_DEFAULT);
                ASSERT_EQ(type == FLOAT ? encoder->encode(1.23456f, bytes)
                                        : encoder->encode(1.23456, bytes),
                          E_OK);
                ASSERT_EQ(encoder->flush(bytes), E_OK);
                uint32_t actual_precision = 99;
                ASSERT_EQ(
                    SerializationUtil::read_var_uint(actual_precision, bytes),
                    E_OK);
                EXPECT_EQ(actual_precision, static_cast<uint32_t>(precision));
                encoder->reset();
            }
            EncoderFactory::free(encoder);
        }
    }
}

TEST(EncodingProps, IgnoresPropertiesForOtherCodecsAndTypes) {
    const std::map<std::string, std::string> props{{"max_point_number", "0"}};
    for (auto type : {INT32, INT64, FLOAT, DOUBLE}) {
        auto* encoder = EncoderFactory::alloc_value_encoder(PLAIN, type, props);
        ASSERT_NE(encoder, nullptr);
        EncoderFactory::free(encoder);
    }
    for (auto type : {INT32, INT64}) {
        auto* encoder =
            EncoderFactory::alloc_value_encoder(TS_2DIFF, type, props);
        ASSERT_NE(encoder, nullptr);
        EncoderFactory::free(encoder);
    }
    EXPECT_EQ(EncoderFactory::alloc_value_encoder(TS_2DIFF, BOOLEAN, props),
              nullptr);
    for (auto type : {FLOAT, DOUBLE}) {
        auto* encoder = EncoderFactory::alloc_value_encoder(TS_2DIFF, type, {});
        ASSERT_NE(encoder, nullptr);
        EncoderFactory::free(encoder);
    }
}

TEST(EncodingProps, InfiniteScaleRoundsZeroLikeJava) {
    for (auto type : {FLOAT, DOUBLE}) {
        auto* encoder = EncoderFactory::alloc_value_encoder(
            TS_2DIFF, type, {{"max_point_number", "1000"}});
        ASSERT_NE(encoder, nullptr);
        ByteStream bytes(64, MOD_DEFAULT);
        for (double value : {0.0, -0.0}) {
            ASSERT_EQ(type == FLOAT
                          ? encoder->encode(static_cast<float>(value), bytes)
                          : encoder->encode(value, bytes),
                      E_OK);
        }
        ASSERT_EQ(encoder->flush(bytes), E_OK);
        if (type == FLOAT) {
            FloatTS2DIFFDecoder decoder;
            for (int i = 0; i < 2; ++i) {
                float value = -1;
                ASSERT_EQ(decoder.read_float(value, bytes), E_OK);
                EXPECT_EQ(value, 0.0f);
                EXPECT_FALSE(std::signbit(value));
            }
        } else {
            DoubleTS2DIFFDecoder decoder;
            for (int i = 0; i < 2; ++i) {
                double value = -1;
                ASSERT_EQ(decoder.read_double(value, bytes), E_OK);
                EXPECT_EQ(value, 0.0);
                EXPECT_FALSE(std::signbit(value));
            }
        }
        EncoderFactory::free(encoder);
    }
}

TEST(EncodingProps, JavaRoundingKeepsSaturationAndNaNBehavior) {
    const double infinity = std::numeric_limits<double>::infinity();
    const double nan = std::numeric_limits<double>::quiet_NaN();
    EXPECT_EQ(java_round<int32_t>(nan), 0);
    EXPECT_EQ(java_round<int64_t>(nan), 0);
    EXPECT_EQ(java_round<int32_t>(infinity),
              std::numeric_limits<int32_t>::max());
    EXPECT_EQ(java_round<int64_t>(infinity),
              std::numeric_limits<int64_t>::max());
    EXPECT_EQ(java_round<int32_t>(-infinity),
              std::numeric_limits<int32_t>::min());
    EXPECT_EQ(java_round<int64_t>(-infinity),
              std::numeric_limits<int64_t>::min());
    EXPECT_EQ(java_round<int32_t>(1.5), 2);
    EXPECT_EQ(java_round<int64_t>(1.5), 2);
    EXPECT_EQ(java_round<int32_t>(-1.5), -1);
    EXPECT_EQ(java_round<int64_t>(-1.5), -1);
}

}  // namespace
