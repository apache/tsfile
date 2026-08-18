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

#include <cmath>
#include <limits>
#include <vector>

#include "encoding/chimp_decoder.h"
#include "encoding/chimp_encoder.h"
#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"

namespace storage {

TEST(ChimpCodecTest, Int32RoundTrip) {
    IntChimpEncoder encoder;
    IntChimpDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    std::vector<int32_t> values = {2000, 2000, 2000, 2003, 2006,
                                   -1,   -9,   0,    4096, INT32_MAX};
    for (int32_t value : values) {
        ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
    }
    ASSERT_EQ(encoder.flush(stream), common::E_OK);

    for (int32_t expected : values) {
        ASSERT_TRUE(decoder.has_remaining(stream));
        int32_t actual = 0;
        ASSERT_EQ(decoder.read_int32(actual, stream), common::E_OK);
        EXPECT_EQ(actual, expected);
    }
    EXPECT_FALSE(decoder.has_remaining(stream));
}

TEST(ChimpCodecTest, Int64RoundTrip) {
    LongChimpEncoder encoder;
    LongChimpDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    std::vector<int64_t> values = {7,
                                   9,
                                   11,
                                   13,
                                   INT64_C(1) << 40,
                                   (INT64_C(1) << 40) + 3,
                                   -17,
                                   INT64_MAX - 1};
    for (int64_t value : values) {
        ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
    }
    ASSERT_EQ(encoder.flush(stream), common::E_OK);

    for (int64_t expected : values) {
        ASSERT_TRUE(decoder.has_remaining(stream));
        int64_t actual = 0;
        ASSERT_EQ(decoder.read_int64(actual, stream), common::E_OK);
        EXPECT_EQ(actual, expected);
    }
    EXPECT_FALSE(decoder.has_remaining(stream));
}

TEST(ChimpCodecTest, FloatRoundTripWithSpecialValues) {
    FloatChimpEncoder encoder;
    FloatChimpDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    std::vector<float> values = {
        0.0f,
        -0.0f,
        7.101f,
        9.101f,
        std::numeric_limits<float>::infinity(),
        -std::numeric_limits<float>::infinity(),
    };
    for (float value : values) {
        ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
    }
    ASSERT_EQ(encoder.flush(stream), common::E_OK);

    for (float expected : values) {
        ASSERT_TRUE(decoder.has_remaining(stream));
        float actual = 0;
        ASSERT_EQ(decoder.read_float(actual, stream), common::E_OK);
        EXPECT_EQ(std::signbit(actual), std::signbit(expected));
        EXPECT_FLOAT_EQ(actual, expected);
    }
    EXPECT_FALSE(decoder.has_remaining(stream));
}

TEST(ChimpCodecTest, DoubleRoundTripWithRepeatedFlushes) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    std::vector<double> values = {0.2, 0.2, 0.2, 0.2003, 0.2006};

    for (int repeat = 0; repeat < 3; ++repeat) {
        DoubleChimpEncoder encoder;
        for (double value : values) {
            ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
        }
        ASSERT_EQ(encoder.flush(stream), common::E_OK);
    }

    for (int repeat = 0; repeat < 3; ++repeat) {
        DoubleChimpDecoder decoder;
        for (double expected : values) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            double actual = 0;
            ASSERT_EQ(decoder.read_double(actual, stream), common::E_OK);
            EXPECT_DOUBLE_EQ(actual, expected);
        }
        EXPECT_FALSE(decoder.has_remaining(stream));
    }
}

TEST(ChimpCodecTest, FactoryAllocatesChimpCodecs) {
    Encoder* int_encoder =
        EncoderFactory::alloc_value_encoder(common::CHIMP, common::INT32);
    Decoder* int_decoder =
        DecoderFactory::alloc_value_decoder(common::CHIMP, common::INT32);
    Encoder* double_encoder =
        EncoderFactory::alloc_value_encoder(common::CHIMP, common::DOUBLE);
    Decoder* double_decoder =
        DecoderFactory::alloc_value_decoder(common::CHIMP, common::DOUBLE);

    ASSERT_NE(int_encoder, nullptr);
    ASSERT_NE(int_decoder, nullptr);
    ASSERT_NE(double_encoder, nullptr);
    ASSERT_NE(double_decoder, nullptr);

    EncoderFactory::free(int_encoder);
    DecoderFactory::free(int_decoder);
    EncoderFactory::free(double_encoder);
    DecoderFactory::free(double_decoder);
}

}  // namespace storage
