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

#include "encoding/camel_decoder.h"
#include "encoding/camel_encoder.h"
#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"

namespace storage {

namespace {

void assert_double_equal(double expected, double actual) {
    if (std::isnan(expected)) {
        EXPECT_TRUE(std::isnan(actual));
    } else {
        EXPECT_EQ(std::signbit(actual), std::signbit(expected));
        EXPECT_DOUBLE_EQ(actual, expected);
    }
}

void round_trip(const std::vector<double>& values) {
    CamelEncoder encoder;
    CamelDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (double value : values) {
        ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
    }
    ASSERT_EQ(encoder.flush(stream), common::E_OK);

    for (double expected : values) {
        ASSERT_TRUE(decoder.has_remaining(stream));
        double actual = 0;
        ASSERT_EQ(decoder.read_double(actual, stream), common::E_OK);
        assert_double_equal(expected, actual);
    }
    EXPECT_FALSE(decoder.has_remaining(stream));
}

}  // namespace

TEST(CamelCodecTest, DecimalAndIntegerPathRoundTrip) {
    round_trip({100.0, 100.0001, 100.0002, 99.9999, -7.101, -9.101, 12345.0,
                21332213.0, 0.1, 0.2, 0.3});
}

TEST(CamelCodecTest, GorillaFallbackRoundTrip) {
    round_trip({std::numeric_limits<double>::quiet_NaN(),
                std::numeric_limits<double>::infinity(),
                -std::numeric_limits<double>::infinity(), +0.0, -0.0,
                std::numeric_limits<double>::min(),
                -std::numeric_limits<double>::min(),
                std::numeric_limits<double>::max(),
                -std::numeric_limits<double>::max()});
}

TEST(CamelCodecTest, RepeatedFlushBlocksRoundTrip) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    std::vector<double> values = {123.456789,  123.456789, 123.456790,
                                  -123.456790, 0.0,        -0.0};

    for (int repeat = 0; repeat < 2; ++repeat) {
        CamelEncoder encoder;
        for (double value : values) {
            ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
        }
        ASSERT_EQ(encoder.flush(stream), common::E_OK);
    }

    CamelDecoder decoder;
    for (int repeat = 0; repeat < 2; ++repeat) {
        for (double expected : values) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            double actual = 0;
            ASSERT_EQ(decoder.read_double(actual, stream), common::E_OK);
            assert_double_equal(expected, actual);
        }
    }
    EXPECT_FALSE(decoder.has_remaining(stream));
}

TEST(CamelCodecTest, FactoryAllocatesDoubleCodecsOnly) {
    Encoder* double_encoder =
        EncoderFactory::alloc_value_encoder(common::CAMEL, common::DOUBLE);
    Decoder* double_decoder =
        DecoderFactory::alloc_value_decoder(common::CAMEL, common::DOUBLE);
    Encoder* float_encoder =
        EncoderFactory::alloc_value_encoder(common::CAMEL, common::FLOAT);
    Decoder* float_decoder =
        DecoderFactory::alloc_value_decoder(common::CAMEL, common::FLOAT);

    ASSERT_NE(double_encoder, nullptr);
    ASSERT_NE(double_decoder, nullptr);
    EXPECT_EQ(float_encoder, nullptr);
    EXPECT_EQ(float_decoder, nullptr);

    EncoderFactory::free(double_encoder);
    DecoderFactory::free(double_decoder);
}

}  // namespace storage
