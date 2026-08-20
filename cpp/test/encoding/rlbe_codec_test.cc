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
#include <cstdint>
#include <limits>
#include <vector>

#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"
#include "encoding/rlbe_decoder.h"
#include "encoding/rlbe_encoder.h"

namespace storage {

namespace {

void append_bits(std::vector<uint8_t>& bytes, int& bit_count, uint32_t value,
                 int width) {
    for (int i = width - 1; i >= 0; --i) {
        if (bytes.empty() || bit_count == 8) {
            bytes.push_back(0);
            bit_count = 0;
        }
        bytes.back() =
            static_cast<uint8_t>((bytes.back() << 1) | ((value >> i) & 1));
        ++bit_count;
    }
}

}  // namespace

TEST(RLBECodecTest, Int32RoundTrip) {
    IntRLBEEncoder encoder;
    IntRLBEDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    std::vector<int32_t> values = {7, 9, 11, 11, 11, -7, -9, 0, INT32_MAX};
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

TEST(RLBECodecTest, Int64RoundTrip) {
    LongRLBEEncoder encoder;
    LongRLBEDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    std::vector<int64_t> values = {
        7,   9,   11,           INT64_C(1) << 40, (INT64_C(1) << 40) + 3,
        -17, -23, INT64_MAX - 7};
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

TEST(RLBECodecTest, FloatRoundTripWithSpecialValues) {
    FloatRLBEEncoder encoder;
    FloatRLBEDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    std::vector<float> values = {934.02F,
                                 122.86F,
                                 33.15F,
                                 33.15F,
                                 std::numeric_limits<float>::infinity(),
                                 -std::numeric_limits<float>::infinity(),
                                 std::numeric_limits<float>::quiet_NaN()};
    for (float value : values) {
        ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
    }
    ASSERT_EQ(encoder.flush(stream), common::E_OK);

    for (float expected : values) {
        ASSERT_TRUE(decoder.has_remaining(stream));
        float actual = 0;
        ASSERT_EQ(decoder.read_float(actual, stream), common::E_OK);
        if (std::isnan(expected)) {
            EXPECT_TRUE(std::isnan(actual));
        } else {
            EXPECT_EQ(std::signbit(actual), std::signbit(expected));
            EXPECT_FLOAT_EQ(actual, expected);
        }
    }
    EXPECT_FALSE(decoder.has_remaining(stream));
}

TEST(RLBECodecTest, DoubleRoundTripWithRepeatedFlushes) {
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    std::vector<double> values = {934.02,
                                  122.86,
                                  33.15,
                                  33.15,
                                  -7.101,
                                  -9.101,
                                  std::numeric_limits<double>::infinity(),
                                  std::numeric_limits<double>::quiet_NaN()};

    for (int repeat = 0; repeat < 2; ++repeat) {
        DoubleRLBEEncoder encoder;
        for (double value : values) {
            ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
        }
        ASSERT_EQ(encoder.flush(stream), common::E_OK);
    }

    DoubleRLBEDecoder decoder;
    for (int repeat = 0; repeat < 2; ++repeat) {
        for (double expected : values) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            double actual = 0;
            ASSERT_EQ(decoder.read_double(actual, stream), common::E_OK);
            if (std::isnan(expected)) {
                EXPECT_TRUE(std::isnan(actual));
            } else {
                EXPECT_EQ(std::signbit(actual), std::signbit(expected));
                EXPECT_DOUBLE_EQ(actual, expected);
            }
        }
    }
    EXPECT_FALSE(decoder.has_remaining(stream));
}

TEST(RLBECodecTest, FactoryAllocatesRLBECodecs) {
    Encoder* float_encoder =
        EncoderFactory::alloc_value_encoder(common::RLBE, common::FLOAT);
    Decoder* float_decoder =
        DecoderFactory::alloc_value_decoder(common::RLBE, common::FLOAT);
    Encoder* double_encoder =
        EncoderFactory::alloc_value_encoder(common::RLBE, common::DOUBLE);
    Decoder* double_decoder =
        DecoderFactory::alloc_value_decoder(common::RLBE, common::DOUBLE);

    ASSERT_NE(float_encoder, nullptr);
    ASSERT_NE(float_decoder, nullptr);
    ASSERT_NE(double_encoder, nullptr);
    ASSERT_NE(double_decoder, nullptr);

    EncoderFactory::free(float_encoder);
    DecoderFactory::free(float_decoder);
    EncoderFactory::free(double_encoder);
    DecoderFactory::free(double_decoder);
}

TEST(RLBECodecTest, RejectsInvalidBlockSize) {
    std::vector<uint8_t> bytes(4, 0);
    common::ByteStream stream;
    stream.wrap_from(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    IntRLBEDecoder decoder;
    int32_t value = 0;
    EXPECT_EQ(decoder.read_int32(value, stream), common::E_TSFILE_CORRUPTED);
}

TEST(RLBECodecTest, RejectsRunLengthBeyondBlock) {
    std::vector<uint8_t> bytes;
    int bit_count = 0;
    append_bits(bytes, bit_count, 1, 32);     // block size
    append_bits(bytes, bit_count, 1, 6);      // segment length (int32 RLBE)
    append_bits(bytes, bit_count, 0b011, 3);  // Fibonacci code for run length 2
    append_bits(bytes, bit_count, 0, 2);      // delta payload (not reached)
    bytes.back() <<= (8 - bit_count);

    common::ByteStream stream;
    stream.wrap_from(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    IntRLBEDecoder decoder;
    int32_t value = 0;
    EXPECT_EQ(decoder.read_int32(value, stream), common::E_TSFILE_CORRUPTED);
}

}  // namespace storage
