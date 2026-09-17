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

#include "common/allocator/byte_stream.h"
#include "common/db_common.h"
#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"
#include "gtest/gtest.h"

namespace storage {

template <typename T>
static bool BitEqual(T lhs, T rhs) {
    return std::memcmp(&lhs, &rhs, sizeof(T)) == 0;
}

static int ReadAll(common::ByteStream& stream, std::vector<uint8_t>& bytes) {
    bytes.clear();
    bytes.reserve(static_cast<size_t>(stream.total_size()));
    common::ByteStream::BufferIterator iter = stream.init_buffer_iterator();
    while (true) {
        common::ByteStream::Buffer buffer = iter.get_next_buf();
        if (buffer.buf_ == NULL || buffer.len_ == 0) {
            break;
        }
        const uint8_t* begin = reinterpret_cast<const uint8_t*>(buffer.buf_);
        bytes.insert(bytes.end(), begin, begin + buffer.len_);
    }
    return bytes.size() == stream.total_size() ? common::E_OK
                                                : common::E_PARTIAL_READ;
}

TEST(AlpIntegrationTest, EncodingNameIsRegistered) {
    EXPECT_STREQ("ALP", common::get_encoding_name(common::ALP));
}

TEST(AlpIntegrationTest, FactoryRejectsNonFloatTypes) {
    EXPECT_EQ(nullptr, EncoderFactory::alloc_value_encoder(
                           common::ALP, common::INT32));
    EXPECT_EQ(nullptr, EncoderFactory::alloc_value_encoder(
                           common::ALP, common::INT64));
    EXPECT_EQ(nullptr, DecoderFactory::alloc_value_decoder(
                           common::ALP, common::INT32));
    EXPECT_EQ(nullptr, DecoderFactory::alloc_value_decoder(
                           common::ALP, common::INT64));
}

TEST(AlpIntegrationTest, FloatEncoderDecoderRoundTrip) {
    std::vector<float> values;
    for (int i = 0; i < 4096; ++i) {
        values.push_back(static_cast<float>(i % 257) * 0.01f - 1.25f);
    }
    values.push_back(std::numeric_limits<float>::quiet_NaN());
    values.push_back(-0.0f);

    Encoder* encoder = EncoderFactory::alloc_value_encoder(
        common::ALP, common::FLOAT);
    ASSERT_NE(nullptr, encoder);
    common::ByteStream encoded(1024, common::MOD_DEFAULT);
    ASSERT_EQ(common::E_OK,
              encoder->encode_batch(&values[0],
                                    static_cast<uint32_t>(values.size()),
                                    encoded));
    ASSERT_EQ(common::E_OK, encoder->flush(encoded));
    EncoderFactory::free(encoder);

    std::vector<uint8_t> bytes;
    ASSERT_EQ(common::E_OK, ReadAll(encoded, bytes));
    ASSERT_FALSE(bytes.empty());

    common::ByteStream wrapped;
    wrapped.wrap_from(reinterpret_cast<const char*>(&bytes[0]),
                      static_cast<int32_t>(bytes.size()));
    Decoder* decoder = DecoderFactory::alloc_value_decoder(
        common::ALP, common::FLOAT);
    ASSERT_NE(nullptr, decoder);
    std::vector<float> decoded(values.size());
    int actual = 0;
    ASSERT_EQ(common::E_OK,
              decoder->read_exact_float(&decoded[0],
                                        static_cast<int>(values.size()),
                                        wrapped));
    (void)actual;
    DecoderFactory::free(decoder);

    ASSERT_EQ(values.size(), decoded.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_TRUE(BitEqual(values[i], decoded[i])) << "index " << i;
    }
}

TEST(AlpIntegrationTest, DoubleEncoderDecoderRoundTrip) {
    std::vector<double> values;
    for (int i = 0; i < 4096; ++i) {
        values.push_back(static_cast<double>(i % 509) * 0.001 - 0.25);
    }
    values.push_back(std::numeric_limits<double>::quiet_NaN());
    values.push_back(-0.0);

    Encoder* encoder = EncoderFactory::alloc_value_encoder(
        common::ALP, common::DOUBLE);
    ASSERT_NE(nullptr, encoder);
    common::ByteStream encoded(1024, common::MOD_DEFAULT);
    ASSERT_EQ(common::E_OK,
              encoder->encode_batch(&values[0],
                                    static_cast<uint32_t>(values.size()),
                                    encoded));
    ASSERT_EQ(common::E_OK, encoder->flush(encoded));
    EncoderFactory::free(encoder);

    std::vector<uint8_t> bytes;
    ASSERT_EQ(common::E_OK, ReadAll(encoded, bytes));
    ASSERT_FALSE(bytes.empty());

    common::ByteStream wrapped;
    wrapped.wrap_from(reinterpret_cast<const char*>(&bytes[0]),
                      static_cast<int32_t>(bytes.size()));
    Decoder* decoder = DecoderFactory::alloc_value_decoder(
        common::ALP, common::DOUBLE);
    ASSERT_NE(nullptr, decoder);
    std::vector<double> decoded(values.size());
    ASSERT_EQ(common::E_OK,
              decoder->read_exact_double(&decoded[0],
                                         static_cast<int>(values.size()),
                                         wrapped));
    DecoderFactory::free(decoder);

    ASSERT_EQ(values.size(), decoded.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_TRUE(BitEqual(values[i], decoded[i])) << "index " << i;
    }
}

}  // namespace storage
