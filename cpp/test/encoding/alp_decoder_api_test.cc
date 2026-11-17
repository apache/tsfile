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

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "common/allocator/my_string.h"
#include "common/allocator/page_arena.h"
#include "encoding/alp/alp_decoder.h"
#include "encoding/alp/alp_scalar.h"

namespace storage {
namespace {

using namespace common;

template <typename T>
std::vector<uint8_t> encode_alp_page(const std::vector<T>& values) {
    std::vector<uint8_t> encoded;
    EXPECT_EQ(alp::ALP_OK, alp::AlpEncodePage(
                               values.empty() ? nullptr : values.data(),
                               static_cast<uint32_t>(values.size()), encoded));
    return encoded;
}

void wrap_bytes(ByteStream& stream, const std::vector<uint8_t>& bytes) {
    stream.wrap_from(reinterpret_cast<const char*>(bytes.data()),
                     static_cast<int32_t>(bytes.size()));
}

TEST(AlpDecoderApiTest, FloatSingleValueReads) {
    const std::vector<float> values = {1.5f, -2.25f, 3.0f, 0.0f, 1.0e10f};
    FloatAlpDecoder decoder;
    const std::vector<uint8_t> encoded = encode_alp_page(values);
    ByteStream stream;
    wrap_bytes(stream, encoded);

    EXPECT_TRUE(decoder.owns_resources());
    EXPECT_TRUE(decoder.has_remaining(stream));
    for (float expected : values) {
        float actual = 0.0f;
        ASSERT_EQ(decoder.read_float(actual, stream), E_OK);
        EXPECT_FLOAT_EQ(actual, expected);
    }
    EXPECT_FALSE(decoder.has_remaining(stream));
    float extra = 0.0f;
    EXPECT_EQ(decoder.read_float(extra, stream), E_NO_MORE_DATA);

    decoder.destroy();
}

TEST(AlpDecoderApiTest, DoubleSingleValueReadsAndBatch) {
    const std::vector<double> values = {1.5, -2.25, 3.0, 0.0, 1.0e100};
    DoubleAlpDecoder decoder;
    const std::vector<uint8_t> encoded = encode_alp_page(values);
    ByteStream stream;
    wrap_bytes(stream, encoded);

    double first = 0.0;
    double second = 0.0;
    ASSERT_EQ(decoder.read_double(first, stream), E_OK);
    ASSERT_EQ(decoder.read_double(second, stream), E_OK);
    EXPECT_DOUBLE_EQ(first, values[0]);
    EXPECT_DOUBLE_EQ(second, values[1]);

    double rest[10] = {0.0};
    int actual = 0;
    ASSERT_EQ(decoder.read_batch_double(rest, 10, actual, stream), E_OK);
    ASSERT_EQ(actual, 3);
    for (int i = 0; i < actual; ++i) {
        EXPECT_DOUBLE_EQ(rest[i], values[static_cast<size_t>(i) + 2]);
    }
    ASSERT_EQ(decoder.read_batch_double(rest, 10, actual, stream), E_OK);
    EXPECT_EQ(actual, 0);
    EXPECT_EQ(decoder.read_double(first, stream), E_NO_MORE_DATA);
}

TEST(AlpDecoderApiTest, TypeMismatchesReturnError) {
    ByteStream stream;
    PageArena pa;
    pa.init(512, MOD_TSFILE_READER);

    FloatAlpDecoder float_decoder;
    double dval = 0.0;
    int32_t i32 = 0;
    int64_t i64 = 0;
    bool bval = false;
    String sval;
    int actual = 0;
    int skipped = 0;
    double double_out[2] = {0.0, 0.0};
    EXPECT_EQ(float_decoder.read_double(dval, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(float_decoder.read_boolean(bval, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(float_decoder.read_int32(i32, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(float_decoder.read_int64(i64, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(float_decoder.read_String(sval, pa, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(float_decoder.read_batch_double(double_out, 2, actual, stream),
              E_TYPE_NOT_MATCH);
    EXPECT_EQ(float_decoder.skip_double(1, skipped, stream), E_TYPE_NOT_MATCH);

    DoubleAlpDecoder double_decoder;
    float fval = 0.0f;
    float float_out[2] = {0.0f, 0.0f};
    EXPECT_EQ(double_decoder.read_float(fval, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(double_decoder.read_boolean(bval, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(double_decoder.read_int32(i32, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(double_decoder.read_int64(i64, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(double_decoder.read_String(sval, pa, stream), E_TYPE_NOT_MATCH);
    EXPECT_EQ(double_decoder.read_batch_float(float_out, 2, actual, stream),
              E_TYPE_NOT_MATCH);
    EXPECT_EQ(double_decoder.skip_float(1, skipped, stream), E_TYPE_NOT_MATCH);

    pa.destroy();
}

TEST(AlpDecoderApiTest, SkipAdvancesAndClamps) {
    std::vector<float> values;
    for (int i = 0; i < 10; ++i) {
        values.push_back(static_cast<float>(i) + 0.5f);
    }
    FloatAlpDecoder decoder;
    const std::vector<uint8_t> encoded = encode_alp_page(values);
    ByteStream stream;
    wrap_bytes(stream, encoded);

    int skipped = -1;
    ASSERT_EQ(decoder.skip_float(0, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 0);
    ASSERT_EQ(decoder.skip_float(-3, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 0);
    ASSERT_EQ(decoder.skip_float(3, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 3);

    float value = 0.0f;
    ASSERT_EQ(decoder.read_float(value, stream), E_OK);
    EXPECT_FLOAT_EQ(value, values[3]);

    ASSERT_EQ(decoder.skip_float(100, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 6);
    EXPECT_EQ(decoder.read_float(value, stream), E_NO_MORE_DATA);

    DoubleAlpDecoder double_decoder;
    EXPECT_TRUE(double_decoder.owns_resources());
    const std::vector<double> double_values = {1.5, 2.5, 3.5, 4.5};
    const std::vector<uint8_t> double_page = encode_alp_page(double_values);
    ByteStream double_stream;
    wrap_bytes(double_stream, double_page);
    ASSERT_EQ(double_decoder.skip_double(2, skipped, double_stream), E_OK);
    EXPECT_EQ(skipped, 2);
    double double_value = 0.0;
    ASSERT_EQ(double_decoder.read_double(double_value, double_stream), E_OK);
    EXPECT_DOUBLE_EQ(double_value, double_values[2]);
    ASSERT_EQ(double_decoder.skip_double(100, skipped, double_stream), E_OK);
    EXPECT_EQ(skipped, 1);
    EXPECT_EQ(double_decoder.read_double(double_value, double_stream),
              E_NO_MORE_DATA);
}

TEST(AlpDecoderApiTest, BatchCapacityAndNullOut) {
    const std::vector<float> values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
    FloatAlpDecoder decoder;
    const std::vector<uint8_t> encoded = encode_alp_page(values);
    ByteStream stream;
    wrap_bytes(stream, encoded);

    float out[10] = {0.0f};
    int actual = -1;
    ASSERT_EQ(decoder.read_batch_float(out, 0, actual, stream), E_OK);
    EXPECT_EQ(actual, 0);
    ASSERT_EQ(decoder.read_batch_float(nullptr, 3, actual, stream), E_OK);
    EXPECT_EQ(actual, 0);

    ASSERT_EQ(decoder.read_batch_float(out, 3, actual, stream), E_OK);
    ASSERT_EQ(actual, 3);
    for (int i = 0; i < 3; ++i) {
        EXPECT_FLOAT_EQ(out[i], values[static_cast<size_t>(i)]);
    }
    ASSERT_EQ(decoder.read_batch_float(out, 10, actual, stream), E_OK);
    ASSERT_EQ(actual, 2);
    EXPECT_FLOAT_EQ(out[0], values[3]);
    EXPECT_FLOAT_EQ(out[1], values[4]);
    ASSERT_EQ(decoder.read_batch_float(out, 10, actual, stream), E_OK);
    EXPECT_EQ(actual, 0);
}

TEST(AlpDecoderApiTest, EmptyPageAndInvalidPage) {
    FloatAlpDecoder empty_decoder;
    const std::vector<uint8_t> empty_page =
        encode_alp_page(std::vector<float>());
    ByteStream empty_stream;
    wrap_bytes(empty_stream, empty_page);
    EXPECT_TRUE(empty_decoder.has_remaining(empty_stream));
    float value = 0.0f;
    EXPECT_EQ(empty_decoder.read_float(value, empty_stream), E_NO_MORE_DATA);
    EXPECT_FALSE(empty_decoder.has_remaining(empty_stream));
    int skipped = -1;
    EXPECT_EQ(empty_decoder.skip_float(5, skipped, empty_stream), E_OK);
    EXPECT_EQ(skipped, 0);

    FloatAlpDecoder invalid_decoder;
    std::vector<uint8_t> garbage(16, 0xff);
    ByteStream invalid_stream;
    wrap_bytes(invalid_stream, garbage);
    EXPECT_EQ(invalid_decoder.read_float(value, invalid_stream), E_DECODE_ERR);
    // The failed decode leaves the decoder initialized with no values.
    EXPECT_EQ(invalid_decoder.read_float(value, invalid_stream),
              E_NO_MORE_DATA);
}

TEST(AlpDecoderApiTest, ZeroLengthStreamIsTreatedAsEmpty) {
    FloatAlpDecoder decoder;
    ByteStream stream;
    float value = 0.0f;
    EXPECT_EQ(decoder.read_float(value, stream), E_NO_MORE_DATA);
}

TEST(AlpDecoderApiTest, InvalidPagePropagatesFromAllEntryPoints) {
    const std::vector<uint8_t> garbage(16, 0xff);
    {
        DoubleAlpDecoder decoder;
        ByteStream stream;
        wrap_bytes(stream, garbage);
        double value = 0.0;
        EXPECT_EQ(decoder.read_double(value, stream), E_DECODE_ERR);
    }
    {
        FloatAlpDecoder decoder;
        ByteStream stream;
        wrap_bytes(stream, garbage);
        float out[2] = {0.0f, 0.0f};
        int actual = -1;
        EXPECT_EQ(decoder.read_batch_float(out, 2, actual, stream),
                  E_DECODE_ERR);
    }
    {
        FloatAlpDecoder decoder;
        ByteStream stream;
        wrap_bytes(stream, garbage);
        int skipped = -1;
        EXPECT_EQ(decoder.skip_float(2, skipped, stream), E_DECODE_ERR);
    }
}

TEST(AlpDecoderApiTest, ResetAllowsReuse) {
    FloatAlpDecoder decoder;
    const std::vector<uint8_t> first_page =
        encode_alp_page(std::vector<float>{1.0f, 2.0f});
    ByteStream first_stream;
    wrap_bytes(first_stream, first_page);
    float value = 0.0f;
    ASSERT_EQ(decoder.read_float(value, first_stream), E_OK);
    EXPECT_FLOAT_EQ(value, 1.0f);

    decoder.reset();
    const std::vector<float> second_values = {7.0f, 8.0f, 9.0f};
    const std::vector<uint8_t> second_page = encode_alp_page(second_values);
    ByteStream second_stream;
    wrap_bytes(second_stream, second_page);
    EXPECT_TRUE(decoder.has_remaining(second_stream));
    for (float expected : second_values) {
        ASSERT_EQ(decoder.read_float(value, second_stream), E_OK);
        EXPECT_FLOAT_EQ(value, expected);
    }
    EXPECT_FALSE(decoder.has_remaining(second_stream));
}

TEST(AlpDecoderApiTest, NonWrappedStreamPath) {
    const std::vector<double> values = {3.25, -4.5, 5.75};
    const std::vector<uint8_t> encoded = encode_alp_page(values);
    ByteStream stream(1024, MOD_TSFILE_READER);
    ASSERT_EQ(
        stream.write_buf(encoded.data(), static_cast<uint32_t>(encoded.size())),
        E_OK);

    DoubleAlpDecoder decoder;
    double actual = 0.0;
    for (double expected : values) {
        ASSERT_EQ(decoder.read_double(actual, stream), E_OK);
        EXPECT_DOUBLE_EQ(actual, expected);
    }
    EXPECT_EQ(decoder.read_double(actual, stream), E_NO_MORE_DATA);
}

}  // namespace
}  // namespace storage
