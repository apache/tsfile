/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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

#include <random>

#include "encoding/int64_rle_decoder.h"
#include "encoding/int64_rle_encoder.h"

namespace storage {

class Int64RleCodecTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
    }

    void encode_and_decode_check(const std::vector<int64_t>& input) {
        common::ByteStream stream(4096, common::MOD_ENCODER_OBJ);

        // Encode
        Int64RleEncoder encoder;
        for (int64_t v : input) {
            encoder.encode(v, stream);
        }
        encoder.flush(stream);

        // Decode
        Int64RleDecoder decoder;
        for (size_t i = 0; i < input.size(); ++i) {
            ASSERT_TRUE(decoder.has_next(stream));
            int64_t value;
            decoder.read_int64(value, stream);
            EXPECT_EQ(value, input[i]) << "Mismatch at index " << i;
        }

        EXPECT_FALSE(decoder.has_next(stream));
    }
};

// All-zero input
TEST_F(Int64RleCodecTest, EncodeAllZeros) {
    std::vector<int64_t> data(64, 0);
    encode_and_decode_check(data);
}

// All INT64_MAX values
TEST_F(Int64RleCodecTest, EncodeAllMaxValues) {
    std::vector<int64_t> data(64, std::numeric_limits<int64_t>::max());
    encode_and_decode_check(data);
}

// All INT64_MIN values
TEST_F(Int64RleCodecTest, EncodeAllMinValues) {
    std::vector<int64_t> data(64, std::numeric_limits<int64_t>::min());
    encode_and_decode_check(data);
}

// Repeating a single constant value
TEST_F(Int64RleCodecTest, EncodeRepeatingSingleValue) {
    std::vector<int64_t> data(100, 123456789012345);
    encode_and_decode_check(data);
}

// Strictly increasing sequence
TEST_F(Int64RleCodecTest, EncodeIncrementalValues) {
    std::vector<int64_t> data;
    for (int64_t i = 0; i < 128; ++i) {
        data.push_back(i);
    }
    encode_and_decode_check(data);
}

// Alternating positive and negative values
TEST_F(Int64RleCodecTest, EncodeAlternatingSigns) {
    std::vector<int64_t> data;
    for (int64_t i = 0; i < 100; ++i) {
        data.push_back(i % 2 == 0 ? i : -i);
    }
    encode_and_decode_check(data);
}

// Random positive int64 values
TEST_F(Int64RleCodecTest, EncodeRandomPositiveValues) {
    std::vector<int64_t> data;
    for (int i = 0; i < 256; ++i) {
        data.push_back(static_cast<int64_t>(std::rand()) << 31 | std::rand());
    }
    encode_and_decode_check(data);
}

// Random negative int64 values
TEST_F(Int64RleCodecTest, EncodeRandomNegativeValues) {
    std::vector<int64_t> data;
    for (int i = 0; i < 256; ++i) {
        int64_t value = static_cast<int64_t>(std::rand()) << 31 | std::rand();
        data.push_back(-value);
    }
    encode_and_decode_check(data);
}

// Mixed boundary values
TEST_F(Int64RleCodecTest, EncodeBoundaryValues) {
    std::vector<int64_t> data = {std::numeric_limits<int64_t>::min(), -1, 0, 1,
                                 std::numeric_limits<int64_t>::max()};
    encode_and_decode_check(data);
}

// Flush without any encoded values
TEST_F(Int64RleCodecTest, EncodeFlushWithoutData) {
    Int64RleEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    encoder.flush(stream);
    EXPECT_EQ(stream.total_size(), 0u);
}

}  // namespace storage
