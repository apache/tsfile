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

#include <limits>
#include <random>
#include <vector>

#include "encoding/int32_rle_decoder.h"
#include "encoding/int32_rle_encoder.h"

namespace storage {

class Int32RleEncoderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
    }

    void encode_and_decode(const std::vector<int32_t>& input) {
        // Encode
        common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
        Int32RleEncoder encoder;
        for (int32_t v : input) {
            encoder.encode(v, stream);
        }
        encoder.flush(stream);

        // Decode
        Int32RleDecoder decoder;
        std::vector<int32_t> decoded;
        while (decoder.has_next(stream)) {
            int32_t v;
            decoder.read_int32(v, stream);
            decoded.push_back(v);
        }

        ASSERT_EQ(input.size(), decoded.size());
        for (size_t i = 0; i < input.size(); ++i) {
            EXPECT_EQ(input[i], decoded[i]);
        }
    }
};

// All-zero input
TEST_F(Int32RleEncoderTest, EncodeAllZeros) {
    std::vector<int32_t> data(64, 0);
    encode_and_decode(data);
}

// All INT32_MAX
TEST_F(Int32RleEncoderTest, EncodeAllMaxValues) {
    std::vector<int32_t> data(64, std::numeric_limits<int32_t>::max());
    encode_and_decode(data);
}

// All INT32_MIN
TEST_F(Int32RleEncoderTest, EncodeAllMinValues) {
    std::vector<int32_t> data(64, std::numeric_limits<int32_t>::min());
    encode_and_decode(data);
}

// Repeating the same value
TEST_F(Int32RleEncoderTest, EncodeRepeatingValue) {
    std::vector<int32_t> data(128, 12345678);
    encode_and_decode(data);
}

// Incremental values (0 to 127)
TEST_F(Int32RleEncoderTest, EncodeIncrementalValues) {
    std::vector<int32_t> data;
    for (int i = 0; i < 128; ++i) {
        data.push_back(i);
    }
    encode_and_decode(data);
}

// Alternating signs: 0, -1, 2, -3, ...
TEST_F(Int32RleEncoderTest, EncodeAlternatingSigns) {
    std::vector<int32_t> data;
    for (int i = 0; i < 100; ++i) {
        data.push_back(i % 2 == 0 ? i : -i);
    }
    encode_and_decode(data);
}

// Random positive numbers
TEST_F(Int32RleEncoderTest, EncodeRandomPositiveValues) {
    std::vector<int32_t> data;
    for (int i = 0; i < 200; ++i) {
        data.push_back(std::rand() & 0x7FFFFFFF);
    }
    encode_and_decode(data);
}

// Random negative numbers
TEST_F(Int32RleEncoderTest, EncodeRandomNegativeValues) {
    std::vector<int32_t> data;
    for (int i = 0; i < 200; ++i) {
        data.push_back(-(std::rand() & 0x7FFFFFFF));
    }
    encode_and_decode(data);
}

// INT32 boundary values
TEST_F(Int32RleEncoderTest, EncodeBoundaryValues) {
    std::vector<int32_t> data = {std::numeric_limits<int32_t>::min(), -1, 0, 1,
                                 std::numeric_limits<int32_t>::max()};
    encode_and_decode(data);
}

// Flush after every 8 values (simulate frequent flush)
TEST_F(Int32RleEncoderTest, EncodeMultipleFlushes) {
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    Int32RleEncoder encoder;
    std::vector<int32_t> data;

    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 8; ++i) {
            int val = i + round * 10;
            encoder.encode(val, stream);
            data.push_back(val);
        }
        encoder.flush(stream);
    }

    // Decode
    Int32RleDecoder decoder;
    std::vector<int32_t> decoded;
    while (decoder.has_next(stream)) {
        int32_t v;
        decoder.read_int32(v, stream);
        decoded.push_back(v);
    }

    ASSERT_EQ(data.size(), decoded.size());
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(data[i], decoded[i]);
    }
}

// Flush with no values encoded
TEST_F(Int32RleEncoderTest, EncodeFlushWithoutData) {
    Int32RleEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    encoder.flush(stream);  // No values encoded

    EXPECT_EQ(stream.total_size(), 0u);
}

}  // namespace storage
