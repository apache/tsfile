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

#include "encoding/zigzag_decoder.h"
#include "encoding/zigzag_encoder.h"

namespace storage {

class ZigzagEncoderTest : public ::testing::Test {};

TEST_F(ZigzagEncoderTest, EncodeInt32) {
    IntZigzagEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ZIGZAG_OBJ);
    int data[] = {1, -1, 12345, -12345, 0, INT32_MAX, INT32_MIN};
    for (auto value : data) {
        EXPECT_EQ(encoder.encode(value), common::E_OK);
    }
    encoder.flush(stream);

    ASSERT_GT(stream.total_size(), 0);

    uint32_t want_len = 21, read_len;
    uint8_t real_buf[21] = {};
    stream.read_buf(real_buf, want_len, read_len);
    EXPECT_EQ(want_len, read_len);
    // Generated using Java Edition
    uint8_t expected_buf[] = {19,  7,   2,   1,   242, 192, 1,   241, 192, 1, 0,
                              254, 255, 255, 255, 15,  255, 255, 255, 255, 15};
    for (int i = 0; i < 21; i++) {
        EXPECT_EQ(real_buf[i], expected_buf[i]);
    }
}

TEST_F(ZigzagEncoderTest, EncodeInt64) {
    LongZigzagEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ZIGZAG_OBJ);
    int64_t data[] = {1, -1, 12345, -12345, 0, INT64_MAX, INT64_MIN};
    for (auto value : data) {
        EXPECT_EQ(encoder.encode(value), common::E_OK);
    }
    encoder.flush(stream);

    ASSERT_GT(stream.total_size(), 0);

    uint32_t want_len = 31, read_len;
    uint8_t real_buf[31] = {};
    stream.read_buf(real_buf, want_len, read_len);
    EXPECT_EQ(want_len, read_len);
    // Generated using Java Edition
    uint8_t expected_buf[] = {29,  7,   2,   1,   242, 192, 1,   241,
                              192, 1,   0,   254, 255, 255, 255, 255,
                              255, 255, 255, 255, 1,   255, 255, 255,
                              255, 255, 255, 255, 255, 255, 1};
    for (int i = 0; i < 31; i++) {
        EXPECT_EQ(real_buf[i], expected_buf[i]);
    }
}

class ZigzagDecoderTest : public ::testing::Test {};

TEST_F(ZigzagDecoderTest, DecodeInt32) {
    IntZigzagEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ZIGZAG_OBJ);
    int32_t data[] = {1, -1, 12345, -12345, 0, INT32_MAX, INT32_MIN};
    for (auto value : data) {
        EXPECT_EQ(encoder.encode(value), common::E_OK);
    }
    encoder.flush(stream);

    IntZigzagDecoder decoder;
    for (int i = 0; i < sizeof(data) / sizeof(int32_t); i++) {
        EXPECT_EQ(data[i], decoder.decode(stream));
    }
}

TEST_F(ZigzagDecoderTest, DecodeInt64) {
    LongZigzagEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ZIGZAG_OBJ);
    int64_t data[] = {1, -1, 12345, -12345, 0, INT64_MAX, INT64_MIN};
    for (auto value : data) {
        EXPECT_EQ(encoder.encode(value), common::E_OK);
    }
    encoder.flush(stream);

    LongZigzagDecoder decoder;
    for (int i = 0; i < sizeof(data) / sizeof(int64_t); i++) {
        EXPECT_EQ(data[i], decoder.decode(stream));
    }
}

TEST_F(ZigzagDecoderTest, DecodeInt32LargeQuantities) {
    IntZigzagEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ZIGZAG_OBJ);
    for (int32_t value = 0; value < 20000; value++) {
        EXPECT_EQ(encoder.encode(value), common::E_OK);
    }
    encoder.flush(stream);

    IntZigzagDecoder decoder;
    for (int32_t value = 0; value < 20000; value++) {
        EXPECT_EQ(value, decoder.decode(stream));
    }
}

TEST_F(ZigzagDecoderTest, DecodeInt64LargeQuantities) {
    LongZigzagEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ZIGZAG_OBJ);
    for (int64_t value = 0; value < 50000; value++) {
        EXPECT_EQ(encoder.encode(value), common::E_OK);
    }
    encoder.flush(stream);

    LongZigzagDecoder decoder;
    for (int64_t value = 0; value < 50000; value++) {
        EXPECT_EQ(value, decoder.decode(stream));
    }
}

}  // namespace storage
