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
    for (int i = 0; i < (int)(sizeof(data) / sizeof(int32_t)); i++) {
        int32_t actual = 0;
        EXPECT_EQ(decoder.read_int32(actual, stream), common::E_OK);
        EXPECT_EQ(data[i], actual);
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
    for (int i = 0; i < (int)(sizeof(data) / sizeof(int64_t)); i++) {
        int64_t actual = 0;
        EXPECT_EQ(decoder.read_int64(actual, stream), common::E_OK);
        EXPECT_EQ(data[i], actual);
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
        int32_t actual = 0;
        EXPECT_EQ(decoder.read_int32(actual, stream), common::E_OK);
        EXPECT_EQ(value, actual);
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
        int64_t actual = 0;
        EXPECT_EQ(decoder.read_int64(actual, stream), common::E_OK);
        EXPECT_EQ(value, actual);
    }
}

namespace {

void WriteZigzagHeader(common::ByteStream& stream, uint32_t zigzag_length,
                       uint32_t int_length) {
    common::SerializationUtil::write_var_uint(zigzag_length, stream);
    common::SerializationUtil::write_var_uint(int_length, stream);
}

}  // namespace

// The page header carries the byte count of every encoded
// value as a var-int. A crafted header used to make the decoder push that many
// bytes into memory one at a time, regardless of what the page holds.
TEST_F(ZigzagDecoderTest, RejectsDeclaredLengthBeyondInput) {
    LongZigzagDecoder decoder;
    common::ByteStream stream(64, common::MOD_ZIGZAG_OBJ);
    WriteZigzagHeader(stream, 0xFFFFFFFFu, 1);
    uint8_t zero = 0;
    stream.write_buf(&zero, 1);
    int64_t value = 0;
    EXPECT_EQ(decoder.read_int64(value, stream), common::E_TSFILE_CORRUPTED);
}

TEST_F(ZigzagDecoderTest, RejectsValueRunningPastDeclaredLength) {
    // Both declared bytes set the continuation bit, so the value asks for a
    // third byte that the page does not contain.
    for (int wide : {0, 1}) {
        common::ByteStream stream(64, common::MOD_ZIGZAG_OBJ);
        WriteZigzagHeader(stream, 2, 1);
        uint8_t continuation = 0x80;
        stream.write_buf(&continuation, 1);
        stream.write_buf(&continuation, 1);
        if (wide) {
            LongZigzagDecoder decoder;
            int64_t value = 0;
            EXPECT_EQ(decoder.read_int64(value, stream),
                      common::E_TSFILE_CORRUPTED);
        } else {
            IntZigzagDecoder decoder;
            int32_t value = 0;
            EXPECT_EQ(decoder.read_int32(value, stream),
                      common::E_TSFILE_CORRUPTED);
        }
    }
}

TEST_F(ZigzagDecoderTest, RejectsValueWithTooManyContinuationBytes) {
    // Ten 7-bit groups are the longest int64 value; a value with more groups
    // used to be written past the ten-byte decode buffer.
    for (int wide : {0, 1}) {
        common::ByteStream stream(64, common::MOD_ZIGZAG_OBJ);
        WriteZigzagHeader(stream, 65, 1);
        for (int i = 0; i < 64; ++i) {
            uint8_t continuation = 0xFF;
            stream.write_buf(&continuation, 1);
        }
        uint8_t last = 0x01;
        stream.write_buf(&last, 1);
        if (wide) {
            LongZigzagDecoder decoder;
            int64_t value = 0;
            EXPECT_EQ(decoder.read_int64(value, stream),
                      common::E_TSFILE_CORRUPTED);
        } else {
            IntZigzagDecoder decoder;
            int32_t value = 0;
            EXPECT_EQ(decoder.read_int32(value, stream),
                      common::E_TSFILE_CORRUPTED);
        }
    }
}

TEST_F(ZigzagDecoderTest, RejectsEmptyAndHeaderlessStreams) {
    {
        IntZigzagDecoder decoder;
        common::ByteStream empty(64, common::MOD_ZIGZAG_OBJ);
        int32_t value = 0;
        EXPECT_EQ(decoder.read_int32(value, empty), common::E_TSFILE_CORRUPTED);
    }
    {
        // A header declaring no encoded bytes cannot describe a value.
        LongZigzagDecoder decoder;
        common::ByteStream stream(64, common::MOD_ZIGZAG_OBJ);
        WriteZigzagHeader(stream, 0, 0);
        int64_t value = 0;
        EXPECT_EQ(decoder.read_int64(value, stream),
                  common::E_TSFILE_CORRUPTED);
    }
}

// The page header fields are unsigned 32-bit var-ints: at most five bytes,
// with four payload bits in the fifth and no continuation after it.
TEST_F(ZigzagDecoderTest, RejectsMalformedHeaderVarint) {
    {
        // A continuation in the fifth byte would need a sixth byte.
        common::ByteStream stream(64, common::MOD_ZIGZAG_OBJ);
        for (int i = 0; i < 5; ++i) {
            uint8_t byte = 0x80;
            stream.write_buf(&byte, 1);
        }
        uint8_t zero = 0;
        stream.write_buf(&zero, 1);
        LongZigzagDecoder decoder;
        int64_t value = 0;
        EXPECT_EQ(decoder.read_int64(value, stream),
                  common::E_TSFILE_CORRUPTED);
    }
    {
        // A fifth byte carrying bits above bit 31 is out of range.
        common::ByteStream stream(64, common::MOD_ZIGZAG_OBJ);
        for (int i = 0; i < 4; ++i) {
            uint8_t byte = 0x80;
            stream.write_buf(&byte, 1);
        }
        uint8_t too_big = 0x10;
        stream.write_buf(&too_big, 1);
        LongZigzagDecoder decoder;
        int64_t value = 0;
        EXPECT_EQ(decoder.read_int64(value, stream),
                  common::E_TSFILE_CORRUPTED);
    }
}

TEST_F(ZigzagDecoderTest, RejectsValueWiderThanInt32) {
    // Four continuation bytes and a fifth group of 0x20 put payload bits
    // above bit 31; the value must be rejected, not truncated.
    common::ByteStream stream(64, common::MOD_ZIGZAG_OBJ);
    WriteZigzagHeader(stream, 5, 1);
    for (int i = 0; i < 4; ++i) {
        uint8_t byte = 0x80;
        stream.write_buf(&byte, 1);
    }
    uint8_t tail = 0x20;
    stream.write_buf(&tail, 1);
    IntZigzagDecoder decoder;
    int32_t value = 0;
    EXPECT_EQ(decoder.read_int32(value, stream), common::E_TSFILE_CORRUPTED);
}

TEST_F(ZigzagDecoderTest, RejectsValueWiderThanInt64) {
    // Nine continuation bytes and a tenth group of 0x02 put payload bits
    // above bit 63; the value must be rejected, not truncated.
    common::ByteStream stream(64, common::MOD_ZIGZAG_OBJ);
    WriteZigzagHeader(stream, 10, 1);
    for (int i = 0; i < 9; ++i) {
        uint8_t byte = 0x80;
        stream.write_buf(&byte, 1);
    }
    uint8_t tail = 0x02;
    stream.write_buf(&tail, 1);
    LongZigzagDecoder decoder;
    int64_t value = 0;
    EXPECT_EQ(decoder.read_int64(value, stream), common::E_TSFILE_CORRUPTED);
}

}  // namespace storage
