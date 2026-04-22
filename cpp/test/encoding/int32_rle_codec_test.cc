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

// Helper: write a manually crafted RLE segment (Java/Parquet hybrid RLE
// format):
//   [length_varint] [bit_width] [group_header_varint] [value_bytes...]
// run_count must be the actual count (written as (run_count<<1)|0 varint).
static void write_rle_segment(common::ByteStream& stream, uint8_t bit_width,
                              uint32_t run_count, int32_t value) {
    common::ByteStream content(32, common::MOD_ENCODER_OBJ);
    common::SerializationUtil::write_ui8(bit_width, content);
    // Group header: (run_count << 1) | 0 = even varint
    common::SerializationUtil::write_var_uint(run_count << 1, content);
    // Value: ceil(bit_width / 8) bytes, little-endian
    int byte_width = (bit_width + 7) / 8;
    uint32_t uvalue = static_cast<uint32_t>(value);
    for (int i = 0; i < byte_width; i++) {
        common::SerializationUtil::write_ui8((uvalue >> (i * 8)) & 0xFF,
                                             content);
    }
    uint32_t length = content.total_size();
    common::SerializationUtil::write_var_uint(length, stream);
    // Append content bytes to stream
    uint8_t buf[64];
    uint32_t read_len = 0;
    content.read_buf(buf, length, read_len);
    stream.write_buf(buf, read_len);
}

// Regression test: run_count=64 requires a 2-byte LEB128 varint header
// ((64<<1)|0 = 128 = [0x80, 0x01]). Before the fix, only 1 byte was read,
// causing byte misalignment and incorrect decoding.
TEST_F(Int32RleEncoderTest, DecodeRleRunCountExactly64) {
    common::ByteStream stream(32, common::MOD_ENCODER_OBJ);
    write_rle_segment(stream, /*bit_width=*/7, /*run_count=*/64,
                      /*value=*/42);

    Int32RleDecoder decoder;
    std::vector<int32_t> decoded;
    while (decoder.has_next(stream)) {
        int32_t v;
        decoder.read_int32(v, stream);
        decoded.push_back(v);
    }

    ASSERT_EQ(decoded.size(), 64u);
    for (int32_t v : decoded) {
        EXPECT_EQ(v, 42);
    }
}

// Run counts of 128 and 256 each need a 2-byte varint header.
TEST_F(Int32RleEncoderTest, DecodeRleRunCountLarge) {
    for (uint32_t count : {128u, 256u, 500u}) {
        common::ByteStream stream(64, common::MOD_ENCODER_OBJ);
        write_rle_segment(stream, /*bit_width=*/8, /*run_count=*/count,
                          /*value=*/100);

        Int32RleDecoder decoder;
        std::vector<int32_t> decoded;
        while (decoder.has_next(stream)) {
            int32_t v;
            decoder.read_int32(v, stream);
            decoded.push_back(v);
        }

        ASSERT_EQ(decoded.size(), (size_t)count)
            << "Failed for run_count=" << count;
        for (int32_t v : decoded) {
            EXPECT_EQ(v, 100);
        }
    }
}

// Multiple consecutive RLE runs including large ones (simulates real sensor
// data with repeated values and occasional changes).
TEST_F(Int32RleEncoderTest, DecodeMultipleRleRunsWithLargeCount) {
    common::ByteStream stream(128, common::MOD_ENCODER_OBJ);
    write_rle_segment(stream, /*bit_width=*/8, /*run_count=*/64,
                      /*value=*/25);
    write_rle_segment(stream, /*bit_width=*/8, /*run_count=*/8,
                      /*value=*/26);
    write_rle_segment(stream, /*bit_width=*/8, /*run_count=*/100,
                      /*value=*/25);

    Int32RleDecoder decoder;
    std::vector<int32_t> decoded;
    while (decoder.has_next(stream)) {
        int32_t v;
        decoder.read_int32(v, stream);
        decoded.push_back(v);
    }

    ASSERT_EQ(decoded.size(), 172u);  // 64 + 8 + 100
    for (size_t i = 0; i < 64; i++) EXPECT_EQ(decoded[i], 25);
    for (size_t i = 64; i < 72; i++) EXPECT_EQ(decoded[i], 26);
    for (size_t i = 72; i < 172; i++) EXPECT_EQ(decoded[i], 25);
}

// Regression test: Int32RleDecoder::reset() previously called delete[] on
// current_buffer_ which was allocated with mem_alloc (malloc). This is
// undefined behaviour and typically causes a crash. The fix uses mem_free.
TEST_F(Int32RleEncoderTest, ResetAfterDecodeNoCrash) {
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    Int32RleEncoder encoder;
    for (int i = 0; i < 16; i++) encoder.encode(i, stream);
    encoder.flush(stream);

    Int32RleDecoder decoder;
    // Decode at least one value to populate current_buffer_ via mem_alloc.
    int32_t v;
    ASSERT_TRUE(decoder.has_next(stream));
    decoder.read_int32(v, stream);

    // reset() must use mem_free, not delete[]. Before the fix this would crash.
    decoder.reset();

    // Verify the decoder is functional after reset.
    common::ByteStream stream2(1024, common::MOD_ENCODER_OBJ);
    Int32RleEncoder encoder2;
    std::vector<int32_t> input = {7, 7, 7, 7, 7, 7, 7, 7};
    for (int32_t x : input) encoder2.encode(x, stream2);
    encoder2.flush(stream2);

    std::vector<int32_t> decoded;
    while (decoder.has_next(stream2)) {
        decoder.read_int32(v, stream2);
        decoded.push_back(v);
    }
    ASSERT_EQ(decoded, input);
}

}  // namespace storage
