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

namespace {

void append_var_uint(std::vector<uint8_t>& out, uint32_t value) {
    while (value & 0xFFFFFF80u) {
        out.push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<uint8_t>(value));
}

// Build one length-prefixed RLE pattern: [length][bit width][body...].
std::vector<char> make_rle_pattern(uint8_t bit_width,
                                   const std::vector<uint8_t>& body) {
    std::vector<uint8_t> pattern;
    append_var_uint(pattern, static_cast<uint32_t>(body.size() + 1));
    pattern.push_back(bit_width);
    pattern.insert(pattern.end(), body.begin(), body.end());
    return std::vector<char>(pattern.begin(), pattern.end());
}

std::vector<char> encode_int32_stream(const std::vector<int32_t>& input,
                                      int split_at = -1) {
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    Int32RleEncoder encoder;
    for (size_t i = 0; i < input.size(); ++i) {
        encoder.encode(input[i], stream);
        if (split_at >= 0 && static_cast<int>(i) + 1 == split_at) {
            encoder.flush(stream);
        }
    }
    encoder.flush(stream);
    std::vector<char> out;
    if (stream.total_size() == 0) {
        return out;
    }
    char* bytes = common::get_bytes_from_bytestream(stream);
    out.assign(bytes, bytes + stream.total_size());
    common::mem_free(bytes);
    return out;
}

enum DecodeMode { kScalar = 0, kBatch = 1, kMixed = 2 };

std::vector<int32_t> decode_int32(const std::vector<char>& bytes, bool wrapped,
                                  DecodeMode mode, int capacity) {
    std::vector<int32_t> out;
    common::ByteStream owning(1024, common::MOD_DECODER_OBJ);
    common::ByteStream wrapped_stream;
    common::ByteStream* in = &owning;
    if (wrapped) {
        wrapped_stream.wrap_from(const_cast<char*>(bytes.data()),
                                 static_cast<int32_t>(bytes.size()));
        in = &wrapped_stream;
    } else {
        owning.write_buf(reinterpret_cast<const uint8_t*>(bytes.data()),
                         static_cast<uint32_t>(bytes.size()));
    }
    Int32RleDecoder decoder;
    decoder.reset();
    if (mode == kScalar) {
        while (decoder.has_remaining(*in)) {
            int32_t v = 0;
            if (decoder.read_int32(v, *in) != common::E_OK) {
                break;
            }
            out.push_back(v);
        }
    } else if (mode == kBatch) {
        std::vector<int32_t> buf(static_cast<size_t>(capacity));
        while (decoder.has_remaining(*in)) {
            int actual = 0;
            if (decoder.read_batch_int32(buf.data(), capacity, actual, *in) !=
                common::E_OK) {
                break;
            }
            if (actual == 0) {
                break;
            }
            out.insert(out.end(), buf.begin(), buf.begin() + actual);
        }
    } else {
        size_t turn = 0;
        while (decoder.has_remaining(*in)) {
            if (turn++ % 2 == 0) {
                int32_t v = 0;
                if (decoder.read_int32(v, *in) != common::E_OK) {
                    break;
                }
                out.push_back(v);
            } else {
                int32_t buf[5];
                int actual = 0;
                if (decoder.read_batch_int32(buf, 5, actual, *in) !=
                    common::E_OK) {
                    break;
                }
                if (actual == 0) {
                    break;
                }
                out.insert(out.end(), buf, buf + actual);
            }
        }
    }
    return out;
}

int decode_int32_expect_error(const std::vector<char>& pattern) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(pattern.data()),
                 static_cast<int32_t>(pattern.size()));
    Int32RleDecoder decoder;
    decoder.reset();
    int32_t value = 0;
    return decoder.read_int32(value, in);
}

int decode_int32_batch_expect_error(const std::vector<char>& pattern) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(pattern.data()),
                 static_cast<int32_t>(pattern.size()));
    Int32RleDecoder decoder;
    decoder.reset();
    int32_t values[8];
    int actual = 0;
    return decoder.read_batch_int32(values, 8, actual, in);
}

std::vector<std::vector<char>> malformed_patterns() {
    std::vector<std::vector<char>> patterns;
    // last_bit_packed_num = 0 (encoders write 1..8).
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (1u << 1) | 1u);
        body.push_back(0);
        body.insert(body.end(), 8, 0);
        patterns.push_back(make_rle_pattern(8, body));
    }
    // last_bit_packed_num > 8.
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (1u << 1) | 1u);
        body.push_back(9);
        body.insert(body.end(), 8, 0);
        patterns.push_back(make_rle_pattern(8, body));
    }
    // Group count claims more bytes than the pattern holds.
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (100u << 1) | 1u);
        body.push_back(8);
        body.insert(body.end(), 8, 0);
        patterns.push_back(make_rle_pattern(8, body));
    }
    // The pre-fix overflow: width 255 * group count 0x01010101 wraps uint32.
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (0x01010101u << 1) | 1u);
        body.push_back(8);
        body.insert(body.end(), 16, 0);
        patterns.push_back(make_rle_pattern(255, body));
    }
    // Invalid bit width 0.
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (1u << 1) | 1u);
        body.push_back(8);
        body.insert(body.end(), 8, 0);
        patterns.push_back(make_rle_pattern(0, body));
    }
    // Length prefix larger than the available bytes.
    {
        std::vector<char> truncated;
        truncated.push_back(32);  // claims 32 bytes
        truncated.push_back(8);
        truncated.push_back(3);
        truncated.push_back(8);
        truncated.push_back(0);
        patterns.push_back(truncated);
    }
    return patterns;
}

}  // namespace

TEST_F(Int32RleEncoderTest, BatchDecodeMatchesScalar) {
    std::vector<std::vector<int32_t>> cases;
    cases.push_back(std::vector<int32_t>(64, 0));
    cases.push_back(std::vector<int32_t>(300, -7));
    {
        std::vector<int32_t> ascending;
        for (int i = 0; i < 257; ++i) {
            ascending.push_back(i);
        }
        cases.push_back(ascending);
    }
    {
        std::vector<int32_t> random_values;
        for (int i = 0; i < 513; ++i) {
            random_values.push_back(static_cast<int32_t>(std::rand()));
        }
        cases.push_back(random_values);
    }
    {
        std::vector<int32_t> negative_values;
        for (int i = 0; i < 257; ++i) {
            negative_values.push_back(
                -1 - static_cast<int32_t>(std::rand() & 0x7FFFFFFF));
        }
        cases.push_back(negative_values);
    }
    cases.push_back(
        std::vector<int32_t>({std::numeric_limits<int32_t>::min(),
                              std::numeric_limits<int32_t>::max(), 0, -1, 1}));

    for (size_t c = 0; c < cases.size(); ++c) {
        const std::vector<int32_t>& input = cases[c];
        const std::vector<char> bytes = encode_int32_stream(input);
        ASSERT_EQ(decode_int32(bytes, false, kScalar, 0), input)
            << "case=" << c;
        ASSERT_EQ(decode_int32(bytes, true, kScalar, 0), input) << "case=" << c;
        for (int capacity : {1, 3, 7, 64, 129, 1024}) {
            ASSERT_EQ(decode_int32(bytes, false, kBatch, capacity), input)
                << "case=" << c << " capacity=" << capacity;
            ASSERT_EQ(decode_int32(bytes, true, kBatch, capacity), input)
                << "case=" << c << " capacity=" << capacity;
        }
        ASSERT_EQ(decode_int32(bytes, true, kMixed, 0), input) << "case=" << c;
    }
}

TEST_F(Int32RleEncoderTest, BatchDecodeSpansMultiplePatterns) {
    std::vector<int32_t> input;
    for (int i = 0; i < 200; ++i) {
        input.push_back(i % 17);
    }
    // Two length-prefixed patterns in one stream; a single large batch call
    // has to cross the pattern boundary.
    const std::vector<char> bytes = encode_int32_stream(input, 37);
    for (int capacity : {1, 37, 129, 1024}) {
        ASSERT_EQ(decode_int32(bytes, true, kBatch, capacity), input)
            << "capacity=" << capacity;
    }
}

TEST_F(Int32RleEncoderTest, MalformedBitPackedInputIsRejected) {
    const std::vector<std::vector<char>> patterns = malformed_patterns();
    for (size_t i = 0; i < patterns.size(); ++i) {
        EXPECT_NE(decode_int32_expect_error(patterns[i]), common::E_OK)
            << "scalar malformed case " << i;
        EXPECT_NE(decode_int32_batch_expect_error(patterns[i]), common::E_OK)
            << "batch malformed case " << i;
    }
}

TEST_F(Int32RleEncoderTest, ResetDropsBorrowedPatternView) {
    std::vector<int32_t> first;
    for (int i = 0; i < 100; ++i) {
        first.push_back(i);
    }
    std::vector<int32_t> second;
    for (int i = 0; i < 50; ++i) {
        second.push_back(1000 + i);
    }
    std::vector<char> first_bytes = encode_int32_stream(first);
    std::vector<char> second_bytes = encode_int32_stream(second);

    common::ByteStream in;
    in.wrap_from(first_bytes.data(), static_cast<int32_t>(first_bytes.size()));
    Int32RleDecoder decoder;
    decoder.reset();
    int32_t value = 0;
    ASSERT_EQ(decoder.read_int32(value, in), common::E_OK);
    EXPECT_EQ(value, first[0]);

    // Abandon the first pattern and switch to a second wrapped buffer.
    decoder.reset();
    // reset() must drop the borrowed view: an empty stream must not look
    // like a partially consumed pattern.
    common::ByteStream empty;
    EXPECT_FALSE(decoder.has_remaining(empty));
    in.wrap_from(second_bytes.data(),
                 static_cast<int32_t>(second_bytes.size()));
    std::vector<int32_t> decoded;
    while (decoder.has_remaining(in)) {
        if (decoder.read_int32(value, in) != common::E_OK) {
            break;
        }
        decoded.push_back(value);
    }
    EXPECT_EQ(decoded, second);
}

}  // namespace storage
