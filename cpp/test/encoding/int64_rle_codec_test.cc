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

std::vector<char> encode_int64_stream(const std::vector<int64_t>& input,
                                      int split_at = -1) {
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    Int64RleEncoder encoder;
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

std::vector<int64_t> decode_int64(const std::vector<char>& bytes, bool wrapped,
                                  DecodeMode mode, int capacity) {
    std::vector<int64_t> out;
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
    Int64RleDecoder decoder;
    decoder.reset();
    if (mode == kScalar) {
        while (decoder.has_remaining(*in)) {
            int64_t v = 0;
            if (decoder.read_int64(v, *in) != common::E_OK) {
                break;
            }
            out.push_back(v);
        }
    } else if (mode == kBatch) {
        std::vector<int64_t> buf(static_cast<size_t>(capacity));
        while (decoder.has_remaining(*in)) {
            int actual = 0;
            if (decoder.read_batch_int64(buf.data(), capacity, actual, *in) !=
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
                int64_t v = 0;
                if (decoder.read_int64(v, *in) != common::E_OK) {
                    break;
                }
                out.push_back(v);
            } else {
                int64_t buf[5];
                int actual = 0;
                if (decoder.read_batch_int64(buf, 5, actual, *in) !=
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

int decode_int64_expect_error(const std::vector<char>& pattern) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(pattern.data()),
                 static_cast<int32_t>(pattern.size()));
    Int64RleDecoder decoder;
    decoder.reset();
    int64_t value = 0;
    return decoder.read_int64(value, in);
}

int decode_int64_batch_expect_error(const std::vector<char>& pattern) {
    common::ByteStream in;
    in.wrap_from(const_cast<char*>(pattern.data()),
                 static_cast<int32_t>(pattern.size()));
    Int64RleDecoder decoder;
    decoder.reset();
    int64_t values[8];
    int actual = 0;
    return decoder.read_batch_int64(values, 8, actual, in);
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
    // Width above Int64Packer's 64-bit limit.
    {
        std::vector<uint8_t> body;
        append_var_uint(body, (1u << 1) | 1u);
        body.push_back(8);
        body.insert(body.end(), 65, 0);
        patterns.push_back(make_rle_pattern(65, body));
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

TEST_F(Int64RleCodecTest, BatchDecodeMatchesScalar) {
    std::vector<std::vector<int64_t>> cases;
    cases.push_back(std::vector<int64_t>(64, 0));
    cases.push_back(std::vector<int64_t>(300, -7));
    {
        std::vector<int64_t> ascending;
        for (int64_t i = 0; i < 257; ++i) {
            ascending.push_back(i);
        }
        cases.push_back(ascending);
    }
    {
        std::vector<int64_t> random_values;
        for (int i = 0; i < 513; ++i) {
            random_values.push_back((static_cast<int64_t>(std::rand()) << 32) |
                                    std::rand());
        }
        cases.push_back(random_values);
    }
    {
        // Exercise the 59/61/62/63-bit scalar fallback and the 64-bit SIMD
        // path.
        std::vector<int64_t> wide;
        for (int shift : {0, 6, 7, 31, 58, 60, 62, 63}) {
            wide.push_back((static_cast<int64_t>(1) << shift) | 1);
        }
        cases.push_back(wide);
    }
    cases.push_back(
        std::vector<int64_t>({std::numeric_limits<int64_t>::min(),
                              std::numeric_limits<int64_t>::max(), 0, -1, 1}));

    for (size_t c = 0; c < cases.size(); ++c) {
        const std::vector<int64_t>& input = cases[c];
        const std::vector<char> bytes = encode_int64_stream(input);
        ASSERT_EQ(decode_int64(bytes, false, kScalar, 0), input)
            << "case=" << c;
        ASSERT_EQ(decode_int64(bytes, true, kScalar, 0), input) << "case=" << c;
        for (int capacity : {1, 3, 7, 64, 129, 1024}) {
            ASSERT_EQ(decode_int64(bytes, false, kBatch, capacity), input)
                << "case=" << c << " capacity=" << capacity;
            ASSERT_EQ(decode_int64(bytes, true, kBatch, capacity), input)
                << "case=" << c << " capacity=" << capacity;
        }
        ASSERT_EQ(decode_int64(bytes, true, kMixed, 0), input) << "case=" << c;
    }
}

TEST_F(Int64RleCodecTest, BatchDecodeSpansMultiplePatterns) {
    std::vector<int64_t> input;
    for (int i = 0; i < 200; ++i) {
        input.push_back((static_cast<int64_t>(i) << 40) |
                        static_cast<int64_t>(i % 17));
    }
    const std::vector<char> bytes = encode_int64_stream(input, 37);
    for (int capacity : {1, 37, 129, 1024}) {
        ASSERT_EQ(decode_int64(bytes, true, kBatch, capacity), input)
            << "capacity=" << capacity;
    }
}

TEST_F(Int64RleCodecTest, MalformedBitPackedInputIsRejected) {
    const std::vector<std::vector<char>> patterns = malformed_patterns();
    for (size_t i = 0; i < patterns.size(); ++i) {
        EXPECT_NE(decode_int64_expect_error(patterns[i]), common::E_OK)
            << "scalar malformed case " << i;
        EXPECT_NE(decode_int64_batch_expect_error(patterns[i]), common::E_OK)
            << "batch malformed case " << i;
    }
}

}  // namespace storage
