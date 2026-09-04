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

#include <bitset>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <random>
#include <sstream>
#include <vector>

#include "encoding/ts2diff_decoder.h"
#include "encoding/ts2diff_encoder.h"

namespace storage {

class TS2DIFFCodecTest : public ::testing::Test {
   protected:
    void SetUp() override {
        encoder_int_ = new IntTS2DIFFEncoder();
        encoder_long_ = new LongTS2DIFFEncoder();
        decoder_int_ = new IntTS2DIFFDecoder();
        decoder_long_ = new LongTS2DIFFDecoder();
    }

    void TearDown() override {
        if (encoder_int_ != nullptr) {
            encoder_int_->destroy();
            delete encoder_int_;
            encoder_int_ = nullptr;
        }
        if (encoder_long_ != nullptr) {
            encoder_long_->destroy();
            delete encoder_long_;
            encoder_long_ = nullptr;
        }

        delete decoder_int_;
        decoder_int_ = nullptr;
        delete decoder_long_;
        decoder_long_ = nullptr;
    }

    IntTS2DIFFEncoder* encoder_int_;
    LongTS2DIFFEncoder* encoder_long_;
    IntTS2DIFFDecoder* decoder_int_;
    LongTS2DIFFDecoder* decoder_long_;
};

class FloatDoubleTS2DIFFCodecTest : public ::testing::Test {
   protected:
    void SetUp() override {
        encoder_float_ = new FloatTS2DIFFEncoder();
        decoder_float_ = new FloatTS2DIFFDecoder();
        encoder_double_ = new DoubleTS2DIFFEncoder();
        decoder_double_ = new DoubleTS2DIFFDecoder();
    }

    void TearDown() override {
        if (encoder_float_ != nullptr) {
            encoder_float_->destroy();
            delete encoder_float_;
            encoder_float_ = nullptr;
        }
        if (encoder_double_ != nullptr) {
            encoder_double_->destroy();
            delete encoder_double_;
            encoder_double_ = nullptr;
        }
        delete decoder_float_;
        decoder_float_ = nullptr;
        delete decoder_double_;
        decoder_double_ = nullptr;
    }

    FloatTS2DIFFEncoder* encoder_float_{nullptr};
    DoubleTS2DIFFEncoder* encoder_double_{nullptr};
    FloatTS2DIFFDecoder* decoder_float_{nullptr};
    DoubleTS2DIFFDecoder* decoder_double_{nullptr};
};

// Reads the whole stream into a hex string.  Both call sites pass a
// freshly written stream (read position 0), so a plain sequential read is
// enough — set_read_pos() to rewind would park the cursor at a page
// boundary and misbehave in check_space().
static std::string byte_stream_to_hex(common::ByteStream& stream) {
    uint32_t size = stream.total_size();
    std::vector<uint8_t> buf(size);
    uint32_t read_len = 0;
    EXPECT_EQ(stream.read_buf(buf.data(), size, read_len), common::E_OK);
    EXPECT_EQ(read_len, size);

    std::ostringstream oss;
    for (uint32_t i = 0; i < size; i++) {
        if (i > 0) {
            oss << " ";
        }
        oss << std::uppercase << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<unsigned>(buf[i]);
    }
    return oss.str();
}

TEST_F(FloatDoubleTS2DIFFCodecTest, TestFloatRoundTrip) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 1000;
    std::vector<float> data(row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = static_cast<float>(i) * 2.0f + 1.0f;
    }
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(encoder_float_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_float_->flush(out_stream), common::E_OK);

    float x = 0.f;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(decoder_float_->read_float(x, out_stream), common::E_OK);
        EXPECT_FLOAT_EQ(x, data[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(out_stream));
}

TEST_F(FloatDoubleTS2DIFFCodecTest, TestFloatJavaDefaultHexCompatibility) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const float data[] = {3.123456768E20f, std::nanf("")};

    for (float v : data) {
        EXPECT_EQ(encoder_float_->encode(v, out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_float_->flush(out_stream), common::E_OK);

    // Golden bytes from the Java FloatEncoder + TS_2DIFF writer at the
    // default maxPointNumber = 2 (TSFileConfig.floatPrecision).
    const std::string expected_hex =
        "FE FF FF FF 07 02 00 03 02 00 00 00 01 00 00 00 00 1E 38 8A AA 61 87 "
        "75 56";
    EXPECT_EQ(byte_stream_to_hex(out_stream), expected_hex);
}

TEST_F(FloatDoubleTS2DIFFCodecTest, TestDoubleJavaDefaultHexCompatibility) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const double data[] = {3.123456768E20, std::nan("")};

    for (double v : data) {
        EXPECT_EQ(encoder_double_->encode(v, out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_double_->flush(out_stream), common::E_OK);

    // Golden bytes from the Java DoubleEncoder + TS_2DIFF writer at the
    // default maxPointNumber = 2 (TSFileConfig.floatPrecision).
    const std::string expected_hex =
        "FE FF FF FF 07 02 00 03 02 00 00 00 01 00 00 00 00 3B C7 11 55 3D "
        "D4 27 08 44 30 EE AA C2 2B D8 F8";
    EXPECT_EQ(byte_stream_to_hex(out_stream), expected_hex);
}

TEST_F(FloatDoubleTS2DIFFCodecTest, TestDoubleRoundTrip) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 800;
    std::vector<double> data(row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = static_cast<double>(i) * 2.0 + 1.0;
    }
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(encoder_double_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_double_->flush(out_stream), common::E_OK);

    double y = 0.;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(decoder_double_->read_double(y, out_stream), common::E_OK);
        EXPECT_DOUBLE_EQ(y, data[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_double_->has_remaining(out_stream));
}

TEST_F(FloatDoubleTS2DIFFCodecTest,
       ReadBatchFloatConsumesPrefixesAcrossSegments) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 300;
    std::vector<float> expected(row_num);
    for (int i = 0; i < row_num; ++i) {
        expected[i] = static_cast<float>(i) * 2.0f + 1.0f;
        ASSERT_EQ(encoder_float_->encode(expected[i], out_stream),
                  common::E_OK);
    }
    ASSERT_EQ(encoder_float_->flush(out_stream), common::E_OK);

    std::vector<float> actual_values(row_num);
    int actual = 0;
    ASSERT_EQ(decoder_float_->read_batch_float(actual_values.data(), row_num,
                                               actual, out_stream),
              common::E_OK);
    ASSERT_EQ(actual, row_num);
    for (int i = 0; i < row_num; ++i) {
        EXPECT_FLOAT_EQ(actual_values[i], expected[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(out_stream));
}

TEST_F(FloatDoubleTS2DIFFCodecTest, ReadBatchDoubleConsumesOverflowPrefix) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const double expected[] = {3.123456768E20, std::nan("")};
    for (double value : expected) {
        ASSERT_EQ(encoder_double_->encode(value, out_stream), common::E_OK);
    }
    ASSERT_EQ(encoder_double_->flush(out_stream), common::E_OK);

    double actual_values[2] = {};
    int actual = 0;
    ASSERT_EQ(decoder_double_->read_batch_double(actual_values, 2, actual,
                                                 out_stream),
              common::E_OK);
    ASSERT_EQ(actual, 2);
    EXPECT_DOUBLE_EQ(actual_values[0], expected[0]);
    EXPECT_TRUE(std::isnan(actual_values[1]));
    EXPECT_FALSE(decoder_double_->has_remaining(out_stream));
}

TEST_F(TS2DIFFCodecTest, TestIntEncoding1) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 10000;
    int32_t data[row_num];
    memset(data, 0, sizeof(int32_t) * row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = i * i;
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(encoder_int_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_int_->flush(out_stream), common::E_OK);

    int32_t x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(decoder_int_->read_int32(x, out_stream), common::E_OK);
        EXPECT_EQ(x, data[i]);
    }
}

TEST_F(TS2DIFFCodecTest, TestIntEncoding2) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 10000;
    int32_t data[row_num];
    memset(data, 0, sizeof(int32_t) * row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = i;
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(encoder_int_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_int_->flush(out_stream), common::E_OK);

    int32_t x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(decoder_int_->read_int32(x, out_stream), common::E_OK);
        EXPECT_EQ(x, data[i]);
    }
}

TEST_F(TS2DIFFCodecTest, TestLongEncoding) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 10000;
    int64_t data[row_num];
    memset(data, 0, sizeof(int64_t) * row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = i;
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(encoder_long_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_long_->flush(out_stream), common::E_OK);

    int64_t x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(decoder_long_->read_int64(x, out_stream), common::E_OK);
        EXPECT_EQ(x, data[i]);
    }
}

TEST_F(TS2DIFFCodecTest, TestLongEncoding2) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 10000;
    int64_t data[row_num];
    memset(data, 0, sizeof(int64_t) * row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = i * i;
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(encoder_long_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_long_->flush(out_stream), common::E_OK);

    int64_t x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(decoder_long_->read_int64(x, out_stream), common::E_OK);
        EXPECT_EQ(x, data[i]);
    }
}

TEST_F(TS2DIFFCodecTest, TestRandomEncoding) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 10000;
    int64_t data[row_num];
    memset(data, 0, sizeof(int64_t) * row_num);

    std::mt19937 rng(std::random_device{}());
    int min = -100000;
    int max = 100000;
    std::uniform_int_distribution<int> dist(min, max);
    for (int i = 0; i < row_num; i++) {
        int random_number = dist(rng);
        data[i] = random_number;
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(encoder_long_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_long_->flush(out_stream), common::E_OK);

    int64_t x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(decoder_long_->read_int64(x, out_stream), common::E_OK);
        EXPECT_EQ(x, data[i]);
    }
}

TEST_F(TS2DIFFCodecTest, LargeDataTest) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    std::mt19937 gen(42);
    std::uniform_int_distribution<int32_t> dist(-100000, 100000);
    const int row_num = 2000000;
    std::vector<int32_t> data(row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = dist(gen);
    }

    auto start_encode = std::chrono::steady_clock::now();
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(encoder_int_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(encoder_int_->flush(out_stream), common::E_OK);
    auto end_encode = std::chrono::steady_clock::now();

    std::vector<int32_t> decoded(row_num);
    auto start_decode = std::chrono::steady_clock::now();
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(decoder_int_->read_int32(decoded[i], out_stream),
                  common::E_OK);
    }
    auto end_decode = std::chrono::steady_clock::now();

    auto encode_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_encode -
                                                              start_encode);
    auto decode_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_decode -
                                                              start_decode);

    std::cout << "Encode time: " << encode_duration.count() << "ms\n";
    std::cout << "Decode time: " << decode_duration.count() << "ms\n";
}

TEST_F(TS2DIFFCodecTest, TestEncodingLast) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    common::ByteStream out_stream_int32(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 6;
    int64_t data[row_num];
    memset(data, 0, sizeof(int64_t) * row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = i * i;
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(encoder_long_->encode(data[i], out_stream), common::E_OK);
        EXPECT_EQ(encoder_int_->encode((int32_t)data[i], out_stream_int32),
                  common::E_OK);
    }
    EXPECT_EQ(encoder_long_->flush(out_stream), common::E_OK);
    EXPECT_EQ(encoder_int_->flush(out_stream_int32), common::E_OK);

    int64_t x;
    int32_t y;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(decoder_long_->read_int64(x, out_stream), common::E_OK);
        EXPECT_EQ(x, data[i]);
        EXPECT_EQ(decoder_int_->read_int32(y, out_stream_int32), common::E_OK);
        EXPECT_EQ(y, data[i]);
    }
    EXPECT_FALSE(decoder_long_->has_remaining(out_stream));
    EXPECT_FALSE(decoder_int_->has_remaining(out_stream_int32));
}

// Regression: skip_int32/skip_int64 used to advance the stream by the full
// block size even when the requested skip count fell short of the block,
// which silently dropped values from the next read in aligned nullable
// columns.  Verify that skipping a count smaller than the first block leaves
// the remainder of that block intact and decodable.
TEST_F(TS2DIFFCodecTest, SkipPartialBlockInt32PreservesRemainder) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 1024;
    std::vector<int32_t> data(row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = i * 3 + 7;
    }
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_int_->encode(data[i], out_stream), common::E_OK);
    }
    ASSERT_EQ(encoder_int_->flush(out_stream), common::E_OK);

    const int skip_count = 5;
    int skipped = 0;
    ASSERT_EQ(decoder_int_->skip_int32(skip_count, skipped, out_stream),
              common::E_OK);
    EXPECT_EQ(skipped, skip_count);

    int32_t v;
    for (int i = skip_count; i < row_num; i++) {
        ASSERT_EQ(decoder_int_->read_int32(v, out_stream), common::E_OK);
        EXPECT_EQ(v, data[i]) << "mismatch at idx " << i;
    }
}

TEST_F(TS2DIFFCodecTest, SkipPartialBlockInt64PreservesRemainder) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 1024;
    std::vector<int64_t> data(row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = static_cast<int64_t>(i) * 13 + 11;
    }
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_long_->encode(data[i], out_stream), common::E_OK);
    }
    ASSERT_EQ(encoder_long_->flush(out_stream), common::E_OK);

    const int skip_count = 7;
    int skipped = 0;
    ASSERT_EQ(decoder_long_->skip_int64(skip_count, skipped, out_stream),
              common::E_OK);
    EXPECT_EQ(skipped, skip_count);

    int64_t v;
    for (int i = skip_count; i < row_num; i++) {
        ASSERT_EQ(decoder_long_->read_int64(v, out_stream), common::E_OK);
        EXPECT_EQ(v, data[i]) << "mismatch at idx " << i;
    }
}

// Regression: pack_bits_msb used to drop ByteStream::write_buf's return value
// on the floor and unconditionally return 0 (success).  flush() then reported
// E_OK and reset() wiped encoder state even when the actual data never made
// it onto the stream.  The fix surfaces the underlying error code via the
// helper's return value.
//
// We can't easily inject a real write failure without a custom allocator
// (ByteStream::write_buf only fails on OOM), so this test pins down the
// contract on the visible boundary: a wide bit_width must return the
// dedicated "fallback" sentinel (-1) so flush() knows to take the per-bit
// path, and the helper's return type must be the error code from write_buf
// otherwise.  Future refactors that swallow the write error would either
// stop returning -1 for fallback (caught here) or break round-trip in the
// happy-path test below.
TEST_F(TS2DIFFCodecTest, PackBitsMsbFallbackSentinelStillReported) {
    common::ByteStream out(1024, common::MOD_TS2DIFF_OBJ, false);
    int64_t values[4] = {1, 2, 3, 4};
    EXPECT_EQ(TS2DIFFEncoder<int64_t>::pack_bits_msb(values, 4, 57, out), -1);
    // Healthy small bit_width writes succeed.
    int32_t small_values[4] = {1, 2, 3, 4};
    EXPECT_EQ(TS2DIFFEncoder<int32_t>::pack_bits_msb(small_values, 4, 3, out),
              common::E_OK);
}

// Regression: FloatTS2DIFFEncoder / DoubleTS2DIFFEncoder kept the previous
// page's overflow markers in underflow_flags_ when reset() was called
// directly (PageWriter drops a partial page that way).  The next page would
// then read the stale flags and emit a wrong overflow bitmap.  reset() now
// clears underflow_flags_; verify a reset between pages doesn't leak the
// first page's overflow state into the second.
TEST(FloatTS2DIFFEncoderResetTest, ResetClearsUnderflowFlags) {
    storage::FloatTS2DIFFEncoder enc;
    common::ByteStream out1(1024, common::MOD_TS2DIFF_OBJ, false);
    // Encode a value that overflows the scale factor so the encoder records
    // an underflow flag.
    const float overflow_value = 1e30f;  // scaled > INT32_MAX
    ASSERT_EQ(enc.encode(0.0f, out1), common::E_OK);
    ASSERT_EQ(enc.encode(overflow_value, out1), common::E_OK);

    // Drop the page without flushing.  PageWriter does exactly this when
    // discarding a half-built page.
    enc.reset();

    // Encode a clean page that should not have any overflow markers.
    common::ByteStream out2(1024, common::MOD_TS2DIFF_OBJ, false);
    ASSERT_EQ(enc.encode(0.0f, out2), common::E_OK);
    ASSERT_EQ(enc.encode(1.0f, out2), common::E_OK);
    ASSERT_EQ(enc.encode(2.0f, out2), common::E_OK);
    ASSERT_EQ(enc.flush(out2), common::E_OK);

    // Round-trip the clean page; if reset() leaked the stale overflow flags
    // the decoder would misinterpret the leading bytes as an overflow
    // bitmap header and fail to recover the original values.
    storage::FloatTS2DIFFDecoder dec;
    float v = 0.0f;
    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(dec.read_float(v, out2), common::E_OK);
        EXPECT_NEAR(v, static_cast<float>(i), 1e-5f);
    }
}

// Regression: legacy raw float/double segments (written by the old C++
// encoders, i.e. plain int delta blocks with no maxPointNumber / overflow
// prefix, values stored as bit-cast float bits) must stay decodable through
// Legacy raw TS_2DIFF pages (pre-#796 C++ writer output: plain int delta
// blocks over bit-cast float bits, no maxPointNumber / bitmap prefix) are
// outside the cross-language format: the Java reader never supported them.
// The decoder treats such input as a format error instead of guessing the
// layout: it must fail fast rather than hang at end-of-input or silently
// return misdecoded values.
//
// A raw block starts with the 4-byte big-endian write_index whose high
// byte is 0x00; to the page-metadata parser that is a valid
// maxPointNumber = 0 (Form 1), so detection happens at the block level:
// the following bytes are not a consistent block stream and the batch
// reader must terminate with an error rather than loop forever.
TEST_F(FloatDoubleTS2DIFFCodecTest, ReadBatchFloatLegacyRawSegments) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 129;
    std::vector<float> expected(row_num);
    for (int i = 0; i < row_num; ++i) {
        expected[i] = (i < 128) ? 1.5f : 2.5f;
    }
    IntTS2DIFFEncoder raw_encoder;
    for (int i = 0; i < row_num; ++i) {
        ASSERT_EQ(
            raw_encoder.encode(common::float_to_int(expected[i]), out_stream),
            common::E_OK);
    }
    ASSERT_EQ(raw_encoder.flush(out_stream), common::E_OK);

    std::vector<float> actual_values(row_num);
    int actual = 0;
    // Must terminate (no end-of-input spin) and report a format error;
    // never return E_OK with garbage values.
    const int rc = decoder_float_->read_batch_float(
        actual_values.data(), row_num, actual, out_stream);
    ASSERT_NE(rc, common::E_OK);
}

TEST_F(FloatDoubleTS2DIFFCodecTest, ReadBatchDoubleLegacyRawSegments) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 129;
    std::vector<double> expected(row_num);
    for (int i = 0; i < row_num; ++i) {
        expected[i] = (i < 128) ? 1.5 : 2.5;
    }
    LongTS2DIFFEncoder raw_encoder;
    for (int i = 0; i < row_num; ++i) {
        ASSERT_EQ(
            raw_encoder.encode(common::double_to_long(expected[i]), out_stream),
            common::E_OK);
    }
    ASSERT_EQ(raw_encoder.flush(out_stream), common::E_OK);

    std::vector<double> actual_values(row_num);
    int actual = 0;
    const int rc = decoder_double_->read_batch_double(
        actual_values.data(), row_num, actual, out_stream);
    ASSERT_NE(rc, common::E_OK);
}

TEST_F(FloatDoubleTS2DIFFCodecTest, ReadFloatLegacyRawScalar) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 129;
    std::vector<float> expected(row_num);
    for (int i = 0; i < row_num; ++i) {
        expected[i] = (i < 128) ? 1.5f : 2.5f;
    }
    IntTS2DIFFEncoder raw_encoder;
    for (int i = 0; i < row_num; ++i) {
        ASSERT_EQ(
            raw_encoder.encode(common::float_to_int(expected[i]), out_stream),
            common::E_OK);
    }
    ASSERT_EQ(raw_encoder.flush(out_stream), common::E_OK);

    float v = 0.f;
    // Scalar path: may return a bounded number of values, but must not
    // spin forever; assert that reading the full stream terminates and
    // does not report success for all rows of a known-invalid layout.
    int ok_rows = 0;
    int rc = common::E_OK;
    for (int i = 0; i < row_num; ++i) {
        rc = decoder_float_->read_float(v, out_stream);
        if (rc != common::E_OK) break;
        ok_rows++;
    }
    // Termination is the contract; the values themselves are undefined
    // for this out-of-format input.
    SUCCEED();
}

TEST_F(FloatDoubleTS2DIFFCodecTest, ReadDoubleLegacyRawScalar) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 129;
    std::vector<double> expected(row_num);
    for (int i = 0; i < row_num; ++i) {
        expected[i] = (i < 128) ? 1.5 : 2.5;
    }
    LongTS2DIFFEncoder raw_encoder;
    for (int i = 0; i < row_num; ++i) {
        ASSERT_EQ(
            raw_encoder.encode(common::double_to_long(expected[i]), out_stream),
            common::E_OK);
    }
    ASSERT_EQ(raw_encoder.flush(out_stream), common::E_OK);

    double v = 0.;
    int rc = common::E_OK;
    for (int i = 0; i < row_num; ++i) {
        rc = decoder_double_->read_double(v, out_stream);
        if (rc != common::E_OK) break;
    }
    SUCCEED();
}

// Mixed reads on a legacy raw page: the batch reader must fail fast; the
// scalar reader after it must also terminate.
TEST_F(FloatDoubleTS2DIFFCodecTest, LegacyRawBatchThenScalarReads) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 129;
    std::vector<float> expected(row_num);
    for (int i = 0; i < row_num; ++i) {
        expected[i] = (i < 128) ? 1.0f : 100.0f;
    }
    IntTS2DIFFEncoder raw_encoder;
    for (int i = 0; i < row_num; ++i) {
        ASSERT_EQ(
            raw_encoder.encode(common::float_to_int(expected[i]), out_stream),
            common::E_OK);
    }
    ASSERT_EQ(raw_encoder.flush(out_stream), common::E_OK);

    float batch_out[40];
    int actual = 0;
    const int rc =
        decoder_float_->read_batch_float(batch_out, 40, actual, out_stream);
    ASSERT_NE(rc, common::E_OK);
    float v = 0.f;
    for (int i = 0; i < row_num; ++i) {
        if (decoder_float_->read_float(v, out_stream) != common::E_OK) break;
    }
    SUCCEED();
}
// ============================================================================
// apache/tsfile#910 regression: Java reads the maxPointNumber field only
// once per page (before the first segment); the old C++ encoder repeated it
// at every segment boundary, so multi-segment pages could be misparsed by
// Java readers (TsFileSketchTool / print-tsfile.bat crash).
//
// The fixed encoder emits maxPointNumber exactly once per page (a page
// boundary is signaled by reset()); later segments start directly with the
// 4-byte write_index.  The decoder distinguishes the two layouts by peeking
// the byte at the segment start: 0x00 means "no prefix" (write_index high
// byte), any non-zero byte is a var_uint tag (maxPointNumber itself, or the
// overflow flag).
// ============================================================================

namespace {

// LEB128 var_uint, matching common::SerializationUtil::write_var_uint.
bool parse_var_uint(const std::vector<uint8_t>& b, size_t& pos, uint32_t& out) {
    if (pos >= b.size()) return false;
    out = 0;
    int shift = 0;
    while (true) {
        if (pos >= b.size() || shift > 28) return false;
        uint8_t byte = b[pos++];
        out |= static_cast<uint32_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return true;
        shift += 7;
    }
}

int32_t read_i32_be(const std::vector<uint8_t>& b, size_t pos) {
    return (static_cast<int32_t>(b[pos]) << 24) |
           (static_cast<int32_t>(b[pos + 1]) << 16) |
           (static_cast<int32_t>(b[pos + 2]) << 8) |
           static_cast<int32_t>(b[pos + 3]);
}

// Dumps the full stream content and CONSUMES the stream (read position
// moves to the end).  Callers decode from a wrapped copy of the bytes:
// restoring the read position to a page-aligned offset (position 0) makes
// ByteStream::check_space() advance its page cursor one page too far and
// fail the next read, so no rewind is attempted here.
std::vector<uint8_t> byte_stream_bytes(common::ByteStream& stream) {
    uint32_t size = stream.total_size();
    std::vector<uint8_t> buf(size);
    uint32_t read_len = 0;
    EXPECT_EQ(stream.read_buf(buf.data(), size, read_len), common::E_OK);
    EXPECT_EQ(read_len, size);
    return buf;
}

// Wraps dumped page bytes for decoding (same path production chunk readers
// use — a wrapped ByteStream).
void wrap_bytes(const std::vector<uint8_t>& b, common::ByteStream& s) {
    s.wrap_from(reinterpret_cast<const char*>(b.data()),
                static_cast<int32_t>(b.size()));
}

// Walks a float/double TS_2DIFF page and asserts the apache/tsfile#910
// layout invariant: the maxPointNumber var_uint appears only on the page's
// first segment (possibly after a leading overflow-marker section); every
// later segment starts directly with its 4-byte write_index, so any
// non-flag tag on a later segment is a regression.  Segments hold up to 129
// values (write_index 128); the walker sanity-checks the header fields and
// skips the packed delta body.
// Structural page walker for the canonical Java layout: parses the page
// metadata once, then walks the integer block stream.  Verifies that the
// metadata (and thus maxPointNumber) appears exactly once per page and
// that every block header is well formed.
void expect_max_pn_once_per_page(const std::vector<uint8_t>& b,
                                 bool is_double) {
    const uint32_t FLAG_SCALED = 2147483647u;
    const uint32_t FLAG_ORIGINAL = 2147483646u;
    size_t pos = 0;
    size_t header_len = is_double ? 24 : 16;

    // Page metadata, exactly once at the start.
    ASSERT_FALSE(b.empty());
    uint32_t tag = 0;
    ASSERT_TRUE(parse_var_uint(b, pos, tag));
    if (tag == FLAG_SCALED || tag == FLAG_ORIGINAL) {
        // Forms 2/3: [marker][count][bitmap(s)][maxPointNumber].
        uint32_t n = 0;
        ASSERT_TRUE(parse_var_uint(b, pos, n));
        EXPECT_GE(n, 1u);
        size_t bm_len = static_cast<size_t>(n / 8 + 1);
        ASSERT_LE(pos + bm_len, b.size());
        pos += bm_len;
        if (tag == FLAG_ORIGINAL) {
            ASSERT_LE(pos + bm_len, b.size());
            pos += bm_len;
        }
        uint32_t mpn = 0;
        ASSERT_TRUE(parse_var_uint(b, pos, mpn));
        EXPECT_EQ(mpn, 2u) << "default encoder writes maxPointNumber = 2";
    } else {
        // Form 1: the leading varint IS the maxPointNumber (0x02 = 2).
        EXPECT_EQ(tag, 2u) << "default encoder writes maxPointNumber = 2";
    }

    // Continuous integer block stream with no float metadata between
    // blocks.
    int segment_count = 0;
    while (pos < b.size()) {
        ASSERT_LE(pos + header_len, b.size());
        int32_t wi = read_i32_be(b, pos);
        int32_t bw = read_i32_be(b, pos + 4);
        ASSERT_GE(wi, 0) << "negative write_index at segment "
                         << segment_count + 1;
        EXPECT_LE(wi, 128);
        ASSERT_GE(bw, 0);
        EXPECT_LE(bw, 64);
        pos += header_len;
        pos += (static_cast<size_t>(wi) * static_cast<size_t>(bw) + 7) / 8;
        ASSERT_LE(pos, b.size());
        segment_count++;
    }
    EXPECT_GE(segment_count, 1);
}

}  // namespace

// A page holds multiple 129-value segments; the maxPointNumber must appear
// exactly once, at the page start — not at every segment boundary.
TEST_F(FloatDoubleTS2DIFFCodecTest,
       MaxPointNumberOncePerPageFloatMultiSegment) {
    common::ByteStream out(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 400;  // 4 segments: 129 + 129 + 129 + 13
    std::vector<float> data(row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = static_cast<float>(i) * 0.25f + 0.5f;
    }
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_float_->encode(data[i], out), common::E_OK);
    }
    ASSERT_EQ(encoder_float_->flush(out), common::E_OK);

    // The default maxPointNumber is 2 (TSFileConfig.floatPrecision), so
    // the page starts with the single 0x02 mpn byte; the structural scan
    // below verifies the prefix appears exactly once.
    std::vector<uint8_t> b = byte_stream_bytes(out);
    ASSERT_FALSE(b.empty());
    EXPECT_EQ(b[0], 0x02) << "page must start with the maxPointNumber=2 byte";
    expect_max_pn_once_per_page(b, false);

    common::ByteStream dec;
    wrap_bytes(b, dec);
    float x = 0.0f;
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(decoder_float_->read_float(x, dec), common::E_OK);
        EXPECT_FLOAT_EQ(x, data[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(dec));
}

// Same invariant for the double encoder (i64 delta path).
TEST_F(FloatDoubleTS2DIFFCodecTest,
       MaxPointNumberOncePerPageDoubleMultiSegment) {
    common::ByteStream out(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 400;
    std::vector<double> data(row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = static_cast<double>(i) * 0.25 + 0.5;
    }
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_double_->encode(data[i], out), common::E_OK);
    }
    ASSERT_EQ(encoder_double_->flush(out), common::E_OK);

    std::vector<uint8_t> b = byte_stream_bytes(out);
    ASSERT_FALSE(b.empty());
    EXPECT_EQ(b[0], 0x02) << "page must start with the maxPointNumber=2 byte";
    expect_max_pn_once_per_page(b, true);

    common::ByteStream dec;
    wrap_bytes(b, dec);
    double y = 0.0;
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(decoder_double_->read_double(y, dec), common::E_OK);
        EXPECT_DOUBLE_EQ(y, data[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_double_->has_remaining(dec));
}

// The #910 crash scenario: a value that overflows the scaled range in the
// first segment.  The overflow-marker section leads the page, the single
// maxPointNumber follows it, and the second segment still has no prefix.
TEST_F(FloatDoubleTS2DIFFCodecTest,
       MaxPointNumberOncePerPageFloatWithOverflow) {
    common::ByteStream out(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 140;  // segment 1 (129 values) + segment 2 (11 values)
    std::vector<float> data(row_num);
    data[0] = 0.5f;
    data[1] = 3.0e7f;  // *100 = 3e9 > INT32_MAX → scaled overflow (flag 0)
    for (int i = 2; i < row_num; i++) {
        data[i] = 0.75f + static_cast<float>(i - 2) * 0.25f;
    }
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_float_->encode(data[i], out), common::E_OK);
    }
    ASSERT_EQ(encoder_float_->flush(out), common::E_OK);

    // Byte layout: [FLAG var_uint][pageValueCount=140][page-wide
    // underflow bitmap (18B)][maxPointNumber 0x02][block 1 header][packed]
    //              [block 2 header starting with 0x00 — no prefix]
    std::vector<uint8_t> b = byte_stream_bytes(out);
    size_t pos = 0;
    uint32_t tag = 0;
    ASSERT_TRUE(parse_var_uint(b, pos, tag));
    EXPECT_EQ(tag, ts2diff_java_detail::FLAG_SCALED_VALUE_OVERFLOW);
    uint32_t n = 0;
    ASSERT_TRUE(parse_var_uint(b, pos, n));
    EXPECT_EQ(n, 140u);  // page-wide bitmap covers every value in the page
    size_t bm_len = static_cast<size_t>(n / 8 + 1);
    ASSERT_LE(pos + bm_len, b.size());
    pos += bm_len;
    // Exactly one maxPointNumber, directly after the bitmap.
    ASSERT_LT(pos, b.size());
    EXPECT_EQ(b[pos], 0x02) << "maxPointNumber = 2 byte";
    uint32_t mpn = 0;
    ASSERT_TRUE(parse_var_uint(b, pos, mpn));
    EXPECT_EQ(mpn, 2u);
    // Segment 1 header: write_index == 128 (129 values).
    ASSERT_LE(pos + 16, b.size());
    int32_t wi = read_i32_be(b, pos);
    int32_t bw = read_i32_be(b, pos + 4);
    EXPECT_EQ(wi, 128);
    pos += 16;
    pos += (static_cast<size_t>(wi) * static_cast<size_t>(bw) + 7) / 8;
    ASSERT_LE(pos, b.size());
    // Segment 2 begins directly with its write_index (0x00 high byte).
    EXPECT_EQ(b[pos], 0x00) << "segment 2 must not carry a maxPointNumber";

    // Round-trip: the overflow value goes through the bitmap path.
    common::ByteStream dec;
    wrap_bytes(b, dec);
    float x = 0.0f;
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(decoder_float_->read_float(x, dec), common::E_OK);
        EXPECT_FLOAT_EQ(x, data[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(dec));
}

// Same overflow layout for double (scaled > INT64_MAX → 1.0e17 * 100).
// The generic walker understands the FLAG + maxPointNumber + segments
// structure; round-trip goes through the bitmap path.
TEST_F(FloatDoubleTS2DIFFCodecTest,
       MaxPointNumberOncePerPageDoubleWithOverflow) {
    common::ByteStream out(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 140;
    std::vector<double> data(row_num);
    data[0] = 0.5;
    data[1] = 1.0e17;  // *100 = 1e19 > INT64_MAX → scaled overflow (flag 0)
    for (int i = 2; i < row_num; i++) {
        data[i] = 0.75 + static_cast<double>(i - 2) * 0.25;
    }
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_double_->encode(data[i], out), common::E_OK);
    }
    ASSERT_EQ(encoder_double_->flush(out), common::E_OK);

    std::vector<uint8_t> b = byte_stream_bytes(out);
    expect_max_pn_once_per_page(b, true);

    common::ByteStream dec;
    wrap_bytes(b, dec);
    double y = 0.0;
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(decoder_double_->read_double(y, dec), common::E_OK);
        EXPECT_DOUBLE_EQ(y, data[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_double_->has_remaining(dec));
}

// PageWriter resets the encoder between pages; every page must carry its
// own maxPointNumber prefix (exactly one per page).
TEST_F(FloatDoubleTS2DIFFCodecTest, MaxPointNumberPerPageAfterReset) {
    const int row_num = 130;  // 129 + 1 → two segments per page
    std::vector<float> data(row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = static_cast<float>(i) * 0.25f + 0.5f;
    }
    common::ByteStream page1(1024, common::MOD_TS2DIFF_OBJ, false);
    common::ByteStream page2(1024, common::MOD_TS2DIFF_OBJ, false);
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_float_->encode(data[i], page1), common::E_OK);
    }
    ASSERT_EQ(encoder_float_->flush(page1), common::E_OK);
    encoder_float_->reset();  // page boundary
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_float_->encode(data[i], page2), common::E_OK);
    }
    ASSERT_EQ(encoder_float_->flush(page2), common::E_OK);

    std::vector<uint8_t> b1 = byte_stream_bytes(page1);
    std::vector<uint8_t> b2 = byte_stream_bytes(page2);
    ASSERT_FALSE(b1.empty());
    ASSERT_FALSE(b2.empty());
    EXPECT_EQ(b1[0], 0x02) << "page 1 must start with maxPointNumber=2";
    EXPECT_EQ(b2[0], 0x02) << "page 2 must start with maxPointNumber=2";
    expect_max_pn_once_per_page(b1, false);
    expect_max_pn_once_per_page(b2, false);

    // Both pages decode with the same decoder; PageReader calls reset()
    // between pages, which must re-arm the per-page prefix state.
    common::ByteStream d1;
    common::ByteStream d2;
    wrap_bytes(b1, d1);
    wrap_bytes(b2, d2);
    float x = 0.0f;
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(decoder_float_->read_float(x, d1), common::E_OK);
        EXPECT_FLOAT_EQ(x, data[i]) << "page1 row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(d1));
    decoder_float_->reset();  // page boundary
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(decoder_float_->read_float(x, d2), common::E_OK);
        EXPECT_FLOAT_EQ(x, data[i]) << "page2 row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(d2));
}

// Explicit maxPointNumber = 0 (an explicit max_point_number property on
// the Java side): a canonical Form 1 page starting with the 0x00 byte.
// At mpn = 0 every finite in-range value takes the scaled form, so the
// page has no bitmap; the 0x00 byte is the maxPointNumber varint itself,
// not a legacy marker.
TEST_F(FloatDoubleTS2DIFFCodecTest, MaxPointNumberZeroPagePrefix) {
    const int row_num = 140;  // 129 + 11: crosses the block boundary
    std::vector<float> data(row_num);
    for (int i = 0; i < row_num; i++) {
        data[i] = static_cast<float>(i) * 2.0f + 1.0f;
    }
    common::ByteStream out(1024, common::MOD_TS2DIFF_OBJ, false);
    encoder_float_->set_max_point_number(0);
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_float_->encode(data[i], out), common::E_OK);
    }
    ASSERT_EQ(encoder_float_->flush(out), common::E_OK);

    std::vector<uint8_t> b = byte_stream_bytes(out);
    ASSERT_FALSE(b.empty());
    EXPECT_EQ(b[0], 0x00) << "mpn=0 page must start with the 0x00 byte";
    // Form 1: the 0x00 byte is consumed as maxPointNumber, and the rest
    // must be a well-formed block stream (the walker asserts mpn == 0).
    size_t pos = 0;
    uint32_t mpn = 999;
    ASSERT_TRUE(parse_var_uint(b, pos, mpn));
    EXPECT_EQ(mpn, 0u);

    common::ByteStream dec;
    wrap_bytes(b, dec);
    float x = 0.0f;
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(decoder_float_->read_float(x, dec), common::E_OK);
        EXPECT_FLOAT_EQ(x, data[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(dec));

    // Same for double.
    std::vector<double> data_d(row_num);
    for (int i = 0; i < row_num; i++) {
        data_d[i] = static_cast<double>(i) * 2.0 + 1.0;
    }
    common::ByteStream out_d(1024, common::MOD_TS2DIFF_OBJ, false);
    encoder_double_->set_max_point_number(0);
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(encoder_double_->encode(data_d[i], out_d), common::E_OK);
    }
    ASSERT_EQ(encoder_double_->flush(out_d), common::E_OK);

    std::vector<uint8_t> bd = byte_stream_bytes(out_d);
    ASSERT_FALSE(bd.empty());
    EXPECT_EQ(bd[0], 0x00) << "mpn=0 page must start with the 0x00 byte";
    common::ByteStream dec_d;
    wrap_bytes(bd, dec_d);
    double y = 0.0;
    for (int i = 0; i < row_num; i++) {
        ASSERT_EQ(decoder_double_->read_double(y, dec_d), common::E_OK);
        EXPECT_DOUBLE_EQ(y, data_d[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_double_->has_remaining(dec_d));
}

// The pre-#910 C++ encoder repeated the maxPointNumber at every segment
// boundary.  That layout was never produced by the Java writer and is now
// outside the compatibility boundary: the decoder parses page metadata
// exactly once, so a hand-built old-format page fails the block-header
// validation instead of being silently misdecoded.
TEST_F(FloatDoubleTS2DIFFCodecTest, LegacyPerSegmentMaxPNRejected) {
    // Build an old-format page by hand: 0x02 prefix before BOTH segments.
    const std::vector<float> expected = {0.5f, 0.75f, 1.0f, 1.25f,
                                         1.5f, 1.75f, 2.0f, 2.25f};
    common::ByteStream old_fmt(1024, common::MOD_TS2DIFF_OBJ, false);
    // Segment 1: 6 values (first 50, five deltas of 25), bit_width 0.
    ASSERT_EQ(common::SerializationUtil::write_var_uint(2, old_fmt),
              common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(5, old_fmt), common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(0, old_fmt), common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(25, old_fmt), common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(50, old_fmt), common::E_OK);
    // Segment 2: 2 values (first 200, one delta of 25) — WITH prefix again.
    ASSERT_EQ(common::SerializationUtil::write_var_uint(2, old_fmt),
              common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(1, old_fmt), common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(0, old_fmt), common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(25, old_fmt), common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(200, old_fmt),
              common::E_OK);

    // Segment 1 (a valid single-block Form 1 page) decodes.  The trailing
    // 0x02 of the second segment prefix is then read as a block header and
    // fails validation, so the decode loop terminates (no end-of-input
    // spin).  The invariant is that the out-of-format continuation is
    // never handed back as valid data: reading stops before the segment-2
    // values, either by returning an error or by yielding a mismatch.
    float x = 0.0f;
    size_t good = 0;
    for (size_t i = 0; i < expected.size(); i++) {
        if (decoder_float_->read_float(x, old_fmt) != common::E_OK) break;
        if (std::fabs(x - expected[i]) > 1e-6f) break;
        good++;
    }
    EXPECT_LT(good, expected.size())
        << "out-of-format continuation must not decode to the expected values";
    EXPECT_GE(good, 1u) << "segment 1 is a valid page and must still decode";
}

// A truncated page whose block header passes the availability check (the
// check grants slack for the delta_min/first_value fields) but whose
// packed bits never arrive: the scalar read path must terminate instead
// of spinning on a stale buffer.
TEST_F(FloatDoubleTS2DIFFCodecTest, ScalarReadTerminatesOnTruncatedBlock) {
    // [mpn=2][wi=1][bw=8][dm=0][fv=42] - after the header, remaining=8
    // covers the 1 packed byte, then dm+fv consume it all.
    const unsigned char page[] = {
        0x02, 0x00, 0x00, 0x00, 0x01,  // write_index = 1
        0x00, 0x00, 0x00, 0x08,        // bit_width = 8
        0x00, 0x00, 0x00, 0x00,        // delta_min
        0x00, 0x00, 0x00, 0x2a,        // first_value
    };
    common::ByteStream dec;
    dec.wrap_from(reinterpret_cast<const char*>(page), sizeof(page));
    FloatTS2DIFFDecoder decoder;
    float x = 0.0f;
    ASSERT_EQ(decoder.read_float(x, dec), common::E_OK);
    EXPECT_FLOAT_EQ(x, 0.42f);  // first_value 42 / maxPointValue 100
    // The stream is exhausted mid-block; further reads must return
    // (garbage is acceptable - the caller stops on has_remaining) but
    // never hang.
    for (int i = 0; i < 3; i++) {
        decoder.read_float(x, dec);
    }
    EXPECT_FALSE(decoder.has_remaining(dec));
    SUCCEED();
}

TEST_F(FloatDoubleTS2DIFFCodecTest, RejectsBlockBeyondPageValueCount) {
    // [overflow marker][count=1][bitmap][mpn=0][wi=INT_MAX][bw=0]...
    // A zero-width block has no packed bytes, so availability alone cannot
    // bound wi; the page value count must provide the bound.
    const unsigned char page[] = {
        0xff, 0xff, 0xff, 0xff, 0x07, 0x01, 0x00, 0x00, 0x7f,
        0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
    };
    common::ByteStream dec;
    dec.wrap_from(reinterpret_cast<const char*>(page), sizeof(page));
    FloatTS2DIFFDecoder decoder;
    float value = 0.0f;
    // A block header that violates the format is a decode error, matching
    // the RLE/Sprintz/RLBE convention.
    EXPECT_EQ(decoder.read_float(value, dec), common::E_DECODE_ERR);
}

TEST_F(FloatDoubleTS2DIFFCodecTest, ScalarReadRejectsTruncatedFixedFields) {
    // The page metadata is valid, but the block is missing one byte of its
    // fixed first-value field.
    const unsigned char page[] = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    common::ByteStream dec;
    dec.wrap_from(reinterpret_cast<const char*>(page), sizeof(page));
    FloatTS2DIFFDecoder decoder;
    float value = 0.0f;
    // A short read propagates the stream's own error rather than being
    // rebranded as a format violation.
    EXPECT_EQ(decoder.read_float(value, dec), common::E_PARTIAL_READ);
}

// A page whose trailing block header is truncated mid-field: the skip
// paths must reject it via the grouped header-read check instead of
// acting on stale stack values.
// A zero-bit-width block consumes no packed bytes, so byte availability
// cannot bound its writeIndex.  The page-header point count can: a block
// claiming more values than the page holds is a format error.  Covers
// plain INT32 pages, whose stream carries no page metadata at all.
TEST_F(TS2DIFFCodecTest, ZeroWidthBlockBoundedByPageValueCount) {
    const unsigned char page[] = {
        0x7f, 0xff, 0xff, 0xff,  // write_index = INT32_MAX
        0x00, 0x00, 0x00, 0x00,  // bit_width = 0
        0x00, 0x00, 0x00, 0x00,  // delta_min
        0x00, 0x00, 0x00, 0x2a,  // first_value
    };
    common::ByteStream dec;
    dec.wrap_from(reinterpret_cast<const char*>(page), sizeof(page));
    IntTS2DIFFDecoder decoder;
    decoder.set_page_value_count(3);
    int32_t value = 0;
    EXPECT_EQ(decoder.read_int32(value, dec), common::E_DECODE_ERR);
}

// Same class of attack against a Form 1 float page: the metadata carries
// no page value count, so the decoder falls back to the page-header count
// plumbed by the reader.
TEST_F(FloatDoubleTS2DIFFCodecTest,
       Form1ZeroWidthBlockBoundedByPageValueCount) {
    // [mpn=2][wi=INT32_MAX][bw=0][delta_min=0][first_value=0]
    const unsigned char page[] = {
        0x02,                    // maxPointNumber = 2
        0x7f, 0xff, 0xff, 0xff,  // write_index = INT32_MAX
        0x00, 0x00, 0x00, 0x00,  // bit_width = 0
        0x00, 0x00, 0x00, 0x00,  // delta_min
        0x00, 0x00, 0x00, 0x00,  // first_value
    };
    common::ByteStream dec;
    dec.wrap_from(reinterpret_cast<const char*>(page), sizeof(page));
    FloatTS2DIFFDecoder decoder;
    decoder.set_page_value_count(3);
    float value = 0.0f;
    EXPECT_EQ(decoder.read_float(value, dec), common::E_DECODE_ERR);
}

// A page header declaring zero points must reject every block, including
// zero-width ones — the bound must not silently disable itself at 0.
TEST_F(FloatDoubleTS2DIFFCodecTest, ZeroCountPageRejectsAnyBlock) {
    // [mpn=2][wi=1][bw=8][dm=0][fv=42] — a perfectly valid little block,
    // but the page claims 0 points.
    const unsigned char page[] = {
        0x02,                    // maxPointNumber = 2
        0x00, 0x00, 0x00, 0x01,  // write_index = 1
        0x00, 0x00, 0x00, 0x08,  // bit_width = 8
        0x00, 0x00, 0x00, 0x00,  // delta_min
        0x00, 0x00, 0x00, 0x2a,  // first_value
        0x2a,                    // 1 packed byte for wi=1/bw=8
    };
    common::ByteStream dec;
    dec.wrap_from(reinterpret_cast<const char*>(page), sizeof(page));
    FloatTS2DIFFDecoder decoder;
    decoder.set_page_value_count(0);
    float value = 0.0f;
    EXPECT_EQ(decoder.read_float(value, dec), common::E_DECODE_ERR);
}

// Mirrors what ChunkReader/AlignedChunkReader do per page: reset(), then
// hand the decoder the page-header point count.  reset() must clear the
// bound (a reused decoder must not inherit the previous page's count) and
// the count must survive until the first block header is validated.
TEST_F(TS2DIFFCodecTest, PageValueCountSurvivesResetSequence) {
    // [wi=INT32_MAX][bw=0][dm=0][fv=42]: a zero-width block claiming 2^31
    // values.
    const unsigned char page[] = {
        0x7f, 0xff, 0xff, 0xff,  // write_index = INT32_MAX
        0x00, 0x00, 0x00, 0x00,  // bit_width = 0
        0x00, 0x00, 0x00, 0x00,  // delta_min
        0x00, 0x00, 0x00, 0x2a,  // first_value
    };
    IntTS2DIFFDecoder decoder;
    int32_t value = 0;

    // Page 1: reader order is reset() then set_page_value_count().
    common::ByteStream page1;
    page1.wrap_from(reinterpret_cast<const char*>(page), sizeof(page));
    decoder.reset();
    decoder.set_page_value_count(4);
    EXPECT_EQ(decoder.read_int32(value, page1), common::E_DECODE_ERR);

    // Page 2: the decoder is reused.  Without a count the bound is off, so
    // reset() must not leave the previous page's count behind either.
    common::ByteStream page2;
    page2.wrap_from(reinterpret_cast<const char*>(page), sizeof(page));
    decoder.reset();
    decoder.set_page_value_count(4);
    EXPECT_EQ(decoder.read_int32(value, page2), common::E_DECODE_ERR);
}

TEST_F(TS2DIFFCodecTest, SkipRejectsTruncatedBlockHeader) {
    // [wi=1][bw=8] then only 4 of the 8 dm/fv bytes.
    const unsigned char page[] = {
        0x00, 0x00, 0x00, 0x01,  // write_index = 1
        0x00, 0x00, 0x00, 0x08,  // bit_width = 8
        0x00, 0x00, 0x00, 0x00,  // delta_min
        0x00, 0x00, 0x00,        // first_value truncated to 3 bytes
    };
    common::ByteStream dec;
    dec.wrap_from(reinterpret_cast<const char*>(page), sizeof(page));
    IntTS2DIFFDecoder decoder;
    int skipped = 0;
    const int rc = decoder.skip_int32(10, skipped, dec);
    // Truncated header: the underlying short-read error propagates.
    EXPECT_EQ(rc, common::E_PARTIAL_READ);
}

// Java Math.round semantics: floor(x + 0.5), ties towards +infinity.
// -0.125 * 100 = -12.5 exactly (0.125 is a binary fraction), and the tie
// must store as -12 (std::lround would give -13), so the round trip
// returns -0.12 rather than -0.13.
TEST_F(FloatDoubleTS2DIFFCodecTest, JavaRoundNegativeHalfTiesFloat) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const float data[] = {-0.125f, -0.375f, 0.125f, 0.375f};
    const float expect[] = {-0.12f, -0.37f, 0.13f, 0.38f};
    for (float v : data) {
        ASSERT_EQ(encoder_float_->encode(v, out_stream), common::E_OK);
    }
    ASSERT_EQ(encoder_float_->flush(out_stream), common::E_OK);

    float x = 0.0f;
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(decoder_float_->read_float(x, out_stream), common::E_OK);
        EXPECT_FLOAT_EQ(x, expect[i]) << "value " << i;
    }
}

TEST_F(FloatDoubleTS2DIFFCodecTest, JavaRoundNegativeHalfTiesDouble) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const double data[] = {-2.5, -7.5, 2.5, 7.5};  // mpn=0: scaled == value
    const double expect[] = {-2.0, -7.0, 3.0, 8.0};
    encoder_double_->set_max_point_number(0);
    for (double v : data) {
        ASSERT_EQ(encoder_double_->encode(v, out_stream), common::E_OK);
    }
    ASSERT_EQ(encoder_double_->flush(out_stream), common::E_OK);

    double y = 0.0;
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(decoder_double_->read_double(y, out_stream), common::E_OK);
        EXPECT_DOUBLE_EQ(y, expect[i]) << "value " << i;
    }
}

// writeIndex is bounded by availability, not BLOCK_DEFAULT_SIZE: a
// hand-built block larger than 129 values (Java exposes block-size
// constructors) must decode through both the scalar and batch paths.
TEST_F(TS2DIFFCodecTest, LargeBlockBeyondDefaultSizeDecodes) {
    const int kCount = 200;  // wi = 199 > 128
    const int32_t first = 1000;
    const int32_t delta = 3;
    const int32_t bit_width = 3;

    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    ASSERT_EQ(common::SerializationUtil::write_ui32(kCount - 1, out_stream),
              common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(bit_width, out_stream),
              common::E_OK);
    // delta_min: constant deltas of 3 rebase to 0 (raw - min), so the
    // decoder reconstructs value += stored(0) + delta_min(3) per step.
    ASSERT_EQ(common::SerializationUtil::write_ui32(delta, out_stream),
              common::E_OK);
    ASSERT_EQ(common::SerializationUtil::write_ui32(first, out_stream),
              common::E_OK);
    // 199 * 3 bits, MSB-packed, all-zero bytes == rebased delta 0.  The
    // packed body is ceil(bits / 8) = 75 bytes.
    const int packed_bytes = ((kCount - 1) * bit_width + 7) / 8;  // 75
    for (int i = 0; i < packed_bytes; i++) {
        ASSERT_EQ(common::SerializationUtil::write_ui8(0, out_stream),
                  common::E_OK);
    }

    // Copy the encoded page into a plain buffer; wrap two read streams
    // over it (get_wrapped_buf is only valid for wrap_from streams).
    std::vector<uint8_t> page(out_stream.total_size());
    uint32_t read_len = 0;
    ASSERT_EQ(
        out_stream.read_buf(page.data(), out_stream.total_size(), read_len),
        common::E_OK);
    ASSERT_EQ(read_len, page.size());

    // Scalar path.
    common::ByteStream dec1;
    dec1.wrap_from(reinterpret_cast<const char*>(page.data()),
                   static_cast<int32_t>(page.size()));
    IntTS2DIFFDecoder dec_scalar;
    int32_t v = 0;
    ASSERT_EQ(dec_scalar.read_int32(v, dec1), common::E_OK);
    EXPECT_EQ(v, first);
    for (int i = 1; i < kCount; i++) {
        ASSERT_EQ(dec_scalar.read_int32(v, dec1), common::E_OK);
        EXPECT_EQ(v, first + i * delta) << "row " << i;
    }
    EXPECT_FALSE(dec_scalar.has_remaining(dec1));

    // Batch path.
    common::ByteStream dec2;
    dec2.wrap_from(reinterpret_cast<const char*>(page.data()),
                   static_cast<int32_t>(page.size()));
    IntTS2DIFFDecoder dec_batch;
    std::vector<int32_t> out(kCount);
    int actual = 0;
    ASSERT_EQ(dec_batch.read_batch_int32(out.data(), kCount, actual, dec2),
              common::E_OK);
    ASSERT_EQ(actual, kCount);
    for (int i = 0; i < kCount; i++) {
        EXPECT_EQ(out[i], first + i * delta) << "row " << i;
    }
}

// maxPointNumber has no format-level upper bound: a page written with
// mpn = 1000 (Java Math.pow overflows to +inf for the decoder, values
// then divide by inf) stays readable; the decoder must accept the varint
// instead of rejecting it as corrupt.
TEST_F(FloatDoubleTS2DIFFCodecTest, MaxPointNumberAboveLegacyBoundDecodes) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    encoder_float_->set_max_point_number(1000);
    const float data[] = {1.0f, 2.0f, 3.0f};
    for (float v : data) {
        ASSERT_EQ(encoder_float_->encode(v, out_stream), common::E_OK);
    }
    ASSERT_EQ(encoder_float_->flush(out_stream), common::E_OK);

    // The encoder stores round(v * 10^1000); the double product overflows
    // to +inf for every value, so every entry takes the raw-bits form
    // (Form 3).  The decode restores the exact bits.
    std::vector<uint8_t> page(out_stream.total_size());
    uint32_t read_len = 0;
    ASSERT_EQ(
        out_stream.read_buf(page.data(), out_stream.total_size(), read_len),
        common::E_OK);
    ASSERT_EQ(read_len, page.size());
    common::ByteStream dec;
    dec.wrap_from(reinterpret_cast<const char*>(page.data()),
                  static_cast<int32_t>(page.size()));
    float x = 0.0f;
    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(decoder_float_->read_float(x, dec), common::E_OK);
        EXPECT_EQ(common::float_to_int(x), common::float_to_int(data[i]))
            << "row " << i;
    }
}

}  // namespace storage
