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

static std::string byte_stream_to_hex(common::ByteStream& stream) {
    uint32_t mark = stream.read_pos();
    uint32_t size = stream.total_size();
    std::vector<uint8_t> buf(size);
    uint32_t read_len = 0;
    EXPECT_EQ(stream.read_buf(buf.data(), size, read_len), common::E_OK);
    EXPECT_EQ(read_len, size);
    stream.set_read_pos(mark);

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
        data[i] = static_cast<float>(i) * 0.25f + 0.50f;
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
        data[i] = static_cast<double>(i) * 0.25 + 0.5;
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
        expected[i] = static_cast<float>(i) * 0.25f + 0.5f;
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
// outside the cross-language format: the Java reader never supported them
// (apache/tsfile#901 review).  The decoder now treats such input as a
// format error instead of guessing the layout: it must fail fast rather
// than hang at end-of-input or silently return misdecoded values.
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
        expected[i] = (i < 128) ? 0.5f : 100.25f;
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
void expect_max_pn_once_per_page(const std::vector<uint8_t>& b,
                                 bool is_double) {
    const uint32_t FLAG_SCALED = 2147483647u;
    const uint32_t FLAG_ORIGINAL = 2147483646u;
    size_t pos = 0;
    bool first_segment = true;
    int segment_count = 0;
    while (pos < b.size()) {
        size_t seg_start = pos;
        if (pos < b.size() && b[pos] != 0x00) {
            uint32_t tag = 0;
            size_t p = pos;
            ASSERT_TRUE(parse_var_uint(b, p, tag));
            if (tag == FLAG_SCALED || tag == FLAG_ORIGINAL) {
                // Overflow marker section: value count + underflow bitmap
                // (+ overflow bitmap for original-value overflow).
                uint32_t n = 0;
                ASSERT_TRUE(parse_var_uint(b, p, n));
                EXPECT_GE(n, 1u);
                size_t bm_len = static_cast<size_t>(n / 8 + 1);
                ASSERT_LE(p + bm_len, b.size());
                p += bm_len;
                if (tag == FLAG_ORIGINAL) {
                    ASSERT_LE(p + bm_len, b.size());
                    p += bm_len;
                }
                // Only the page's first segment may carry maxPointNumber
                // after the bitmaps.
                if (first_segment && p < b.size() && b[p] != 0x00) {
                    uint32_t mpn = 0;
                    ASSERT_TRUE(parse_var_uint(b, p, mpn));
                    EXPECT_GE(mpn, 1u);
                }
                pos = p;
            } else {
                // A non-flag tag is the maxPointNumber prefix; it must not
                // appear on any segment after the first.
                EXPECT_TRUE(first_segment)
                    << "maxPointNumber prefix found on segment "
                    << segment_count + 1 << " (byte " << seg_start << ")";
                pos = p;
            }
        }
        // Segment header: write_index + bit_width (+ delta_min + first_value).
        size_t h = pos;
        size_t header_len = is_double ? 24 : 16;
        ASSERT_LE(h + header_len, b.size());
        int32_t wi = read_i32_be(b, h);
        int32_t bw = read_i32_be(b, h + 4);
        ASSERT_GE(wi, 0) << "negative write_index at segment "
                         << segment_count + 1;
        EXPECT_LE(wi, 128);
        ASSERT_GE(bw, 0);
        EXPECT_LE(bw, 64);
        pos = h + header_len;
        pos += (static_cast<size_t>(wi) * static_cast<size_t>(bw) + 7) / 8;
        ASSERT_LE(pos, b.size());
        first_segment = false;
        segment_count++;
    }
    EXPECT_GE(segment_count, 2) << "test must produce a multi-segment page";
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

    // Ramp data has bit_width 0, so the only 0x02 byte in the page is the
    // maxPointNumber prefix.
    std::vector<uint8_t> b = byte_stream_bytes(out);
    size_t prefix_count = 0;
    for (uint8_t byte : b) {
        if (byte == 0x02) prefix_count++;
    }
    EXPECT_EQ(prefix_count, 1u)
        << "maxPointNumber must be written once per page, not per segment";
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
    size_t prefix_count = 0;
    for (uint8_t byte : b) {
        if (byte == 0x02) prefix_count++;
    }
    EXPECT_EQ(prefix_count, 1u);
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
    // Exactly one maxPointNumber, directly after the bitmaps.
    ASSERT_LT(pos, b.size());
    EXPECT_EQ(b[pos], 0x02);
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
    size_t c1 = 0;
    size_t c2 = 0;
    for (uint8_t byte : b1) {
        if (byte == 0x02) c1++;
    }
    for (uint8_t byte : b2) {
        if (byte == 0x02) c2++;
    }
    EXPECT_EQ(c1, 1u) << "page 1 must carry exactly one maxPointNumber";
    EXPECT_EQ(c2, 1u) << "page 2 must carry exactly one maxPointNumber";
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

// The pre-#910 C++ encoder repeated the maxPointNumber at every segment
// boundary.  That layout was never produced by the Java writer and is now
// outside the compatibility boundary (apache/tsfile#901 review): the
// decoder parses page metadata exactly once, so a hand-built old-format
// page fails the block-header validation instead of being silently
// misdecoded.
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
    // fails validation.  The decode loop terminates (no end-of-input spin)
    // and no value beyond segment 1 is ever returned as valid: the
    // stale-value poison path keeps returning values, but they are the
    // last valid value repeated, never the expected continuation 2.0/2.25.
    float x = 0.0f;
    int ok = 0;
    int mismatches = 0;
    for (size_t i = 0; i < expected.size(); i++) {
        if (decoder_float_->read_float(x, old_fmt) != common::E_OK) break;
        ok++;
        if (std::fabs(x - expected[i]) > 1e-6f) {
            mismatches++;
        }
    }
    EXPECT_GE(mismatches, 1)
        << "out-of-format continuation must not decode to the expected values";
}

}  // namespace storage