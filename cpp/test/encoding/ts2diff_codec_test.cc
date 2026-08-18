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
// read_batch_float / read_batch_double.  The per-block prefix heuristic
// used to misclassify a valid raw header as a maxPointNumber prefix,
// desyncing the stream and spinning at end-of-input (PR #901 review).
TEST_F(FloatDoubleTS2DIFFCodecTest, ReadBatchFloatLegacyRawSegments) {
    common::ByteStream out_stream(1024, common::MOD_TS2DIFF_OBJ, false);
    const int row_num = 129;
    std::vector<float> expected(row_num);
    // 128 equal values followed by a change: the first legacy block has
    // bit_width = 0 and delta_min = 0, exactly the pattern the old
    // heuristic misclassified.  The trailing 1-value block (write_index = 0)
    // exercised the same heuristic again.
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
    int decoded = 0;
    // Small batches exercise the layout decision plus repeated block
    // transitions.
    while (decoded < row_num) {
        int actual = 0;
        ASSERT_EQ(decoder_float_->read_batch_float(
                      actual_values.data() + decoded, 16, actual, out_stream),
                  common::E_OK);
        ASSERT_GT(actual, 0);
        decoded += actual;
    }
    for (int i = 0; i < row_num; ++i) {
        EXPECT_EQ(actual_values[i], expected[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(out_stream));
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
    int decoded = 0;
    while (decoded < row_num) {
        int actual = 0;
        ASSERT_EQ(decoder_double_->read_batch_double(
                      actual_values.data() + decoded, 16, actual, out_stream),
                  common::E_OK);
        ASSERT_GT(actual, 0);
        decoded += actual;
    }
    for (int i = 0; i < row_num; ++i) {
        EXPECT_EQ(actual_values[i], expected[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_double_->has_remaining(out_stream));
}

// The layout decision must also cover the scalar path: legacy raw pages
// read one value at a time via read_float / read_double keep the bit-cast
// semantics across the 128-value block boundary.
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
    for (int i = 0; i < row_num; ++i) {
        ASSERT_EQ(decoder_float_->read_float(v, out_stream), common::E_OK);
        EXPECT_EQ(v, expected[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(out_stream));
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
    for (int i = 0; i < row_num; ++i) {
        ASSERT_EQ(decoder_double_->read_double(v, out_stream), common::E_OK);
        EXPECT_EQ(v, expected[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_double_->has_remaining(out_stream));
}

// Mixed reads must not re-trigger the layout scan mid-page: batch first,
// then scalar reads must continue on the same layout decision.
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
    ASSERT_EQ(
        decoder_float_->read_batch_float(batch_out, 40, actual, out_stream),
        common::E_OK);
    ASSERT_EQ(actual, 40);
    for (int i = 0; i < 40; ++i) {
        EXPECT_EQ(batch_out[i], expected[i]) << "row " << i;
    }
    float v = 0.f;
    for (int i = 40; i < row_num; ++i) {
        ASSERT_EQ(decoder_float_->read_float(v, out_stream), common::E_OK);
        EXPECT_EQ(v, expected[i]) << "row " << i;
    }
    EXPECT_FALSE(decoder_float_->has_remaining(out_stream));
}

}  // namespace storage
