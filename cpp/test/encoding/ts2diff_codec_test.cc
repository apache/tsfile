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

}  // namespace storage
