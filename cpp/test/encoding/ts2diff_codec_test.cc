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

TEST_F(TS2DIFFCodecTest, TestIntEncoding) {
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

TEST_F(TS2DIFFCodecTest, TestLongEncoding) {
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

}  // namespace storage
