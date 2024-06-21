#include <gtest/gtest.h>
#include "encoding/ts2diff_encoder.h"
#include "encoding/ts2diff_decoder.h"

#include <bitset>

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
        delete encoder_int_;
        delete encoder_long_;
        delete decoder_int_;
        delete decoder_long_;
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

}

