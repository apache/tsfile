/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#include <cstdint>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "common/allocator/my_string.h"
#include "common/allocator/page_arena.h"
#include "encoding/decoder.h"

namespace storage {
namespace {

using namespace common;

// Implements only the abstract interface; the batch/skip/exact helpers under
// test come from the Decoder base class.
class FakeDecoder : public Decoder {
   public:
    explicit FakeDecoder(std::vector<int64_t> values)
        : values_(std::move(values)) {}

    void reset() override { index_ = 0; }

    bool has_remaining(const ByteStream&) override {
        return index_ < values_.size();
    }

    int read_boolean(bool& ret_value, ByteStream& in) override {
        int64_t value = 0;
        const int ret = read_int64(value, in);
        if (ret == E_OK) {
            ret_value = value != 0;
        }
        return ret;
    }

    int read_int32(int32_t& ret_value, ByteStream& in) override {
        int64_t value = 0;
        const int ret = read_int64(value, in);
        if (ret == E_OK) {
            ret_value = static_cast<int32_t>(value);
        }
        return ret;
    }

    int read_int64(int64_t& ret_value, ByteStream& in) override {
        if (index_ >= values_.size()) {
            return E_NO_MORE_DATA;
        }
        ret_value = values_[index_++];
        return E_OK;
    }

    int read_float(float& ret_value, ByteStream& in) override {
        int64_t value = 0;
        const int ret = read_int64(value, in);
        if (ret == E_OK) {
            ret_value = static_cast<float>(value);
        }
        return ret;
    }

    int read_double(double& ret_value, ByteStream& in) override {
        int64_t value = 0;
        const int ret = read_int64(value, in);
        if (ret == E_OK) {
            ret_value = static_cast<double>(value);
        }
        return ret;
    }

    int read_String(String&, PageArena&, ByteStream&) override {
        return E_NOT_SUPPORT;
    }

   protected:
    std::vector<int64_t> values_;
    size_t index_ = 0;
};

class FailingDecoder : public FakeDecoder {
   public:
    using FakeDecoder::FakeDecoder;
    int read_int64(int64_t&, ByteStream&) override { return E_DECODE_ERR; }
};

class CorruptBatchDecoder : public FakeDecoder {
   public:
    using FakeDecoder::FakeDecoder;
    int read_batch_int64(int64_t*, int, int& actual, ByteStream&) override {
        actual = -1;
        return E_OK;
    }
};

TEST(DecoderContractTest, BatchDefaultsRespectCapacityAndExhaustion) {
    ByteStream stream;
    FakeDecoder decoder({1, 2, 3});
    int64_t out[8] = {0};
    int actual = -1;

    ASSERT_EQ(decoder.read_batch_int64(out, 2, actual, stream), E_OK);
    ASSERT_EQ(actual, 2);
    EXPECT_EQ(out[0], 1);
    EXPECT_EQ(out[1], 2);

    ASSERT_EQ(decoder.read_batch_int64(out, 8, actual, stream), E_OK);
    ASSERT_EQ(actual, 1);
    EXPECT_EQ(out[0], 3);

    ASSERT_EQ(decoder.read_batch_int64(out, 8, actual, stream), E_OK);
    EXPECT_EQ(actual, 0);

    FakeDecoder int32_decoder({7, 8});
    int32_t out32[4] = {0};
    ASSERT_EQ(int32_decoder.read_batch_int32(out32, 4, actual, stream), E_OK);
    ASSERT_EQ(actual, 2);
    EXPECT_EQ(out32[0], 7);
    EXPECT_EQ(out32[1], 8);

    FakeDecoder float_decoder({3, 4});
    float outf[4] = {0.0f};
    ASSERT_EQ(float_decoder.read_batch_float(outf, 1, actual, stream), E_OK);
    ASSERT_EQ(actual, 1);
    EXPECT_FLOAT_EQ(outf[0], 3.0f);
    ASSERT_EQ(float_decoder.read_batch_float(outf, 4, actual, stream), E_OK);
    ASSERT_EQ(actual, 1);
    EXPECT_FLOAT_EQ(outf[0], 4.0f);

    FakeDecoder double_decoder({5, 6});
    double outd[4] = {0.0};
    ASSERT_EQ(double_decoder.read_batch_double(outd, 4, actual, stream), E_OK);
    ASSERT_EQ(actual, 2);
    EXPECT_DOUBLE_EQ(outd[0], 5.0);
    EXPECT_DOUBLE_EQ(outd[1], 6.0);
}

TEST(DecoderContractTest, BatchDefaultsPropagateReadErrors) {
    ByteStream stream;
    FailingDecoder decoder({1, 2});
    int64_t out[2] = {0};
    int actual = -1;
    EXPECT_EQ(decoder.read_batch_int64(out, 2, actual, stream), E_DECODE_ERR);
    EXPECT_EQ(actual, 0);

    int32_t out32[2] = {0};
    EXPECT_EQ(decoder.read_batch_int32(out32, 2, actual, stream), E_DECODE_ERR);
    EXPECT_EQ(actual, 0);

    float outf[2] = {0.0f};
    EXPECT_EQ(decoder.read_batch_float(outf, 2, actual, stream), E_DECODE_ERR);
    EXPECT_EQ(actual, 0);

    double outd[2] = {0.0};
    EXPECT_EQ(decoder.read_batch_double(outd, 2, actual, stream), E_DECODE_ERR);
    EXPECT_EQ(actual, 0);
}

TEST(DecoderContractTest, SkipDefaultsClampAtEnd) {
    ByteStream stream;
    FakeDecoder decoder({1, 2, 3, 4, 5});
    int skipped = -1;

    ASSERT_EQ(decoder.skip_int64(2, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 2);
    ASSERT_EQ(decoder.skip_int64(100, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 3);
    ASSERT_EQ(decoder.skip_int64(1, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 0);

    FakeDecoder int32_decoder({1, 2});
    ASSERT_EQ(int32_decoder.skip_int32(1, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 1);

    FakeDecoder float_decoder({1, 2});
    ASSERT_EQ(float_decoder.skip_float(50, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 2);

    FakeDecoder double_decoder({1, 2});
    ASSERT_EQ(double_decoder.skip_double(0, skipped, stream), E_OK);
    EXPECT_EQ(skipped, 0);
}

TEST(DecoderContractTest, SkipDefaultsPropagateReadErrors) {
    ByteStream stream;
    FailingDecoder decoder({1, 2});
    int skipped = -1;
    EXPECT_EQ(decoder.skip_int64(1, skipped, stream), E_DECODE_ERR);
    EXPECT_EQ(skipped, 0);
    EXPECT_EQ(decoder.skip_int32(1, skipped, stream), E_DECODE_ERR);
    EXPECT_EQ(skipped, 0);
    EXPECT_EQ(decoder.skip_float(1, skipped, stream), E_DECODE_ERR);
    EXPECT_EQ(skipped, 0);
    EXPECT_EQ(decoder.skip_double(1, skipped, stream), E_DECODE_ERR);
    EXPECT_EQ(skipped, 0);
}

TEST(DecoderContractTest, ExactReadHelpers) {
    ByteStream stream;
    FakeDecoder decoder({1, 2, 3, 4, 5});
    int64_t out[5] = {0};

    ASSERT_EQ(decoder.read_exact_int64(out, 3, stream), E_OK);
    EXPECT_EQ(out[0], 1);
    EXPECT_EQ(out[1], 2);
    EXPECT_EQ(out[2], 3);

    ASSERT_EQ(decoder.read_exact_int64(out, 2, stream), E_OK);
    EXPECT_EQ(out[0], 4);
    EXPECT_EQ(out[1], 5);

    EXPECT_EQ(decoder.read_exact_int64(out, 1, stream), E_NO_MORE_DATA);
    EXPECT_EQ(decoder.read_exact_int64(out, -1, stream), E_INVALID_ARG);
    EXPECT_EQ(decoder.read_exact_int64(nullptr, 1, stream), E_INVALID_ARG);
    EXPECT_EQ(decoder.read_exact_int64(out, 0, stream), E_OK);

    FakeDecoder int32_decoder({7, 8});
    int32_t out32[2] = {0};
    ASSERT_EQ(int32_decoder.read_exact_int32(out32, 2, stream), E_OK);
    EXPECT_EQ(out32[0], 7);
    EXPECT_EQ(out32[1], 8);

    FakeDecoder float_decoder({9, 10});
    float outf[2] = {0.0f};
    ASSERT_EQ(float_decoder.read_exact_float(outf, 2, stream), E_OK);
    EXPECT_FLOAT_EQ(outf[0], 9.0f);
    EXPECT_FLOAT_EQ(outf[1], 10.0f);

    FakeDecoder double_decoder({11, 12});
    double outd[2] = {0.0};
    ASSERT_EQ(double_decoder.read_exact_double(outd, 2, stream), E_OK);
    EXPECT_DOUBLE_EQ(outd[0], 11.0);
    EXPECT_DOUBLE_EQ(outd[1], 12.0);
}

TEST(DecoderContractTest, ExactSkipHelpers) {
    ByteStream stream;
    FakeDecoder decoder({1, 2, 3, 4, 5});

    ASSERT_EQ(decoder.skip_exact_int64(3, stream), E_OK);
    EXPECT_EQ(decoder.skip_exact_int64(5, stream), E_NO_MORE_DATA);
    EXPECT_EQ(decoder.skip_exact_int64(-1, stream), E_INVALID_ARG);

    FakeDecoder int32_decoder({1});
    ASSERT_EQ(int32_decoder.skip_exact_int32(1, stream), E_OK);

    FakeDecoder float_decoder({1});
    ASSERT_EQ(float_decoder.skip_exact_float(1, stream), E_OK);

    FakeDecoder double_decoder({1});
    ASSERT_EQ(double_decoder.skip_exact_double(1, stream), E_OK);
}

TEST(DecoderContractTest, ExactReadRejectsCorruptBatchCounts) {
    ByteStream stream;
    CorruptBatchDecoder decoder({1, 2});
    int64_t out[2] = {0};
    EXPECT_EQ(decoder.read_exact_int64(out, 2, stream), E_TSFILE_CORRUPTED);
}

TEST(DecoderContractTest, DefaultBlockHooksAndOwnership) {
    ByteStream stream;
    FakeDecoder decoder({1});
    int64_t block_min = 0;
    int64_t block_max = 0;
    int block_count = 0;
    EXPECT_FALSE(decoder.peek_next_block_range_int64(stream, block_min,
                                                     block_max, block_count));
    int skipped = -1;
    EXPECT_EQ(decoder.skip_peeked_block_int64(stream, skipped), E_NOT_SUPPORT);
    EXPECT_FALSE(decoder.owns_resources());
    decoder.destroy();
    decoder.reset();
    int64_t value = 0;
    EXPECT_EQ(decoder.read_int64(value, stream), E_OK);
    EXPECT_EQ(value, 1);
}

}  // namespace
}  // namespace storage
