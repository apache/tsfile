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

#include "encoding/gorilla_decoder.h"
#include "encoding/gorilla_encoder.h"

namespace storage {

class GorillaCodecTest : public ::testing::Test {};

TEST_F(GorillaCodecTest, BasicEncoding) {
    storage::IntGorillaEncoder int_encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int32_t data[] = {100, 102, 105, 107, 110, 115, 120, 1000000, 1000005};
    for (int32_t value : data) {
        EXPECT_EQ(int_encoder.encode(value, stream), common::E_OK);
    }
    int_encoder.flush(stream);

    ASSERT_EQ(stream.total_size(), 24);

    uint32_t want_len = 24, read_len;
    uint8_t real_buf[24] = {};
    stream.read_buf(real_buf, want_len, read_len);
    EXPECT_EQ(want_len, read_len);
    // Generated using Java Edition
    uint8_t expected_buf[] = {0,   0,   0,  100, 252, 15,  193, 252,
                              82,  251, 39, 101, 236, 135, 161, 31,
                              232, 174, 15, 192, 7,   161, 34,  128};
    for (int i = 0; i < 24; i++) {
        EXPECT_EQ(real_buf[i], expected_buf[i]);
    }
}

TEST_F(GorillaCodecTest, Int32EncodingDecoding) {
    storage::IntGorillaEncoder int_encoder;
    storage::IntGorillaDecoder int_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int32_t data[] = {100, 102, 105, 107, 110, 115, 120, 1000000, 1000005};
    for (int32_t value : data) {
        EXPECT_EQ(int_encoder.encode(value, stream), common::E_OK);
    }
    int_encoder.flush(stream);

    for (int i = 0; i < (int)(sizeof(data) / sizeof(int32_t)); i++) {
        EXPECT_EQ(data[i], int_decoder.decode(stream));
    }
}

TEST_F(GorillaCodecTest, Int32EncodingDecodingLargeQuantities) {
    storage::IntGorillaEncoder int_encoder;
    storage::IntGorillaDecoder int_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (int32_t value = 0; value < 10000; value++) {
        EXPECT_EQ(int_encoder.encode(value, stream), common::E_OK);
    }
    int_encoder.flush(stream);

    for (int32_t value = 0; value < 10000; value++) {
        EXPECT_EQ(value, int_decoder.decode(stream));
    }
}

TEST_F(GorillaCodecTest, Int64EncodingDecoding) {
    storage::LongGorillaEncoder long_encoder;
    storage::LongGorillaDecoder long_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int64_t data[] = {100, 102, 105, 107, 110, 115, 120, 1000000, 1000005};
    for (int64_t value : data) {
        EXPECT_EQ(long_encoder.encode(value, stream), common::E_OK);
    }
    long_encoder.flush(stream);

    for (int i = 0; i < (int)(sizeof(data) / sizeof(int64_t)); i++) {
        EXPECT_EQ(data[i], long_decoder.decode(stream));
    }
}

TEST_F(GorillaCodecTest, Int64EncodingDecodingLargeQuantities) {
    storage::LongGorillaEncoder long_encoder;
    storage::LongGorillaDecoder long_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (int64_t value = 0; value < 10000; value++) {
        EXPECT_EQ(long_encoder.encode(value, stream), common::E_OK);
    }
    long_encoder.flush(stream);

    for (int64_t value = 0; value < 10000; value++) {
        EXPECT_EQ(value, long_decoder.decode(stream));
    }
}

}  // namespace storage
