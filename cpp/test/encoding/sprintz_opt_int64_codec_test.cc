/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#include <random>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/sprintz_opt_int64_codec.h"

using storage::SprintzOptInt64Decoder;
using storage::SprintzOptInt64Encoder;

namespace {

static std::vector<int64_t> gen_data(int n) {
    std::mt19937_64 rng(42);
    std::normal_distribution<double> dist(0.0, 1000.0);
    std::vector<int64_t> out;
    out.reserve(n);
    int64_t base = 1000000;
    for (int i = 0; i < n; i++) {
        out.push_back(base + (int64_t)dist(rng));
    }
    return out;
}

TEST(SprintzOptInt64CodecTest, RoundTripFixedPackSize8) {
    // On Apple M1 (arm64), we expect NEON to be available in release builds.
    // This does NOT guarantee speedup; it just confirms the SIMD path is compiled in.
    ASSERT_TRUE(SprintzOptInt64Encoder::kNeonEnabled);
    auto data = gen_data(4096);
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    SprintzOptInt64Encoder enc(/*use_optimal=*/false, /*fixed_pack_size=*/8, "delta", 256);
    for (auto v : data) {
        ASSERT_EQ(enc.encode(v, stream), common::E_OK);
    }
    ASSERT_EQ(enc.flush(stream), common::E_OK);
    ASSERT_GT(stream.total_size(), 0u);

    SprintzOptInt64Decoder dec("delta");
    std::vector<int64_t> decoded;
    decoded.reserve(data.size());
    char *buf = common::get_bytes_from_bytestream(stream);
    ASSERT_NE(buf, nullptr);
    common::ByteStream in(1024, common::MOD_DECODER_OBJ);
    ASSERT_EQ(in.write_buf((const uint8_t *)buf, stream.total_size()), common::E_OK);
    ASSERT_GT(in.remaining_size(), 0u);
    {
        char hdr[2] = {0, 0};
        uint32_t rl = 0;
        int ret = in.read_buf(hdr, 2, rl);
        ASSERT_EQ(ret, common::E_OK);
        ASSERT_EQ(rl, 2u);
        // recreate 'in' from scratch for actual decode
        in.reset();
        ASSERT_EQ(in.write_buf((const uint8_t *)buf, stream.total_size()), common::E_OK);
    }
    for (size_t i = 0; i < data.size(); i++) {
        int64_t v;
        ASSERT_EQ(dec.read_int64(v, in), common::E_OK);
        decoded.push_back(v);
    }
    free(buf);
    ASSERT_EQ(decoded, data);
}

TEST(SprintzOptInt64CodecTest, RoundTripOptimalPackSize) {
    auto data = gen_data(4096);
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    SprintzOptInt64Encoder enc(/*use_optimal=*/true, /*fixed_pack_size=*/8, "delta", 256);
    for (auto v : data) {
        ASSERT_EQ(enc.encode(v, stream), common::E_OK);
    }
    ASSERT_EQ(enc.flush(stream), common::E_OK);
    ASSERT_GT(stream.total_size(), 0u);

    SprintzOptInt64Decoder dec("delta");
    std::vector<int64_t> decoded;
    decoded.reserve(data.size());
    char *buf = common::get_bytes_from_bytestream(stream);
    ASSERT_NE(buf, nullptr);
    common::ByteStream in(1024, common::MOD_DECODER_OBJ);
    ASSERT_EQ(in.write_buf((const uint8_t *)buf, stream.total_size()), common::E_OK);
    ASSERT_GT(in.remaining_size(), 0u);
    {
        char hdr[2] = {0, 0};
        uint32_t rl = 0;
        int ret = in.read_buf(hdr, 2, rl);
        ASSERT_EQ(ret, common::E_OK);
        ASSERT_EQ(rl, 2u);
        in.reset();
        ASSERT_EQ(in.write_buf((const uint8_t *)buf, stream.total_size()), common::E_OK);
    }
    for (size_t i = 0; i < data.size(); i++) {
        int64_t v;
        ASSERT_EQ(dec.read_int64(v, in), common::E_OK);
        decoded.push_back(v);
    }
    free(buf);
    ASSERT_EQ(decoded, data);
}

}  // namespace

