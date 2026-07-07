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

#include "compress/lzma2_compressor.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "compress/compressor_factory.h"

namespace {

std::vector<char> make_payload() {
    std::vector<char> payload;
    payload.reserve(128 * 1024);
    for (int i = 0; i < 4096; ++i) {
        std::string row = "timestamp=";
        row += std::to_string(1700000000000LL + i);
        row += ",temperature=";
        row += std::to_string(20 + (i % 8));
        row += ",status=ok\n";
        payload.insert(payload.end(), row.begin(), row.end());
    }
    return payload;
}

}  // namespace

namespace storage {

TEST(LZMA2CompressorTest, RoundTrip) {
    std::vector<char> uncompressed = make_payload();
    LZMA2Compressor compressor;

    char* compressed_buf = nullptr;
    uint32_t compressed_buf_len = 0;
    ASSERT_EQ(compressor.compress(uncompressed.data(), uncompressed.size(),
                                  compressed_buf, compressed_buf_len),
              common::E_OK);
    ASSERT_NE(compressed_buf, nullptr);

    char* decompressed_buf = nullptr;
    uint32_t decompressed_buf_len = 0;
    ASSERT_EQ(compressor.uncompress(compressed_buf, compressed_buf_len,
                                    decompressed_buf, decompressed_buf_len),
              common::E_OK);
    ASSERT_NE(decompressed_buf, nullptr);
    EXPECT_EQ(decompressed_buf_len, uncompressed.size());
    EXPECT_EQ(
        memcmp(decompressed_buf, uncompressed.data(), uncompressed.size()), 0);

    compressor.after_compress(compressed_buf);
    compressor.after_uncompress(decompressed_buf);
}

TEST(LZMA2CompressorTest, FactoryAllocatesLZMA2) {
    Compressor* compressor =
        CompressorFactory::alloc_compressor(common::CompressionType::LZMA2);
    ASSERT_NE(compressor, nullptr);
    CompressorFactory::free(compressor);
}

}  // namespace storage
