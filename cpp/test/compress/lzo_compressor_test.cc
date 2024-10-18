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
#include "compress/lzo_compressor.h"

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <vector>

namespace {

class LZOTest : public ::testing::Test {
   protected:
    void SetUp() override {}

    void TearDown() override {}

    std::string RandomString(int length) {
        static std::random_device rd;
        static std::mt19937 generator(rd());
        static std::uniform_int_distribution<> dis(33, 127);

        std::string result;
        result.reserve(length);
        for (int i = 0; i < length; ++i) {
            result.push_back(static_cast<char>(dis(generator)));
        }
        return result;
    }
};

TEST_F(LZOTest, TestBytes1) {
    std::string input = RandomString(2000000);
    std::vector<char> uncompressed(input.begin(), input.end());

    storage::LZOCompressor compressor;
    compressor.reset(true);

    char *compressed_buf = nullptr;
    uint32_t compressed_buf_len = 0;
    auto start_time = std::chrono::high_resolution_clock::now();
    compressor.compress(uncompressed.data(), uncompressed.size(),
                        compressed_buf, compressed_buf_len);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto compression_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count();

    std::cout << "Compression time cost: " << compression_duration << " ms"
              << std::endl;
    std::cout << "Ratio: "
              << static_cast<double>(compressed_buf_len) / uncompressed.size()
              << std::endl;

    char *decompressed_buf = nullptr;
    uint32_t decompressed_buf_len = uncompressed.size();
    compressor.reset(false);
    start_time = std::chrono::high_resolution_clock::now();
    compressor.uncompress(compressed_buf, compressed_buf_len, decompressed_buf,
                          decompressed_buf_len);
    end_time = std::chrono::high_resolution_clock::now();
    auto decompression_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count();

    std::cout << "Decompression time cost: " << decompression_duration << " ms"
              << std::endl;
    std::vector<char> decompressed(decompressed_buf,
                                   decompressed_buf + decompressed_buf_len);
    EXPECT_EQ(uncompressed, decompressed);

    compressor.after_compress(compressed_buf);
    compressor.after_uncompress(decompressed_buf);
}

TEST_F(LZOTest, TestBytes2) {
    storage::LZOCompressor compressor;

    int n = 500000;
    std::string input = RandomString(n);
    std::vector<char> uncompressed(input.begin(), input.end());

    char *compressed_buf = nullptr;
    uint32_t compressed_buf_len = 0;
    uint32_t compressed_buf_len_new = 0;
    compressor.reset(true);
    compressor.compress(uncompressed.data(), uncompressed.size(),
                        compressed_buf, compressed_buf_len);
    compressor.after_compress(compressed_buf);

    compressor.compress(uncompressed.data(), uncompressed.size(),
                        compressed_buf, compressed_buf_len_new);
    EXPECT_EQ(compressed_buf_len_new, compressed_buf_len);

    char *decompressed_buf = nullptr;
    uint32_t decompressed_buf_len = uncompressed.size();
    compressor.reset(false);
    compressor.uncompress(compressed_buf, compressed_buf_len, decompressed_buf,
                          decompressed_buf_len);

    std::vector<char> decompressed(decompressed_buf,
                                   decompressed_buf + decompressed_buf_len);
    EXPECT_EQ(uncompressed, decompressed);

    compressor.after_compress(compressed_buf);
    compressor.after_uncompress(decompressed_buf);
}
}  // namespace
