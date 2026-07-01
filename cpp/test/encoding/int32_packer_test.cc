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
#include "encoding/int32_packer.h"

#include <gtest/gtest.h>

#include <bitset>
#include <random>

namespace storage {

TEST(IntPackerTest, SequentialValues) {
    for (int width = 3; width < 32; ++width) {
        int32_t arr[8];
        for (int i = 0; i < 8; ++i) arr[i] = i;
        Int32Packer packer(width);
        const int bufSize = NUM_OF_INTS * width / 8;
        std::vector<unsigned char> buf(bufSize, 0);
        packer.pack_8values(arr, 0, buf.data());
        int32_t res[8] = {0};
        packer.unpack_8values(buf.data(), 0, res);
        for (int i = 0; i < 8; ++i) {
            EXPECT_EQ(res[i], arr[i]) << "Width=" << width << " Index=" << i;
        }
    }
}

TEST(IntPackerStressTest, PackUnpackRandomPositiveValues) {
    const int width = 31;
    const int count = 100000;
    const int total_values = count * 8;

    Int32Packer packer(width);
    std::vector<int32_t> pre_values;
    std::vector<unsigned char> buffer;
    pre_values.reserve(total_values);
    buffer.resize(count * width);
    int idx = 0;
    std::srand(12345);  // Optional: deterministic seed
    for (int i = 0; i < count; ++i) {
        int32_t vs[8];
        for (int j = 0; j < 8; ++j) {
            vs[j] = std::rand() &
                    0x7FFFFFFF;  // ensure non-negative (Java `nextInt`)
            pre_values.push_back(vs[j]);
        }

        unsigned char temp_buf[32] = {0};
        packer.pack_8values(vs, 0, temp_buf);
        std::memcpy(buffer.data() + idx, temp_buf, width);
        idx += width;
    }

    std::vector<int32_t> res(total_values);
    packer.unpack_all_values(buffer.data(), static_cast<int>(buffer.size()),
                             res.data());
    std::string diff_msg;
    for (int i = 0; i < total_values; ++i) {
        if (res[i] != pre_values[i]) {
            diff_msg += "\nMismatch at index " + std::to_string(i) +
                        ": expected=" + std::to_string(pre_values[i]) +
                        ", actual=" + std::to_string(res[i]);
        }
    }
    ASSERT_TRUE(diff_msg.empty()) << diff_msg;
}

// Test all zeros for various widths
TEST(Int32PackerTest, AllZeroValues) {
    for (int width = 1; width <= 31; ++width) {
        int32_t arr[NUM_OF_INTS] = {0};
        Int32Packer packer(width);
        const int bufSize = NUM_OF_INTS * width / 8;
        std::vector<unsigned char> buf(bufSize, 0);
        packer.pack_8values(arr, 0, buf.data());
        int32_t res[NUM_OF_INTS] = {
            1};  // initialize non-zero to catch failures
        packer.unpack_8values(buf.data(), 0, res);
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            EXPECT_EQ(res[i], 0) << "Width=" << width << " Index=" << i;
        }
    }
}

// Test boundary width = 1 with alternating bits
TEST(Int32PackerTest, BoundaryWidthOneAlternating) {
    const int width = 1;
    int32_t arr[NUM_OF_INTS] = {0, 1, 0, 1, 0, 1, 0, 1};
    Int32Packer packer(width);
    const int bufSize = NUM_OF_INTS * width / 8;
    std::vector<unsigned char> buf(bufSize, 0);
    packer.pack_8values(arr, 0, buf.data());
    int32_t res[NUM_OF_INTS] = {0};
    packer.unpack_8values(buf.data(), 0, res);
    for (int i = 0; i < NUM_OF_INTS; ++i) {
        EXPECT_EQ(res[i], arr[i]) << "Index=" << i;
    }
}

// Test maximum width (32 bits)
TEST(Int32PackerTest, MaxWidth32Random) {
    const int width = 32;
    const int times = 100000;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int32_t> dist(INT32_MIN, INT32_MAX);
    for (int t = 0; t < times; ++t) {
        int32_t arr[NUM_OF_INTS];
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            arr[i] = dist(gen);
        }
        Int32Packer packer(width);
        const int bufSize = NUM_OF_INTS * width / 8;
        std::vector<unsigned char> buf(bufSize, 0);
        packer.pack_8values(arr, 0, buf.data());
        int32_t res[NUM_OF_INTS] = {0};
        packer.unpack_8values(buf.data(), 0, res);
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            EXPECT_EQ(res[i], arr[i]) << "Index=" << i;
        }
    }
}

TEST(Int32PackerTest, AllNegative32Random) {
    const int width = 32;
    const int times = 100000;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int32_t> dist(INT32_MIN, -1);
    for (int t = 0; t < times; ++t) {
        int32_t arr[NUM_OF_INTS];
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            arr[i] = dist(gen);
        }
        Int32Packer packer(width);
        const int bufSize = NUM_OF_INTS * width / 8;
        std::vector<unsigned char> buf(bufSize, 0);
        packer.pack_8values(arr, 0, buf.data());
        int32_t res[NUM_OF_INTS] = {0};
        packer.unpack_8values(buf.data(), 0, res);
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            EXPECT_EQ(res[i], arr[i]) << "Index=" << i;
        }
    }
}

// Test unpack_all_values for multiple blocks
TEST(Int32PackerTest, UnpackAllValuesMultipleBlocks) {
    const int width = 16;
    // pack 10 blocks sequentially
    const int blocks = 10;
    Int32Packer packer(width);
    std::vector<int32_t> orig(blocks * NUM_OF_INTS);
    std::vector<unsigned char> buf(blocks * width);

    // Fill orig with pattern: block * 16 + index
    // Example: block 0 = [0,1,...,7], block 1 = [16,17,...,23], etc.
    for (int b = 0; b < blocks; ++b) {
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            orig[b * NUM_OF_INTS + i] = (b << 4) | i;
        }
        packer.pack_8values(
            orig.data() + b * NUM_OF_INTS, 0,
            buf.data() + b * width);  // pack each block into buf
    }

    std::vector<int32_t> res(blocks * NUM_OF_INTS, 0);
    // Unpack all blocks at once
    packer.unpack_all_values(buf.data(), static_cast<int>(buf.size()),
                             res.data());

    // Verify each unpacked value matches the original sequence
    for (size_t i = 0; i < orig.size(); ++i) {
        EXPECT_EQ(res[i], orig[i]) << "Index=" << i;
    }
}

}  // namespace storage