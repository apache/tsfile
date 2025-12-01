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
#include "encoding/int64_packer.h"

#include <gtest/gtest.h>

#include <bitset>
#include <cmath>
#include <random>

namespace storage {

TEST(Int64PackerTest, SequentialValues) {
    for (int width = 4; width < 63; ++width) {
        int64_t arr[8];
        for (int i = 0; i < 8; ++i) arr[i] = i;
        Int64Packer packer(width);
        const int bufSize = NUM_OF_INTS * width / 8;
        std::vector<unsigned char> buf(bufSize, 0);
        packer.pack_8values(arr, 0, buf.data());
        int64_t res[8] = {0};
        packer.unpack_8values(buf.data(), 0, res);
        for (int i = 0; i < 8; ++i) {
            EXPECT_EQ(res[i], arr[i]) << "Width=" << width << " Index=" << i;
        }
    }
}

TEST(Int64PackerTest, PackUnpackSingleBatchRandomPositiveLongs) {
    const int byte_count = 63;  // total bytes for 8 packed uint64_t values
    const int count = 1;
    const int total_values = count * 8;

    Int64Packer packer(byte_count);
    std::vector<uint64_t> pre_values;
    std::vector<unsigned char> buffer(count * byte_count);
    pre_values.reserve(total_values);

    int idx = 0;
    std::srand(12345);  // optional fixed seed

    for (int i = 0; i < count; ++i) {
        int64_t vs[8];
        for (int j = 0; j < 8; ++j) {
            // Emulate Java's nextLong() then Math.abs(): remove sign bit
            uint64_t v = ((uint64_t)std::rand() << 32) | std::rand();
            vs[j] = v & 0x7FFFFFFFFFFFFFFFULL;  // clear sign bit
            pre_values.push_back(vs[j]);
        }

        unsigned char temp_buf[64] = {0};  // temp output buffer
        packer.pack_8values(vs, 0, temp_buf);

        std::memcpy(buffer.data() + idx, temp_buf, byte_count);
        idx += byte_count;
    }

    std::vector<int64_t> result(total_values);
    packer.unpack_all_values(buffer.data(), static_cast<int>(buffer.size()),
                             result.data());

    for (int i = 0; i < total_values; ++i) {
        ASSERT_EQ(result[i], pre_values[i]) << "Mismatch at index " << i;
    }
}

// Utility to compute the maximum bit width needed to store all values
int get_long_max_bit_width(const std::vector<uint64_t>& values) {
    uint64_t max_val = 0;
    for (uint64_t v : values) {
        max_val = std::max(max_val, v);
    }
    if (max_val == 0) return 1;
    return static_cast<int>(std::floor(std::log2(max_val)) + 1);
}

TEST(Int64PackerTest, PackAllManualBitWidth) {
    std::vector<uint64_t> bp_list;
    int bp_count = 15;
    uint64_t bp_start = 11;
    for (int i = 0; i < bp_count; ++i) {
        bp_list.push_back(bp_start);
        bp_start *= 3;
    }
    bp_list.push_back(0);  // Add one zero
    ASSERT_EQ(bp_list.size(), 16u);

    // Calculate max bit width
    int bp_bit_width = get_long_max_bit_width(bp_list);

    Int64Packer packer(bp_bit_width);
    std::ostringstream oss(std::ios::binary);

    // Split into two blocks of 8
    int64_t value1[8];
    int64_t value2[8];
    for (int i = 0; i < 8; ++i) {
        value1[i] = bp_list[i];
        value2[i] = bp_list[i + 8];
    }

    unsigned char bytes1[64] = {0};
    unsigned char bytes2[64] = {0};
    packer.pack_8values(value1, 0, bytes1);
    packer.pack_8values(value2, 0, bytes2);
    oss.write(reinterpret_cast<const char*>(bytes1), bp_bit_width);
    oss.write(reinterpret_cast<const char*>(bytes2), bp_bit_width);

    std::string packed_data = oss.str();
    ASSERT_EQ(static_cast<int>(packed_data.size()), 2 * bp_bit_width);

    // Decode
    int64_t read_array[16] = {0};
    packer.unpack_all_values(
        reinterpret_cast<const unsigned char*>(packed_data.data()),
        2 * bp_bit_width, read_array);

    // Compare
    for (int i = 0; i < 16; ++i) {
        ASSERT_EQ(read_array[i], bp_list[i]) << "Mismatch at index " << i;
    }
}

// Test all zeros for various widths
TEST(Int64PackerTest, AllZeroValues) {
    for (int width = 1; width <= 31; ++width) {
        int64_t arr[NUM_OF_INTS] = {0};
        Int64Packer packer(width);
        const int bufSize = NUM_OF_INTS * width / 8;
        std::vector<unsigned char> buf(bufSize, 0);
        packer.pack_8values(arr, 0, buf.data());
        int64_t res[NUM_OF_INTS] = {
            1};  // initialize non-zero to catch failures
        packer.unpack_8values(buf.data(), 0, res);
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            EXPECT_EQ(res[i], 0) << "Width=" << width << " Index=" << i;
        }
    }
}

// Test boundary width = 1 with alternating bits
TEST(Int64PackerTest, BoundaryWidthOneAlternating) {
    const int width = 1;
    int64_t arr[NUM_OF_INTS] = {0, 1, 0, 1, 0, 1, 0, 1};
    Int64Packer packer(width);
    const int bufSize = NUM_OF_INTS * width / 8;
    std::vector<unsigned char> buf(bufSize, 0);
    packer.pack_8values(arr, 0, buf.data());
    int64_t res[NUM_OF_INTS] = {0};
    packer.unpack_8values(buf.data(), 0, res);
    for (int i = 0; i < NUM_OF_INTS; ++i) {
        EXPECT_EQ(res[i], arr[i]) << "Index=" << i;
    }
}

// Test maximum width (64 bits)
TEST(Int64PackerTest, MaxWidth64Random) {
    const int width = 64;
    const int times = 100000;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dist(INT64_MIN, INT64_MAX);
    for (int t = 0; t < times; ++t) {
        int64_t arr[NUM_OF_INTS];
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            arr[i] = dist(gen);
        }
        Int64Packer packer(width);
        const int bufSize = NUM_OF_INTS * width / 8;
        std::vector<unsigned char> buf(bufSize, 0);
        packer.pack_8values(arr, 0, buf.data());
        int64_t res[NUM_OF_INTS] = {0};
        packer.unpack_8values(buf.data(), 0, res);
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            EXPECT_EQ(res[i], arr[i]) << "Index=" << i;
        }
    }
}

TEST(Int64PackerTest, AllNegative64Random) {
    const int width = 64;
    const int times = 100000;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dist(INT64_MIN, -1);
    for (int t = 0; t < times; ++t) {
        int64_t arr[NUM_OF_INTS];
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            arr[i] = dist(gen);
        }
        Int64Packer packer(width);
        const int bufSize = NUM_OF_INTS * width / 8;
        std::vector<unsigned char> buf(bufSize, 0);
        packer.pack_8values(arr, 0, buf.data());
        int64_t res[NUM_OF_INTS] = {0};
        packer.unpack_8values(buf.data(), 0, res);
        for (int i = 0; i < NUM_OF_INTS; ++i) {
            EXPECT_EQ(res[i], arr[i]) << "Index=" << i;
        }
    }
}

// Test unpack_all_values for multiple blocks
TEST(Int64PackerTest, UnpackAllValuesMultipleBlocks) {
    const int width = 16;
    // pack 10 blocks sequentially
    const int blocks = 10;
    Int64Packer packer(width);
    std::vector<int64_t> orig(blocks * NUM_OF_INTS);
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

    std::vector<int64_t> res(blocks * NUM_OF_INTS, 0);
    // Unpack all blocks at once
    packer.unpack_all_values(buf.data(), static_cast<int>(buf.size()),
                             res.data());

    // Verify each unpacked value matches the original sequence
    for (size_t i = 0; i < orig.size(); ++i) {
        EXPECT_EQ(res[i], orig[i]) << "Index=" << i;
    }
}

}  // namespace storage