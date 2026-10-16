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
#include "common/allocator/alloc_base.h"

#include <gtest/gtest.h>

#include <limits>

namespace common {

const uint32_t HEADER_SIZE_4B = 4;
const uint32_t HEADER_SIZE_8B = 8;

TEST(AllocBaseTest, BaseAllocator) {
    BaseAllocator allocator;
    for (AllocModID mod_id = MOD_DEFAULT; mod_id <= MOD_ZIGZAG_OBJ;
         mod_id = (AllocModID)(mod_id + 1)) {
        void* ptr = allocator.alloc(100, mod_id);
        ASSERT_NE(ptr, nullptr);
        allocator.free(ptr);
    }
}

TEST(AllocBaseTest, AllocSmallSize) {
    uint32_t size = 1024;
    AllocModID mid = MOD_DEFAULT;
    void* ptr = mem_alloc(size, mid);
    ASSERT_NE(ptr, nullptr);
    char* p = (char*)ptr - HEADER_SIZE_4B;
    uint32_t header = *((uint32_t*)p);
    EXPECT_EQ(header >> 8, size);
    EXPECT_EQ(header & 0x7F, mid);
    mem_free(ptr);
}

TEST(AllocBaseTest, AllocLargeSize) {
    uint32_t size = 0x1000000;
    AllocModID mid = MOD_DEFAULT;
    void* ptr = mem_alloc(size, mid);
    ASSERT_NE(ptr, nullptr);
    char* p = (char*)ptr - HEADER_SIZE_8B;
    uint32_t high4b = *(uint32_t*)p;
    uint32_t low4b = *(uint32_t*)(p + 4);
    uint64_t header = ((uint64_t)high4b << 32) | low4b;
    EXPECT_EQ((header >> 8), size);
    EXPECT_EQ((header & 0x7F), mid);
    mem_free(ptr);
}

TEST(AllocBaseTest, SizeAboveUint32ReachesAllocatorWithoutTruncation) {
    if (sizeof(size_t) < sizeof(uint64_t)) GTEST_SKIP();
    const size_t size = static_cast<uint64_t>(UINT32_MAX) + 1;
    TEST_fail_mem_alloc_after(MOD_DEFAULT, 0);
    BaseAllocator allocator;
    EXPECT_EQ(allocator.alloc(size, MOD_DEFAULT), nullptr);
    EXPECT_EQ(TEST_get_failed_mem_alloc_size(), size);

    char* ptr = static_cast<char*>(allocator.alloc(8, MOD_DEFAULT));
    ASSERT_NE(ptr, nullptr);
    memset(ptr, 'x', 8);
    TEST_fail_next_mem_realloc();
    EXPECT_EQ(mem_realloc(ptr, size), nullptr);
    EXPECT_EQ(TEST_get_failed_mem_alloc_size(), size);
    for (int i = 0; i < 8; ++i) EXPECT_EQ(ptr[i], 'x');
    allocator.free(ptr);
}

TEST(AllocBaseTest, UnrepresentableSizeReturnsNullAndPreservesReallocInput) {
    const size_t size = std::numeric_limits<size_t>::max();
    void* oversized = mem_alloc(size, MOD_DEFAULT);
    EXPECT_EQ(oversized, nullptr);
    if (oversized) mem_free(oversized);

    char* ptr = static_cast<char*>(mem_alloc(8, MOD_DEFAULT));
    ASSERT_NE(ptr, nullptr);
    memset(ptr, 'x', 8);
    void* resized = mem_realloc(ptr, size);
    EXPECT_EQ(resized, nullptr);
    if (resized) {
        mem_free(resized);
    } else {
        for (int i = 0; i < 8; ++i) EXPECT_EQ(ptr[i], 'x');
        mem_free(ptr);
    }
}

TEST(AllocBaseTest, ReallocateToLargerSize) {
    const uint32_t initial_size = 1000;
    const uint32_t new_size = 2000;

    void* ptr = mem_alloc(initial_size, MOD_DEFAULT);
    ASSERT_NE(ptr, nullptr);

    ptr = mem_realloc(ptr, new_size);
    ASSERT_NE(ptr, nullptr);

    uint32_t* header = (uint32_t*)((char*)ptr - HEADER_SIZE_4B);
    ASSERT_EQ(*header >> 8, new_size);

    mem_free(ptr);
}

TEST(AllocBaseTest, ReallocateToSmallerSize) {
    const uint32_t initial_size = 2000;
    const uint32_t new_size = 1000;

    void* ptr = mem_alloc(initial_size, MOD_DEFAULT);
    ASSERT_NE(ptr, nullptr);

    ptr = mem_realloc(ptr, new_size);
    ASSERT_NE(ptr, nullptr);

    uint32_t* header = (uint32_t*)((char*)ptr - HEADER_SIZE_4B);
    ASSERT_EQ(*header >> 8, new_size);

    mem_free(ptr);
}

}  // namespace common
