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
#include "common/allocator/page_arena.h"

#include <gtest/gtest.h>

namespace common {

TEST(PageArenaTest, PageArenaInit) {
    PageArena page_arena;
    int page_size = 1024;
    page_arena.init(page_size, MOD_DEFAULT);
}

TEST(PageArenaTest, PageArenaAlloc) {
    PageArena page_arena;
    int page_size = 1024;
    page_arena.init(page_size, MOD_DEFAULT);
    void* ptr = page_arena.alloc(page_size);
    ASSERT_NE(ptr, nullptr);
    page_arena.reset();
}

TEST(PageArenaTest, AllocWithinPageSize) {
    PageArena arena;
    arena.init(1024, MOD_DEFAULT);
    char* ptr = arena.alloc(512);
    ASSERT_NE(ptr, nullptr);
    ASSERT_EQ(ptr + 512, arena.alloc(512));
    arena.reset();
}

TEST(PageArenaTest, AllocMoreThanPageSize) {
    PageArena page_arena;
    int page_size = 1024;
    page_arena.init(page_size, MOD_DEFAULT);
    void* ptr = page_arena.alloc(page_size * 2);
    ASSERT_NE(ptr, nullptr);
    page_arena.reset();
}

}  // namespace common