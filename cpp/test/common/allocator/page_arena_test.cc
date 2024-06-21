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
    void* ptr = arena.alloc(512);
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