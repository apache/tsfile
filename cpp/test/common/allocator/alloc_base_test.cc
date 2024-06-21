#include "common/allocator/alloc_base.h"

#include <gtest/gtest.h>

namespace common {

const uint32_t HEADER_SIZE_4B = 4;
const uint32_t HEADER_SIZE_8B = 8;

TEST(AllocBaseTest, BaseAllocator) {
    BaseAllocator allocator;
    for (AllocModID mod_id = MOD_DEFAULT; mod_id <= MOD_ZIGZAG_OBJ;
         mod_id = (AllocModID)(mod_id + 1)) {
        void *ptr = allocator.alloc(100, mod_id);
        ASSERT_NE(ptr, nullptr);
        allocator.free(ptr);
    }
}

TEST(AllocBaseTest, AllocSmallSize) {
    uint32_t size = 1024;
    AllocModID mid = MOD_DEFAULT;
    void *ptr = mem_alloc(size, mid);
    ASSERT_NE(ptr, nullptr);
    char *p = (char *)ptr - HEADER_SIZE_4B;
    uint32_t header = *((uint32_t *)p);
    EXPECT_EQ(header >> 8, size);
    EXPECT_EQ(header & 0x7F, mid);
    mem_free(ptr);
}

TEST(AllocBaseTest, AllocLargeSize) {
    uint32_t size = 0x1000000;
    AllocModID mid = MOD_DEFAULT;
    void *ptr = mem_alloc(size, mid);
    ASSERT_NE(ptr, nullptr);
    char *p = (char *)ptr - HEADER_SIZE_8B;
    uint32_t high4b = *(uint32_t *)p;
    uint32_t low4b = *(uint32_t *)(p + 4);
    uint64_t header = ((uint64_t)high4b << 32) | low4b;
    EXPECT_EQ((header >> 8), size);
    EXPECT_EQ((header & 0x7F), mid);
    mem_free(ptr);
}

}  // namespace common