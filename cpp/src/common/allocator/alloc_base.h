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

#ifndef COMMON_ALLOCATOR_ALLOC_BASE_H
#define COMMON_ALLOCATOR_ALLOC_BASE_H

#include <stddef.h>

#include <cstring>

#include "utils/util_define.h"

namespace common {

enum AllocModID {
    __FIRST_MOD_ID = 0,
    MOD_DEFAULT = 0,
    MOD_TVLIST_DATA = 1,
    MOD_TSBLOCK = 2,
    MOD_PAGE_WRITER_OUTPUT_STREAM = 3,
    MOD_CW_PAGES_DATA = 4,
    MOD_STATISTIC_OBJ = 5,
    MOD_ENCODER_OBJ = 6,
    MOD_DECODER_OBJ = 7,
    MOD_TSFILE_WRITER_META = 8,
    MOD_TSFILE_WRITE_STREAM = 9,
    MOD_TIMESERIES_INDEX_OBJ = 10,
    MOD_BLOOM_FILTER = 11,
    MOD_TSFILE_READER = 12,
    MOD_CHUNK_READER = 13,
    MOD_COMPRESSOR_OBJ = 14,
    MOD_ARRAY = 15,
    MOD_HASH_TABLE = 16,
    MOD_WRITER_INDEX_NODE = 17,
    MOD_TS2DIFF_OBJ = 18,
    MOD_BITENCODE_OBJ = 19,
    MOD_DICENCODE_OBJ = 20,
    MOD_ZIGZAG_OBJ = 21,
    MOD_DEVICE_META_ITER = 22,
    MOD_DEVICE_TASK_ITER = 23,
    MOD_TABLET = 24,
    __LAST_MOD_ID = 25,
    __MAX_MOD_ID = 127,
};

extern const char* g_mod_names[__LAST_MOD_ID];

/* very basic alloc/free interface in C style */
void* mem_alloc(uint32_t size, AllocModID mid);
void mem_free(void* ptr);
void* mem_realloc(void* ptr, uint32_t size);

class ModStat {
   public:
    ModStat() : stat_arr_(NULL) {}
    ~ModStat() { destroy(); }

    static ModStat& get_instance() {
        static ModStat gms;
#ifdef ENABLE_MEM_STAT
        if (UNLIKELY(gms.stat_arr_ == NULL)) {
            gms.init();
        }
#endif
        return gms;
    }
    void init();
    void destroy();
    INLINE void update_alloc(AllocModID mid, int32_t size) {
#ifdef ENABLE_MEM_STAT
        ASSERT(mid < __LAST_MOD_ID);
        ATOMIC_FAA(get_item(mid), size);
#endif
    }
    void update_free(AllocModID mid, uint32_t size) {
#ifdef ENABLE_MEM_STAT
        ASSERT(mid < __LAST_MOD_ID);
        ATOMIC_FAA(get_item(mid), 0 - size);
#endif
    }
    void print_stat();

#ifdef ENABLE_TEST
    int32_t TEST_get_stat(int8_t mid) { return ATOMIC_FAA(get_item(mid), 0); }
#endif

   private:
    INLINE int32_t* get_item(int8_t mid) {
        return &(stat_arr_[mid * (ITEM_SIZE / sizeof(int32_t))]);
    }

   private:
    static const int32_t ITEM_SIZE = CACHE_LINE_SIZE;
    static const int32_t ITEM_COUNT = __LAST_MOD_ID;
    int32_t* stat_arr_;

    STATIC_ASSERT((ITEM_SIZE % sizeof(int32_t) == 0), ModStat_ITEM_SIZE_ERROR);
};

/* base allocator */
class BaseAllocator {
   public:
    void* alloc(uint32_t size, AllocModID mid) { return mem_alloc(size, mid); }
    void free(void* ptr) { mem_free(ptr); }
};

extern BaseAllocator g_base_allocator;

}  // end namespace common

#endif  // COMMON_ALLOCATOR_ALLOC_BASE_H
