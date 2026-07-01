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

#ifndef _WIN32
#include <execinfo.h>
#endif
#include <string.h>

#include <iostream>

#include "alloc_base.h"
#include "common/logger/elog.h"
#include "stdio.h"
#include "stdlib.h"
#include "utils/util_define.h"

namespace common {

const char *g_mod_names[__LAST_MOD_ID] = {
    /*  0 */ "DEFAULT",
    /*  1 */ "MEMTABLE",
    /*  2 */ "SCHEMA",
    /*  3 */ "SQL",
    /*  4 */ "NET",
    /*  5 */ "NET_DATA",
    /*  6 */ "TVLIST_DATA",
    /*  7 */ "TVLIST_OBJ",
    /*  8 */ "TSBLOCK",
    /*  9 */ "CONTAINER",
    /* 10 */ "TSSTORE_OBJ",
    /* 11 */ "FLUSH_TASK_OBJ",
    /* 12 */ "PAGE_WRITER_OUTPUT_STREAM",
    /* 13 */ "CW_PAGES_DATA",
    /* 14 */ "CHUNK_WRITER_OBJ",
    /* 15 */ "STATISTIC_OBJ",
    /* 16 */ "ENCODER_OBJ",
    /* 17 */ "DECODER_OBJ",
    /* 18 */ "TSFILE_WRITER_META",
    /* 19 */ "TSFILE_WRITE_STREAM",
    /* 20 */ "TIMESERIES_INDEX_OBJ",
    /* 21 */ "BLOOM_FILTER",
    /* 22 */ "OPEN_FILE_OBJ",
    /* 23 */ "TSFILE_READER",
    /* 24 */ "CHUNK_READER",
    /* 25 */ "COMPRESSOR_OBJ",
    /* 26 */ "ARRAY",
    /* 27 */ "HASH_TABLE",
};

// Most modern CPUs (e.g., x86_64, Arm) support at least 8-byte alignment,
// and C++ mandates that alignof(std::max_align_t) reflects the strictest
// alignment requirement for built-in types (typically 8 or 16 bytes, especially
// with SIMD)

// To ensure that the returned memory pointer from mem_alloc is properly aligned
// for any type, we standardize on an 8-byte header(HEADER_SIZE_8B).
// If the actual header content is smaller, additional padding is inserted
// automatically before the aligned payload to preserve alignment.
// constexpr uint32_t HEADER_SIZE_4B = 4;
constexpr size_t HEADER_PTR_SIZE = 8;
// Default alignment is 8 bytes, sufficient for basic types.
// If SIMD (e.g., SSE/AVX) is introduced later, increase ALIGNMENT to 16/32/64
// as needed.
// constexpr size_t ALIGNMENT = alignof(std::max_align_t);
constexpr size_t ALIGNMENT = 8;

void *mem_alloc(uint32_t size, AllocModID mid) {
    // use 7bit at most
    ASSERT(mid <= 127);
    static_assert(HEADER_PTR_SIZE <= ALIGNMENT,
                  "Header must fit within alignment");
    constexpr size_t header_size = ALIGNMENT;
    const size_t total_size = size + header_size;
    auto raw = static_cast<char *>(malloc(total_size));
    if (UNLIKELY(raw == nullptr)) {
        return nullptr;
    }
    uint64_t data_size = size;
    uint64_t header = (data_size << 8) | static_cast<uint32_t>(mid);
    auto low4b = static_cast<uint32_t>(header & 0xFFFFFFFF);
    auto high4b = static_cast<uint32_t>(header >> 32);
    *reinterpret_cast<uint32_t *>(raw) = high4b;
    *reinterpret_cast<uint32_t *>(raw + 4) = low4b;
    return raw + header_size;
}

#ifndef _WIN32
void printCallers() {
    int layers = 0, i = 0;
    char **symbols = NULL;

    const int64_t MAX_FRAMES = 32;

    void *frames[MAX_FRAMES];
    memset(frames, 0, sizeof(frames));
    layers = backtrace(frames, MAX_FRAMES);
    for (i = 0; i < layers; i++) {
        printf("Layer %d: %p\n", i, frames[i]);
    }
    printf("------------------\n");

    symbols = backtrace_symbols(frames, layers);
    if (symbols) {
        for (i = 0; i < layers; i++) {
            printf("SYMBOL layer %d: %s\n", i, symbols[i]);
        }
        free(symbols);
    } else {
        printf("Failed to parse function names\n");
    }
}
#endif

void mem_free(void *ptr) {
    char *p = static_cast<char *>(ptr);
    char *raw_ptr = p - ALIGNMENT;
    uint64_t header =
        static_cast<uint64_t>(*reinterpret_cast<uint32_t *>(raw_ptr + 4)) |
        (static_cast<uint64_t>(*reinterpret_cast<uint32_t *>(raw_ptr)) << 32);
    auto mid = static_cast<AllocModID>(header & 0x7F);
    auto size = static_cast<uint32_t>(header >> 8);
    ModStat::get_instance().update_free(mid, size);
    ::free(raw_ptr);
}

void *mem_realloc(void *ptr, uint32_t size) {
    char *p = static_cast<char *>(ptr);
    char *raw_ptr = p - ALIGNMENT;
    const uint64_t header =
        static_cast<uint64_t>(*reinterpret_cast<uint32_t *>(raw_ptr + 4)) |
        (static_cast<uint64_t>(*reinterpret_cast<uint32_t *>(raw_ptr)) << 32);
    auto mid = static_cast<AllocModID>(header & 0x7F);
    auto original_size = static_cast<uint32_t>(header >> 8);
    p = static_cast<char *>(realloc(raw_ptr, size + ALIGNMENT));
    if (UNLIKELY(p == nullptr)) {
        return nullptr;
    }

    uint64_t data_size = size;
    uint64_t header_new = (data_size << 8) | static_cast<uint32_t>(mid);
    auto low4b = static_cast<uint32_t>(header_new & 0xFFFFFFFF);
    auto high4b = static_cast<uint32_t>(header_new >> 32);
    *reinterpret_cast<uint32_t *>(p) = high4b;
    *reinterpret_cast<uint32_t *>(p + 4) = low4b;
    ModStat::get_instance().update_alloc(
        mid, int32_t(size) - int32_t(original_size));
    return p + ALIGNMENT;
}

void ModStat::init() {
    stat_arr_ = (int32_t *)(::malloc(ITEM_SIZE * ITEM_COUNT));
    for (int8_t i = 0; i < __LAST_MOD_ID; i++) {
        int32_t *item = get_item(i);
        *item = 0;
    }
}

void ModStat::destroy() { ::free(stat_arr_); }

BaseAllocator g_base_allocator;

}  // end namespace common