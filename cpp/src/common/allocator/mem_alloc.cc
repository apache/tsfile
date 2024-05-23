
#include <execinfo.h>
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

const uint32_t HEADER_SIZE_4B = 4;
const uint32_t HEADER_SIZE_8B = 8;

void *mem_alloc(uint32_t size, AllocModID mid) {
    // use 7bit at most
    ASSERT(mid <= 127);

    if (size <= 0xFFFFFF) {
        // use 3B size + 1B mod
        char *p = (char *)malloc(size + HEADER_SIZE_4B);
        if (UNLIKELY(p == nullptr)) {
            return nullptr;
        } else {
            uint32_t header = (size << 8) | ((uint32_t)mid);
            *((uint32_t *)p) = header;
            ModStat::get_instance().update_alloc(mid, size);
            // cppcheck-suppress memleak
            // cppcheck-suppress unmatchedSuppression
            return p + HEADER_SIZE_4B;
        }
    } else {
        char *p = (char *)malloc(size + HEADER_SIZE_8B);
        if (UNLIKELY(p == nullptr)) {
            std::cout << "alloc big filed for size " << size + HEADER_SIZE_4B
                      << std::endl;
            return nullptr;
        } else {
            uint64_t large_size = size;
            uint64_t header = ((large_size) << 8) | (((uint32_t)mid) | (0x80));
            uint32_t low4b = (uint32_t)(header & 0xFFFFFFFF);
            uint32_t high4b = (uint32_t)(header >> 32);
            *(uint32_t *)p = high4b;
            *(uint32_t *)(p + 4) = low4b;
            ModStat::get_instance().update_alloc(mid, size);
            // cppcheck-suppress unmatchedSuppression
            // cppcheck-suppress memleak
            return p + HEADER_SIZE_8B;
        }
    }
}

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

void mem_free(void *ptr) {
    // try as 4Byte header
    char *p = (char *)ptr;
    uint32_t header = *(uint32_t *)(p - HEADER_SIZE_4B);
    if ((header & 0x80) == 0) {
        // 4Byte header
        uint32_t size = header >> 8;
        AllocModID mid = (AllocModID)(header & 0x7F);
        ModStat::get_instance().update_free(mid, size);
        ::free(p - HEADER_SIZE_4B);
    } else {
        // 8Byte header
        uint64_t header8b = ((uint64_t)(*(uint32_t *)(p - 4))) |
                            ((uint64_t)(*(uint32_t *)(p - 8)) << 32);
        AllocModID mid = (AllocModID)(header8b & 0x7F);
        uint32_t size = (uint32_t)(header8b >> 8);
        ModStat::get_instance().update_free(mid, size);
        ::free(p - HEADER_SIZE_8B);
    }
}

void *mem_realloc(void *ptr, uint32_t size) {
    AllocModID mid_org;
    uint32_t size_org;
    char *p = (char *)ptr;
    uint32_t header_org =
        *(uint32_t *)(p - HEADER_SIZE_4B);  // try as 4Byte header
    if ((header_org & 0x80) == 0) {
        // header_org is 4byte
        size_org = header_org >> 8;
        mid_org = (AllocModID)(header_org & 0x7F);
        if (size <= 0xFFFFFF) {
            p = (char *)realloc(p - HEADER_SIZE_4B, size + HEADER_SIZE_4B);
            if (UNLIKELY(p == nullptr)) {
                return nullptr;
            } else {
                uint32_t header =
                    (size << 8) | ((uint32_t)mid_org);  // size changed
                *((uint32_t *)p) = header;
                ModStat::get_instance().update_alloc(
                    mid_org, int32_t(size) - int32_t(size_org));
                return p + HEADER_SIZE_4B;
            }
        } else {  // size > 0xFFFFFF, realloc(os_p, size + header_len)
            p = (char *)realloc(p - HEADER_SIZE_4B, size + HEADER_SIZE_8B);
            if (UNLIKELY(p == nullptr)) {
                return nullptr;
            } else {
                std::memmove(p + HEADER_SIZE_8B, p + HEADER_SIZE_4B, size_org);
                // reconstruct 8-byte header
                uint64_t large_size = size;
                uint64_t header =
                    ((large_size) << 8) | (((uint32_t)mid_org) | (0x80));
                uint32_t low4b = (uint32_t)(header & 0xFFFFFFFF);
                uint32_t high4b = (uint32_t)(header >> 32);
                *(uint32_t *)p = high4b;
                *(uint32_t *)(p + 4) = low4b;
                ModStat::get_instance().update_alloc(
                    mid_org, int32_t(size) - int32_t(size_org));
                return p + HEADER_SIZE_8B;
            }
        }
    } else {  // header_org is 8byte
        uint64_t header =
            ((uint64_t)(*(uint32_t *)(p - 4))) |
            ((uint64_t)(*(uint32_t *)(p - 8)) << 32);  // 8Byte header
        mid_org = (AllocModID)(header & 0x7F);
        size_org = (uint32_t)(header >> 8);
        if (size <= 0xFFFFFF) {
            uint32_t save_data =
                *(uint32_t *)(p - HEADER_SIZE_8B + HEADER_SIZE_4B + size);
            p = (char *)realloc(p - HEADER_SIZE_8B, size + HEADER_SIZE_4B);
            if (UNLIKELY(p == nullptr)) {
                return nullptr;
            } else {
                std::memmove(p + HEADER_SIZE_4B, p + HEADER_SIZE_8B,
                             size - HEADER_SIZE_4B);
                // reconstruct 4-byte header
                uint32_t header4b = (size << 8) | (((uint32_t)mid_org));
                *((uint32_t *)p) = header4b;
                // reconstruct data
                *(uint32_t *)((char *)p + size - 4) = save_data;
                ModStat::get_instance().update_alloc(
                    mid_org, int32_t(size) - int32_t(size_org));
                return p + HEADER_SIZE_4B;
            }
        } else {
            p = (char *)realloc(p - HEADER_SIZE_8B, size + HEADER_SIZE_8B);
            if (UNLIKELY(p == nullptr)) {
                return nullptr;
            } else {
                uint64_t large_size = size;
                uint64_t header8b =
                    ((large_size) << 8) | (((uint32_t)mid_org) | (0x80));
                uint32_t low4b = (uint32_t)(header8b & 0xFFFFFFFF);
                uint32_t high4b = (uint32_t)(header8b >> 32);
                *(uint32_t *)p = high4b;
                *(uint32_t *)(p + 4) = low4b;
                ModStat::get_instance().update_alloc(
                    mid_org, int32_t(size) - int32_t(size_org));
                return p + HEADER_SIZE_8B;
            }
        }
    }
}

void ModStat::init() {
    stat_arr_ = (int32_t *)(::malloc(ITEM_SIZE * ITEM_COUNT));
    for (int8_t i = 0; i < __LAST_MOD_ID; i++) {
        int32_t *item = get_item(i);
        *item = 0;
    }
}

void ModStat::destroy() { ::free(stat_arr_); }

// TODO return to SQL
void ModStat::print_stat() {
    //
}

BaseAllocator g_base_allocator;

}  // end namespace common