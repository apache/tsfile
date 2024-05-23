
#ifndef COMMON_ALLOCATOR_ALLOC_BASE_H
#define COMMON_ALLOCATOR_ALLOC_BASE_H

#include <cstring>
#include <stddef.h>
#include "../util_define.h"

namespace timecho
{
namespace common
{

enum AllocModID
{
  __FIRST_MOD_ID = 0,
  // if you are sure you will not consume too much memory, you can use MOD_DEFAULT.
  MOD_DEFAULT = 0,
  MOD_MEMTABLE = 1,
  MOD_SCHEMA = 2,
  MOD_SQL = 3,
  MOD_NET = 4,
  MOD_NET_DATA = 5,
  MOD_TVLIST_DATA = 6,
  MOD_TVLIST_OBJ = 7,
  MOD_TSBLOCK = 8,
  MOD_CONTAINER = 9,
  MOD_TSSTORE_OBJ = 10,
  MOD_FLUSH_TASK_OBJ = 11,
  MOD_PAGE_WRITER_OUTPUT_STREAM = 12,
  MOD_CW_PAGES_DATA = 13,
  MOD_CHUNK_WRITER_OBJ = 14,
  MOD_STATISTIC_OBJ = 15,
  MOD_ENCODER_OBJ = 16,
  MOD_DECODER_OBJ = 17,
  MOD_TSFILE_WRITER_META = 18,
  MOD_TSFILE_WRITE_STREAM = 19,
  MOD_TIMESERIES_INDEX_OBJ = 20,
  MOD_BLOOM_FILTER = 21,
  MOD_OPEN_FILE_OBJ = 22,
  MOD_TSFILE_READER = 23,
  MOD_CHUNK_READER = 24,
  MOD_COMPRESSOR_OBJ = 25,
  MOD_ARRAY = 26,
  MOD_HASH_TABLE = 27,
  MOD_WRITER_INDEX_NODE = 28,
  MOD_TS2DIFF_OBJ = 29,
  MOD_BITENCODE_OBJ = 30,
  MOD_DICENCODE_OBJ = 31,
  MOD_ZIGZAG_OBJ = 32,
  __LAST_MOD_ID = 33, // prev + 1,
  __MAX_MOD_ID = 127, // leave 1 bit to detect header size
};

extern const char *g_mod_names[__LAST_MOD_ID];

/* very basic alloc/free interface in C style */
void *mem_alloc(uint32_t size, AllocModID mid);
void mem_free(void *ptr);
void *mem_realloc(void *ptr, uint32_t size);

class ModStat
{
public:
  ModStat() : stat_arr_(NULL) {}

  static ModStat &get_instance()
  {
    /*
     * This is the singleton of Mod Memory Statistic.
     * gms is short for Global Mod Statistic
     */
    static ModStat gms;
    return gms;
  }
  void init();
  void destroy();
  INLINE void update_alloc(AllocModID mid, int32_t size)
  {
//    ASSERT(mid < __LAST_MOD_ID);
//     ATOMIC_FAA(get_item(mid), size);
  }
  void update_free(AllocModID mid, uint32_t size)
  {
//    ASSERT(mid < __LAST_MOD_ID);
//    ATOMIC_FAA(get_item(mid), 0 - size);
  }
  void print_stat();

#ifdef ENABLE_TEST
  int32_t TEST_get_stat(int8_t mid) { return ATOMIC_FAA(get_item(mid), 0); }
#endif

private:
  INLINE int32_t *get_item(int8_t mid)
  { 
    return &(stat_arr_[mid * (ITEM_SIZE/sizeof(int32_t))]); 
  }

private:
  static const int32_t ITEM_SIZE = CACHE_LINE_SIZE;
  static const int32_t ITEM_COUNT = __LAST_MOD_ID;
  int32_t *stat_arr_;

  STATIC_ASSERT((ITEM_SIZE % sizeof(int32_t) == 0), ModStat_ITEM_SIZE_ERROR);
};

/* base allocator */
class BaseAllocator
{
public:
  void *alloc(uint32_t size, AllocModID mid)
  {
    return mem_alloc(size, mid);
  }
  void free(void *ptr)
  {
    mem_free(ptr);
  }
};

extern BaseAllocator g_base_allocator;

} // end namespace common
} // end namespace timecho

#endif // COMMON_ALLOCATOR_ALLOC_BASE_H

