#ifndef COMMON_CONTAINER_BIT_MAP_H
#define COMMON_CONTAINER_BIT_MAP_H

#include <string.h>
#include "common/errno_define.h"
#include "common/util_define.h"

namespace timecho
{
namespace common
{

class BitMap
{
public:
  BitMap() : bitmap_(nullptr), size_(0), init_as_zero_(true) { }
  ~BitMap();
  int init(uint32_t item_size, bool init_as_zero = true);

  FORCE_INLINE void reset()
  {
    const char initial_char = init_as_zero_ ? 0x00 : 0xFF;
    memset(bitmap_, initial_char, size_);
  }

  FORCE_INLINE void set(uint32_t index)
  {
    uint32_t offset = index >> 3;
    ASSERT(offset < size_);

    char *start_addr = bitmap_ + offset;
    uint8_t bit_mask = get_bit_mask(index);
    *start_addr = (*start_addr) | (bit_mask);
  }

  FORCE_INLINE void clear(uint32_t index)
  {
    uint32_t offset = index >> 3;
    ASSERT(offset < size_);

    char *start_addr = bitmap_ + offset;
    uint8_t bit_mask = get_bit_mask(index);
    *start_addr = (*start_addr) ^ (~bit_mask);
  }

  FORCE_INLINE bool test(uint32_t index)
  {
    uint32_t offset = index >> 3;
    ASSERT(offset < size_);

    char *start_addr = bitmap_ + offset;
    uint8_t bit_mask = get_bit_mask(index);
    return (*start_addr & bit_mask);
  }

  FORCE_INLINE uint32_t get_size() { return size_; }

  FORCE_INLINE char* get_bitmap() { return bitmap_; }  // for debug

private:
  FORCE_INLINE uint8_t get_bit_mask(uint32_t index) { return 1 << (index & 7); }

private:
  char      *bitmap_;
  uint32_t  size_;
  bool init_as_zero_;
};
}  // common
}  // timecho

#endif  // COMMON_CONTAINER_BIT_MAP_H
