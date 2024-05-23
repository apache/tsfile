
#include "bit_map.h"
#include "common/allocator/alloc_base.h"

namespace timecho
{
namespace common
{

BitMap::~BitMap()
{
  if (bitmap_ && (size_ > 0)) {
    mem_free(bitmap_);
    bitmap_ = nullptr;
    size_ = 0;
  }
}

int BitMap::init(uint32_t item_size, bool init_as_zero)
{
  uint32_t size = (item_size + 7) / 8;
  bitmap_ = static_cast<char*>(mem_alloc(size, MOD_TSBLOCK));
  // need set to 0, otherwise there will be wrong data
  const char initial_char = init_as_zero ? 0x00 : 0xFF;
  memset(bitmap_, initial_char, size);
  size_ = size;
  init_as_zero_ = init_as_zero;
  return common::E_OK;
}

}
}
