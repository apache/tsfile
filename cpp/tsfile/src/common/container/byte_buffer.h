#ifndef COMMON_CONTAINER_BYTE_BUFFER_H
#define COMMON_CONTAINER_BYTE_BUFFER_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "common/allocator/alloc_base.h"
#include "common/global.h"
#include "common/util_define.h"

namespace timecho
{
namespace common
{
class ByteBuffer
{
public:
  ByteBuffer() : data_(nullptr), variable_type_len_(sizeof(uint32_t)), real_data_size_(0), reserved_size_(0) {}

  ~ByteBuffer()
  {
    if (data_ && (reserved_size_ > 0)) {
      mem_free(data_);
      data_ = nullptr;
      real_data_size_ = 0;
      reserved_size_ = 0;
    }
  }

  FORCE_INLINE void init(uint32_t size)
  {
    data_ = static_cast<char*>(mem_alloc(size, MOD_TSBLOCK));
    reserved_size_ = size;
  }

  FORCE_INLINE void reset()
  {
    real_data_size_ = 0;
  }

  FORCE_INLINE void extend_memory(uint32_t new_size)
  {
    ASSERT(new_size > reserved_size_);
    data_ = static_cast<char*>(mem_realloc(data_, new_size));
    reserved_size_ = new_size;
  }

  FORCE_INLINE void append_variable_value(const char *value, uint32_t len)
  {
    // dynamic growth
    if (UNLIKELY((real_data_size_ + len + variable_type_len_) > reserved_size_)) {
      // extreme scenarios, when encountering very long string
      uint32_t growth_size = g_config_value_.tsblock_mem_inc_step_size_ > len ?
        g_config_value_.tsblock_mem_inc_step_size_ : (len + 1);
      extend_memory(reserved_size_ + growth_size);
    }

    ASSERT(data_);
    // append len
    memcpy(&data_[real_data_size_], reinterpret_cast<char*>(&len), variable_type_len_);
    real_data_size_ += variable_type_len_;
    if (len > 0) {
      // append data
      memcpy(&data_[real_data_size_], value, len);
      real_data_size_ += len;
    }
  }

  FORCE_INLINE void append_fixed_value(const char *value, uint32_t len)
  {
    // dynamic growth
    if (UNLIKELY(real_data_size_ + len > reserved_size_)) {
      // extreme scenarios, when encountering very long string
      uint32_t growth_size = g_config_value_.tsblock_mem_inc_step_size_ > len ?
        g_config_value_.tsblock_mem_inc_step_size_ : (len + 1);
      extend_memory(reserved_size_ + growth_size);
    }

    ASSERT(data_);
    memcpy(&data_[real_data_size_], value, len);
    real_data_size_ += len;
  }

  // for fixed len value
  FORCE_INLINE char* read(uint32_t offset, uint32_t len)
  {
    ASSERT((offset + len) <= real_data_size_);
    char *p = &data_[offset];
    return p;
  }

  // for variable len value
  FORCE_INLINE char* read(uint32_t offset, uint32_t *len)
  {
    // get len
    *len = *reinterpret_cast<uint32_t*>(&data_[offset]);
    // get value
    char *p = &data_[offset + variable_type_len_];
    return p;
  }

  FORCE_INLINE char* get_data() { return data_; }

private:
  char         *data_;
  uint8_t      variable_type_len_;
  uint32_t     real_data_size_;
  uint32_t     reserved_size_;   // malloc memory size from system
};

}  // common
}  // timecho
#endif  // COMMON_CONTAINER_BYTE_BUFFER_H