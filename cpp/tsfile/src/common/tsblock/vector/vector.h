#ifndef COMMON_TSBLOCK_VECTOR_VECTOR_H
#define COMMON_TSBLOCK_VECTOR_VECTOR_H

#include "common/container/bit_map.h"
#include "common/container/byte_buffer.h"
#include "common/db_utils.h"
#include "common/util_define.h"
#include "common/allocator/page_arena.h"

namespace timecho
{
namespace common
{

class TsBlock;

class Vector
{
public:
  Vector(common::TSDataType type, uint32_t max_row_num, common::TsBlock *tsblock)
    : has_null_(false), type_(type), max_row_num_(max_row_num), row_num_(0), offset_(0), tsblock_(tsblock)
    {
      nulls_.init(max_row_num, true);
    }

  virtual ~Vector() { }

  // len == 0 is '' or ""
  virtual void append(const char *value, uint32_t len) = 0;

  virtual char* read(uint32_t* __restrict len, bool* __restrict null, uint32_t rowid) = 0;

  // For ColIterator, it is known that no null value exists in this column
  virtual char* read(uint32_t *len) = 0;

  virtual void reset() = 0;

  virtual void update_offset() = 0;

  FORCE_INLINE void reset_offset() { offset_ = 0; } 

  FORCE_INLINE bool is_null(uint32_t row_id) { return nulls_.test(row_id); }

  FORCE_INLINE void set_null(uint32_t row_id)
  {
    nulls_.set(row_id);
    has_null_ = true;
  }

  FORCE_INLINE void set_vector_type(common::TSDataType type) { type_ = type; }

  FORCE_INLINE common::TSDataType get_vector_type() { return type_; }

  FORCE_INLINE uint32_t get_row_num() { return row_num_; }

  FORCE_INLINE common::TsBlock* get_tsblock() { return tsblock_; }

  FORCE_INLINE bool has_null() { return has_null_; }

// We want derived class to have access to base class members, so it is defined as protected
protected:
  bool               has_null_;     // mark whether there is a null value
  common::TSDataType type_;
  uint32_t           max_row_num_;  // max row num
  uint32_t           row_num_;      // real row num
  uint32_t           offset_;

  common::TsBlock    *tsblock_;     // parent tsblock
  common::BitMap     nulls_;        // null bit value
  common::ByteBuffer values_;       // real user data
};
}  // common
}  // timecho

#endif // COMMON_TSBLOCK_VECTOR_VECTOR_H
