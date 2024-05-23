#ifndef COMMON_TSBLOCK_VECTOR_FIX_LENGTH_VECTOR_H
#define COMMON_TSBLOCK_VECTOR_FIX_LENGTH_VECTOR_H

#include "vector.h"

namespace timecho
{
namespace common
{
class FixedLengthVector : public Vector
{
public:
  FixedLengthVector(common::TSDataType type, uint32_t max_row_num,
      uint32_t type_size, common::TsBlock *tsblock) :
    Vector(type, max_row_num, tsblock), type_len_(type_size)
  {
    values_.init(type_size * max_row_num);
  }

  ~FixedLengthVector() { }

 // cppcheck-suppress missingOverride
  FORCE_INLINE void reset() OVERRIDE
  {
    has_null_ = false;
    row_num_ = 0;
    offset_ = 0;
    nulls_.reset();
    values_.reset();
  }

  // cppcheck-suppress missingOverride
  FORCE_INLINE void update_offset() OVERRIDE
  {
    offset_ += type_len_;
  }

  // cppcheck-suppress missingOverride
  FORCE_INLINE void append(const char *value, uint32_t len) OVERRIDE
  {
    values_.append_fixed_value(value, len);
  }

  // cppcheck-suppress missingOverride
  FORCE_INLINE char* read(uint32_t* __restrict len, bool* __restrict null, uint32_t rowid) OVERRIDE
  {
    *len = type_len_;
    if (UNLIKELY(has_null_)) {
      *null = nulls_.test(rowid);
    } else {
      *null = false;
    }
    if (LIKELY(!(*null))) {
      char *result = values_.read(offset_, type_len_);
      return result;
    } else {
      return nullptr;
    }
  }

  // cppcheck-suppress missingOverride
  FORCE_INLINE char* read(uint32_t *len) OVERRIDE
  {
    *len = type_len_;
    char *result = values_.read(offset_, type_len_);
    return result;
  }

private:
  uint32_t type_len_;
};

}  // common
}  // timecho

#endif  // COMMON_TSBLOCK_VECTOR_FIX_LENGTH_VECTOR_H
