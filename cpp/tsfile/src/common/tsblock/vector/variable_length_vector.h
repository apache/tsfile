#ifndef COMMON_TSBLOCK_VECTOR_VARIABLE_LENGTH_VECTOR_H
#define COMMON_TSBLOCK_VECTOR_VARIABLE_LENGTH_VECTOR_H

#include "vector.h"

namespace timecho
{
namespace common
{
class VariableLengthVector : public Vector
{
public:
  VariableLengthVector(common::TSDataType type, uint32_t max_row_num,
      uint32_t type_size, common::TsBlock *tsblock) : Vector(type, max_row_num, tsblock),
      variable_type_len_(sizeof(uint32_t)), last_value_len_(0)
  {
    values_.init(type_size * max_row_num);
  }

  ~VariableLengthVector() {}
  // cppcheck-suppress missingOverride
  FORCE_INLINE void reset() OVERRIDE
  {
    last_value_len_ = 0;
    has_null_ = false;
    row_num_ = 0;
    offset_ = 0;
    nulls_.reset();
    values_.reset();
  }

  // cppcheck-suppress missingOverride
  FORCE_INLINE void update_offset() OVERRIDE
  {
    offset_ += variable_type_len_;
    offset_ += last_value_len_;
  }

  // cppcheck-suppress missingOverride
  FORCE_INLINE void append(const char *value, uint32_t len) OVERRIDE
  {
    values_.append_variable_value(value, len);
  }

  // cppcheck-suppress missingOverride
  FORCE_INLINE char* read(uint32_t* __restrict len, bool* __restrict null, uint32_t rowid) OVERRIDE
  {
    if (UNLIKELY(has_null_)) {
      *null = nulls_.test(rowid);
    } else {
      *null = false;
    }
    if (LIKELY(!(*null))) {
      char *result = values_.read(offset_, len);
      last_value_len_ = *len;
      return result;
    } else {
      return nullptr;
    }
  }

  // cppcheck-suppress missingOverride
  FORCE_INLINE char* read(uint32_t *len) OVERRIDE
  {
    char *result = values_.read(offset_, len);
    last_value_len_ = *len;
    return result;
  }

private:
  uint8_t  variable_type_len_;
  uint32_t last_value_len_;
};
}
}

#endif  // COMMON_TSBLOCK_VECTOR_VARIABLE_LENGTH_VECTOR_H
