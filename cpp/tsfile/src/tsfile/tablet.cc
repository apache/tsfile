
#include "tablet.h"
#include <stdlib.h>
#include "common/errno_define.h"

using namespace timecho::common;

namespace timecho
{
namespace storage
{

int Tablet::init()
{
  ASSERT(timestamps_ == NULL);
  timestamps_ = (int64_t*)malloc(sizeof(int64_t) * max_rows_);

  size_t schema_count = schema_vec_->size();
  std::pair<std::map<std::string, int>::iterator, bool> ins_res;
  for (size_t c = 0; c < schema_count; c++) {
    ins_res = schema_map_.insert(std::make_pair(schema_vec_->at(c).measurement_name_, c));
    if (!ins_res.second) {
      ASSERT(false);
      // maybe dup measurement_name
      return E_INVALID_ARG;
    }
  }
  ASSERT(schema_map_.size() == schema_count);

  value_matrix_ = (void**)malloc(sizeof(void*) * schema_count);
  for (size_t c = 0; c < schema_count; c++) {
    const MeasurementSchema &schema = schema_vec_->at(c);
    value_matrix_[c] = malloc(get_data_type_size(schema.data_type_) * max_rows_);
  }

  bitmaps_ = new BitMap[schema_count];
  for (size_t c = 0; c < schema_count; c++) {
    bitmaps_[c].init(max_rows_, /*init_as_zero=*/true);
  }
  return E_OK;
}

void Tablet::destroy()
{
  if (timestamps_ != NULL) {
    free(timestamps_);
    timestamps_ = NULL;
  }
  if (value_matrix_ != NULL) {
    for (size_t c = 0; c < schema_vec_->size(); c++) {
      free(value_matrix_[c]);
    }
    free(value_matrix_);
    value_matrix_ = NULL;
  }
  if (bitmaps_ != NULL) {
    delete[] bitmaps_;
  }
}

int Tablet::set_timestamp(int row_index, int64_t timestamp)
{
  ASSERT(timestamps_ != NULL);
  if (UNLIKELY(row_index >= max_rows_)) {
    ASSERT(false);
    return E_OUT_OF_RANGE;
  }
  timestamps_[row_index] = timestamp;
  return E_OK;
}

#define DO_SET_VALUE_BY_COL_NAME(row_index, measurement_name, val)               \
  do {                                                                           \
    SchemaMapIterator find_iter = schema_map_.find(measurement_name);            \
    if (LIKELY(find_iter == schema_map_.end())) {                               \
      ASSERT(false);                                                             \
      return E_INVALID_ARG;                                                      \
    }                                                                            \
    return set_value(row_index, find_iter->second, val);                         \
  } while (false)

#define DO_SET_VALUE_BY_COL_INDEX(row_index, schema_index, CppType, val)         \
  do {                                                                           \
    if (LIKELY(schema_index >= schema_vec_->size())) {                           \
      ASSERT(false);                                                             \
      return E_OUT_OF_RANGE;                                                     \
    }                                                                            \
    const MeasurementSchema &schema = schema_vec_->at(schema_index);             \
    if (LIKELY(GetDataTypeFromTemplateType<CppType>() != schema.data_type_)) {  \
      return E_TYPE_NOT_MATCH;                                                   \
    }                                                                            \
    CppType *column_values = (CppType*)value_matrix_[schema_index];              \
    column_values[row_index] = val;                                              \
    bitmaps_[schema_index].set(row_index); /* mark as non-null*/                 \
  } while (false)

int Tablet::set_value(int row_index, const std::string &measurement_name, bool val)
{
  DO_SET_VALUE_BY_COL_NAME(row_index, measurement_name, val);
}

int Tablet::set_value(int row_index, const std::string &measurement_name, int32_t val)
{
  DO_SET_VALUE_BY_COL_NAME(row_index, measurement_name, val);
}

int Tablet::set_value(int row_index, const std::string &measurement_name, int64_t val)
{
  DO_SET_VALUE_BY_COL_NAME(row_index, measurement_name, val);
}

int Tablet::set_value(int row_index, const std::string &measurement_name, float val)
{
  DO_SET_VALUE_BY_COL_NAME(row_index, measurement_name, val);
}

int Tablet::set_value(int row_index, const std::string &measurement_name, double val)
{
  DO_SET_VALUE_BY_COL_NAME(row_index, measurement_name, val);
}

int Tablet::set_value(int row_index, uint32_t schema_index, bool val)
{
  DO_SET_VALUE_BY_COL_INDEX(row_index, schema_index, bool, val);
  return E_OK;
}

int Tablet::set_value(int row_index, uint32_t schema_index, int32_t val)
{
  DO_SET_VALUE_BY_COL_INDEX(row_index, schema_index, int32_t, val);
  return E_OK;
}

int Tablet::set_value(int row_index, uint32_t schema_index, int64_t val)
{
  DO_SET_VALUE_BY_COL_INDEX(row_index, schema_index, int64_t, val);
  return E_OK;
}

int Tablet::set_value(int row_index, uint32_t schema_index, float val)
{
  DO_SET_VALUE_BY_COL_INDEX(row_index, schema_index, float, val);
  return E_OK;
}

int Tablet::set_value(int row_index, uint32_t schema_index, double val)
{
  DO_SET_VALUE_BY_COL_INDEX(row_index, schema_index, double, val);
  return E_OK;
}

} // end namespace storage
} // end namespace timecho

