#include "tuple_desc.h"

namespace timecho
{
namespace common
{
  uint32_t TupleDesc::get_single_row_len(int *erro_code)
  {
    int size = get_column_count();
    int totol_len = 0;
    for (int i = 0; i < size; ++i) {
      switch (column_list_[i].type_) {
      case timecho::common::BOOLEAN: {
        totol_len += sizeof(bool);
        break;
      }
      case timecho::common::INT32: {
        totol_len += sizeof(int32_t);
        break;
      }
      case timecho::common::INT64: {
        totol_len += sizeof(int64_t);
        break;
      }
      case timecho::common::FLOAT: {
        totol_len += sizeof(float);
        break;
      }
      case timecho::common::DOUBLE: {
        totol_len += sizeof(double);
        break;
      }
      case timecho::common::TEXT: {
        totol_len += DEFAULT_RESERVED_SIZE_OF_TEXT + TEXT_LEN;
        break;
      }
      default: {
        log_error("TsBlock::BuildVector unknown type %d", static_cast<int>(column_list_[i].type_));
        *erro_code = E_TYPE_NOT_SUPPORTED;
        break;
      }
      }
    }
    return totol_len;
  }

  uint32_t get_len(TSDataType type)
  {
    switch (type)
    {
    case timecho::common::BOOLEAN: {
      return sizeof(bool);
    }
    case timecho::common::INT32: {
      return sizeof(int32_t);
    }
    case timecho::common::INT64: {
      return sizeof(int64_t);
    }
    case timecho::common::FLOAT: {
      return sizeof(float);
    }
    case timecho::common::DOUBLE: {
      return sizeof(double);
    }
    case timecho::common::TEXT: {
      return DEFAULT_RESERVED_SIZE_OF_TEXT + TEXT_LEN;
    }
    default: {
      // todo: log error
    }
    }
    return 0;
  }
}  // common
}  // timecho
