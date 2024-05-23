#include "tuple_desc.h"

namespace common {
uint32_t TupleDesc::get_single_row_len(int *erro_code) {
    int size = get_column_count();
    int totol_len = 0;
    for (int i = 0; i < size; ++i) {
        switch (column_list_[i].type_) {
            case common::BOOLEAN: {
                totol_len += sizeof(bool);
                break;
            }
            case common::INT32: {
                totol_len += sizeof(int32_t);
                break;
            }
            case common::INT64: {
                totol_len += sizeof(int64_t);
                break;
            }
            case common::FLOAT: {
                totol_len += sizeof(float);
                break;
            }
            case common::DOUBLE: {
                totol_len += sizeof(double);
                break;
            }
            case common::TEXT: {
                totol_len += DEFAULT_RESERVED_SIZE_OF_TEXT + TEXT_LEN;
                break;
            }
            default: {
                // log_err("TsBlock::BuildVector unknown type %d",
                // static_cast<int>(column_list_[i].type_));
                *erro_code = E_TYPE_NOT_SUPPORTED;
                break;
            }
        }
    }
    return totol_len;
}

uint32_t get_len(TSDataType type) {
    switch (type) {
        case common::BOOLEAN: {
            return sizeof(bool);
        }
        case common::INT32: {
            return sizeof(int32_t);
        }
        case common::INT64: {
            return sizeof(int64_t);
        }
        case common::FLOAT: {
            return sizeof(float);
        }
        case common::DOUBLE: {
            return sizeof(double);
        }
        case common::TEXT: {
            return DEFAULT_RESERVED_SIZE_OF_TEXT + TEXT_LEN;
        }
        default: {
            // todo: log error
        }
    }
    return 0;
}
}  // namespace common
