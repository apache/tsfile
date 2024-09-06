/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef COMMON_DATATYPE_VALUE_H
#define COMMON_DATATYPE_VALUE_H

#include <float.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <utility>
#include <vector>

#include "common/logger/elog.h"
#include "utils/db_utils.h"

namespace common {

struct Value {
    Value(TSDataType type) : type_(type) {
        switch (type) {
            case BOOLEAN:
                value_.bval_ = false;
                break;
            case INT32:
                value_.ival_ = 0;
                break;
            case INT64:
                value_.lval_ = 0;
                break;
            case FLOAT:
                value_.fval_ = 0.0f;
                break;
            case DOUBLE:
                value_.dval_ = 0.0;
                break;
            case TEXT:
                value_.sval_ = nullptr;
                break;
            case NULL_TYPE:
                break;
            default:
                LOGE("unknown data type");
        }
    }

    ~Value() {
        if (is_type(TEXT) && value_.sval_) {
            free(value_.sval_);
        }
    }

    FORCE_INLINE void free_memory() {
        if (is_type(TEXT) && value_.sval_) {
            free(value_.sval_);
            value_.sval_ = nullptr;
        }
    }

    FORCE_INLINE bool is_type(TSDataType type) const { return type == type_; }

    FORCE_INLINE bool is_literal() const {
        return is_type(BOOLEAN) || is_type(DOUBLE) || is_type(TEXT) ||
               is_type(INT64) || is_type(NULL_TYPE);
    }

    template <class T>
    FORCE_INLINE void set_value(TSDataType type, T val) {
        type_ = type;
        switch (type) {
            case common::BOOLEAN: {
                value_.bval_ = *(bool *)val;
                break;
            }
            case common::INT32: {
                value_.ival_ = *(int32_t *)val;
                break;
            }
            case common::INT64: {
                value_.lval_ = *(int64_t *)val;
                break;
            }
            case common::FLOAT: {
                value_.fval_ = *(float *)val;
                break;
            }
            case common::DOUBLE: {
                value_.dval_ = *(double *)val;
                break;
            }
            case common::TEXT: {
                value_.sval_ = strdup((const char *)val);
                break;
            }
            default: {
                LOGE("unknown data type");
            }
        }
    }

   public:
    TSDataType type_;

    union {
        bool bval_;
        int64_t lval_;
        int32_t ival_;
        float fval_;
        double dval_;
        char *sval_;
    } value_;
};

FORCE_INLINE Value *make(TSDataType type) {
    Value *value = new Value(type);
    return value;
}

FORCE_INLINE Value *make_literal(int64_t val) {
    Value *value = new Value(INT64);
    value->value_.lval_ = val;
    return value;
}

FORCE_INLINE Value *make_literal(double val) {
    Value *value = new Value(DOUBLE);
    value->value_.dval_ = val;
    return value;
}

FORCE_INLINE Value *make_literal(char *string) {
    Value *value = new Value(TEXT);
    value->value_.sval_ = strdup(string);
    return value;
}

FORCE_INLINE Value *make_literal(bool val) {
    Value *value = new Value(BOOLEAN);
    value->value_.bval_ = val;
    return value;
}

FORCE_INLINE Value *make_null_literal() {
    Value *value = new Value(NULL_TYPE);
    return value;
}

FORCE_INLINE std::string value_to_string(Value *value) {
    if (value->type_ == common::TEXT) {
        return std::string(value->value_.sval_);
    } else {
        std::stringstream ss;
        switch (value->type_) {
            case common::BOOLEAN:
                ss << (value->value_.bval_ ? "true" : "false");
                break;
            case common::INT32:
            case common::INT64:
                ss << value->value_.lval_;
                break;
            case common::FLOAT:
            case common::DOUBLE:
                ss << value->value_.dval_;
                break;
            case common::NULL_TYPE:
                ss << "NULL";
                break;
            default:
                ASSERT(false);
                break;
        }
        return ss.str();
    }
}

// return true if cast succ.
template <typename T>
FORCE_INLINE int get_typed_data_from_value(Value *value, T &ret_data) {
    return E_OK;
}

template <>
FORCE_INLINE int get_typed_data_from_value<bool>(Value *value, bool &ret_data) {
    ret_data = value->value_.bval_;
    return (value->type_ == BOOLEAN) ? E_OK : E_TYPE_NOT_MATCH;
}

template <>
FORCE_INLINE int get_typed_data_from_value<int32_t>(Value *value,
                                                    int32_t &ret_data) {
    if (value->value_.lval_ > INT32_MAX || value->value_.lval_ < INT32_MIN) {
        return E_OVERFLOW;
    }
    ret_data = (int32_t)value->value_.lval_;
    // number value will be parsed as INT64
    return (value->type_ == INT64) ? E_OK : E_TYPE_NOT_MATCH;
}

template <>
FORCE_INLINE int get_typed_data_from_value<int64_t>(Value *value,
                                                    int64_t &ret_data) {
    ret_data = value->value_.lval_;
    return (value->type_ == INT64) ? E_OK : E_TYPE_NOT_MATCH;
}

template <>
FORCE_INLINE int get_typed_data_from_value<float>(Value *value,
                                                  float &ret_data) {
    if (value->value_.dval_ > FLT_MAX || value->value_.dval_ < FLT_MIN) {
        return E_OVERFLOW;
    }
    ret_data = (float)value->value_.dval_;
    return (value->type_ == DOUBLE) ? E_OK : E_TYPE_NOT_MATCH;
}

template <>
FORCE_INLINE int get_typed_data_from_value<double>(Value *value,
                                                   double &ret_data) {
    ret_data = value->value_.dval_;
    return (value->type_ == DOUBLE) ? E_OK : E_TYPE_NOT_MATCH;
}

// template<> FORCE_INLINE int get_typed_data_from_value<Text>(Value *value,
// Text &ret_data)
// {
//   ret_data.buf_ = value->value_.sval_;
//   ret_data.len_ = strlen(ret_data.buf_);
//   return value->type_ == TEXT;
// }

}  // namespace common

#endif  // COMMON_DATATYPE_VALUE_H