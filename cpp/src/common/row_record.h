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
#ifndef COMMON_READ_COMMON_ROW_RECORD_H
#define COMMON_READ_COMMON_ROW_RECORD_H

#include <sstream>
#include <vector>

#include "common/allocator/my_string.h"
#include "common/datatype/date_converter.h"
#include "common/db_common.h"

namespace storage {
struct Field {
    Field() : type_(common::INVALID_DATATYPE) {}
    Field(common::TSDataType type) : type_(type) {}

    ~Field() { free_memory(); }

    FORCE_INLINE void free_memory() {
        if (type_ == common::BLOB || type_ == common::TEXT ||
            type_ == common::STRING) {
            if (value_.strval_ != nullptr) {
                delete value_.strval_;
                value_.strval_ = nullptr;
            }
        }
    }

    FORCE_INLINE bool is_type(common::TSDataType type) const {
        return type == type_;
    }

    FORCE_INLINE bool is_literal() const {
        return is_type(common::BOOLEAN) || is_type(common::DOUBLE) ||
               is_type(common::TEXT) || is_type(common::INT64) ||
               is_type(common::NULL_TYPE);
    }

    void set_value(common::TSDataType type, void *val, size_t len,
                   common::PageArena &pa) {
        if (val == nullptr) {
            type_ = common::NULL_TYPE;
            return;
        }
        type_ = type;
        switch (type) {
            case common::BOOLEAN: {
                value_.bval_ = *(bool *)val;
                break;
            }
            case common::DATE:
            case common::INT32: {
                value_.ival_ = *(int32_t *)val;
                break;
            }
            case common::TIMESTAMP:
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
            case common::TEXT:
            case common::BLOB:
            case common::STRING: {
                value_.strval_ = new common::String();
                value_.strval_->dup_from(
                    std::string(static_cast<char *>(val), len), pa);
                break;
            }
            default: {
                assert(false);
                std::cout << "unknown data type" << std::endl;
            }
        }
    }

    template <typename T>
    FORCE_INLINE T get_value() {
        switch (type_) {
            case common::TSDataType::BOOLEAN:
                return value_.bval_;
            case common::TSDataType::INT32:
                return value_.ival_;
            case common::TSDataType::TIMESTAMP:
            case common::TSDataType::INT64:
                return value_.lval_;
            case common::TSDataType::FLOAT:
                return value_.fval_;
            case common::TSDataType::DOUBLE:
                return value_.dval_;
            default:
                std::cout << "unknown data type" << std::endl;
                break;
        }
        return -1;  // when data type is unknown
    }

    FORCE_INLINE std::tm get_date_value() {
        std::tm date_value{};
        if (type_ == common::DATE) {
            common::DateConverter::int_to_date(value_.ival_, date_value);
            return date_value;
        }
        return date_value;
    }

    FORCE_INLINE common::String *get_string_value() {
        if (type_ == common::STRING || type_ == common::TEXT ||
            type_ == common::BLOB) {
            return value_.strval_;
        } else {
            return nullptr;
        }
    }

   public:
    common::TSDataType type_;
    std::string column_name;
    union {
        bool bval_;
        int64_t lval_;
        int32_t ival_;
        float fval_;
        double dval_;
        common::String *strval_;
        char *sval_;
    } value_;
};

FORCE_INLINE Field *make(common::TSDataType type) {
    Field *value = new Field(type);
    return value;
}

FORCE_INLINE Field *make_literal(int64_t val) {
    Field *value = new Field(common::INT64);
    value->value_.lval_ = val;
    return value;
}

FORCE_INLINE Field *make_literal(double val) {
    Field *value = new Field(common::DOUBLE);
    value->value_.dval_ = val;
    return value;
}

FORCE_INLINE Field *make_literal(char *string) {
    Field *value = new Field(common::TEXT);
    value->value_.sval_ = string;
    return value;
}

FORCE_INLINE Field *make_literal(bool val) {
    Field *value = new Field(common::BOOLEAN);
    value->value_.bval_ = val;
    return value;
}

FORCE_INLINE Field *make_null_literal() {
    Field *value = new Field(common::NULL_TYPE);
    return value;
}

class RowRecord {
   public:
    explicit RowRecord(uint32_t col_num) : col_num_(col_num) {
        fields_ = new std::vector<Field *>();
        fields_->reserve(col_num);
        for (uint32_t i = 0; i < col_num; ++i) {
            Field *val = make_null_literal();
            fields_->push_back(val);
        }
    }

    RowRecord(int64_t time, uint32_t col_num) : time_(time), col_num_(col_num) {
        fields_ = new std::vector<Field *>();
        fields_->reserve(col_num_);
        for (uint32_t i = 0; i < col_num_; ++i) {
            Field *val = make_null_literal();
            fields_->push_back(val);
        }
    }

    ~RowRecord() {
        if (fields_) {
            int size = fields_->size();
            for (int i = 0; i < size; ++i) {
                delete fields_->at(i);
            }
            delete fields_;
        }
    }

    FORCE_INLINE void reset() {
        for (uint32_t i = 0; i < col_num_; ++i) {
            if ((*fields_)[i]->type_ == common::TEXT ||
                (*fields_)[i]->type_ == common::BLOB ||
                (*fields_)[i]->type_ == common::STRING) {
                (*fields_)[i]->free_memory();
            }
            (*fields_)[i]->type_ = common::NULL_TYPE;
        }
    }

    FORCE_INLINE void add_field(Field *field) { fields_->push_back(field); }

    FORCE_INLINE void set_timestamp(int64_t time) { time_ = time; }

    FORCE_INLINE int64_t get_timestamp() { return time_; }

    FORCE_INLINE Field *get_field(uint32_t index) { return (*fields_)[index]; }

    FORCE_INLINE std::vector<Field *> *get_fields() { return fields_; }

    FORCE_INLINE uint32_t get_col_num() { return col_num_; }

   private:
    int64_t time_;                  // time value
    uint32_t col_num_;              // measurement num
    std::vector<Field *> *fields_;  // measurement value
};

}  // namespace storage

#endif  // COMMON_READ_COMMON_ROW_RECORD_H
