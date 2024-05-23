#ifndef COMMON_READ_COMMON_ROW_RECORD_H
#define COMMON_READ_COMMON_ROW_RECORD_H

#include <sstream>
#include <vector>

#include "common/db_common.h"

namespace storage {
struct Field {
    Field() : type_(common::INVALID_DATATYPE) {}
    Field(common::TSDataType type) : type_(type) {}

    ~Field() {
        if (type_ == common::TEXT && value_.sval_) {
            free(value_.sval_);
        }
    }

    FORCE_INLINE void free_memory() {
        if (value_.sval_) {
            free(value_.sval_);
            value_.sval_ = nullptr;
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

    template <class T>
    FORCE_INLINE void set_value(common::TSDataType type, T val) {
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
            // case common::TEXT: {
            //   value_.sval_ = strdup(val);
            //   break;
            // }
            default: {
                std::cout << "unknown data type" << std::endl;
            }
        }
    }

   public:
    common::TSDataType type_;

    union {
        bool bval_;
        int64_t lval_;
        int32_t ival_;
        float fval_;
        double dval_;
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
            if ((*fields_)[i]->type_ == common::TEXT) {
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

   private:
    int64_t time_;                  // time value
    uint32_t col_num_;              // measurement num
    std::vector<Field *> *fields_;  // measurement value
};

}  // namespace storage

#endif  // COMMON_READ_COMMON_ROW_RECORD_H
