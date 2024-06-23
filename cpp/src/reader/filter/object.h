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
#ifndef READER_FILTER_BASIC_OBJECT_H
#define READER_FILTER_BASIC_OBJECT_H

#include "common/db_common.h"
#include "string.h"

namespace storage {

class Object {
   public:
    union Value {
        bool bval_;
        int32_t ival_;
        int64_t lval_;
        float fval_;
        double dval_;
        char *sval_;
    } values_;

    Object() {}
    ~Object() {}

    Object(const bool &val) {
        type_ = common::BOOLEAN;
        values_.bval_ = val;
    }

    Object(const int32_t &val) {
        type_ = common::INT32;
        values_.ival_ = val;
    }

    Object(const int64_t &val) {
        type_ = common::INT64;
        values_.lval_ = val;
    }

    Object(const float &val) {
        type_ = common::FLOAT;
        values_.fval_ = val;
    }

    Object(const double &val) {
        type_ = common::DOUBLE;
        values_.dval_ = val;
    }

    Object(char *val) {
        type_ = common::TEXT;
        values_.sval_ = val;
    }

    Object(const std::string &val) {
        type_ = common::TEXT;
        values_.sval_ = const_cast<char *>(val.c_str());
    }

    bool equals(const Object &object) {
        if (object.get_type() != type_) {
            return false;
        }
        switch (object.get_type()) {
            case common::BOOLEAN:
                return values_.bval_ == object.values_.bval_;
            case common::INT32:
                return values_.ival_ == object.values_.ival_;
            case common::INT64:
                return values_.lval_ == object.values_.lval_;
            case common::FLOAT:
                return values_.fval_ == object.values_.fval_;
            case common::DOUBLE:
                return values_.dval_ == object.values_.dval_;
            case common::TEXT:
                return !strcmp(values_.sval_, object.values_.sval_);
            default:
                return false;
        }
    }

    bool operator==(const Object &object) const {
        if (object.get_type() != type_) {
            return false;
        }
        switch (object.get_type()) {
            case common::BOOLEAN:
                return values_.bval_ == object.values_.bval_;
            case common::INT32:
                return values_.ival_ == object.values_.ival_;
            case common::INT64:
                return values_.lval_ == object.values_.lval_;
            case common::FLOAT:
                return values_.fval_ == object.values_.fval_;
            case common::DOUBLE:
                return values_.dval_ == object.values_.dval_;
            case common::TEXT:
                return !strcmp(values_.sval_, object.values_.sval_);
            default:
                return false;
        }
    }

    bool operator!=(const Object &object) const {
        if (object.get_type() != type_) {
            return false;
        }
        switch (object.get_type()) {
            case common::BOOLEAN:
                return values_.bval_ != object.values_.bval_;
            case common::INT32:
                return values_.ival_ != object.values_.ival_;
            case common::INT64:
                return values_.lval_ != object.values_.lval_;
            case common::FLOAT:
                return values_.fval_ != object.values_.fval_;
            case common::DOUBLE:
                return values_.dval_ != object.values_.dval_;
            case common::TEXT:
                return strcmp(values_.sval_, object.values_.sval_);
            default:
                return false;
        }
    }

    bool operator<(const Object &object) const {
        if (object.get_type() != type_) {
            return false;
        }
        switch (object.get_type()) {
            case common::BOOLEAN:
                return values_.bval_ < object.values_.bval_;
            case common::INT32:
                return values_.ival_ < object.values_.ival_;
            case common::INT64:
                return values_.lval_ < object.values_.lval_;
            case common::FLOAT:
                return values_.fval_ < object.values_.fval_;
            case common::DOUBLE:
                return values_.dval_ < object.values_.dval_;
            case common::TEXT:
                return strcmp(values_.sval_, object.values_.sval_) < 0;
            default:
                return false;
        }
    }

    bool operator<=(const Object &object) const {
        if (object.get_type() != type_) {
            return false;
        }
        switch (object.get_type()) {
            case common::BOOLEAN:
                return values_.bval_ <= object.values_.bval_;
            case common::INT32:
                return values_.ival_ <= object.values_.ival_;
            case common::INT64:
                return values_.lval_ <= object.values_.lval_;
            case common::FLOAT:
                return values_.fval_ <= object.values_.fval_;
            case common::DOUBLE:
                return values_.dval_ <= object.values_.dval_;
            case common::TEXT:
                return strcmp(values_.sval_, object.values_.sval_) <= 0;
            default:
                return false;
        }
    }

    bool operator>(const Object &object) const {
        if (object.get_type() != type_) {
            return false;
        }
        switch (object.get_type()) {
            case common::BOOLEAN:
                return values_.bval_ > object.values_.bval_;
            case common::INT32:
                return values_.ival_ > object.values_.ival_;
            case common::INT64:
                return values_.lval_ > object.values_.lval_;
            case common::FLOAT:
                return values_.fval_ > object.values_.fval_;
            case common::DOUBLE:
                return values_.dval_ > object.values_.dval_;
            case common::TEXT:
                return strcmp(values_.sval_, object.values_.sval_) > 0;
            default:
                return false;
        }
    }

    bool operator>=(const Object &object) const {
        if (object.get_type() != type_) {
            return false;
        }
        switch (object.get_type()) {
            case common::BOOLEAN:
                return values_.bval_ >= object.values_.bval_;
            case common::INT32:
                return values_.ival_ >= object.values_.ival_;
            case common::INT64:
                return values_.lval_ >= object.values_.lval_;
            case common::FLOAT:
                return values_.fval_ >= object.values_.fval_;
            case common::DOUBLE:
                return values_.dval_ >= object.values_.dval_;
            case common::TEXT:
                return strcmp(values_.sval_, object.values_.sval_) >= 0;
            default:
                return false;
        }
    }

    friend bool operator>=(const Object &object1, const Object &objec2) {
        return object1.operator>=(objec2);
    }
    friend bool operator>(const Object &object1, const Object &objec2) {
        return object1.operator>(objec2);
    }
    friend bool operator==(const Object &object1, const Object &objec2) {
        return object1.operator==(objec2);
    }
    friend bool operator<=(const Object &object1, const Object &objec2) {
        return object1.operator<=(objec2);
    }
    friend bool operator<(const Object &object1, const Object &objec2) {
        return object1.operator<(objec2);
    }
    friend bool operator!=(const Object &object1, const Object &objec2) {
        return object1.operator!=(objec2);
    }

    bool operator>=(const int64_t &time) const {}
    bool operator<=(const int64_t &time) const {}
    bool operator==(const int64_t &time) const {}
    bool operator>(const int64_t &time) const {}
    bool operator<(const int64_t &time) const {}
    bool operator!=(const int64_t &time) const {}

    FORCE_INLINE const common::TSDataType get_type() const { return type_; }

   private:
    common::TSDataType type_;
};

}  // namespace storage

#endif  // READER_FILTER_BASIC_OBJECT_H
