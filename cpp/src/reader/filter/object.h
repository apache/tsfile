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

    FORCE_INLINE const common::TSDataType get_type() const { return type_; }

   private:
    common::TSDataType type_;
};

}  // namespace storage
}  // namespace timecho

#endif  // READER_FILTER_BASIC_OBJECT_H
