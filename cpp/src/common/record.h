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

#ifndef COMMON_RECORD_H
#define COMMON_RECORD_H

#include <string>
#include <vector>

#include "common/allocator/my_string.h"
#include "common/datatype/date_converter.h"
#include "common/db_common.h"
#include "utils/errno_define.h"

namespace storage {

// TODO: use std::move
// #define MOVE(var) std::move(var)

// TODO use common/allocator/my_string.h
struct TextType {
    char *buf_;
    int32_t len_;

    TextType() : buf_(nullptr), len_(0) {}
};

/*
 * DataPoint is a data value of one measurement of some device.
 */
struct DataPoint {
    bool isnull = false;
    std::string measurement_name_;
    union {
        bool bool_val_;
        int32_t i32_val_;
        int64_t i64_val_;
        float float_val_;
        double double_val_;
        common::String *str_val_;
    } u_;
    TextType text_val_;

    DataPoint(const std::string &measurement_name, bool b)
        : measurement_name_(measurement_name), text_val_() {
        u_.bool_val_ = b;
    }

    DataPoint(const std::string &measurement_name, int32_t i32)
        : measurement_name_(measurement_name), text_val_() {
        u_.i32_val_ = i32;
    }

    DataPoint(const std::string &measurement_name, int64_t i64)
        : measurement_name_(measurement_name), text_val_() {
        u_.i64_val_ = i64;
    }

    DataPoint(const std::string &measurement_name, float f)
        : measurement_name_(measurement_name), text_val_() {
        u_.float_val_ = f;
    }

    DataPoint(const std::string &measurement_name, double d)
        : measurement_name_(measurement_name), text_val_() {
        u_.double_val_ = d;
    }

    DataPoint(const std::string &measurement_name, common::String &str,
              common::PageArena &pa)
        : measurement_name_(measurement_name), text_val_() {
        char *p_buf = (char *)pa.alloc(sizeof(common::String));
        u_.str_val_ = new (p_buf) common::String();
        u_.str_val_->dup_from(str, pa);
    }

    // DataPoint(const std::string &measurement_name, Text &text),
    //   : measurement_name_(measurement_name),
    //     data_type_(common::TEXT),
    //     text_val_(text) {}

    DataPoint(const std::string &measurement_name)
        : isnull(true), measurement_name_(measurement_name) {}
    void set_i32(int32_t i32) {
        u_.i32_val_ = i32;
        isnull = false;
    }
    void set_i64(int64_t i64) {
        u_.i64_val_ = i64;
        isnull = false;
    }
    void set_float(float f) {
        u_.float_val_ = f;
        isnull = false;
    }
    void set_double(double d) {
        u_.double_val_ = d;
        isnull = false;
    }
};

struct TsRecord {
    int64_t timestamp_;
    std::string device_id_;
    std::vector<DataPoint> points_;
    common::PageArena pa;

    TsRecord(const std::string &device_name) : device_id_(device_name) {
        pa.init(512, common::MOD_TSFILE_READER);
    }

    TsRecord(int64_t timestamp, const std::string &device_name,
             int32_t point_count_in_row = 0)
        : timestamp_(timestamp), device_id_(device_name), points_() {
        if (point_count_in_row > 0) {
            points_.reserve(point_count_in_row);
        }
    }

    template <typename T>
    int add_point(const std::string &measurement_name, T val) {
        int ret = common::E_OK;
        points_.emplace_back(DataPoint(measurement_name, val));
        return ret;
    }
};

template <>
inline int TsRecord::add_point(const std::string &measurement_name,
                               common::String val) {
    int ret = common::E_OK;
    points_.emplace_back(DataPoint(measurement_name, val, pa));
    return ret;
}

template <>
inline int TsRecord::add_point(const std::string &measurement_name,
                               std::tm val) {
    int ret = common::E_OK;
    int data_int;
    if (RET_SUCC(common::DateConverter::date_to_int(val, data_int))) {
        points_.emplace_back(DataPoint(measurement_name, data_int));
    }
    return ret;
}

}  // end namespace storage
#endif  // COMMON_RECORD_H
