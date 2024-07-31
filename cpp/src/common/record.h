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

#include "common/db_common.h"

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
    common::TSDataType data_type_;
    union {
        bool bool_val_;
        int32_t i32_val_;
        int64_t i64_val_;
        float float_val_;
        double double_val_;
    } u_;
    TextType text_val_;

    DataPoint(const std::string &measurement_name, bool b)
        : measurement_name_(measurement_name),
          data_type_(common::BOOLEAN),
          text_val_() {
        u_.bool_val_ = b;
    }

    DataPoint(const std::string &measurement_name, int32_t i32)
        : measurement_name_(measurement_name),
          data_type_(common::INT32),
          text_val_() {
        u_.i32_val_ = i32;
    }

    DataPoint(const std::string &measurement_name, int64_t i64)
        : measurement_name_(measurement_name),
          data_type_(common::INT64),
          text_val_() {
        u_.i64_val_ = i64;
    }

    DataPoint(const std::string &measurement_name, float f)
        : measurement_name_(measurement_name),
          data_type_(common::FLOAT),
          text_val_() {
        u_.float_val_ = f;
    }

    DataPoint(const std::string &measurement_name, double d)
        : measurement_name_(measurement_name),
          data_type_(common::DOUBLE),
          text_val_() {
        u_.double_val_ = d;
    }

    // DataPoint(const std::string &measurement_name, Text &text),
    //   : measurement_name_(measurement_name),
    //     data_type_(common::TEXT),
    //     text_val_(text) {}

    DataPoint(const std::string &measurement_name)
        : isnull(true), measurement_name_(measurement_name) {}
    void set_i32(int32_t i32) {
        data_type_ = common::INT32;
        u_.i32_val_ = i32;
        isnull = false;
    }
    void set_i64(int64_t i64) {
        data_type_ = common::INT64;
        u_.i64_val_ = i64;
        isnull = false;
    }
    void set_float(float f) {
        data_type_ = common::FLOAT;
        u_.float_val_ = f;
        isnull = false;
    }
    void set_double(double d) {
        data_type_ = common::DOUBLE;
        u_.double_val_ = d;
        isnull = false;
    }
};

struct TsRecord {
    int64_t timestamp_;
    std::string device_name_;
    std::vector<DataPoint> points_;

    TsRecord(const std::string &device_name) : device_name_(device_name) {}

    TsRecord(int64_t timestamp, const std::string &device_name,
             int32_t point_count_in_row = 0)
        : timestamp_(timestamp), device_name_(device_name), points_() {
        if (point_count_in_row > 0) {
            points_.reserve(point_count_in_row);
        }
    }

    void append_data_point(const DataPoint &point) {
        // points_.emplace_back(point); C++11
        points_.push_back(point);
    }
};

}  // end namespace storage
#endif  // COMMON_RECORD_H
