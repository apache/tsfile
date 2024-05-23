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
#ifndef UTILS_STORAGE_UTILS_H
#define UTILS_STORAGE_UTILS_H

#include <inttypes.h>
#include <stdint.h>

#include "common/datatype/value.h"
#include "common/tsblock/tsblock.h"
#include "utils/db_utils.h"

namespace storage {

struct InsertContext {
    bool has_time_val_;
    common::TsBlock *tsblock_;
    common::TupleDesc *tuple_desc_;
    std::vector<std::vector<common::Value *> *> *values_;

    // add device name, etc

    explicit InsertContext(common::TsBlock *tsblock)
        : has_time_val_(false),
          tsblock_(tsblock),
          tuple_desc_(nullptr),
          values_(nullptr) {}

    InsertContext(bool has_time_val, common::TupleDesc *tuple_desc,
                  std::vector<std::vector<common::Value *> *> *values)
        : has_time_val_(has_time_val),
          tsblock_(nullptr),
          tuple_desc_(tuple_desc),
          values_(values) {}

    FORCE_INLINE bool use_tsblock() const { return tsblock_ != nullptr; }
};

struct InsertResult {
   public:
    InsertResult() : succ_rows_(0), has_err_(false), err_msg_() {}
    void set_err_msg(const std::string &e) {
        has_err_ = true;
        err_msg_ = e;
    }
    FORCE_INLINE bool has_error() const { return has_err_; }
    void inc_succ_rows() { succ_rows_++; }
    int32_t get_succ_rows() const { return succ_rows_; }

   private:
    int32_t succ_rows_;
    bool has_err_;
    std::string err_msg_;
};

FORCE_INLINE std::string get_file_path_from_file_id(
    const common::FileID &file_id) {
    // TODO prefix len + number len
    const int len = 256;
    char path_buf[len];
    memset(path_buf, 0, len);
    // TODO config
    snprintf(path_buf, len, "./%" PRId64 "-%d-%d.tsfile", file_id.seq_,
             file_id.version_, file_id.merge_);
    return std::string(path_buf);
}

}  // end namespace storage

#endif  // UTILS_STORAGE_UTILS_H
