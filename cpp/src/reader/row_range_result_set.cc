/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#include "reader/row_range_result_set.h"

namespace storage {

RowRangeResultSet::RowRangeResultSet(ResultSet* inner, int offset, int limit)
    : inner_(inner),
      offset_(offset < 0 ? 0 : offset),
      limit_(limit),
      returned_count_(0),
      offset_skipped_(false) {}

RowRangeResultSet::~RowRangeResultSet() { close(); }

int RowRangeResultSet::next(bool& has_next) {
    // ① Skip the first `offset_` rows on the first call.
    if (!offset_skipped_) {
        for (int i = 0; i < offset_; i++) {
            int ret = inner_->next(has_next);
            if (ret != common::E_OK) return ret;
            if (!has_next) {
                has_next = false;
                offset_skipped_ = true;
                return common::E_OK;
            }
        }
        offset_skipped_ = true;
    }

    // ② Limit reached: return immediately without touching inner ResultSet.
    //    This is the key "pushdown" effect: no further chunk/page loading
    //    occurs.
    if (limit_ >= 0 && returned_count_ >= limit_) {
        has_next = false;
        return common::E_OK;
    }

    // ③ Normal delegation.
    int ret = inner_->next(has_next);
    if (ret == common::E_OK && has_next) {
        returned_count_++;
    }
    return ret;
}

bool RowRangeResultSet::is_null(const std::string& column_name) {
    return inner_->is_null(column_name);
}

bool RowRangeResultSet::is_null(uint32_t column_index) {
    return inner_->is_null(column_index);
}

RowRecord* RowRangeResultSet::get_row_record() {
    return inner_->get_row_record();
}

std::shared_ptr<ResultSetMetadata> RowRangeResultSet::get_metadata() {
    return inner_->get_metadata();
}

void RowRangeResultSet::close() {
    if (inner_ != nullptr) {
        delete inner_;
        inner_ = nullptr;
    }
}

}  // namespace storage
