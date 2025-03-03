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
#include "reader/table_result_set.h"

namespace storage {
void TableResultSet::init() {
    row_record_ = new RowRecord(column_names_.size() + 1);
    pa_.reset();
    pa_.init(512, common::MOD_TSFILE_READER);
    index_lookup_.reserve(column_names_.size());
    for (uint32_t i = 0; i < column_names_.size(); ++i) {
        index_lookup_.insert({column_names_[i], i + 1});
    }
    result_set_metadata_ = std::make_shared<ResultSetMetadata>(column_names_, data_types_);
}

TableResultSet::~TableResultSet() {
    close();
}

int TableResultSet::next(bool &has_next) {
    int ret = common::E_OK;
    while(row_iterator_ == nullptr || !row_iterator_->has_next()) {
        if (RET_FAIL(tsblock_reader_->has_next(has_next))) {
            return ret;
        } else if (!has_next) {
            break;
        }
        if (RET_FAIL(tsblock_reader_->next(tsblock_))) {
            break;
        }
        if (row_iterator_) {
            delete row_iterator_;
            row_iterator_ = nullptr;
        }
        row_iterator_ = new common::RowIterator(tsblock_);
    }
    if (row_iterator_ == nullptr || !row_iterator_->has_next()) {
        has_next = false;
    }

    if (has_next && IS_SUCC(ret)) {
        uint32_t len = 0;
        bool null = false;
        row_record_->reset();
        for (uint32_t i = 0; i < row_iterator_->get_column_count(); ++i) {
            row_record_->get_field(i)->set_value(row_iterator_->get_data_type(i), row_iterator_->read(i, &len, &null), pa_);
        }
        row_iterator_->next();
    }
    return ret;
}

bool TableResultSet::is_null(const std::string& column_name) {
    auto iter = index_lookup_.find(column_name);
    if (iter == index_lookup_.end()) {
        return true;
    } else {
        return is_null(iter->second + 1);
    }
}

bool TableResultSet::is_null(uint32_t column_index) {
    ASSERT(1 <= column_index && column_index <= row_record_->get_col_num());
    return row_record_->get_field(column_index - 1) == nullptr;
}

RowRecord* TableResultSet::get_row_record() {
    return row_record_;
}

std::shared_ptr<ResultSetMetadata> TableResultSet::get_metadata() {
    return result_set_metadata_;
}

void TableResultSet::close() {
    tsblock_reader_->close();
    pa_.destroy();
    if (row_record_) {
        delete row_record_;
        row_record_ = nullptr;
    }
    if (row_iterator_) {
        delete row_iterator_;
        row_iterator_ = nullptr;
    }
    if (tsblock_) {
        delete tsblock_;
        tsblock_ = nullptr;
    } 
}



}