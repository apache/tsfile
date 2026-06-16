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

#include <utils/storage_utils.h>

namespace storage {
void TableResultSet::init() {
    row_record_ = new RowRecord(column_names_.size() + 1);
    pa_.reset();
    pa_.init(512, common::MOD_TSFILE_READER);
    index_lookup_.reserve(column_names_.size() + 1);
    index_lookup_.insert({"time", 0});
    for (uint32_t i = 0; i < column_names_.size(); ++i) {
        index_lookup_.insert({column_names_[i], i + 1});
    }
    result_set_metadata_ =
        std::make_shared<ResultSetMetadata>(column_names_, data_types_);
}

TableResultSet::~TableResultSet() { close(); }

int TableResultSet::next(bool& has_next) {
    if (return_mode_ != RETURN_ROW) {
        return tsblock_reader_->has_next(has_next);
    }

    int ret = common::E_OK;

    // Advance past the row yielded by the previous next() call, if any.
    // Row iterator's next() advances all per-column offsets, so on the next
    // read the vectors point to the new row's data.
    if (row_ready_) {
        row_iterator_->next();
        row_ready_ = false;
        row_materialized_ = false;
    }

    // Find the next non-empty TsBlock.
    while (row_iterator_ == nullptr || !row_iterator_->has_next()) {
        if (RET_FAIL(tsblock_reader_->has_next(has_next))) {
            return ret;
        }

        if (!has_next) {
            if (row_iterator_) {
                delete row_iterator_;
                row_iterator_ = nullptr;
            }
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
        return ret;
    }

    // A row is now available at row_iterator_'s current row_id_; the per-
    // column vector offsets are pointing at that row's data.  We do NOT
    // populate row_record_ here — typed accessors read straight from the
    // vectors, and get_row_record() lazily materializes on demand.
    has_next = true;
    row_ready_ = true;
    return ret;
}

void TableResultSet::materialize_current_row() {
    uint32_t len = 0;
    bool null = false;
    row_record_->reset();
    for (uint32_t i = 0; i < row_iterator_->get_column_count(); ++i) {
        const auto value = row_iterator_->read(i, &len, &null);
        if (!null) {
            row_record_->get_field(i)->set_value(
                row_iterator_->get_data_type(i), value, len, pa_);
        }
    }
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
    if (!row_ready_) return true;
    return row_iterator_->is_null_at(column_index - 1);
}

// Direct buffer access — skips Vector::read's virtual dispatch.  Caller is
// expected to have checked is_null() (we still null-guard for safety).
// For fixed-width primitives the vector keeps its value buffer in
// values_ and tracks the current row's byte offset in offset_; the
// element at the active row is simply *(T*)(values_.get_data() + offset_).
// The ASSERT enforces strict typed access: the requested C++ type must match
// the column's physical storage width (DATE is int32, not int64).  On a
// mismatch it fires in debug instead of silently splicing the adjacent cell's
// bytes into the result.
#define TSFILE_FAST_PRIMITIVE_READ(TYPE, DFLT)                         \
    if (!row_ready_) return DFLT;                                      \
    common::Vector* vec = row_iterator_->get_vector(column_index - 1); \
    ASSERT(common::TypeMatch<TYPE>(vec->get_vector_type()));           \
    if (vec->has_null() && vec->is_null(row_iterator_->get_row_id()))  \
        return DFLT;                                                   \
    return *reinterpret_cast<TYPE*>(vec->get_value_data().get_data() + \
                                    vec->get_offset())

bool TableResultSet::get_bool_at(uint32_t column_index) {
    TSFILE_FAST_PRIMITIVE_READ(bool, false);
}

int32_t TableResultSet::get_int32_at(uint32_t column_index) {
    TSFILE_FAST_PRIMITIVE_READ(int32_t, 0);
}

int64_t TableResultSet::get_int64_at(uint32_t column_index) {
    TSFILE_FAST_PRIMITIVE_READ(int64_t, 0);
}

float TableResultSet::get_float_at(uint32_t column_index) {
    TSFILE_FAST_PRIMITIVE_READ(float, 0.0f);
}

double TableResultSet::get_double_at(uint32_t column_index) {
    TSFILE_FAST_PRIMITIVE_READ(double, 0.0);
}

#undef TSFILE_FAST_PRIMITIVE_READ

RowRecord* TableResultSet::get_row_record() {
    if (row_ready_ && !row_materialized_) {
        materialize_current_row();
        row_materialized_ = true;
    }
    return row_record_;
}

std::shared_ptr<ResultSetMetadata> TableResultSet::get_metadata() {
    return result_set_metadata_;
}

int TableResultSet::get_next_tsblock(common::TsBlock*& block) {
    int ret = common::E_OK;
    block = nullptr;

    if (return_mode_ == RETURN_ROW) {
        return common::E_INVALID_ARG;
    }

    bool has_next = false;
    if (RET_FAIL(tsblock_reader_->has_next(has_next))) {
        return ret;
    }

    if (!has_next) {
        return common::E_NO_MORE_DATA;
    }

    if (RET_FAIL(tsblock_reader_->next(tsblock_))) {
        return ret;
    }

    if (tsblock_ == nullptr) {
        return common::E_NO_MORE_DATA;
    }

    block = tsblock_;
    return common::E_OK;
}

void TableResultSet::close() {
    if (closed_) {
        return;
    }
    closed_ = true;
    if (tsblock_reader_) {
        tsblock_reader_->close();
    }
    pa_.destroy();
    if (row_record_) {
        delete row_record_;
        row_record_ = nullptr;
    }
    if (row_iterator_) {
        delete row_iterator_;
        row_iterator_ = nullptr;
    }
}

}  // namespace storage
