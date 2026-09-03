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

#include "reader/block/prepared_series_tsblock_reader.h"

#include <algorithm>

#include "file/tsfile_io_reader.h"
#include "reader/filter/filter.h"
#include "reader/prepared_series.h"
#include "reader/tsfile_series_scan_iterator.h"
#include "utils/errno_define.h"

namespace storage {

namespace {
constexpr uint32_t PREPARED_BATCH_ROWS = 65536;

common::TSDataType prepared_value_data_type(ITimeseriesIndex* index) {
    if (index == nullptr) {
        return common::INVALID_DATATYPE;
    }
    common::TSDataType data_type = index->get_data_type();
    if (data_type == common::VECTOR) {
        auto* aligned = dynamic_cast<AlignedTimeseriesIndex*>(index);
        if (aligned == nullptr || aligned->value_ts_idx_ == nullptr) {
            return common::INVALID_DATATYPE;
        }
        data_type = aligned->value_ts_idx_->get_data_type();
    }
    return data_type;
}
}  // namespace

int PreparedSeriesTsBlockReader::init(
    TsFileIOReader* io_reader, const std::shared_ptr<PreparedSeries>& prepared,
    Filter* owned_time_filter, int offset, int limit) {
    owned_time_filter_ = owned_time_filter;
    if (io_reader == nullptr || prepared == nullptr ||
        prepared->index() == nullptr || offset < 0 || closed_) {
        return common::E_INVALID_ARG;
    }

    io_reader_ = io_reader;
    prepared_ = prepared;
    remaining_limit_ = limit;
    value_data_type_ = prepared_value_data_type(prepared->index());
    if (value_data_type_ == common::INVALID_DATATYPE ||
        value_data_type_ == common::VECTOR) {
        return common::E_TYPE_NOT_SUPPORTED;
    }
    value_data_types_.assign(1, value_data_type_);

    pa_.init(512, common::MOD_TSFILE_READER);
    if (limit == 0) {
        exhausted_ = true;
        return common::E_OK;
    }

    int ret = io_reader_->alloc_prepared_ssi(prepared_, ssi_, pa_,
                                             owned_time_filter_);
    if (ret == common::E_NO_MORE_DATA) {
        exhausted_ = true;
        return common::E_OK;
    }
    if (ret != common::E_OK) {
        return ret;
    }
    // The table-model multi-aligned reader applies offset through its
    // chunk/page plan.  Limit is enforced by sizing each native output block;
    // this keeps the returned TsBlock direct and avoids RowRecord slicing.
    ssi_->set_row_range(offset, -1);
    return common::E_OK;
}

int PreparedSeriesTsBlockReader::init_multi(
    TsFileIOReader* io_reader,
    const std::vector<std::shared_ptr<PreparedSeries>>& prepared,
    Filter* owned_time_filter, int offset, int limit) {
    owned_time_filter_ = owned_time_filter;
    if (io_reader == nullptr || prepared.empty() || offset < 0 || closed_) {
        return common::E_INVALID_ARG;
    }

    io_reader_ = io_reader;
    remaining_limit_ = limit;
    value_data_types_.reserve(prepared.size());
    for (const auto& entry : prepared) {
        common::TSDataType data_type =
            entry == nullptr ? common::INVALID_DATATYPE
                             : prepared_value_data_type(entry->index());
        if (data_type == common::INVALID_DATATYPE ||
            data_type == common::VECTOR) {
            return common::E_TYPE_NOT_SUPPORTED;
        }
        value_data_types_.push_back(data_type);
    }
    value_data_type_ = value_data_types_.front();

    pa_.init(512, common::MOD_TSFILE_READER);
    if (limit == 0) {
        exhausted_ = true;
        return common::E_OK;
    }

    int ret = io_reader_->alloc_prepared_multi_ssi(prepared, ssi_, pa_,
                                                   owned_time_filter_);
    if (ret == common::E_NO_MORE_DATA) {
        exhausted_ = true;
        return common::E_OK;
    }
    if (ret != common::E_OK) {
        return ret;
    }
    ssi_->set_row_range(offset, -1);
    return common::E_OK;
}

int PreparedSeriesTsBlockReader::has_next(bool& has_next) {
    has_next = false;
    if (closed_) {
        return common::E_INVALID_ARG;
    }
    if (block_ready_) {
        has_next = true;
        return common::E_OK;
    }
    if (exhausted_ || ssi_ == nullptr || remaining_limit_ == 0) {
        exhausted_ = true;
        return common::E_OK;
    }

    while (true) {
        const uint32_t desired_rows =
            remaining_limit_ > 0
                ? std::min<uint32_t>(PREPARED_BATCH_ROWS,
                                     static_cast<uint32_t>(remaining_limit_))
                : PREPARED_BATCH_ROWS;
        if (block_ != nullptr) {
            if (block_->get_max_row_count() != desired_rows) {
                ssi_->revert_tsblock();
                block_ = nullptr;
            } else {
                block_->reset();
            }
        }
        ssi_->set_max_block_rows(desired_rows);
        int ret = ssi_->get_next(block_, true);
        if (ret == common::E_NO_MORE_DATA) {
            exhausted_ = true;
            return common::E_OK;
        }
        if (ret != common::E_OK) {
            return ret;
        }
        if (block_ != nullptr && block_->get_row_count() > 0) {
            if (remaining_limit_ > 0) {
                remaining_limit_ -= static_cast<int>(block_->get_row_count());
            }
            block_ready_ = true;
            has_next = true;
            return common::E_OK;
        }
    }
}

int PreparedSeriesTsBlockReader::next(common::TsBlock*& ret_block) {
    ret_block = nullptr;
    bool available = false;
    int ret = has_next(available);
    if (ret != common::E_OK) {
        return ret;
    }
    if (!available) {
        return common::E_NO_MORE_DATA;
    }
    ret_block = block_;
    block_ready_ = false;
    return common::E_OK;
}

void PreparedSeriesTsBlockReader::close() {
    if (closed_) {
        return;
    }
    closed_ = true;
    block_ready_ = false;
    exhausted_ = true;
    if (ssi_ != nullptr) {
        ssi_->revert_tsblock();
        block_ = nullptr;
        io_reader_->revert_ssi(ssi_);
        ssi_ = nullptr;
    }
    delete owned_time_filter_;
    owned_time_filter_ = nullptr;
    pa_.destroy();
    prepared_.reset();
    std::vector<common::TSDataType>().swap(value_data_types_);
    io_reader_ = nullptr;
}

}  // namespace storage
