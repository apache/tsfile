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

#include "single_device_tsblock_reader.h"

#include <algorithm>
#include <set>

#include "common/db_common.h"

namespace storage {

SingleDeviceTsBlockReader::SingleDeviceTsBlockReader(
    DeviceQueryTask* device_query_task, uint32_t block_size,
    IMetadataQuerier* metadata_querier, TsFileIOReader* tsfile_io_reader,
    Filter* time_filter, Filter* field_filter)
    : device_query_task_(device_query_task),
      field_filter_(field_filter),
      block_size_(block_size),
      tuple_desc_(),
      tsfile_io_reader_(tsfile_io_reader) {}

int SingleDeviceTsBlockReader::init(DeviceQueryTask* device_query_task,
                                    uint32_t block_size, Filter* time_filter,
                                    Filter* field_filter) {
    remaining_offset_ = 0;
    remaining_limit_ = -1;
    dense_row_count_ = -1;
    return init_internal(device_query_task, block_size, time_filter,
                         field_filter);
}

int SingleDeviceTsBlockReader::init(DeviceQueryTask* device_query_task,
                                    uint32_t block_size, Filter* time_filter,
                                    Filter* field_filter, int row_offset,
                                    int row_limit) {
    remaining_offset_ = row_offset;
    remaining_limit_ = row_limit;
    dense_row_count_ = -1;
    return init_internal(device_query_task, block_size, time_filter,
                         field_filter);
}

int32_t SingleDeviceTsBlockReader::compute_dense_row_count(
    const std::vector<ITimeseriesIndex*>& ts_indexes) {
    int64_t reference_time_count = -1;
    for (const auto* ts_index : ts_indexes) {
        if (ts_index == nullptr) {
            continue;
        }

        int64_t time_count = 0;
        int64_t value_count = 0;

        if (ts_index->get_data_type() == common::VECTOR) {
            auto* time_list = ts_index->get_time_chunk_meta_list();
            auto* value_list = ts_index->get_value_chunk_meta_list();
            if (time_list == nullptr || value_list == nullptr) {
                return -1;
            }

            for (auto it = time_list->begin(); it != time_list->end(); it++) {
                if (it.get()->statistic_) {
                    time_count += it.get()->statistic_->count_;
                }
            }
            for (auto it = value_list->begin(); it != value_list->end(); it++) {
                if (it.get()->statistic_) {
                    value_count += it.get()->statistic_->count_;
                }
            }
        } else {
            auto* list = ts_index->get_chunk_meta_list();
            if (list == nullptr) {
                return -1;
            }
            for (auto it = list->begin(); it != list->end(); it++) {
                if (it.get()->statistic_) {
                    time_count += it.get()->statistic_->count_;
                }
            }
            value_count = time_count;
        }

        if (time_count == 0 || value_count == 0) {
            return -1;
        }
        if (reference_time_count < 0) {
            reference_time_count = time_count;
        } else if (time_count != reference_time_count) {
            return -1;
        }
        if (value_count != reference_time_count) {
            return -1;
        }
    }

    if (reference_time_count < 0) {
        return -1;
    }
    return static_cast<int32_t>(reference_time_count);
}

int SingleDeviceTsBlockReader::init_internal(DeviceQueryTask* device_query_task,
                                             uint32_t block_size,
                                             Filter* time_filter,
                                             Filter* field_filter) {
    int ret = common::E_OK;
    pa_.init(512, common::AllocModID::MOD_TSFILE_READER);
    tuple_desc_.reset();
    auto table_schema = device_query_task->get_table_schema();
    tuple_desc_.push_back(common::g_time_column_schema);
    for (const auto& column_name : device_query_task_->get_column_names()) {
        common::ColumnSchema column_schema(
            table_schema->get_column_schema(column_name));
        if (column_schema.is_valid() &&
            column_schema.data_type_ != common::VECTOR) {
            tuple_desc_.push_back(column_schema);
        }
    }
    time_column_index_ = 0;
    if (RET_FAIL(common::TsBlock::create_tsblock(&tuple_desc_, current_block_,
                                                 block_size))) {
        return ret;
    }
    col_appenders_.resize(tuple_desc_.get_column_count());
    for (uint32_t i = 0; i < tuple_desc_.get_column_count(); i++) {
        col_appenders_[i] = new common::ColAppender(i, current_block_);
    }
    row_appender_ = new common::RowAppender(current_block_);
    std::vector<ITimeseriesIndex*> time_series_indexs(
        device_query_task_->get_column_mapping()
            ->get_measurement_columns()
            .size());
    if (RET_FAIL(tsfile_io_reader_->get_timeseries_indexes(
            device_query_task->get_device_id(),
            device_query_task->get_column_mapping()->get_measurement_columns(),
            time_series_indexs, pa_))) {
        return ret;
    }
    dense_row_count_ = compute_dense_row_count(time_series_indexs);
    // Table queries allow sparse aligned fields. Until dense-ness can be
    // proven robustly for a device, fall back to the per-column merge path.
    const bool enable_dense_aligned_fast_path = false;
    // Early device-level time skip: if time_filter is set and ALL chunks of
    // this device have statistics that fall outside the filter range, skip the
    // entire device.  Chunks without statistics are assumed to satisfy.
    if (time_filter != nullptr) {
        bool all_outside = true;
        for (const auto* ts_idx : time_series_indexs) {
            if (ts_idx == nullptr) continue;
            auto* chunk_list = (ts_idx->get_data_type() == common::VECTOR)
                                   ? ts_idx->get_time_chunk_meta_list()
                                   : ts_idx->get_chunk_meta_list();
            if (chunk_list == nullptr) {
                all_outside = false;
                break;
            }
            for (auto it = chunk_list->begin(); it != chunk_list->end(); it++) {
                if (it.get()->statistic_ == nullptr ||
                    time_filter->satisfy(it.get()->statistic_)) {
                    all_outside = false;
                    break;
                }
            }
            if (!all_outside) break;
        }
        if (all_outside) {
            // No data in this device matches the time filter.
            delete current_block_;
            current_block_ = nullptr;
            return common::E_OK;
        }
    }
    // Try multi-value aligned path: one SSI reads all aligned value columns
    // at once. This is valid even for sparse aligned fields; the merge layer
    // must simply avoid visiting the shared context more than once.
    bool used_multi = false;
    std::set<std::string> multi_names;
    if (time_series_indexs.size() > 1) {
        bool can_multi = true;
        auto& meas_cols =
            device_query_task->get_column_mapping()->get_measurement_columns();
        for (const auto& ts_idx : time_series_indexs) {
            if (ts_idx == nullptr ||
                ts_idx->get_data_type() != common::VECTOR) {
                can_multi = false;
                break;
            }
        }
        if (can_multi) {
            std::vector<std::string> meas_names(meas_cols.begin(),
                                                meas_cols.end());
            std::sort(
                meas_names.begin(), meas_names.end(),
                [device_query_task](const std::string& lhs,
                                    const std::string& rhs) {
                    const auto& lhs_pos =
                        device_query_task->get_column_mapping()->get_column_pos(
                            lhs);
                    const auto& rhs_pos =
                        device_query_task->get_column_mapping()->get_column_pos(
                            rhs);
                    const int lhs_first =
                        lhs_pos.empty() ? INT32_MAX : lhs_pos.front();
                    const int rhs_first =
                        rhs_pos.empty() ? INT32_MAX : rhs_pos.front();
                    if (lhs_first != rhs_first) {
                        return lhs_first < rhs_first;
                    }
                    return lhs < rhs;
                });
            std::vector<std::vector<int32_t>> pos_list;
            pos_list.reserve(meas_names.size());
            for (const auto& name : meas_names) {
                const auto& pos =
                    device_query_task->get_column_mapping()->get_column_pos(
                        name);
                pos_list.push_back(
                    std::vector<int32_t>(pos.begin(), pos.end()));
            }

            auto* ctx = new VectorMeasurementColumnContext(tsfile_io_reader_);
            if (common::E_OK == ctx->init(device_query_task_, meas_names,
                                          time_filter, pos_list, pa_)) {
                for (const auto& name : meas_names) {
                    field_column_contexts_.insert(std::make_pair(name, ctx));
                    multi_names.insert(name);
                }
                aligned_col_count_ = meas_names.size();
                used_multi = true;
            } else {
                delete ctx;
            }
        }
    }

    for (const auto& time_series_index : time_series_indexs) {
        if (time_series_index == nullptr) {
            continue;
        }
        const std::string measurement_name =
            time_series_index->get_measurement_name().to_std_string();
        if (used_multi && multi_names.count(measurement_name) > 0) {
            continue;
        }
        construct_column_context(time_series_index, time_filter, 0, -1);
    }

    // Detect aligned fast path: every field column comes from an aligned chunk.
    if (!field_column_contexts_.empty() && enable_dense_aligned_fast_path &&
        dense_row_count_ >= 0 &&
        aligned_col_count_ == field_column_contexts_.size()) {
        all_aligned_ = true;
        aligned_vec_.reserve(field_column_contexts_.size());
        if (used_multi) {
            // Single VectorMeasurementColumnContext handles all columns.
            aligned_vec_.push_back(field_column_contexts_.begin()->second);
        } else {
            for (auto& kv : field_column_contexts_) {
                aligned_vec_.push_back(kv.second);
            }
        }
    }

    if (field_column_contexts_.empty()) {
        delete current_block_;
        current_block_ = nullptr;
        return common::E_OK;
    }

    for (const auto& id_column :
         device_query_task->get_column_mapping()->get_id_columns()) {
        const auto& column_pos_in_result =
            device_query_task->get_column_mapping()->get_column_pos(id_column);
        int column_pos_in_id = table_schema->find_id_column_order(id_column) +
                               (!table_schema->is_virtual_table());
        id_column_contexts_.insert(std::make_pair(
            id_column,
            IdColumnContext(column_pos_in_result, column_pos_in_id)));
    }
    return ret;
}

int SingleDeviceTsBlockReader::has_next(bool& has_next) {
    if (!last_block_returned_) {
        has_next = true;
        return common::E_OK;
    }

    if (field_column_contexts_.empty()) {
        has_next = false;
        return common::E_OK;
    }

    if (remaining_limit_ == 0) {
        has_next = false;
        return common::E_OK;
    }

    for (auto col_appender : col_appenders_) {
        col_appender->reset();
    }

    current_block_->reset();

    if (all_aligned_) {
        return has_next_aligned(has_next);
    }

    bool next_time_set = false;
    next_time_ = -1;

    std::vector<MeasurementColumnContext*> min_time_columns;
    while (current_block_->get_row_count() < block_size_) {
        if (remaining_limit_ > 0 &&
            current_block_->get_row_count() >=
                static_cast<uint32_t>(remaining_limit_)) {
            break;
        }
        std::set<MeasurementColumnContext*> visited_contexts;
        for (auto& column_context : field_column_contexts_) {
            if (!visited_contexts.insert(column_context.second).second) {
                continue;
            }
            int64_t time;
            if (IS_FAIL(column_context.second->get_current_time(time))) {
                continue;
            }
            if (!next_time_set || time < next_time_) {
                next_time_set = true;
                next_time_ = time;
                min_time_columns.clear();
                min_time_columns.push_back(column_context.second);
            } else if (time == next_time_) {
                min_time_columns.push_back(column_context.second);
            }
        }

        if (!next_time_set) {
            break;
        }

        if (remaining_offset_ > 0) {
            for (auto* col_ctx : min_time_columns) {
                if (IS_FAIL(advance_column(col_ctx))) {
                    break;
                }
            }
            remaining_offset_--;
            min_time_columns.clear();
            next_time_set = false;
            next_time_ = -1;
            if (field_column_contexts_.empty()) {
                break;
            }
            continue;
        }

        if (IS_FAIL(fill_measurements(min_time_columns))) {
            has_next = false;
            return common::E_OK;
        } else {
            next_time_set = false;
            next_time_ = -1;
        }

        if (field_column_contexts_.empty()) {
            break;
        }
    }
    if (remaining_limit_ > 0 && current_block_->get_row_count() > 0) {
        remaining_limit_ -= current_block_->get_row_count();
    }
    int ret = common::E_OK;
    if (current_block_->get_row_count() > 0) {
        if (RET_FAIL(fill_ids())) {
            return ret;
        }
        current_block_->fill_trailling_nulls();
        last_block_returned_ = false;
        has_next = true;
        return ret;
    }
    has_next = false;
    return ret;
}

int SingleDeviceTsBlockReader::has_next_aligned(bool& result_has_next) {
    int ret = common::E_OK;
    int time_in_query_index = tuple_desc_.get_time_column_index();

    while (current_block_->get_row_count() < block_size_) {
        if (aligned_vec_.empty()) break;

        if (remaining_limit_ == 0) break;

        // Check if first column has data.
        uint32_t avail = aligned_vec_[0]->available_rows();
        if (avail == 0) {
            for (auto* ctx : aligned_vec_) {
                ctx->remove_from(field_column_contexts_);
            }
            aligned_vec_.clear();
            break;
        }

        // Find the batch size: min of output capacity and all SSI
        // availabilities.
        uint32_t batch = block_size_ - current_block_->get_row_count();
        for (auto* ctx : aligned_vec_) {
            uint32_t ctx_avail = ctx->available_rows();
            if (ctx_avail == 0) {
                batch = 0;
                break;
            }
            if (ctx_avail < batch) batch = ctx_avail;
        }
        if (batch == 0) {
            for (auto* ctx : aligned_vec_) {
                ctx->remove_from(field_column_contexts_);
            }
            aligned_vec_.clear();
            break;
        }

        // Handle offset: skip rows before copying.
        if (remaining_offset_ > 0) {
            uint32_t skip = std::min(batch, (uint32_t)remaining_offset_);
            for (auto* ctx : aligned_vec_) {
                ctx->skip_rows(skip);
            }
            remaining_offset_ -= skip;
            continue;
        }

        // Handle limit: cap the batch size.
        if (remaining_limit_ > 0) {
            batch = std::min(batch, (uint32_t)remaining_limit_);
        }

        // First SSI: bulk copy time + values + row_count.
        int copy_ret = aligned_vec_[0]->bulk_copy_into(
            col_appenders_, col_appenders_[time_column_index_], row_appender_,
            batch);

        // Also copy time to explicit time column if requested.
        if (time_in_query_index != -1) {
            common::Vector* time_vec =
                current_block_->get_vector(time_column_index_);
            char* time_src =
                time_vec->get_value_data().get_data() +
                (current_block_->get_row_count() - batch) * sizeof(int64_t);
            col_appenders_[time_in_query_index]->bulk_append_fixed(
                time_src, batch, sizeof(int64_t));
        }

        // Other SSIs: bulk copy values only (no time, no row_count).
        for (size_t i = 1; i < aligned_vec_.size(); i++) {
            aligned_vec_[i]->bulk_copy_into(col_appenders_, nullptr, nullptr,
                                            batch);
        }

        // Decrement limit for data already copied.
        if (remaining_limit_ > 0) {
            remaining_limit_ -= batch;
        }

        // If first SSI signaled no-more-data, stop after accounting.
        if (copy_ret != common::E_OK) break;
    }

    if (current_block_->get_row_count() > 0) {
        if (RET_FAIL(fill_ids())) return ret;
        current_block_->fill_trailling_nulls();
        last_block_returned_ = false;
        result_has_next = true;
    } else {
        result_has_next = false;
    }
    return ret;
}

int SingleDeviceTsBlockReader::fill_measurements(
    std::vector<MeasurementColumnContext*>& column_contexts) {
    int ret = common::E_OK;
    if (field_filter_ ==
        nullptr /*TODO: || field_filter_->satisfy(column_contexts)*/) {
        row_appender_->add_row();
        if (!col_appenders_[time_column_index_]->add_row()) {
            assert(false);
        }
        col_appenders_[time_column_index_]->append((const char*)&next_time_,
                                                   sizeof(next_time_));
        int time_in_query_index = tuple_desc_.get_time_column_index();
        if (time_in_query_index != -1) {
            if (!col_appenders_[time_in_query_index]->add_row()) {
                assert(false);
            }
            col_appenders_[time_in_query_index]->append(
                (const char*)&next_time_, sizeof(next_time_));
        }
        for (auto& column_context : column_contexts) {
            column_context->fill_into(col_appenders_);
            if (RET_FAIL(advance_column(column_context))) {
                break;
            }
        }

        // Align all columns, filling with nulls where data is missing.
        uint32_t row_count =
            col_appenders_[time_column_index_]->get_col_row_count();
        for (auto& col_appender : col_appenders_) {
            if (tuple_desc_.get_column_category(
                    col_appender->get_column_index()) !=
                common::ColumnCategory::FIELD) {
                continue;
            }
            while (col_appender->get_col_row_count() < row_count) {
                col_appender->add_row();
                col_appender->append_null();
            }
        }
    }
    return ret;
}

int SingleDeviceTsBlockReader::advance_column(
    MeasurementColumnContext* column_context) {
    int ret = column_context->move_iter();
    if (ret == common::E_NO_MORE_DATA) {
        column_context->remove_from(field_column_contexts_);
        ret = common::E_OK;
    }
    return ret;
}

void SingleMeasurementColumnContext::remove_from(
    std::map<std::string, MeasurementColumnContext*>& column_context_map) {
    auto iter = column_context_map.find(column_name_);
    if (iter != column_context_map.end()) {
        delete iter->second;
        column_context_map.erase(iter);
    }
}

int SingleDeviceTsBlockReader::fill_ids() {
    int ret = common::E_OK;
    for (const auto& entry : id_column_contexts_) {
        const auto& id_column_context = entry.second;
        for (int32_t pos : id_column_context.pos_in_result_) {
            std::string* device_tag = nullptr;
            auto device_id = device_query_task_->get_device_id();
            int32_t pos_in_device_id = id_column_context.pos_in_device_id_;
            if (pos_in_device_id >= 0 && static_cast<size_t>(pos_in_device_id) <
                                             device_id->get_split_seg_num()) {
                device_tag = device_id->get_split_segname_at(pos_in_device_id);
            }

            if (device_tag == nullptr) {
                ret = col_appenders_[pos + 1]->fill_null(
                    current_block_->get_row_count());
                if (ret != common::E_OK) {
                    return ret;
                }
                continue;
            }

            if (RET_FAIL(col_appenders_[pos + 1]->fill(
                    device_tag->c_str(), device_tag->length(),
                    current_block_->get_row_count()))) {
                return ret;
            }
        }
    }
    return ret;
}

int SingleDeviceTsBlockReader::next(common::TsBlock*& ret_block) {
    bool next = false;
    has_next(next);
    if (!next) {
        return common::E_NO_MORE_DATA;
    }
    last_block_returned_ = true;
    ret_block = current_block_;
    return common::E_OK;
}

void SingleDeviceTsBlockReader::close() {
    aligned_vec_.clear();  // non-owning; owned by field_column_contexts_
    // De-duplicate pointers before deleting: VectorMeasurementColumnContext
    // has multiple map entries pointing to the same object.
    std::set<MeasurementColumnContext*> unique_contexts;
    for (auto& column_context : field_column_contexts_) {
        unique_contexts.insert(column_context.second);
    }
    for (auto* ctx : unique_contexts) {
        delete ctx;
    }
    for (auto& col_appender : col_appenders_) {
        if (col_appender) {
            delete col_appender;
            col_appender = nullptr;
        }
    }
    if (row_appender_) {
        delete row_appender_;
        row_appender_ = nullptr;
    }
    device_query_task_ = nullptr;  // owned by the task iterator arena
    if (current_block_) {
        delete current_block_;
        current_block_ = nullptr;
    }
}

int SingleDeviceTsBlockReader::construct_column_context(
    const ITimeseriesIndex* time_series_index, Filter* time_filter,
    int ssi_offset, int ssi_limit) {
    int ret = common::E_OK;
    if (time_series_index == nullptr ||
        (time_series_index->get_data_type() != common::TSDataType::VECTOR &&
         time_series_index->get_chunk_meta_list()->empty())) {
    } else if (time_series_index->get_data_type() == common::VECTOR) {
        const int effective_ssi_offset = dense_row_count_ >= 0 ? ssi_offset : 0;
        const int effective_ssi_limit = dense_row_count_ >= 0 ? ssi_limit : -1;
        const AlignedTimeseriesIndex* aligned_time_series_index =
            dynamic_cast<const AlignedTimeseriesIndex*>(time_series_index);
        if (aligned_time_series_index == nullptr) {
            assert(false);
        }
        SingleMeasurementColumnContext* column_context =
            new SingleMeasurementColumnContext(tsfile_io_reader_);
        if (RET_FAIL(column_context->init(
                device_query_task_, time_series_index, time_filter,
                device_query_task_->get_column_mapping()->get_column_pos(
                    time_series_index->get_measurement_name().to_std_string()),
                pa_, effective_ssi_offset, effective_ssi_limit))) {
            delete column_context;
            return ret;
        }
        field_column_contexts_.insert(std::make_pair(
            time_series_index->get_measurement_name().to_std_string(),
            column_context));
        aligned_col_count_++;
    } else {
        SingleMeasurementColumnContext* column_context =
            new SingleMeasurementColumnContext(tsfile_io_reader_);
        if (RET_FAIL(column_context->init(
                device_query_task_, time_series_index, time_filter,
                device_query_task_->get_column_mapping()->get_column_pos(
                    time_series_index->get_measurement_name().to_std_string()),
                pa_, ssi_offset, ssi_limit))) {
            delete column_context;
            return ret;
        }

        field_column_contexts_.insert(std::make_pair(
            time_series_index->get_measurement_name().to_std_string(),
            column_context));
    }
    return ret;
}

int SingleMeasurementColumnContext::init(
    DeviceQueryTask* device_query_task,
    const ITimeseriesIndex* time_series_index, Filter* time_filter,
    const std::vector<int32_t>& pos_in_result, common::PageArena& pa,
    int ssi_offset, int ssi_limit) {
    int ret = common::E_OK;
    pos_in_result_ = pos_in_result;
    column_name_ = time_series_index->get_measurement_name().to_std_string();
    if (RET_FAIL(tsfile_io_reader_->alloc_ssi(
            device_query_task->get_device_id(),
            time_series_index->get_measurement_name().to_std_string(), ssi_, pa,
            time_filter))) {
    } else {
        ssi_->set_row_range(ssi_offset, ssi_limit);
        if (RET_FAIL(get_next_tsblock(true))) {
        }
    }
    return ret;
}

int SingleMeasurementColumnContext::get_next_tsblock(bool alloc_mem) {
    int ret = common::E_OK;
    if (tsblock_ != nullptr) {
        if (time_iter_) {
            delete time_iter_;
            time_iter_ = nullptr;
        }
        if (value_iter_) {
            delete value_iter_;
            value_iter_ = nullptr;
        }
        tsblock_->reset();
    }
    if (RET_FAIL(ssi_->get_next(tsblock_, alloc_mem))) {
        if (time_iter_) {
            delete time_iter_;
            time_iter_ = nullptr;
        }
        if (value_iter_) {
            delete value_iter_;
            value_iter_ = nullptr;
        }
        if (tsblock_) {
            ssi_->destroy();
            tsblock_ = nullptr;
        }
    } else {
        time_iter_ = new common::ColIterator(0, tsblock_);
        value_iter_ = new common::ColIterator(1, tsblock_);
    }
    return ret;
}

int SingleMeasurementColumnContext::get_current_time(int64_t& time) {
    if (time_iter_->end()) {
        return common::E_NO_MORE_DATA;
    }
    uint32_t len = 0;
    time = *(int64_t*)(time_iter_->read(&len));
    return common::E_OK;
}

int SingleMeasurementColumnContext::get_current_value(char*& value,
                                                      uint32_t& len) {
    if (value_iter_->end()) {
        return common::E_NO_MORE_DATA;
    }
    bool is_null = false;
    value = value_iter_->read(&len, &is_null);
    return common::E_OK;
}

int SingleMeasurementColumnContext::move_iter() {
    int ret = common::E_OK;
    time_iter_->next();
    value_iter_->next();
    if (time_iter_->end()) {
        if (RET_FAIL(get_next_tsblock(false))) {
            return ret;
        }
    }
    return ret;
}

void SingleMeasurementColumnContext::fill_into(
    std::vector<common::ColAppender*>& col_appenders) {
    char* val = nullptr;
    uint32_t len = 0;
    if (IS_FAIL(get_current_value(val, len))) {
        return;
    }
    for (int32_t pos : pos_in_result_) {
        col_appenders[pos + 1]->add_row();
        if (val == nullptr) {
            col_appenders[pos + 1]->append_null();
        } else {
            col_appenders[pos + 1]->append(val, len);
        }
    }
}

uint32_t SingleMeasurementColumnContext::available_rows() const {
    if (!time_iter_ || time_iter_->end()) return 0;
    return time_iter_->remaining();
}

int SingleMeasurementColumnContext::bulk_copy_into(
    std::vector<common::ColAppender*>& col_appenders,
    common::ColAppender* time_appender, common::RowAppender* row_appender,
    uint32_t count) {
    int ret = common::E_OK;
    const uint32_t time_elem_size = sizeof(int64_t);
    auto dt = value_iter_->get_data_type();
    bool is_varlen =
        (dt == common::STRING || dt == common::TEXT || dt == common::BLOB);

    // Bulk copy time column (only first SSI does this).
    if (time_appender) {
        time_appender->bulk_append_fixed(time_iter_->data_ptr(), count,
                                         time_elem_size);
    }

    // Advance output row count (only first SSI does this).
    if (row_appender) {
        row_appender->add_rows(count);
    }

    if (is_varlen || value_iter_->has_null()) {
        for (uint32_t r = 0; r < count; r++) {
            uint32_t len = 0;
            bool is_null = false;
            char* val = value_iter_->read(&len, &is_null);
            for (int32_t pos : pos_in_result_) {
                auto* appender = col_appenders[pos + 1];
                appender->add_row();
                if (is_null) {
                    appender->append_null();
                } else {
                    appender->append(val, len);
                }
            }
            value_iter_->next();
        }
    } else {
        const uint32_t val_elem_size = common::get_data_type_size(dt);
        char* val_ptr = value_iter_->data_ptr();
        for (int32_t pos : pos_in_result_) {
            col_appenders[pos + 1]->bulk_append_fixed(val_ptr, count,
                                                      val_elem_size);
        }
        value_iter_->advance(count, val_elem_size);
    }

    // Advance source iterators.
    time_iter_->advance(count, time_elem_size);

    // If source TsBlock exhausted, load next.
    if (time_iter_->end()) {
        if (RET_FAIL(get_next_tsblock(false))) {
            return ret;
        }
    }
    return ret;
}

void SingleMeasurementColumnContext::skip_rows(uint32_t count) {
    if (!time_iter_ || time_iter_->end()) return;
    const uint32_t time_elem_size = sizeof(int64_t);
    auto dt = value_iter_->get_data_type();
    bool is_varlen =
        (dt == common::STRING || dt == common::TEXT || dt == common::BLOB);
    uint32_t to_skip = std::min(count, time_iter_->remaining());
    time_iter_->advance(to_skip, time_elem_size);
    if (is_varlen || value_iter_->has_null()) {
        for (uint32_t r = 0; r < to_skip; r++) {
            value_iter_->next();
        }
    } else {
        const uint32_t val_elem_size = common::get_data_type_size(dt);
        value_iter_->advance(to_skip, val_elem_size);
    }
    if (time_iter_->end()) {
        get_next_tsblock(false);
    }
}

// ── VectorMeasurementColumnContext implementation ───────────────────────

VectorMeasurementColumnContext::~VectorMeasurementColumnContext() {
    if (time_iter_) {
        delete time_iter_;
        time_iter_ = nullptr;
    }
    for (auto* vi : value_iters_) {
        if (vi) delete vi;
    }
    value_iters_.clear();
    if (ssi_) {
        ssi_->revert_tsblock();
    }
    tsfile_io_reader_->revert_ssi(ssi_);
    ssi_ = nullptr;
}

int VectorMeasurementColumnContext::init(
    DeviceQueryTask* device_query_task,
    const std::vector<std::string>& measurement_names, Filter* time_filter,
    std::vector<std::vector<int32_t>>& pos_in_result, common::PageArena& pa) {
    int ret = common::E_OK;
    pos_in_result_ = pos_in_result;
    column_names_ = measurement_names;
    if (RET_FAIL(tsfile_io_reader_->alloc_multi_ssi(
            device_query_task->get_device_id(), measurement_names, ssi_, pa,
            time_filter))) {
        return ret;
    }
    if (RET_FAIL(get_next_tsblock(true))) {
        return ret;
    }
    return ret;
}

int VectorMeasurementColumnContext::get_next_tsblock(bool alloc_mem) {
    int ret = common::E_OK;
    if (tsblock_ != nullptr) {
        if (time_iter_) {
            delete time_iter_;
            time_iter_ = nullptr;
        }
        for (auto* vi : value_iters_) {
            if (vi) delete vi;
        }
        value_iters_.clear();
        tsblock_->reset();
    }
    if (RET_FAIL(ssi_->get_next(tsblock_, alloc_mem))) {
        if (time_iter_) {
            delete time_iter_;
            time_iter_ = nullptr;
        }
        for (auto* vi : value_iters_) {
            if (vi) delete vi;
        }
        value_iters_.clear();
        if (tsblock_) {
            ssi_->destroy();
            tsblock_ = nullptr;
        }
    } else {
        time_iter_ = new common::ColIterator(0, tsblock_);
        uint32_t num_value_cols = tsblock_->get_column_count() - 1;
        value_iters_.reserve(num_value_cols);
        for (uint32_t c = 0; c < num_value_cols; c++) {
            value_iters_.push_back(new common::ColIterator(c + 1, tsblock_));
        }
    }
    return ret;
}

int VectorMeasurementColumnContext::get_current_time(int64_t& time) {
    if (!time_iter_ || time_iter_->end()) return common::E_NO_MORE_DATA;
    uint32_t len = 0;
    time = *(int64_t*)(time_iter_->read(&len));
    return common::E_OK;
}

int VectorMeasurementColumnContext::get_current_value(char*& value,
                                                      uint32_t& len) {
    if (value_iters_.empty() || value_iters_[0]->end())
        return common::E_NO_MORE_DATA;
    bool is_null = false;
    value = value_iters_[0]->read(&len, &is_null);
    return common::E_OK;
}

int VectorMeasurementColumnContext::move_iter() {
    int ret = common::E_OK;
    time_iter_->next();
    for (auto* vi : value_iters_) vi->next();
    if (time_iter_->end()) {
        if (RET_FAIL(get_next_tsblock(false))) return ret;
    }
    return ret;
}

void VectorMeasurementColumnContext::fill_into(
    std::vector<common::ColAppender*>& col_appenders) {
    for (uint32_t c = 0; c < value_iters_.size() && c < pos_in_result_.size();
         c++) {
        uint32_t len = 0;
        bool is_null = false;
        char* val = value_iters_[c]->read(&len, &is_null);
        for (int32_t pos : pos_in_result_[c]) {
            col_appenders[pos + 1]->add_row();
            if (is_null) {
                col_appenders[pos + 1]->append_null();
            } else {
                col_appenders[pos + 1]->append(val, len);
            }
        }
    }
}

void VectorMeasurementColumnContext::remove_from(
    std::map<std::string, MeasurementColumnContext*>& column_context_map) {
    for (const auto& name : column_names_) {
        column_context_map.erase(name);
    }
    delete this;
}

uint32_t VectorMeasurementColumnContext::available_rows() const {
    if (!time_iter_ || time_iter_->end()) return 0;
    return time_iter_->remaining();
}

int VectorMeasurementColumnContext::bulk_copy_into(
    std::vector<common::ColAppender*>& col_appenders,
    common::ColAppender* time_appender, common::RowAppender* row_appender,
    uint32_t count) {
    int ret = common::E_OK;
    const uint32_t time_elem_size = sizeof(int64_t);

    // Bulk copy time column (only when time_appender is provided).
    if (time_appender) {
        time_appender->bulk_append_fixed(time_iter_->data_ptr(), count,
                                         time_elem_size);
    }

    // Advance output row count.
    if (row_appender) {
        row_appender->add_rows(count);
    }

    // Bulk copy each value column to its output positions, propagating nulls.
    for (uint32_t c = 0; c < value_iters_.size() && c < pos_in_result_.size();
         c++) {
        auto dt = value_iters_[c]->get_data_type();
        bool is_varlen =
            (dt == common::STRING || dt == common::TEXT || dt == common::BLOB);
        bool src_has_null = value_iters_[c]->has_null();

        if (is_varlen || src_has_null) {
            // Row-by-row copy for variable-length columns using the
            // ColIterator next()/read() which properly tracks offsets. Fixed
            // length columns with nulls also need this path because their
            // payload buffer only stores non-null values.
            auto* iter = value_iters_[c];
            for (uint32_t r = 0; r < count; r++) {
                uint32_t len = 0;
                bool is_null = false;
                char* val = iter->read(&len, &is_null);
                for (int32_t pos : pos_in_result_[c]) {
                    auto* appender = col_appenders[pos + 1];
                    appender->add_row();
                    if (is_null) {
                        appender->append_null();
                    } else {
                        appender->append(val, len);
                    }
                }
                iter->next();
            }
        } else {
            // Bulk copy for fixed-length columns
            uint32_t val_elem_size = common::get_data_type_size(dt);
            char* val_ptr = value_iters_[c]->data_ptr();
            for (int32_t pos : pos_in_result_[c]) {
                col_appenders[pos + 1]->bulk_append_fixed(val_ptr, count,
                                                          val_elem_size);
            }
        }
    }

    // Advance all source iterators.
    time_iter_->advance(count, time_elem_size);
    for (uint32_t c = 0; c < value_iters_.size(); c++) {
        auto dt = value_iters_[c]->get_data_type();
        bool is_varlen =
            (dt == common::STRING || dt == common::TEXT || dt == common::BLOB);
        if (!is_varlen && !value_iters_[c]->has_null()) {
            uint32_t val_elem_size = common::get_data_type_size(dt);
            value_iters_[c]->advance(count, val_elem_size);
        }
        // Variable-length iterators and fixed-length iterators with nulls were
        // already advanced in the copy loop above.
    }

    // If source TsBlock exhausted, load next.
    if (time_iter_->end()) {
        if (RET_FAIL(get_next_tsblock(false))) return ret;
    }
    return ret;
}

void VectorMeasurementColumnContext::skip_rows(uint32_t count) {
    if (!time_iter_ || time_iter_->end()) return;
    const uint32_t time_elem_size = sizeof(int64_t);
    uint32_t to_skip = std::min(count, time_iter_->remaining());
    time_iter_->advance(to_skip, time_elem_size);
    for (uint32_t c = 0; c < value_iters_.size(); c++) {
        auto dt = value_iters_[c]->get_data_type();
        bool is_varlen =
            (dt == common::STRING || dt == common::TEXT || dt == common::BLOB);
        if (!is_varlen && !value_iters_[c]->has_null()) {
            uint32_t val_elem_size = common::get_data_type_size(dt);
            value_iters_[c]->advance(to_skip, val_elem_size);
        } else {
            // Variable-length and fixed-length-with-null vectors need next()
            // to keep the payload offset aligned with non-null rows.
            for (uint32_t r = 0; r < to_skip; r++) {
                value_iters_[c]->next();
            }
        }
    }
    if (time_iter_->end()) {
        get_next_tsblock(false);
    }
}

}  // namespace storage
