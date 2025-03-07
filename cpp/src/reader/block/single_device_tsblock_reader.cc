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

namespace storage {

SingleDeviceTsBlockReader::SingleDeviceTsBlockReader(
    DeviceQueryTask* device_query_task, uint32_t block_size,
    IMetadataQuerier* metadata_querier, TsFileIOReader* tsfile_io_reader,
    Filter* time_filter, Filter* field_filter)
    : device_query_task_(device_query_task),
      field_filter_(field_filter),
      block_size_(block_size),
      tuple_desc_(),
      tsfile_io_reader_(tsfile_io_reader) {
    init(device_query_task, block_size, time_filter, field_filter);
}

int SingleDeviceTsBlockReader::init(DeviceQueryTask* device_query_task,
                                    uint32_t block_size, Filter* time_filter,
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
    tsfile_io_reader_->get_timeseries_indexes(
        device_query_task->get_device_id(),
        device_query_task->get_column_mapping()->get_measurement_columns(),
        time_series_indexs, pa_);
    for (auto measurement_column :
         device_query_task->get_column_mapping()->get_measurement_columns()) {
    }
    for (const auto& time_series_index : time_series_indexs) {
        construct_column_context(time_series_index, time_filter);
    }

    for (const auto& id_column :
         device_query_task->get_column_mapping()->get_id_columns()) {
        const auto& column_pos_in_result =
            device_query_task->get_column_mapping()->get_column_pos(id_column);
        int column_pos_in_id =
            table_schema->find_id_column_order(id_column) + 1;
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
    for (auto col_appender : col_appenders_) {
        col_appender->reset();
    }
    current_block_->reset();

    bool next_time_set = false;
    next_time_ = -1;

    std::vector<MeasurementColumnContext*> min_time_columns;
    while (current_block_->get_row_count() < block_size_) {
        for (auto& column_context : field_column_contexts_) {
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
    return ret;  // return value is not used
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
        for (auto& column_context : column_contexts) {
            column_context->fill_into(col_appenders_);
            if (RET_FAIL(advance_column(column_context))) {
                break;
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
            common::String device_id(
                device_query_task_->get_device_id()->get_segments().at(
                    id_column_context.pos_in_device_id_));
            if (RET_FAIL(col_appenders_[pos + 1]->fill(
                    (char*)&device_id, sizeof(device_id),
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
    for (auto& column_context : field_column_contexts_) {
        delete column_context.second;
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
    if (device_query_task_) {
        device_query_task_->~DeviceQueryTask();
    }
}

int SingleDeviceTsBlockReader::construct_column_context(
    const ITimeseriesIndex* time_series_index, Filter* time_filter) {
    int ret = common::E_OK;
    if (time_series_index == nullptr ||
        (time_series_index->get_data_type() != common::TSDataType::VECTOR &&
         time_series_index->get_chunk_meta_list()->empty())) {
    } else if (time_series_index->get_data_type() == common::VECTOR) {
        const AlignedTimeseriesIndex* aligned_time_series_index =
            dynamic_cast<const AlignedTimeseriesIndex*>(time_series_index);
        if (aligned_time_series_index == nullptr) {
            assert(false);
        }
        // Todo: when multi value index is supported in aligned time series
        // index, we need to change the column context to
        // VectorMeasurementColumnContext
        SingleMeasurementColumnContext* column_context =
            new SingleMeasurementColumnContext(tsfile_io_reader_);
        // May no more data. just return to avoid null pointer.
        if (RET_FAIL(column_context->init(
                device_query_task_, time_series_index, time_filter,
                device_query_task_->get_column_mapping()->get_column_pos(
                    time_series_index->get_measurement_name().to_std_string()),
                pa_))) {
            return ret;
        }
        field_column_contexts_.insert(std::make_pair(
            time_series_index->get_measurement_name().to_std_string(),
            column_context));
    } else {
        SingleMeasurementColumnContext* column_context =
            new SingleMeasurementColumnContext(tsfile_io_reader_);
        column_context->init(
            device_query_task_, time_series_index, time_filter,
            device_query_task_->get_column_mapping()->get_column_pos(
                time_series_index->get_measurement_name().to_std_string()),
            pa_);
        field_column_contexts_.insert(std::make_pair(
            time_series_index->get_measurement_name().to_std_string(),
            column_context));
    }
    return ret;
}

int SingleMeasurementColumnContext::init(
    DeviceQueryTask* device_query_task,
    const ITimeseriesIndex* time_series_index, Filter* time_filter,
    const std::vector<int32_t>& pos_in_result, common::PageArena& pa) {
    int ret = common::E_OK;
    pos_in_result_ = pos_in_result;
    column_name_ = time_series_index->get_measurement_name().to_std_string();
    if (RET_FAIL(tsfile_io_reader_->alloc_ssi(
            device_query_task->get_device_id(),
            time_series_index->get_measurement_name().to_std_string(), ssi_, pa,
            time_filter))) {
    } else if (RET_FAIL(get_next_tsblock(true))) {
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
    value = value_iter_->read(&len);
    assert(value != nullptr);
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
        col_appenders[pos + 1]->append(val, len);
    }
}
}  // namespace storage
