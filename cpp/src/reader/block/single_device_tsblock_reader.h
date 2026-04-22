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
#ifndef READER_SINGLE_DEVICE_TSBLOCK_READER_H
#define READER_SINGLE_DEVICE_TSBLOCK_READER_H

#include "common/tsblock/tsblock.h"
#include "reader/block/tsblock_reader.h"
#include "reader/filter/filter.h"
#include "reader/imeta_data_querier.h"
#include "reader/task/device_query_task.h"

namespace storage {

class DeviceQueryTask;
class MeasurementColumnContext;
class IdColumnContext;

class SingleDeviceTsBlockReader : public TsBlockReader {
   public:
    explicit SingleDeviceTsBlockReader(DeviceQueryTask* device_query_task,
                                       uint32_t block_size,
                                       IMetadataQuerier* metadata_querier,
                                       TsFileIOReader* tsfile_io_reader,
                                       Filter* time_filter,
                                       Filter* field_filter);
    ~SingleDeviceTsBlockReader() { close(); }
    int has_next(bool& has_next) override;
    int next(common::TsBlock*& ret_block) override;
    int init(DeviceQueryTask* device_query_task, uint32_t block_size,
             Filter* time_filter, Filter* field_filter);
    int init(DeviceQueryTask* device_query_task, uint32_t block_size,
             Filter* time_filter, Filter* field_filter, int row_offset,
             int row_limit);
    void close() override;

    int get_remaining_offset() const { return remaining_offset_; }
    int get_remaining_limit() const { return remaining_limit_; }
    int32_t get_dense_row_count() const { return dense_row_count_; }

   private:
    int init_internal(DeviceQueryTask* device_query_task, uint32_t block_size,
                      Filter* time_filter, Filter* field_filter);
    int construct_column_context(const ITimeseriesIndex* time_series_index,
                                 Filter* time_filter, int ssi_offset,
                                 int ssi_limit);
    int fill_measurements(
        std::vector<MeasurementColumnContext*>& column_contexts);
    int fill_ids();
    int advance_column(MeasurementColumnContext* column_context);
    int32_t compute_dense_row_count(
        const std::vector<ITimeseriesIndex*>& ts_indexes);

    DeviceQueryTask* device_query_task_;
    Filter* field_filter_;
    uint32_t block_size_;
    common::TsBlock* current_block_ = nullptr;
    std::vector<common::ColAppender*> col_appenders_;
    common::RowAppender* row_appender_;
    common::TupleDesc tuple_desc_;
    bool last_block_returned_ = true;
    std::map<std::string, MeasurementColumnContext*> field_column_contexts_;
    std::map<std::string, IdColumnContext> id_column_contexts_;
    int64_t next_time_ = 0;
    int64_t time_column_index_ = 0;
    TsFileIOReader* tsfile_io_reader_;
    common::PageArena pa_;
    int remaining_offset_ = 0;
    int remaining_limit_ = -1;
    int32_t dense_row_count_ = -1;
};

class MeasurementColumnContext {
   public:
    explicit MeasurementColumnContext(TsFileIOReader* tsfile_io_reader)
        : tsfile_io_reader_(tsfile_io_reader) {}

    virtual ~MeasurementColumnContext() = default;

    virtual void fill_into(
        std::vector<common::ColAppender*>& col_appenders) = 0;

    virtual void remove_from(std::map<std::string, MeasurementColumnContext*>&
                                 column_context_map) = 0;

    virtual int get_next_tsblock(bool alloc_mem) = 0;

    virtual int get_current_time(int64_t& time) = 0;

    virtual int get_current_value(char*& value, uint32_t& len) = 0;

    virtual int move_iter() = 0;

    virtual void set_ssi_row_range(int offset, int limit) {
        if (ssi_) ssi_->set_row_range(offset, limit);
    }
    virtual int get_ssi_row_offset() const {
        return ssi_ ? ssi_->get_row_offset() : 0;
    }
    virtual int get_ssi_row_limit() const {
        return ssi_ ? ssi_->get_row_limit() : -1;
    }

   protected:
    TsFileIOReader* tsfile_io_reader_;
    TsFileSeriesScanIterator* ssi_ = nullptr;
    common::TsBlock* tsblock_ = nullptr;
    common::ColIterator* time_iter_ = nullptr;
    common::ColIterator* value_iter_ = nullptr;
};

class SingleMeasurementColumnContext final : public MeasurementColumnContext {
   public:
    explicit SingleMeasurementColumnContext(TsFileIOReader* tsfile_io_reader)
        : MeasurementColumnContext(tsfile_io_reader) {}
    ~SingleMeasurementColumnContext() override {
        if (time_iter_) {
            delete time_iter_;
            time_iter_ = nullptr;
        }
        if (value_iter_) {
            delete value_iter_;
            value_iter_ = nullptr;
        }
        if (ssi_) {
            ssi_->revert_tsblock();
        }
        tsfile_io_reader_->revert_ssi(ssi_);
        ssi_ = nullptr;
    }

    void fill_into(std::vector<common::ColAppender*>& col_appenders) override;
    void remove_from(std::map<std::string, MeasurementColumnContext*>&
                         column_context_map) override;
    int init(DeviceQueryTask* device_query_task,
             const ITimeseriesIndex* time_series_index, Filter* time_filter,
             const std::vector<int32_t>& pos_in_result, common::PageArena& pa,
             int ssi_offset = 0, int ssi_limit = -1);
    int get_next_tsblock(bool alloc_mem) override;
    int get_current_time(int64_t& time) override;
    int get_current_value(char*& value, uint32_t& len) override;
    int move_iter() override;

   private:
    std::string column_name_;
    std::vector<int32_t> pos_in_result_;
};

class VectorMeasurementColumnContext final : public MeasurementColumnContext {
   public:
    explicit VectorMeasurementColumnContext(TsFileIOReader* tsfile_io_reader)
        : MeasurementColumnContext(tsfile_io_reader) {}

    void fill_into(std::vector<common::ColAppender*>& col_appenders) override;
    void remove_from(std::map<std::string, MeasurementColumnContext*>&
                         column_context_map) override;
    int init(DeviceQueryTask* device_query_task,
             const ITimeseriesIndex* time_series_index, Filter* time_filter,
             std::vector<std::vector<int32_t>>& pos_in_result,
             common::PageArena& pa);
    int get_next_tsblock(bool alloc_mem) override;
    int get_current_time(int64_t& time) override;
    int get_current_value(char*& value, uint32_t& len) override;
    int move_iter() override;

   private:
    std::vector<std::vector<int32_t>> pos_in_result_;
};

class IdColumnContext {
   public:
    explicit IdColumnContext(const std::vector<int32_t>& pos_in_result,
                             int32_t pos_in_device_id)
        : pos_in_result_(pos_in_result), pos_in_device_id_(pos_in_device_id) {}
    const std::vector<int32_t> pos_in_result_;
    const int32_t pos_in_device_id_;
};
}  // namespace storage

#endif  // READER_SINGLE_DEVICE_TSBLOCK_READER_H
