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

#ifndef READER_TSFILE_SERIES_SCAN_ITERATOR_H
#define READER_TSFILE_SERIES_SCAN_ITERATOR_H

#include <string>

#include "aligned_chunk_reader.h"
#include "common/tsblock/tsblock.h"
#include "file/read_file.h"
#include "file/tsfile_io_reader.h"
#include "reader/chunk_reader.h"
#include "reader/filter/filter.h"
#include "utils/util_define.h"

namespace storage {

class TsFileIOReader;

class TsFileSeriesScanIterator {
   public:
    TsFileSeriesScanIterator()
        : read_file_(nullptr),
          device_id_(),
          measurement_name_(),
          itimeseries_index_(),
          timeseries_index_pa_(),
          data_pa_(nullptr),
          chunk_meta_cursor_(),
          chunk_reader_(nullptr),
          tuple_desc_(),
          tsblock_(nullptr),
          time_filter_(nullptr),
          is_aligned_(false),
          is_multi_value_(false) {}
    ~TsFileSeriesScanIterator() { destroy(); }
    int init(std::shared_ptr<IDeviceID> device_id,
             const std::string& measurement_name, ReadFile* read_file,
             Filter* time_filter, common::PageArena& data_pa) {
        ASSERT(read_file != nullptr);
        device_id_ = device_id;
        measurement_name_ = measurement_name;
        read_file_ = read_file;
        time_filter_ = time_filter;
        data_pa_ = &data_pa;
        return common::E_OK;
    }
    void destroy();
    /*
     * If oneshoot filter specified, use it instead of this->time_filter_
     */
    int get_next(common::TsBlock*& ret_tsblock, bool alloc_tsblock,
                 Filter* oneshoot_filter = nullptr);
    void revert_tsblock();

    // Multi-value: number of value columns in the TsBlock
    uint32_t get_value_column_count() const {
        if (is_multi_value_ && chunk_reader_) {
            auto* acr = static_cast<AlignedChunkReader*>(chunk_reader_);
            return acr->get_value_column_count();
        }
        return 1;
    }

    bool is_multi_value() const { return is_multi_value_; }

    friend class TsFileIOReader;

   private:
    int init_chunk_reader();
    int init_chunk_reader_multi();
    FORCE_INLINE bool has_next_chunk() const {
        if (is_multi_value_) {
            // All value cursors advance in lockstep; check first one
            return !value_chunk_meta_cursors_.empty() &&
                   value_chunk_meta_cursors_[0] !=
                       itimeseries_index_->get_value_chunk_meta_list(0)->end();
        }
        if (is_aligned_) {
            return value_chunk_meta_cursor_ !=
                   itimeseries_index_->get_value_chunk_meta_list()->end();
        } else {
            return chunk_meta_cursor_ !=
                   itimeseries_index_->get_chunk_meta_list()->end();
        }
    }
    FORCE_INLINE void advance_to_next_chunk() {
        if (is_multi_value_) {
            time_chunk_meta_cursor_++;
            for (auto& cur : value_chunk_meta_cursors_) cur++;
        } else if (is_aligned_) {
            time_chunk_meta_cursor_++;
            value_chunk_meta_cursor_++;
        } else {
            chunk_meta_cursor_++;
        }
    }
    FORCE_INLINE ChunkMeta* get_current_chunk_meta() {
        return chunk_meta_cursor_.get();
    }
    common::TsBlock* alloc_tsblock();
    common::TsBlock* alloc_tsblock_multi();

   private:
    ReadFile* read_file_;
    std::shared_ptr<IDeviceID> device_id_;
    std::string measurement_name_;

    ITimeseriesIndex* itimeseries_index_;
    common::PageArena timeseries_index_pa_;
    common::PageArena* data_pa_;
    common::SimpleList<ChunkMeta*>::Iterator chunk_meta_cursor_;
    common::SimpleList<ChunkMeta*>::Iterator time_chunk_meta_cursor_;
    common::SimpleList<ChunkMeta*>::Iterator value_chunk_meta_cursor_;
    // Multi-value: one cursor per value column
    std::vector<common::SimpleList<ChunkMeta*>::Iterator>
        value_chunk_meta_cursors_;
    IChunkReader* chunk_reader_;

    common::TupleDesc tuple_desc_;
    common::TsBlock* tsblock_;
    Filter* time_filter_;
    bool is_aligned_ = false;
    bool is_multi_value_ = false;
};

}  // end namespace storage
#endif  // READER_TSFILE_SERIES_SCAN_ITERATOR_H
