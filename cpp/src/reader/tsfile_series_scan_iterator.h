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

#include <limits>
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
          is_multi_value_(false),
          row_offset_(0),
          row_limit_(-1) {}
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

    /**
     * Set row-level offset and limit for single-path optimization.
     * When set, the SSI uses chunk/page statistics (count) to skip
     * entire chunks/pages without decoding.
     */
    void set_row_range(int offset, int limit) {
        row_offset_ = offset;
        row_limit_ = limit;
    }

    /** Current row offset/limit after chunk/page skip; used to sync with QDS
     * for single-path. */
    int get_row_offset() const { return row_offset_; }
    int get_row_limit() const { return row_limit_; }

    /*
     * If oneshoot filter specified, use it instead of this->time_filter_.
     * @param min_time_hint  When not INT64_MIN, chunks whose end_time
     *                       < min_time_hint are skipped without loading.
     *                       Used by merge layer to push down the current
     *                       merge cursor.
     */
    int get_next(common::TsBlock*& ret_tsblock, bool alloc_tsblock,
                 Filter* oneshoot_filter = nullptr,
                 int64_t min_time_hint = std::numeric_limits<int64_t>::min());
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

    /**
     * Data type of the (value) column from the loaded timeseries index.
     * Available as soon as the SSI is allocated, i.e. independent of whether
     * any TsBlock has been materialized.  Callers building result-set metadata
     * should prefer this over deriving the type from a decoded TsBlock, since
     * offset/limit (e.g. limit==0) may skip all rows and leave no block.
     */
    common::TSDataType get_data_type() const {
        return itimeseries_index_ == nullptr
                   ? common::INVALID_DATATYPE
                   : itimeseries_index_->get_data_type();
    }

    friend class TsFileIOReader;

   private:
    int init_chunk_reader();
    int init_chunk_reader_multi();
    FORCE_INLINE bool has_next_chunk() const {
        if (is_multi_value_) {
            // Anchor on the time chunk list and require every value column
            // to still have a chunk available.  Checking only value[0] used
            // to read past end() for columns with fewer chunks (e.g. a
            // column added after some chunk groups had already been
            // flushed), which dereferenced freed memory and paired the
            // wrong time/value chunks.
            if (time_chunk_meta_cursor_ ==
                itimeseries_index_->get_time_chunk_meta_list()->end()) {
                return false;
            }
            for (uint32_t c = 0; c < value_chunk_meta_cursors_.size(); c++) {
                if (value_chunk_meta_cursors_[c] ==
                    itimeseries_index_->get_value_chunk_meta_list(c)->end()) {
                    return false;
                }
            }
            return true;
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
            // Guard each cursor against advancing past end().  Same defense
            // as has_next_chunk(): per-column chunk counts can diverge in
            // files with schema evolution.
            auto time_end =
                itimeseries_index_->get_time_chunk_meta_list()->end();
            if (time_chunk_meta_cursor_ != time_end) time_chunk_meta_cursor_++;
            for (uint32_t c = 0; c < value_chunk_meta_cursors_.size(); c++) {
                auto end =
                    itimeseries_index_->get_value_chunk_meta_list(c)->end();
                if (value_chunk_meta_cursors_[c] != end) {
                    value_chunk_meta_cursors_[c]++;
                }
            }
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
    bool should_skip_chunk_by_time(ChunkMeta* cm, int64_t min_time_hint);
    bool should_skip_chunk_by_offset(ChunkMeta* cm);
    bool should_skip_aligned_chunk_by_offset(ChunkMeta* time_cm,
                                             ChunkMeta* value_cm);
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
    int row_offset_;
    int row_limit_;
};

}  // end namespace storage
#endif  // READER_TSFILE_SERIES_SCAN_ITERATOR_H
