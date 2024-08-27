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
          device_path_(),
          measurement_name_(),
          itimeseries_index_(),
          timeseries_index_pa_(),
          chunk_meta_cursor_(),
          chunk_reader_(nullptr),
          tuple_desc_(),
          tsblock_(nullptr),
          time_filter_(nullptr),
          is_aligned_(false) {}
    ~TsFileSeriesScanIterator() { destroy(); }
    int init(const std::string &device_path,
             const std::string &measurement_name, ReadFile *read_file,
             Filter *time_filter) {
        ASSERT(read_file != nullptr);
        device_path_ = device_path;
        measurement_name_ = measurement_name;
        read_file_ = read_file;
        time_filter_ = time_filter;
        return common::E_OK;
    }
    void destroy();
    /*
     * If oneshoot filter specified, use it instead of this->time_filter_
     */
    int get_next(common::TsBlock *&ret_tsblock, bool alloc_tsblock,
                 Filter *oneshoot_filter = nullptr);
    void revert_tsblock();

    friend class TsFileIOReader;

   private:
    int init_chunk_reader();
    FORCE_INLINE bool has_next_chunk() const {
        if (is_aligned_) {
            return value_chunk_meta_cursor_ !=
                   itimeseries_index_->get_value_chunk_meta_list()->end();
        } else {
            return chunk_meta_cursor_ !=
                   itimeseries_index_->get_chunk_meta_list()->end();
        }
    }
    FORCE_INLINE void advance_to_next_chunk() {
        if (is_aligned_) {
            time_chunk_meta_cursor_++;
            value_chunk_meta_cursor_++;
        } else {
            chunk_meta_cursor_++;
        }
    }
    FORCE_INLINE ChunkMeta *get_current_chunk_meta() {
        return chunk_meta_cursor_.get();
    }
    common::TsBlock *alloc_tsblock();

   private:
    ReadFile *read_file_;
    std::string device_path_;
    std::string measurement_name_;

    ITimeseriesIndex *itimeseries_index_;
    common::PageArena timeseries_index_pa_;
    common::SimpleList<ChunkMeta *>::Iterator chunk_meta_cursor_;
    common::SimpleList<ChunkMeta *>::Iterator time_chunk_meta_cursor_;
    common::SimpleList<ChunkMeta *>::Iterator value_chunk_meta_cursor_;
    IChunkReader *chunk_reader_;

    common::TupleDesc tuple_desc_;
    common::TsBlock *tsblock_;
    Filter *time_filter_;
    bool is_aligned_ = false;
};

}  // end namespace storage
#endif  // READER_TSFILE_SERIES_SCAN_ITERATOR_H
