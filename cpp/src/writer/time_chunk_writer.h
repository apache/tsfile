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

#ifndef WRITER_CHUNK_TIME_WRITER_H
#define WRITER_CHUNK_TIME_WRITER_H

#include "common/allocator/byte_stream.h"
#include "common/global.h"
#include "common/statistic.h"
#include "common/tsfile_common.h"
#include "time_page_writer.h"

namespace storage {

class TimeChunkWriter {
   public:
    static const int32_t PAGES_DATA_PAGE_SIZE = 1024;

   public:
    TimeChunkWriter()
        : time_page_writer_(),
          chunk_statistic_(nullptr),
          chunk_data_(PAGES_DATA_PAGE_SIZE, common::MOD_CW_PAGES_DATA),
          first_page_data_(),
          first_page_statistic_(nullptr),
          chunk_header_(),
          num_of_pages_(0) {}
    ~TimeChunkWriter() { destroy(); }
    int init(const common::ColumnDesc &col_desc);
    int init(const std::string &measurement_name, common::TSEncoding encoding,
             common::CompressionType compression_type);
    void destroy();

    storage::ChunkHeader get_chunk_header() const { return chunk_header_; }

    FORCE_INLINE int write(int64_t timestamp) {
        int ret = common::E_OK;
        if (RET_FAIL(time_page_writer_.write(timestamp))) {
            return ret;
        }
        if (RET_FAIL(seal_cur_page_if_full())) {
            return ret;
        }
        return ret;
    }

    int end_encode_chunk();
    common::ByteStream &get_chunk_data() { return chunk_data_; }
    Statistic *get_chunk_statistic() { return chunk_statistic_; }
    FORCE_INLINE int32_t num_of_pages() const { return num_of_pages_; }

    int64_t estimate_max_series_mem_size();

   private:
    FORCE_INLINE bool is_cur_page_full() const {
        // FIXME
        return (time_page_writer_.get_point_numer() >=
                common::g_config_value_.page_writer_max_point_num_) ||
               (time_page_writer_.get_page_memory_size() >=
                common::g_config_value_.page_writer_max_memory_bytes_);
    }
    FORCE_INLINE int seal_cur_page_if_full() {
        if (UNLIKELY(is_cur_page_full())) {
            return seal_cur_page(false);
        }
        return common::E_OK;
    }
    FORCE_INLINE void free_first_writer_data() {
        // free memory
        first_page_data_.destroy();
        StatisticFactory::free(first_page_statistic_);
        first_page_statistic_ = nullptr;
    }
    int seal_cur_page(bool end_chunk);
    void save_first_page_data(TimePageWriter &first_time_page_writer);
    int write_first_page_data(common::ByteStream &pages_data);

   private:
    TimePageWriter time_page_writer_;
    Statistic *chunk_statistic_;
    common::ByteStream chunk_data_;

    // to save first page data
    TimePageData first_page_data_;
    Statistic *first_page_statistic_;

    ChunkHeader chunk_header_;
    int32_t num_of_pages_;
};

}  // end namespace storage

#endif  // WRITER_CHUNK_TIME_WRITER_H
