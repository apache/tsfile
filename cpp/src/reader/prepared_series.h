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

#ifndef READER_PREPARED_SERIES_H
#define READER_PREPARED_SERIES_H

#include <stdint.h>

#include <mutex>
#include <vector>

#include "common/allocator/page_arena.h"
#include "common/tsfile_common.h"

namespace storage {

struct FileGeneration {
    uint64_t mapped_index_identity;
    uint32_t file_id;
    uint64_t file_size;
    uint64_t file_fingerprint;

    FileGeneration()
        : mapped_index_identity(0),
          file_id(0),
          file_size(0),
          file_fingerprint(0) {}
};

struct PreparedLocator {
    uint32_t locator_id;
    uint16_t layout;
    uint16_t flags;
    uint64_t value_metadata_offset;
    uint32_t value_metadata_length;
    uint64_t time_metadata_offset;
    uint32_t time_metadata_length;
    uint32_t chunk_count_hint;

    PreparedLocator()
        : locator_id(0),
          layout(0),
          flags(0),
          value_metadata_offset(0),
          value_metadata_length(0),
          time_metadata_offset(0),
          time_metadata_length(0),
          chunk_count_hint(0) {}
};

struct PageLocator {
    uint64_t row_begin;
    uint64_t row_end;
    uint32_t chunk_ordinal;
    uint64_t chunk_header_offset;
    uint64_t page_header_offset;
    uint64_t page_data_offset;
    uint64_t value_chunk_header_offset;
    uint64_t value_page_header_offset;
    uint64_t value_page_data_offset;

    PageLocator()
        : row_begin(0),
          row_end(0),
          chunk_ordinal(0),
          chunk_header_offset(0),
          page_header_offset(0),
          page_data_offset(0),
          value_chunk_header_offset(0),
          value_page_header_offset(0),
          value_page_data_offset(0) {}
};

class PagePositionIndex {
   public:
    PagePositionIndex() : covered_rows_(0) {}

    // Publishes only a complete, gap-free prefix. A failed append leaves the
    // visible prefix unchanged.
    bool append_complete(const PageLocator& locator);
    bool find(uint64_t row, PageLocator& result) const;
    uint64_t covered_rows() const;
    size_t size() const;

   private:
    mutable std::mutex mutex_;
    std::vector<PageLocator> pages_;
    uint64_t covered_rows_;
};

class PreparedSeries {
   public:
    PreparedSeries(const FileGeneration& generation,
                   const PreparedLocator& locator);
    ~PreparedSeries();

    const FileGeneration& generation() const { return generation_; }
    const PreparedLocator& locator() const { return locator_; }
    ITimeseriesIndex* index() const { return index_; }
    common::PageArena& arena() { return arena_; }
    void set_index(ITimeseriesIndex* index) { index_ = index; }
    PagePositionIndex& page_positions() { return page_positions_; }

   private:
    FileGeneration generation_;
    PreparedLocator locator_;
    common::PageArena arena_;
    ITimeseriesIndex* index_;
    PagePositionIndex page_positions_;
};

}  // namespace storage

#endif  // READER_PREPARED_SERIES_H
