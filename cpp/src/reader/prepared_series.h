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

#include <memory>

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

    PreparedLocator()
        : locator_id(0),
          layout(0),
          flags(0),
          value_metadata_offset(0),
          value_metadata_length(0),
          time_metadata_offset(0),
          time_metadata_length(0) {}
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
    void set_aligned_time_owner(
        const std::shared_ptr<PreparedSeries>& aligned_time_owner) {
        aligned_time_owner_ = aligned_time_owner;
    }

   private:
    FileGeneration generation_;
    PreparedLocator locator_;
    common::PageArena arena_;
    ITimeseriesIndex* index_;
    // Optional owner of the aligned time index referenced by index_. This
    // lets several value PreparedSeries share one parsed time metadata arena.
    std::shared_ptr<PreparedSeries> aligned_time_owner_;
};

}  // namespace storage

#endif  // READER_PREPARED_SERIES_H
