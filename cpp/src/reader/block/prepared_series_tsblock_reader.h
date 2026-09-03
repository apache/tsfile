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
#ifndef READER_BLOCK_PREPARED_SERIES_TSBLOCK_READER_H
#define READER_BLOCK_PREPARED_SERIES_TSBLOCK_READER_H

#include <memory>

#include "common/allocator/page_arena.h"
#include "reader/block/tsblock_reader.h"

namespace storage {

class Filter;
class PreparedSeries;
class TsFileIOReader;
class TsFileSeriesScanIterator;

// Adapts one locator-backed PreparedSeries to the table-model batch contract.
// The underlying SSI already decodes directly into a two-column TsBlock
// (time, value), so this reader must not materialize RowRecord objects.
class PreparedSeriesTsBlockReader final : public TsBlockReader {
   public:
    PreparedSeriesTsBlockReader() = default;
    ~PreparedSeriesTsBlockReader() override { close(); }

    int init(TsFileIOReader* io_reader,
             const std::shared_ptr<PreparedSeries>& prepared,
             Filter* owned_time_filter, int offset, int limit);
    int init_multi(TsFileIOReader* io_reader,
                   const std::vector<std::shared_ptr<PreparedSeries>>& prepared,
                   Filter* owned_time_filter, int offset, int limit);

    int has_next(bool& has_next) override;
    int next(common::TsBlock*& ret_block) override;
    void close() override;

    common::TSDataType value_data_type() const { return value_data_type_; }
    const std::vector<common::TSDataType>& value_data_types() const {
        return value_data_types_;
    }

   private:
    TsFileIOReader* io_reader_ = nullptr;
    std::shared_ptr<PreparedSeries> prepared_;
    Filter* owned_time_filter_ = nullptr;
    TsFileSeriesScanIterator* ssi_ = nullptr;
    common::PageArena pa_;
    common::TsBlock* block_ = nullptr;
    common::TSDataType value_data_type_ = common::INVALID_DATATYPE;
    std::vector<common::TSDataType> value_data_types_;
    int remaining_limit_ = -1;
    bool block_ready_ = false;
    bool exhausted_ = false;
    bool closed_ = false;
};

}  // namespace storage

#endif  // READER_BLOCK_PREPARED_SERIES_TSBLOCK_READER_H
