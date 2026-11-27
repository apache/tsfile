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
#ifndef READER_ARROW_QUERY_SCAN_EXECUTOR_H
#define READER_ARROW_QUERY_SCAN_EXECUTOR_H

#include "common/arrow_batch.h"
#include "file/tsfile_io_reader.h"
#include "reader/prepared_series.h"
#include "reader/task/device_task_iterator.h"

namespace storage {
// Query-wide output cursor. Device/page boundaries never end an output batch.
// The implementation owns filters and tasks; exported batches own only data.
class QueryScanExecutor {
   public:
    QueryScanExecutor(std::unique_ptr<DeviceTaskIterator> tasks,
                      TsFileIOReader* io, std::vector<std::string> names,
                      std::vector<common::TSDataType> types,
                      uint32_t batch_size, Filter* time_filter,
                      Filter* field_filter, int offset = 0, int limit = -1);
    ~QueryScanExecutor();
    int bind_prepared(
        const std::vector<std::shared_ptr<PreparedSeries>>& prepared);
    int next(std::shared_ptr<common::ArrowBatch>& batch);
    void close();

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace storage
#endif
