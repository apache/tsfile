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
#ifndef READER_TABLE_QUERY_EXECUTOR_H
#define READER_TABLE_QUERY_EXECUTOR_H

#include "common/schema.h"
#include "expression.h"
#include "imeta_data_querier.h"
#include "reader/block/device_ordered_tsblock_reader.h"
#include "reader/block/tsblock_reader.h"
#include "reader/column_mapping.h"
#include "reader/task/device_task_iterator.h"
#include "result_set.h"
#include "table_result_set.h"
#include "utils/errno_define.h"
namespace storage {

class DeviceTaskIterator;

class TableQueryExecutor {
   public:
    enum class TableQueryOrdering { TIME, DEVICE };

    TableQueryExecutor(IMetadataQuerier *meta_data_querier,
                       TsFileIOReader *tsfile_io_reader,
                       TableQueryOrdering table_query_ordering,
                       int block_size = 1024)
        : meta_data_querier_(meta_data_querier),
          tsfile_io_reader_(tsfile_io_reader),
          table_query_ordering_(table_query_ordering),
          block_size_(block_size) {}
    TableQueryExecutor(ReadFile *read_file) {
        tsfile_io_reader_ = new TsFileIOReader();
        tsfile_io_reader_->init(read_file);
        meta_data_querier_ = new MetadataQuerier(tsfile_io_reader_);
        table_query_ordering_ = TableQueryOrdering::DEVICE;
        block_size_ = 1024;
    }
    ~TableQueryExecutor() {
        if (meta_data_querier_ != nullptr) {
            delete meta_data_querier_;
            meta_data_querier_ = nullptr;
        }
        if (tsfile_io_reader_ != nullptr) {
            delete tsfile_io_reader_;
            tsfile_io_reader_ = nullptr;
        }
    }
    int query(const std::string &table_name,
              const std::vector<std::string> &columns, Filter *time_filter,
              Filter *id_filter, Filter *field_filter, ResultSet *&ret_qds);
    void destroy_query_data_set(ResultSet *qds);

   private:
    IMetadataQuerier *meta_data_querier_;
    TsFileIOReader *tsfile_io_reader_;
    TableQueryOrdering table_query_ordering_;
    int32_t block_size_;
};

}  // namespace storage

#endif  // READER_TABLE_QUERY_EXECUTOR_H