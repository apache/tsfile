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

#include "reader/table_query_executor.h"

namespace storage {
int TableQueryExecutor::query(const std::string &table_name,
                              const std::vector<std::string> &columns,
                              Filter *time_filter, Filter *id_filter,
                              Filter *field_filter, ResultSet *&ret_qds) {
    int ret = common::E_OK;
    TsFileMeta *file_metadata = nullptr;
    file_metadata = tsfile_io_reader_->get_tsfile_meta();
    common::PageArena pa;
    pa.init(512, common::MOD_TSFILE_READER);
    common::String table_name_str;
    table_name_str.dup_from(table_name, pa);
    MetaIndexNode *table_root = nullptr;
    std::shared_ptr<TableSchema> table_schema;
    if (RET_FAIL(file_metadata->get_table_metaindex_node(table_name_str,
                                                         table_root))) {
    } else if (RET_FAIL(
                   file_metadata->get_table_schema(table_name, table_schema))) {
    }

    if (IS_FAIL(ret)) {
        ret_qds = nullptr;
        return ret;
    }
    std::shared_ptr<ColumnMapping> column_mapping = std::make_shared<ColumnMapping>();
    for (size_t i = 0; i < columns.size(); ++i) {
        column_mapping->add(columns[i], static_cast<int>(i), *table_schema);
    }
    // column_mapping.add(*measurement_filter);

    auto device_task_iterator = std::unique_ptr<DeviceTaskIterator>(
        new DeviceTaskIterator(columns, table_root, column_mapping,
                               meta_data_querier_, id_filter, table_schema));

    std::unique_ptr<TsBlockReader> tsblock_reader;
    switch (table_query_ordering_) {
        case TableQueryOrdering::DEVICE:
            tsblock_reader = std::unique_ptr<DeviceOrderedTsBlockReader>(
                new DeviceOrderedTsBlockReader(
                    std::move(device_task_iterator), meta_data_querier_,
                    block_size_, tsfile_io_reader_, time_filter, field_filter));
            break;
        case TableQueryOrdering::TIME:
        default:
            ret = common::E_UNSUPPORTED_ORDER;
    }
    assert(tsblock_reader != nullptr);
    ret_qds = new TableResultSet(std::move(tsblock_reader), columns,
                                 table_schema->get_data_types());
    return ret;
}

void TableQueryExecutor::destroy_query_data_set(ResultSet *qds) { delete qds; }

}  // end namespace storage
