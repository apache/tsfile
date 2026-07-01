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

#include "utils/db_utils.h"

namespace storage {
int TableQueryExecutor::query(const std::string& table_name,
                              const std::vector<std::string>& columns,
                              Filter* time_filter, Filter* id_filter,
                              Filter* field_filter, ResultSet*& ret_qds) {
    int ret = common::E_OK;
    TsFileMeta* file_metadata = nullptr;
    file_metadata = tsfile_io_reader_->get_tsfile_meta();
    common::PageArena pa;
    pa.init(512, common::MOD_TSFILE_READER);
    MetaIndexNode* table_root = nullptr;
    std::shared_ptr<TableSchema> table_schema;
    if (RET_FAIL(
            file_metadata->get_table_metaindex_node(table_name, table_root))) {
    } else if (RET_FAIL(
                   file_metadata->get_table_schema(table_name, table_schema))) {
    }

    if (IS_FAIL(ret)) {
        ret_qds = nullptr;
        return ret;
    }
    std::vector<std::string> lower_case_column_names(columns);
    for (auto& column : lower_case_column_names) {
        to_lowercase_inplace(column);
    }
    std::shared_ptr<ColumnMapping> column_mapping =
        std::make_shared<ColumnMapping>();
    for (size_t i = 0; i < lower_case_column_names.size(); ++i) {
        column_mapping->add(lower_case_column_names[i], static_cast<int>(i),
                            *table_schema);
    }
    std::vector<common::TSDataType> data_types;
    data_types.reserve(lower_case_column_names.size());
    for (size_t i = 0; i < lower_case_column_names.size(); ++i) {
        auto ind = table_schema->find_column_index(lower_case_column_names[i]);
        if (ind < 0) {
            delete time_filter;
            return common::E_COLUMN_NOT_EXIST;
        }
        data_types.push_back(table_schema->get_data_types()[ind]);
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
    ret_qds =
        new TableResultSet(std::move(tsblock_reader), columns, data_types);
    return ret;
}

int TableQueryExecutor::query_on_tree(
    const std::vector<std::shared_ptr<IDeviceID>>& devices,
    const std::vector<std::string>& tag_columns,
    const std::vector<std::string>& field_columns, Filter* time_filter,
    ResultSet*& ret_qds) {
    common::PageArena pa;
    pa.init(512, common::MOD_TSFILE_READER);
    int ret = common::E_OK;
    TsFileMeta* file_meta = tsfile_io_reader_->get_tsfile_meta();
    std::unordered_set<MetaIndexNode*> table_inodes;
    for (auto const& device : devices) {
        MetaIndexNode* table_inode;
        if (RET_FAIL(file_meta->get_table_metaindex_node(
                device->get_table_name(), table_inode))) {
        };
        table_inodes.insert(table_inode);
    }

    std::vector<common::ColumnSchema> col_schema;
    for (auto const& tag : tag_columns) {
        col_schema.emplace_back(tag, common::TSDataType::STRING,
                                common::ColumnCategory::TAG);
    }

    std::unordered_map<std::string, common::TSDataType> column_types_map;

    for (auto const& device : devices) {
        bool all_collected = true;
        for (const auto& field_col : field_columns) {
            if (column_types_map.find(field_col) == column_types_map.end()) {
                all_collected = false;
                break;
            }
        }
        if (all_collected) {
            break;
        }

        std::unordered_set<std::string> measurements(field_columns.begin(),
                                                     field_columns.end());
        std::vector<ITimeseriesIndex*> index(measurements.size());
        if (RET_FAIL(tsfile_io_reader_->get_timeseries_indexes(
                device, measurements, index, pa))) {
            return ret;
        }

        for (auto* ts_index : index) {
            if (ts_index != nullptr) {
                std::string measurement_name =
                    ts_index->get_measurement_name().to_std_string();
                if (column_types_map.find(measurement_name) ==
                    column_types_map.end()) {
                    common::TSDataType type = ts_index->get_data_type();
                    column_types_map[measurement_name] = type;
                }
            }
        }
    }

    for (const auto& field_col : field_columns) {
        if (column_types_map.find(field_col) != column_types_map.end()) {
            col_schema.emplace_back(field_col, column_types_map[field_col],
                                    common::ColumnCategory::FIELD);
        } else {
            col_schema.emplace_back(field_col,
                                    common::TSDataType::INVALID_DATATYPE,
                                    common::ColumnCategory::FIELD);
        }
    }

    auto schema = std::make_shared<TableSchema>("default", col_schema);
    schema->set_virtual_table();
    std::shared_ptr<ColumnMapping> column_mapping =
        std::make_shared<ColumnMapping>();
    for (size_t i = 0; i < col_schema.size(); ++i) {
        column_mapping->add(col_schema[i].column_name_, i, *schema);
    }
    std::vector<common::TSDataType> datatypes = schema->get_data_types();
    std::vector<MetaIndexNode*> index_nodes(table_inodes.begin(),
                                            table_inodes.end());
    auto device_task_iterator =
        std::unique_ptr<DeviceTaskIterator>(new DeviceTaskIterator(
            schema->get_measurement_names(), index_nodes, column_mapping,
            meta_data_querier_, nullptr, schema));
    std::unique_ptr<TsBlockReader> tsblock_reader;
    switch (table_query_ordering_) {
        case TableQueryOrdering::DEVICE:
            tsblock_reader = std::unique_ptr<DeviceOrderedTsBlockReader>(
                new DeviceOrderedTsBlockReader(
                    std::move(device_task_iterator), meta_data_querier_,
                    block_size_, tsfile_io_reader_, time_filter, nullptr));
            break;
        case TableQueryOrdering::TIME:
        default:
            ret = common::E_UNSUPPORTED_ORDER;
    }
    assert(tsblock_reader != nullptr);
    ret_qds = new TableResultSet(std::move(tsblock_reader),
                                 schema->get_measurement_names(),
                                 schema->get_data_types());
    return ret;
}

void TableQueryExecutor::destroy_query_data_set(ResultSet* qds) { delete qds; }
}  // end namespace storage
