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

#include "tsfile_table_writer.h"

#include "file/restorable_tsfile_io_writer.h"

namespace storage {

// Constructor for appending after recovery: schema comes from restored file.
TsFileTableWriter::TsFileTableWriter(
    storage::RestorableTsFileIOWriter* restorable_writer,
    uint64_t memory_threshold)
    : error_number(common::E_OK) {
    tsfile_writer_ = std::make_shared<TsFileWriter>();
    error_number = tsfile_writer_->init(restorable_writer);
    if (error_number != common::E_OK) {
        return;
    }
    tsfile_writer_->set_generate_table_schema(false);
    std::shared_ptr<Schema> schema = restorable_writer->get_known_schema();
    if (schema && schema->table_schema_map_.size() == 1) {
        exclusive_table_name_ = schema->table_schema_map_.begin()->first;
    } else {
        exclusive_table_name_.clear();
    }
    common::g_config_value_.chunk_group_size_threshold_ = memory_threshold;
}

}  // namespace storage

storage::TsFileTableWriter::~TsFileTableWriter() = default;

int storage::TsFileTableWriter::register_table(
    const std::shared_ptr<TableSchema>& table_schema) {
    int ret = tsfile_writer_->register_table(table_schema);
    // if multiple tables are registered, set
    exclusive_table_name_ = "";
    return ret;
}

int storage::TsFileTableWriter::write_table(storage::Tablet& tablet) const {
    // DIRTY CODE...
    if (common::E_OK != error_number) {
        return error_number;
    }
    if (tablet.get_table_name().empty()) {
        tablet.set_table_name(exclusive_table_name_);
    } else if (!exclusive_table_name_.empty() &&
               tablet.get_table_name() != exclusive_table_name_) {
        return common::E_TABLE_NOT_EXIST;
    }
    tablet.set_table_name(to_lower(tablet.get_table_name()));
    for (size_t i = 0; i < tablet.get_column_count(); i++) {
        tablet.set_column_name(i, to_lower(tablet.get_column_name(i)));
    }

    auto schema_map = tablet.get_schema_map();
    std::map<std::string, int> schema_map_;
    for (auto iter = schema_map.begin(); iter != schema_map.end(); iter++) {
        schema_map_[to_lower(iter->first)] = iter->second;
    }
    tablet.set_schema_map(schema_map_);

    return tsfile_writer_->write_table(tablet);
}

int storage::TsFileTableWriter::flush() { return tsfile_writer_->flush(); }

int storage::TsFileTableWriter::close() { return tsfile_writer_->close(); }
