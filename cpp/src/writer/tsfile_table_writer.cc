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

storage::TsFileTableWriter::~TsFileTableWriter() { close(); }

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
    // Always lowercase the incoming tablet's table / column / schema-map
    // names: each call may carry a fresh tablet with mixed-case identifiers,
    // and the underlying engine expects lowercase. Lowering is idempotent so
    // reusing the same tablet across calls remains cheap.
    tablet.set_table_name(to_lower(tablet.get_table_name()));
    for (size_t i = 0; i < tablet.get_column_count(); i++) {
        tablet.set_column_name(i, to_lower(tablet.get_column_name(i)));
    }

    auto schema_map = tablet.get_schema_map();
    std::map<std::string, int> new_schema_map;
    for (auto iter = schema_map.begin(); iter != schema_map.end(); iter++) {
        new_schema_map[to_lower(iter->first)] = iter->second;
    }
    tablet.set_schema_map(new_schema_map);

    return tsfile_writer_->write_table(tablet);
}

int storage::TsFileTableWriter::flush() {
    if (closed_) {
        return common::E_OK;
    }
    return tsfile_writer_->flush();
}

int storage::TsFileTableWriter::add_tsfile_property(const std::string& key,
                                                    const uint8_t* value,
                                                    uint32_t value_len) {
    if (closed_ || !tsfile_writer_) {
        return common::E_FILE_WRITE_ERR;
    }
    return tsfile_writer_->add_tsfile_property(key, value, value_len);
}

int storage::TsFileTableWriter::add_tsfile_property(
    const std::string& key, const std::vector<uint8_t>& value) {
    if (closed_ || !tsfile_writer_) {
        return common::E_FILE_WRITE_ERR;
    }
    return tsfile_writer_->add_tsfile_property(key, value);
}

int storage::TsFileTableWriter::close() {
    if (closed_) {
        return common::E_OK;
    }
    if (!tsfile_writer_) {
        closed_ = true;
        return common::E_OK;
    }
    // Don't latch closed_ until the underlying writer reports success: a
    // failed footer write / sync / file close should be retryable, and the
    // destructor must still be able to drive a final close attempt.  The
    // previous order returned E_OK on every retry after the first failure,
    // potentially leaving the file unfinished and leaking the fd.
    int ret = tsfile_writer_->close();
    if (ret == common::E_OK) {
        closed_ = true;
    }
    return ret;
}
