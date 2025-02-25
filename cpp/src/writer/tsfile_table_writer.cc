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

storage::TsFileTableWriter::~TsFileTableWriter() = default;

int storage::TsFileTableWriter::register_table(const std::shared_ptr<TableSchema>& table_schema) {
    int ret = tsfile_writer_->register_table(table_schema);
    // if multiple tables are registered, set
    exclusive_table_name_ = "";
    return ret;
}

int storage::TsFileTableWriter::write_table(storage::Tablet& tablet) const {
    if (tablet.get_table_name().empty()) {
        tablet.set_table_name(exclusive_table_name_);
    } else if (!exclusive_table_name_.empty() && tablet.get_table_name() != exclusive_table_name_) {
        return common::E_TABLE_NOT_EXIST;
    }
    return tsfile_writer_->write_table(tablet);
}

int storage::TsFileTableWriter::flush() { return tsfile_writer_->flush(); }

int storage::TsFileTableWriter::close() { return tsfile_writer_->close(); }
