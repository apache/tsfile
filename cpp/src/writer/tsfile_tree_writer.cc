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

#include "writer/tsfile_tree_writer.h"

namespace storage {

TsFileTreeWriter::TsFileTreeWriter(storage::WriteFile* writer_file,
                                   uint64_t memory_threshold) {
    tsfile_writer_ = std::make_shared<TsFileWriter>();
    tsfile_writer_->init(writer_file);
    common::g_config_value_.chunk_group_size_threshold_ = memory_threshold;
}

int TsFileTreeWriter::register_timeseries(std::string& device_id,
                                          MeasurementSchema* schema) {
    return tsfile_writer_->register_timeseries(device_id, *schema);
}

int TsFileTreeWriter::register_timeseries(
    std::string& device_id, std::vector<MeasurementSchema*> schemas) {
    return tsfile_writer_->register_aligned_timeseries(device_id, schemas);
}

int TsFileTreeWriter::write(const Tablet& tablet) {
    return tsfile_writer_->write_tree(tablet);
}

int TsFileTreeWriter::write(const TsRecord& record) {
    return tsfile_writer_->write_tree(record);
}

int TsFileTreeWriter::flush() { return tsfile_writer_->flush(); }

int TsFileTreeWriter::close() { return tsfile_writer_->close(); }

}  // namespace storage