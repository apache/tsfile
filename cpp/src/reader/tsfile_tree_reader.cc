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

#include "reader/tsfile_tree_reader.h"

namespace storage {

TsFileTreeReader::TsFileTreeReader() {
    tsfile_reader_ = std::make_shared<TsFileReader>();
}

TsFileTreeReader::~TsFileTreeReader() = default;

int TsFileTreeReader::open(const std::string& file_path) {
    return tsfile_reader_->open(file_path);
}

int TsFileTreeReader::close() { return tsfile_reader_->close(); }

int TsFileTreeReader::query(const std::vector<std::string>& device_ids,
                            const std::vector<std::string>& measurement_names,
                            int64_t start_time, int64_t end_time,
                            ResultSet*& result_set) {
    std::vector<std::string> path_list;
    for (auto& device_id : device_ids) {
        for (auto& measurement : measurement_names) {
            path_list.emplace_back(device_id + PATH_SEPARATOR_CHAR +
                                   measurement);
        }
    }
    return tsfile_reader_->query(path_list, start_time, end_time, result_set);
}

void TsFileTreeReader::destroy_query_data_set(ResultSet* qds) {
    tsfile_reader_->destroy_query_data_set(qds);
}

std::vector<MeasurementSchema> TsFileTreeReader::get_device_schema(
    const std::string& device_id) {
    std::vector<MeasurementSchema> schemas;
    tsfile_reader_->get_timeseries_schema(
        std::make_shared<StringArrayDeviceID>(device_id), schemas);
    return schemas;
}

std::vector<std::string> TsFileTreeReader::get_all_device_ids() {
    std::vector<std::string> ret_device_ids;
    auto device_ids = tsfile_reader_->get_all_device_ids();
    for (auto device_id : device_ids) {
        ret_device_ids.emplace_back(device_id->get_device_name());
    }
    return ret_device_ids;
}

std::vector<std::shared_ptr<IDeviceID>> TsFileTreeReader::get_all_devices() {
    return tsfile_reader_->get_all_devices();
}

DeviceTimeseriesMetadataMap TsFileTreeReader::get_timeseries_metadata(
    const std::vector<std::shared_ptr<IDeviceID>>& device_ids) {
    return tsfile_reader_->get_timeseries_metadata(device_ids);
}

DeviceTimeseriesMetadataMap TsFileTreeReader::get_timeseries_metadata() {
    return tsfile_reader_->get_timeseries_metadata();
}

}  // namespace storage
