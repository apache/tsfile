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

#include "commands/statistics.h"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <set>
#include <sstream>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/datatype/date_converter.h"
#include "common/device_id.h"
#include "common/statistic.h"
#include "format/output_format.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "utils/storage_utils.h"

namespace tsfile_cli {
namespace {

template <typename T>
std::string value_to_string(const T& value) {
    std::ostringstream ss;
    ss << value;
    return ss.str();
}

std::string value_to_string(float value) {
    std::ostringstream ss;
    ss << std::setprecision(std::numeric_limits<float>::max_digits10) << value;
    return ss.str();
}

std::string value_to_string(double value) {
    std::ostringstream ss;
    ss << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
    return ss.str();
}

std::string bool_to_string(bool value) { return value ? "true" : "false"; }

std::string string_to_std(const common::String& value) {
    return value.to_std_string();
}

std::string date_to_string(int32_t value) {
    std::tm date = std::tm();
    if (common::DateConverter::int_to_date(value, date) != common::E_OK) {
        return "";
    }
    char result[16];
    std::snprintf(result, sizeof(result), "%04d-%02d-%02d", date.tm_year + 1900,
                  date.tm_mon + 1, date.tm_mday);
    return result;
}

long long file_size(const std::string& path) {
    std::ifstream in(path.c_str(), std::ios::binary | std::ios::ate);
    if (!in.good()) {
        return 0;
    }
    return static_cast<long long>(in.tellg());
}

}  // namespace

StatisticCells statistic_value_cells(storage::Statistic* st) {
    StatisticCells cells;
    cells.values.assign(5, "");
    cells.is_null.assign(5, true);
    if (st == nullptr || st->get_count() == 0) {
        return cells;
    }

    switch (st->get_type()) {
        case common::BOOLEAN: {
            auto* s = static_cast<storage::BooleanStatistic*>(st);
            cells.values = {"", "", bool_to_string(s->first_value_),
                            bool_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {true, true, false, false, false};
            break;
        }
        case common::INT32: {
            auto* s = static_cast<storage::Int32Statistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {false, false, false, false, false};
            break;
        }
        case common::DATE: {
            auto* s = static_cast<storage::Int32Statistic*>(st);
            cells.values = {
                date_to_string(s->min_value_), date_to_string(s->max_value_),
                date_to_string(s->first_value_), date_to_string(s->last_value_),
                ""};
            cells.is_null = {false, false, false, false, true};
            break;
        }
        case common::INT64:
        case common::TIMESTAMP: {
            // Int64Statistic stores sum in double, which can lose precision
            // for large INT64 values; a sum is not meaningful for timestamps.
            // Keep the externally visible sum cell null for both types.
            auto* s = static_cast<storage::Int64Statistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_), ""};
            cells.is_null = {false, false, false, false, true};
            break;
        }
        case common::FLOAT: {
            auto* s = static_cast<storage::FloatStatistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {false, false, false, false, false};
            break;
        }
        case common::DOUBLE: {
            auto* s = static_cast<storage::DoubleStatistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {false, false, false, false, false};
            break;
        }
        case common::STRING: {
            auto* s = static_cast<storage::StringStatistic*>(st);
            cells.values = {string_to_std(s->min_value_),
                            string_to_std(s->max_value_),
                            string_to_std(s->first_value_),
                            string_to_std(s->last_value_), ""};
            cells.is_null = {false, false, false, false, true};
            break;
        }
        case common::TEXT: {
            auto* s = static_cast<storage::TextStatistic*>(st);
            cells.values = {"", "", string_to_std(s->first_value_),
                            string_to_std(s->last_value_), ""};
            cells.is_null = {true, true, false, false, true};
            break;
        }
        default:
            break;
    }
    return cells;
}

int collect_series_stats(const ParsedArgs& args, storage::TsFileReader& reader,
                         std::vector<SeriesStatRow>& rows, std::ostream& err) {
    rows.clear();
    const auto all_devices = reader.get_all_device_ids();
    std::vector<std::shared_ptr<storage::IDeviceID>> devices;
    for (const auto& device : all_devices) {
        if (device &&
            (args.device.empty() || device->get_device_name() == args.device)) {
            devices.push_back(device);
        }
    }
    if (!args.device.empty() && devices.empty()) {
        err << "Error: device '" << args.device << "' does not exist\n";
        return kExitUsage;
    }

    storage::DeviceTimeseriesMetadataMap metadata =
        reader.get_timeseries_metadata(devices);
    std::map<std::string,
             std::map<std::string, std::shared_ptr<storage::ITimeseriesIndex>>>
        metadata_by_name;
    for (auto& entry : metadata) {
        if (!entry.first) {
            continue;
        }
        const std::string device_name = entry.first->get_device_name();
        for (const auto& series : entry.second) {
            if (series) {
                metadata_by_name[device_name][series->get_measurement_name()
                                                  .to_std_string()] = series;
            }
        }
    }

    std::set<std::string> matched_measurements;
    for (const auto& device : devices) {
        const std::string device_name = device->get_device_name();
        std::vector<storage::MeasurementSchema> schemas;
        int schema_ret = reader.get_timeseries_schema(device, schemas);
        if (schema_ret != common::E_OK) {
            err << "Error: failed to read schema for device '" << device_name
                << "': " << error_code_message(schema_ret) << "\n";
            return kExitFile;
        }
        std::set<std::string> known;
        std::vector<std::string> paths;
        for (const auto& schema : schemas) {
            known.insert(schema.measurement_name_);
            paths.push_back(device_name + "." + schema.measurement_name_);
        }
        if (!args.device.empty()) {
            for (const std::string& requested : args.measurements) {
                if (known.find(requested) != known.end()) {
                    continue;
                }
                err << "Error: FIELD '" << requested
                    << "' does not exist in device '" << device_name << "'\n";
                return kExitUsage;
            }
        }

        long long row_count = 0;
        if (!paths.empty()) {
            storage::ResultSet* result = nullptr;
            int query_ret =
                reader.query(paths, std::numeric_limits<int64_t>::min(),
                             std::numeric_limits<int64_t>::max(), result);
            if (query_ret != common::E_OK || result == nullptr) {
                if (result != nullptr) {
                    reader.destroy_query_data_set(result);
                }
                err << "Error: failed to count rows for device '" << device_name
                    << "': " << error_code_message(query_ret) << "\n";
                return kExitFile;
            }
            bool has_next = false;
            int next_ret = common::E_OK;
            while ((next_ret = result->next(has_next)) == common::E_OK &&
                   has_next) {
                ++row_count;
            }
            reader.destroy_query_data_set(result);
            if (next_ret != common::E_OK) {
                err << "Error: failed to count rows for device '" << device_name
                    << "': " << error_code_message(next_ret) << "\n";
                return kExitFile;
            }
        }

        for (const auto& schema : schemas) {
            if (!args.measurements.empty() &&
                std::find(args.measurements.begin(), args.measurements.end(),
                          schema.measurement_name_) ==
                    args.measurements.end()) {
                continue;
            }
            matched_measurements.insert(schema.measurement_name_);
            SeriesStatRow row;
            row.target = device_name;
            row.measurement = schema.measurement_name_;
            row.data_type = schema.data_type_;
            row.row_count = row_count;
            auto device_meta = metadata_by_name.find(device_name);
            if (device_meta != metadata_by_name.end()) {
                auto series =
                    device_meta->second.find(schema.measurement_name_);
                if (series != device_meta->second.end() && series->second) {
                    storage::Statistic* statistic =
                        series->second->get_statistic();
                    if (statistic != nullptr) {
                        row.has_statistic = true;
                        row.count = statistic->get_count();
                        row.start_time = statistic->start_time_;
                        row.end_time = statistic->end_time_;
                        row.value_cells = statistic_value_cells(statistic);
                    }
                }
            }
            if (row.value_cells.values.empty()) {
                row.value_cells.values.assign(5, "");
                row.value_cells.is_null.assign(5, true);
            }
            rows.push_back(row);
        }
    }
    if (args.device.empty()) {
        for (const std::string& requested : args.measurements) {
            if (matched_measurements.find(requested) ==
                matched_measurements.end()) {
                err << "Error: FIELD '" << requested
                    << "' does not exist in the file\n";
                return kExitUsage;
            }
        }
    }
    return kExitOk;
}

FileSummary collect_file_summary(const ParsedArgs& args,
                                 storage::TsFileReader& reader) {
    FileSummary s;
    s.file = args.file;
    s.model = is_table_model(args, reader) ? "table" : "tree";
    s.device_count = static_cast<long long>(reader.get_all_device_ids().size());
    s.table_count =
        static_cast<long long>(reader.get_all_table_schemas().size());
    s.file_size_bytes = file_size(args.file);

    storage::DeviceTimeseriesMetadataMap metadata =
        reader.get_timeseries_metadata();
    int64_t min_start = std::numeric_limits<int64_t>::max();
    int64_t max_end = std::numeric_limits<int64_t>::min();
    for (const auto& device : metadata) {
        for (const auto& series : device.second) {
            if (!series) {
                continue;
            }
            ++s.series_count;
            storage::Statistic* statistic = series->get_statistic();
            if (statistic == nullptr || statistic->get_count() <= 0) {
                continue;
            }
            min_start = std::min(min_start, statistic->start_time_);
            max_end = std::max(max_end, statistic->get_end_time());
            s.has_time_range = true;
        }
    }
    if (s.has_time_range) {
        s.start_time = min_start;
        s.end_time = max_end;
    }
    return s;
}

}  // namespace tsfile_cli
