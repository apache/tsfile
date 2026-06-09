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
#include <limits>
#include <sstream>

#include "commands/commands.h"
#include "common/statistic.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {
namespace {

template <typename T>
std::string value_to_string(const T& value) {
    std::ostringstream ss;
    ss << value;
    return ss.str();
}

std::string bool_to_string(bool value) { return value ? "true" : "false"; }

std::string string_to_std(const common::String& value) {
    return value.to_std_string();
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
        case common::INT32:
        case common::DATE: {
            auto* s = static_cast<storage::Int32Statistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {false, false, false, false, false};
            break;
        }
        case common::INT64:
        case common::TIMESTAMP: {
            auto* s = static_cast<storage::Int64Statistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {false, false, false, false, false};
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

std::vector<SeriesStatRow> collect_series_stats(const ParsedArgs& args,
                                                storage::TsFileReader& reader) {
    std::vector<SeriesStatRow> rows;
    storage::DeviceTimeseriesMetadataMap meta =
        reader.get_timeseries_metadata();
    for (auto& kv : meta) {
        std::string target = kv.first ? kv.first->get_device_name() : "";
        if (!args.device.empty() && target != args.device) {
            continue;
        }
        if (!args.table.empty() && kv.first &&
            kv.first->get_table_name() != args.table) {
            continue;
        }
        for (auto& ts : kv.second) {
            if (!ts) {
                continue;
            }
            std::string measurement =
                ts->get_measurement_name().to_std_string();
            if (!args.measurements.empty() &&
                std::find(args.measurements.begin(), args.measurements.end(),
                          measurement) == args.measurements.end()) {
                continue;
            }
            storage::Statistic* st = ts->get_statistic();
            SeriesStatRow row;
            row.target = target;
            row.measurement = measurement;
            if (st != nullptr) {
                row.count = st->get_count();
                row.start_time = st->start_time_;
                row.end_time = st->end_time_;
                row.value_cells = statistic_value_cells(st);
            } else {
                row.value_cells.values.assign(5, "");
                row.value_cells.is_null.assign(5, true);
            }
            rows.push_back(row);
        }
    }
    return rows;
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

    // The file summary (series_count + overall time range) describes the whole
    // file, so clear any -d/-t/-m filters from the args copy before collecting
    // per-series stats; otherwise meta would only count the filtered subset.
    ParsedArgs all = args;
    all.device.clear();
    all.table.clear();
    all.measurements.clear();
    std::vector<SeriesStatRow> rows = collect_series_stats(all, reader);
    s.series_count = static_cast<long long>(rows.size());
    long long min_start = std::numeric_limits<long long>::max();
    long long max_end = std::numeric_limits<long long>::min();
    for (const SeriesStatRow& row : rows) {
        if (row.count <= 0) {
            continue;
        }
        min_start = std::min(min_start, row.start_time);
        max_end = std::max(max_end, row.end_time);
        s.has_time_range = true;
    }
    if (s.has_time_range) {
        s.start_time = min_start;
        s.end_time = max_end;
    }
    return s;
}

}  // namespace tsfile_cli
