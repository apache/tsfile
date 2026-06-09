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

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/device_id.h"
#include "common/schema.h"
#include "format/result_set_format.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

std::vector<std::string> collect_tree_query_paths(
    const ParsedArgs& args, storage::TsFileReader& reader) {
    std::vector<std::string> paths;
    const bool has_projection = !args.measurements.empty();
    auto include_measurement = [&](const std::string& m) {
        return !has_projection ||
               std::find(args.measurements.begin(), args.measurements.end(),
                         m) != args.measurements.end();
    };

    if (!args.device.empty()) {
        // A single device was requested: resolve its series and keep only the
        // ones matching the projection. Filtering against the device's real
        // schema means a provided measurement that doesn't exist on the device
        // is dropped rather than queried blindly (matching the no-device path).
        auto did = std::make_shared<storage::StringArrayDeviceID>(args.device);
        std::vector<storage::MeasurementSchema> sch;
        if (reader.get_timeseries_schema(did, sch) == 0) {
            for (auto& m : sch) {
                if (include_measurement(m.measurement_name_)) {
                    paths.push_back(args.device + "." + m.measurement_name_);
                }
            }
        }
        return paths;
    }

    // No device filter: collect every device/series from a single whole-file
    // metadata call instead of querying each device one by one.
    storage::DeviceTimeseriesMetadataMap meta =
        reader.get_timeseries_metadata();
    for (auto& kv : meta) {
        if (!kv.first) {
            continue;
        }
        const std::string dev = kv.first->get_device_name();
        for (auto& ts : kv.second) {
            if (!ts) {
                continue;
            }
            const std::string m = ts->get_measurement_name().to_std_string();
            if (include_measurement(m)) {
                paths.push_back(dev + "." + m);
            }
        }
    }
    return paths;
}

int run_row_query(const ParsedArgs& args, storage::TsFileReader& reader,
                  OutputFormat fmt, std::ostream& out, std::ostream& err,
                  long long offset, long long limit) {
    const int64_t start = args.has_start ? static_cast<int64_t>(args.start)
                                         : std::numeric_limits<int64_t>::min();
    const int64_t end = args.has_end ? static_cast<int64_t>(args.end)
                                     : std::numeric_limits<int64_t>::max();

    storage::ResultSet* rs = nullptr;
    int qret = 0;

    if (is_table_model(args, reader)) {
        std::string table_name = args.table;
        if (table_name.empty()) {
            auto schemas = reader.get_all_table_schemas();
            if (schemas.empty() || !schemas[0]) {
                err << "Error: no table found in file\n";
                return kExitRuntime;
            }
            table_name = schemas[0]->get_table_name();
        }
        std::vector<std::string> cols = args.measurements;
        if (cols.empty()) {
            auto ts = reader.get_table_schema(table_name);
            if (ts) {
                cols = ts->get_measurement_names();
            }
        }
        qret = reader.query(table_name, cols, start, end, rs);
    } else {
        std::vector<std::string> paths = collect_tree_query_paths(args, reader);
        if (paths.empty()) {
            err << "Error: no time series found\n";
            return kExitRuntime;
        }
        qret = reader.query(paths, start, end, rs);
    }

    if (qret != 0 || rs == nullptr) {
        err << "Error: query failed: " << error_code_message(qret) << "\n";
        if (rs != nullptr) {
            reader.destroy_query_data_set(rs);
        }
        return kExitRuntime;
    }

    int wret = emit_result_set(rs, fmt, args.no_header, out, offset, limit);
    reader.destroy_query_data_set(rs);
    if (wret != 0) {
        err << "Error: failed to read rows: " << error_code_message(wret)
            << "\n";
        return kExitRuntime;
    }
    return kExitOk;
}

}  // namespace tsfile_cli
