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
        std::vector<std::string> devices;
        if (!args.device.empty()) {
            devices.push_back(args.device);
        } else {
            for (auto& d : reader.get_all_device_ids()) {
                if (d) {
                    devices.push_back(d->get_device_name());
                }
            }
        }

        std::vector<std::string> paths;
        for (const std::string& dev : devices) {
            std::vector<std::string> ms = args.measurements;
            if (ms.empty()) {
                auto did = std::make_shared<storage::StringArrayDeviceID>(dev);
                std::vector<storage::MeasurementSchema> sch;
                if (reader.get_timeseries_schema(did, sch) == 0) {
                    for (auto& m : sch) {
                        ms.push_back(m.measurement_name_);
                    }
                }
            }
            for (const std::string& m : ms) {
                paths.push_back(dev + "." + m);
            }
        }
        if (paths.empty()) {
            err << "Error: no time series found\n";
            return kExitRuntime;
        }
        qret = reader.query(paths, start, end, rs);
    }

    if (qret != 0 || rs == nullptr) {
        err << "Error: query failed (code " << qret << ")\n";
        if (rs != nullptr) {
            reader.destroy_query_data_set(rs);
        }
        return kExitRuntime;
    }

    int wret = write_result_set(rs, fmt, args.no_header, out, offset, limit);
    reader.destroy_query_data_set(rs);
    return wret == 0 ? kExitOk : kExitRuntime;
}

}  // namespace tsfile_cli
