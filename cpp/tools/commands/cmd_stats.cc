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
#include <string>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/statistic.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int cmd_stats(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(out, fmt,
                {"target", "measurement", "count", "start_time", "end_time"},
                {common::STRING, common::STRING, common::INT64, common::INT64,
                 common::INT64},
                args.no_header);

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
            std::string m = ts->get_measurement_name().to_std_string();
            if (!args.measurements.empty() &&
                std::find(args.measurements.begin(), args.measurements.end(),
                          m) == args.measurements.end()) {
                continue;
            }
            storage::Statistic* st = ts->get_statistic();
            if (st != nullptr) {
                w.write({target, m, std::to_string(st->get_count()),
                         std::to_string(st->start_time_),
                         std::to_string(st->end_time_)},
                        {false, false, false, false, false});
            } else {
                w.write({target, m, "", "", ""},
                        {false, false, true, true, true});
            }
        }
    }
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
