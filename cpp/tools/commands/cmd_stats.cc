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

#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "commands/statistics.h"

namespace tsfile_cli {

int cmd_stats(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(out, fmt,
                {"target", "measurement", "count", "start_time", "end_time",
                 "min", "max", "first", "last", "sum"},
                {common::STRING, common::STRING, common::INT64, common::INT64,
                 common::INT64, common::STRING, common::STRING, common::STRING,
                 common::STRING, common::STRING},
                args.no_header);

    std::vector<SeriesStatRow> rows = collect_series_stats(args, reader);
    for (const SeriesStatRow& row : rows) {
        std::vector<std::string> cells = {
            row.target, row.measurement, std::to_string(row.count),
            std::to_string(row.start_time), std::to_string(row.end_time)};
        cells.insert(cells.end(), row.value_cells.values.begin(),
                     row.value_cells.values.end());

        std::vector<bool> nulls = {false, false, false, row.count == 0,
                                   row.count == 0};
        nulls.insert(nulls.end(), row.value_cells.is_null.begin(),
                     row.value_cells.is_null.end());
        w.write(cells, nulls);
    }
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
