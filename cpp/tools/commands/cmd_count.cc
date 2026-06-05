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

int cmd_count(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(out, fmt, {"target", "measurement", "count"},
                {common::STRING, common::STRING, common::INT64},
                args.no_header);

    long long total = 0;
    std::vector<SeriesStatRow> rows = collect_series_stats(args, reader);
    for (const SeriesStatRow& row : rows) {
        total += row.count;
        w.write({row.target, row.measurement, std::to_string(row.count)},
                {false, false, false});
    }
    w.write({"total", "", std::to_string(total)}, {false, true, false});
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
