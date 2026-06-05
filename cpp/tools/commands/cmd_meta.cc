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

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "commands/statistics.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int cmd_meta(const ParsedArgs& args, storage::TsFileReader& reader,
             OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(out, fmt,
                {"file", "model", "device_count", "table_count", "series_count",
                 "start_time", "end_time", "file_size_bytes"},
                {common::STRING, common::STRING, common::INT64, common::INT64,
                 common::INT64, common::INT64, common::INT64, common::INT64},
                args.no_header);

    FileSummary s = collect_file_summary(args, reader);
    w.write({s.file, s.model, std::to_string(s.device_count),
             std::to_string(s.table_count), std::to_string(s.series_count),
             s.has_time_range ? std::to_string(s.start_time) : "",
             s.has_time_range ? std::to_string(s.end_time) : "",
             std::to_string(s.file_size_bytes)},
            {false, false, false, false, false, !s.has_time_range,
             !s.has_time_range, false});
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
