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
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/schema.h"
#include "format/result_set_format.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int cmd_sample(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err) {
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

    const long long limit = args.limit < 0 ? 10 : args.limit;
    const unsigned long long seed =
        args.has_seed ? static_cast<unsigned long long>(args.seed) : 0ULL;
    int wret =
        emit_result_set_sampled(rs, fmt, args.no_header, out, limit, seed);
    reader.destroy_query_data_set(rs);
    if (wret != 0) {
        err << "Error: failed to read rows: " << error_code_message(wret)
            << "\n";
        return kExitRuntime;
    }
    return kExitOk;
}

}  // namespace tsfile_cli
