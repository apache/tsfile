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
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

bool is_table_model(const ParsedArgs& args, storage::TsFileReader& reader) {
    if (args.model == "tree") {
        return false;
    }
    if (args.model == "table") {
        return true;
    }
    return !reader.get_all_table_schemas().empty();
}

int cmd_ls(const ParsedArgs& args, storage::TsFileReader& reader,
           OutputFormat fmt, std::ostream& out, std::ostream& err) {
    std::vector<std::string> names;
    if (is_table_model(args, reader)) {
        for (auto& ts : reader.get_all_table_schemas()) {
            if (ts) {
                names.push_back(ts->get_table_name());
            }
        }
    } else {
        for (auto& dev : reader.get_all_device_ids()) {
            if (dev) {
                names.push_back(dev->get_device_name());
            }
        }
    }

    const std::string model = is_table_model(args, reader) ? "table" : "tree";
    RowWriter w(out, fmt, {"model", "object"}, {common::STRING, common::STRING},
                false);
    for (const std::string& n : names) {
        if (!w.write({model, n}, {false, false})) {
            err << "Error: failed to write output\n";
            return kExitRuntime;
        }
    }
    if (!w.finish()) {
        err << "Error: failed to write output\n";
        return kExitRuntime;
    }
    return kExitOk;
}

}  // namespace tsfile_cli
