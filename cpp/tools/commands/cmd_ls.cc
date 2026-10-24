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
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int resolve_table_model(const ParsedArgs& args, storage::TsFileReader& reader,
                        bool& table_model, std::ostream& err) {
    table_model = false;
    if (args.model == "tree") {
        return kExitOk;
    }
    if (args.model == "table") {
        table_model = true;
        return kExitOk;
    }
    std::vector<std::shared_ptr<storage::TableSchema>> schemas;
    const int ret = reader.get_all_table_schemas(schemas);
    if (ret != common::E_OK) {
        err << "Error: failed to read table schemas: "
            << error_code_message(ret) << "\n";
        return kExitFile;
    }
    table_model = !schemas.empty();
    return kExitOk;
}

std::vector<std::shared_ptr<storage::TableSchema>> sorted_table_schemas(
    storage::TsFileReader& reader) {
    auto schemas = reader.get_all_table_schemas();
    std::sort(schemas.begin(), schemas.end(),
              [](const std::shared_ptr<storage::TableSchema>& lhs,
                 const std::shared_ptr<storage::TableSchema>& rhs) {
                  if (!lhs) return false;
                  if (!rhs) return true;
                  return lhs->get_table_name() < rhs->get_table_name();
              });
    return schemas;
}

int cmd_ls(const ParsedArgs& args, storage::TsFileReader& reader,
           OutputFormat fmt, std::ostream& out, std::ostream& err) {
    bool table_model = false;
    const int model_ret = resolve_table_model(args, reader, table_model, err);
    if (model_ret != kExitOk) {
        return model_ret;
    }
    std::vector<std::string> names;
    if (table_model) {
        for (auto& ts : sorted_table_schemas(reader)) {
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

    OutputNameValidator validator;
    for (const std::string& name : names) {
        if (!validator.add_object(name)) {
            err << "Error: " << validator.error() << "\n";
            return kExitFile;
        }
    }
    const std::string model = table_model ? "table" : "tree";
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
