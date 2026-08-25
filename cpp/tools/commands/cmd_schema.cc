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
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/schema.h"
#include "reader/tsfile_reader.h"
#include "utils/storage_utils.h"

namespace tsfile_cli {
namespace {

const char* column_category_name(common::ColumnCategory category) {
    switch (category) {
        case common::ColumnCategory::TIME:
            return "TIME";
        case common::ColumnCategory::TAG:
            return "TAG";
        case common::ColumnCategory::ATTRIBUTE:
            return "ATTRIBUTE";
        case common::ColumnCategory::FIELD:
        default:
            return "FIELD";
    }
}

struct SchemaRow {
    std::vector<std::string> cells;
    std::vector<bool> nulls;
};

bool table_measurement_selected(const ParsedArgs& args,
                                const std::string& name) {
    if (args.measurements.empty()) {
        return true;
    }
    const std::string folded_name = storage::to_lower(name);
    for (const std::string& requested : args.measurements) {
        if (storage::to_lower(requested) == folded_name) {
            return true;
        }
    }
    return false;
}

int collect_table_schema_rows(const ParsedArgs& args,
                              storage::TsFileReader& reader,
                              std::vector<SchemaRow>& rows, std::ostream& err) {
    const std::string target_table_name = storage::to_lower(args.table);
    auto schemas = reader.get_all_table_schemas();
    bool matched_table = target_table_name.empty();
    std::set<std::string> matched_measurements;
    for (auto& schema : schemas) {
        if (!schema) {
            continue;
        }
        if (!target_table_name.empty() &&
            schema->get_table_name() != target_table_name) {
            continue;
        }
        matched_table = true;
        auto categories = schema->get_column_categories();
        auto measurements = schema->get_measurement_schemas();
        for (size_t i = 0; i < measurements.size(); ++i) {
            const auto& ms = measurements[i];
            if (!ms) {
                continue;
            }
            const std::string& name = ms->measurement_name_;
            if (!table_measurement_selected(args, name)) {
                continue;
            }
            matched_measurements.insert(storage::to_lower(name));
            common::ColumnCategory category =
                i < categories.size() ? categories[i]
                                      : common::ColumnCategory::FIELD;
            const bool physical_null =
                category == common::ColumnCategory::TIME ||
                category == common::ColumnCategory::ATTRIBUTE;
            rows.push_back(
                {{"table", schema->get_table_name(), name,
                  column_category_name(category),
                  tsdatatype_name(ms->data_type_),
                  physical_null ? "" : tsencoding_name(ms->encoding_),
                  physical_null ? "" : compression_name(ms->compression_type_)},
                 {false, false, false, false, false, physical_null,
                  physical_null}});
        }
    }
    if (!matched_table) {
        err << "Error: table '" << args.table << "' does not exist\n";
        return kExitUsage;
    }
    for (const std::string& requested : args.measurements) {
        if (matched_measurements.find(storage::to_lower(requested)) ==
            matched_measurements.end()) {
            err << "Error: column '" << requested << "' does not exist";
            if (!args.table.empty()) {
                err << " in table '" << target_table_name << "'";
            }
            err << "\n";
            return kExitUsage;
        }
    }
    return kExitOk;
}

}  // namespace

int cmd_schema(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err) {
    std::vector<SchemaRow> rows;
    if (is_table_model(args, reader)) {
        int ret = collect_table_schema_rows(args, reader, rows, err);
        if (ret != kExitOk) {
            return ret;
        }
    } else {
        storage::DeviceTimeseriesMetadataMap meta =
            reader.get_timeseries_metadata();
        bool matched_device = args.device.empty();
        std::set<std::string> matched_measurements;
        for (auto& kv : meta) {
            std::string target = kv.first ? kv.first->get_device_name() : "";
            if (!args.device.empty() && target != args.device) {
                continue;
            }
            matched_device = true;

            std::map<std::string, std::pair<std::string, std::string>> enc_comp;
            if (kv.first) {
                std::vector<storage::MeasurementSchema> ms;
                if (reader.get_timeseries_schema(kv.first, ms) == 0) {
                    for (auto& m : ms) {
                        enc_comp[m.measurement_name_] = std::make_pair(
                            tsencoding_name(m.encoding_),
                            compression_name(m.compression_type_));
                    }
                }
            }

            for (auto& ts : kv.second) {
                if (!ts) {
                    continue;
                }
                std::string m = ts->get_measurement_name().to_std_string();
                if (!args.measurements.empty() &&
                    std::find(args.measurements.begin(),
                              args.measurements.end(),
                              m) == args.measurements.end()) {
                    continue;
                }
                matched_measurements.insert(m);
                std::string enc;
                std::string comp;
                auto it = enc_comp.find(m);
                if (it != enc_comp.end()) {
                    enc = it->second.first;
                    comp = it->second.second;
                }
                rows.push_back(
                    {{"tree", target, m, "FIELD",
                      tsdatatype_name(ts->get_data_type()), enc, comp},
                     {false, false, false, false, false, enc.empty(),
                      comp.empty()}});
            }
        }
        if (!matched_device) {
            err << "Error: device '" << args.device << "' does not exist\n";
            return kExitUsage;
        }
        for (const std::string& requested : args.measurements) {
            if (matched_measurements.find(requested) ==
                matched_measurements.end()) {
                err << "Error: measurement '" << requested
                    << "' does not exist";
                if (!args.device.empty()) {
                    err << " in device '" << args.device << "'";
                }
                err << "\n";
                return kExitUsage;
            }
        }
    }

    RowWriter w(out, fmt,
                {"model", "object", "column", "category", "data_type",
                 "encoding", "compression"},
                {common::STRING, common::STRING, common::STRING, common::STRING,
                 common::STRING, common::STRING, common::STRING},
                args.no_header);
    for (const SchemaRow& row : rows) {
        if (!w.write(row.cells, row.nulls)) {
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
