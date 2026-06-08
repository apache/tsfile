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
#include <string>
#include <utility>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/schema.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {
namespace {

void write_table_schema_rows(const ParsedArgs& args,
                             storage::TsFileReader& reader, RowWriter& w) {
    auto schemas = reader.get_all_table_schemas();
    for (auto& schema : schemas) {
        if (!schema) {
            continue;
        }
        if (!args.table.empty() && schema->get_table_name() != args.table) {
            continue;
        }
        for (const auto& ms : schema->get_measurement_schemas()) {
            if (!ms) {
                continue;
            }
            const std::string& name = ms->measurement_name_;
            if (!args.measurements.empty() &&
                std::find(args.measurements.begin(), args.measurements.end(),
                          name) == args.measurements.end()) {
                continue;
            }
            w.write({schema->get_table_name(), name,
                     tsdatatype_name(ms->data_type_),
                     tsencoding_name(ms->encoding_),
                     compression_name(ms->compression_type_)},
                    {false, false, false, false, false});
        }
    }
}

}  // namespace

int cmd_schema(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(
        out, fmt,
        {"target", "measurement", "datatype", "encoding", "compression"},
        {common::STRING, common::STRING, common::STRING, common::STRING,
         common::STRING},
        args.no_header);

    if (is_table_model(args, reader)) {
        write_table_schema_rows(args, reader, w);
        w.finish();
        return kExitOk;
    }

    storage::DeviceTimeseriesMetadataMap meta =
        reader.get_timeseries_metadata();
    for (auto& kv : meta) {
        std::string target = kv.first ? kv.first->get_device_name() : "";
        if (!args.device.empty() && target != args.device) {
            continue;
        }

        // The timeseries metadata (kv.second) carries the data type but not the
        // encoding/compression, so fetch the measurement schema separately to
        // fill those two columns, keyed by measurement name.
        std::map<std::string, std::pair<std::string, std::string>> enc_comp;
        if (kv.first) {
            std::vector<storage::MeasurementSchema> ms;
            if (reader.get_timeseries_schema(kv.first, ms) == 0) {
                for (auto& m : ms) {
                    enc_comp[m.measurement_name_] =
                        std::make_pair(tsencoding_name(m.encoding_),
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
                std::find(args.measurements.begin(), args.measurements.end(),
                          m) == args.measurements.end()) {
                continue;
            }
            std::string enc;
            std::string comp;
            auto it = enc_comp.find(m);
            if (it != enc_comp.end()) {
                enc = it->second.first;
                comp = it->second.second;
            }
            w.write(
                {target, m, tsdatatype_name(ts->get_data_type()), enc, comp},
                {false, false, false, enc.empty(), comp.empty()});
        }
    }
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
