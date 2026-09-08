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
#include <climits>
#include <limits>
#include <memory>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/device_id.h"
#include "common/schema.h"
#include "format/result_set_format.h"
#include "reader/filter/tag_filter.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {
namespace {

class CliFullMatchTagRegExp : public storage::TagFilter {
   public:
    CliFullMatchTagRegExp(int col_idx, const std::string& pattern)
        : storage::TagFilter(col_idx, pattern), pattern_(pattern) {}

    bool satisfyRow(std::vector<std::string*> segments) const override {
        if (col_idx_ >= segments.size() || segments[col_idx_] == nullptr) {
            return false;
        }
        return std::regex_match(*segments[col_idx_], pattern_);
    }

   private:
    std::regex pattern_;
};
bool can_push_down_row_window(const ParsedArgs& args, long long offset,
                              long long limit) {
    return !args.has_start && !args.has_end && offset == 0 &&
           (limit < 0 || limit <= INT_MAX);
}

int to_reader_row_bound(long long value) {
    return value < 0 ? -1 : static_cast<int>(value);
}

int resolve_table_fields(const ParsedArgs& args,
                         const std::shared_ptr<storage::TableSchema>& schema,
                         std::vector<std::string>& fields, std::ostream& err) {
    if (!schema) {
        err << "Error: table '" << args.table << "' does not exist\n";
        return kExitUsage;
    }
    auto measurements = schema->get_measurement_schemas();
    auto categories = schema->get_column_categories();
    for (size_t i = 0; i < measurements.size(); ++i) {
        if (i < categories.size() && measurements[i] &&
            categories[i] == common::ColumnCategory::TAG) {
            fields.push_back(measurements[i]->measurement_name_);
        }
    }
    if (args.measurements.empty()) {
        for (size_t i = 0; i < measurements.size(); ++i) {
            if (i < categories.size() && measurements[i] &&
                categories[i] == common::ColumnCategory::FIELD) {
                fields.push_back(measurements[i]->measurement_name_);
            }
        }
        return kExitOk;
    }
    for (const std::string& requested : args.measurements) {
        const int index = schema->find_column_index(requested);
        if (index < 0 || static_cast<size_t>(index) >= measurements.size() ||
            !measurements[index]) {
            err << "Error: FIELD '" << requested
                << "' does not exist in table '" << schema->get_table_name()
                << "'\n";
            return kExitUsage;
        }
        const common::ColumnCategory category =
            static_cast<size_t>(index) < categories.size()
                ? categories[index]
                : common::ColumnCategory::FIELD;
        if (category != common::ColumnCategory::FIELD) {
            err << "Error: column '" << requested
                << "' is not a FIELD in table '" << schema->get_table_name()
                << "'\n";
            return kExitUsage;
        }
        fields.push_back(measurements[index]->measurement_name_);
    }
    return kExitOk;
}

int resolve_tree_paths(const ParsedArgs& args, storage::TsFileReader& reader,
                       std::vector<std::string>& paths, std::ostream& err) {
    auto devices = reader.get_all_device_ids();
    std::shared_ptr<storage::IDeviceID> target_device;
    for (const auto& device : devices) {
        if (device && device->get_device_name() == args.device) {
            target_device = device;
            break;
        }
    }
    if (!target_device) {
        err << "Error: device '" << args.device << "' does not exist\n";
        return kExitUsage;
    }
    std::vector<storage::MeasurementSchema> schemas;
    if (reader.get_timeseries_schema(target_device, schemas) != common::E_OK) {
        err << "Error: failed to read schema for device '" << args.device
            << "'\n";
        return kExitFile;
    }
    std::set<std::string> known;
    for (const auto& schema : schemas) {
        known.insert(schema.measurement_name_);
    }
    for (const std::string& requested : args.measurements) {
        if (known.find(requested) == known.end()) {
            err << "Error: FIELD '" << requested
                << "' does not exist in device '" << args.device << "'\n";
            return kExitUsage;
        }
    }
    paths = collect_tree_query_paths(args, reader);
    if (paths.empty()) {
        err << "Error: device '" << args.device << "' has no FIELD columns\n";
        return kExitUsage;
    }
    return kExitOk;
}

}  // namespace

std::unique_ptr<storage::Filter> build_table_tag_filter(
    const ParsedArgs& args, storage::TsFileReader& reader,
    const std::string& table_name, std::ostream& err) {
    if (!args.has_tag_filter) {
        return std::unique_ptr<storage::Filter>();
    }
    auto schema = reader.get_table_schema(table_name);
    if (!schema) {
        err << "Error: no schema found for table " << table_name << "\n";
        return std::unique_ptr<storage::Filter>();
    }

    storage::TagFilterBuilder builder(schema.get());
    std::unique_ptr<storage::Filter> combined;
    for (const ParsedArgs::TagFilterSpec& spec : args.tag_filters) {
        storage::Filter* filter = nullptr;
        switch (spec.op) {
            case ParsedArgs::TagFilterOp::kEq:
                filter = builder.eq(spec.column, spec.value);
                break;
            case ParsedArgs::TagFilterOp::kNeq:
                filter = builder.neq(spec.column, spec.value);
                break;
            case ParsedArgs::TagFilterOp::kRegexp:
                try {
                    std::regex pattern(spec.value);
                    (void)pattern;
                } catch (const std::regex_error&) {
                    err << "Error: invalid regular expression for TAG '"
                        << spec.column << "'\n";
                    return std::unique_ptr<storage::Filter>();
                }
                {
                    int tag_order = schema->find_id_column_order(spec.column);
                    if (tag_order >= 0) {
                        filter = new CliFullMatchTagRegExp(tag_order + 1,
                                                           spec.value);
                    }
                }
                break;
            case ParsedArgs::TagFilterOp::kIsNull:
                filter = builder.is_null(spec.column);
                break;
            case ParsedArgs::TagFilterOp::kNotNull:
                filter = builder.is_not_null(spec.column);
                break;
            case ParsedArgs::TagFilterOp::kNone:
                break;
        }
        if (filter == nullptr) {
            err << "Error: invalid tag filter column '" << spec.column
                << "' for table " << table_name << "\n";
            return std::unique_ptr<storage::Filter>();
        }
        if (!combined) {
            combined.reset(filter);
        } else if (args.tag_match == "any") {
            combined.reset(storage::TagFilterBuilder::or_filter(
                combined.release(), filter));
        } else {
            combined.reset(storage::TagFilterBuilder::and_filter(
                combined.release(), filter));
        }
    }
    return combined;
}

std::vector<std::string> collect_tree_query_paths(
    const ParsedArgs& args, storage::TsFileReader& reader) {
    std::vector<std::string> paths;
    const bool has_projection = !args.measurements.empty();
    auto include_measurement = [&](const std::string& m) {
        return !has_projection ||
               std::find(args.measurements.begin(), args.measurements.end(),
                         m) != args.measurements.end();
    };

    if (!args.device.empty()) {
        // A single device was requested: resolve its series and keep only the
        // ones matching the projection. Filtering against the device's real
        // schema means a provided measurement that doesn't exist on the device
        // is dropped rather than queried blindly (matching the no-device path).
        auto did = std::make_shared<storage::StringArrayDeviceID>(args.device);
        std::vector<storage::MeasurementSchema> sch;
        if (reader.get_timeseries_schema(did, sch) == 0) {
            for (auto& m : sch) {
                if (include_measurement(m.measurement_name_)) {
                    paths.push_back(args.device + "." + m.measurement_name_);
                }
            }
        }
        return paths;
    }

    // No device filter: collect every device/series from a single whole-file
    // metadata call instead of querying each device one by one.
    storage::DeviceTimeseriesMetadataMap meta =
        reader.get_timeseries_metadata();
    for (auto& kv : meta) {
        if (!kv.first) {
            continue;
        }
        const std::string dev = kv.first->get_device_name();
        for (auto& ts : kv.second) {
            if (!ts) {
                continue;
            }
            const std::string m = ts->get_measurement_name().to_std_string();
            if (include_measurement(m)) {
                paths.push_back(dev + "." + m);
            }
        }
    }
    return paths;
}

int run_row_query(const ParsedArgs& args, storage::TsFileReader& reader,
                  OutputFormat fmt, std::ostream& out, std::ostream& err,
                  long long offset, long long limit, long long* emitted_rows) {
    const int64_t start = args.has_start ? static_cast<int64_t>(args.start)
                                         : std::numeric_limits<int64_t>::min();
    const int64_t end = args.has_end ? static_cast<int64_t>(args.end)
                                     : std::numeric_limits<int64_t>::max();

    storage::ResultSet* rs = nullptr;
    int qret = 0;
    const bool push_down = can_push_down_row_window(args, offset, limit);
    std::unique_ptr<storage::Filter> tag_filter;

    if (is_table_model(args, reader)) {
        std::string table_name = args.table;
        if (table_name.empty()) {
            auto schemas = reader.get_all_table_schemas();
            if (schemas.empty() || !schemas[0]) {
                err << "Error: no table found in file\n";
                return kExitRuntime;
            }
            if (schemas.size() != 1) {
                err << "Error: head/cat requires -t/--table when the file "
                       "contains multiple tables\n";
                return kExitUsage;
            }
            table_name = schemas[0]->get_table_name();
        }
        auto table_schema = reader.get_table_schema(table_name);
        std::vector<std::string> cols;
        int selection_ret = resolve_table_fields(args, table_schema, cols, err);
        if (selection_ret != kExitOk) {
            return selection_ret;
        }
        tag_filter = build_table_tag_filter(args, reader, table_name, err);
        if (args.has_tag_filter && tag_filter == nullptr) {
            return kExitUsage;
        }
        if (push_down) {
            qret = reader.queryByRow(
                table_name, cols, to_reader_row_bound(offset),
                to_reader_row_bound(limit), rs, tag_filter.get());
        } else {
            qret = reader.query(table_name, cols, start, end, rs,
                                tag_filter.get());
        }
    } else {
        if (args.has_tag_filter) {
            err << "Error: tag filter flags are only valid for table model\n";
            return kExitUsage;
        }
        ParsedArgs effective_args = args;
        if (effective_args.device.empty()) {
            auto devices = reader.get_all_device_ids();
            if (devices.empty() || !devices[0]) {
                err << "Error: no device found in file\n";
                return kExitRuntime;
            }
            if (devices.size() != 1) {
                err << "Error: head/cat requires -d/--device when the file "
                       "contains multiple devices\n";
                return kExitUsage;
            }
            effective_args.device = devices[0]->get_device_name();
        }
        std::vector<std::string> paths;
        int selection_ret =
            resolve_tree_paths(effective_args, reader, paths, err);
        if (selection_ret != kExitOk) {
            return selection_ret;
        }
        if (push_down) {
            qret = reader.queryByRow(paths, to_reader_row_bound(offset),
                                     to_reader_row_bound(limit), rs);
        } else {
            qret = reader.query(paths, start, end, rs);
        }
    }

    if (qret != 0 || rs == nullptr) {
        err << "Error: query failed: " << error_code_message(qret) << "\n";
        if (rs != nullptr) {
            reader.destroy_query_data_set(rs);
        }
        return kExitFile;
    }

    // Stage the complete result before publishing it.  A decode/read failure
    // can occur after the result header or earlier rows have been rendered;
    // input failures must not leave a partial machine-readable stdout stream
    // that callers could mistake for a complete result.  The final write is
    // still checked separately so stdout errors remain runtime failures.
    std::ostringstream staged;
    int wret = push_down ? emit_result_set(rs, fmt, args.no_header, staged, 0,
                                           -1, emitted_rows)
                         : emit_result_set(rs, fmt, args.no_header, staged,
                                           offset, limit, emitted_rows);
    reader.destroy_query_data_set(rs);
    if (wret == common::E_OK) {
        const std::string bytes = staged.str();
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        out.flush();
        if (!out.good()) {
            wret = common::E_FILE_WRITE_ERR;
        }
    }
    if (wret == common::E_OUT_OF_RANGE) {
        err << "Error: offset exceeds matched row count\n";
        return kExitUsage;
    }
    if (wret != 0) {
        err << "Error: failed to read rows: " << error_code_message(wret)
            << "\n";
        return wret == common::E_FILE_WRITE_ERR ? kExitRuntime : kExitFile;
    }
    return kExitOk;
}

}  // namespace tsfile_cli
