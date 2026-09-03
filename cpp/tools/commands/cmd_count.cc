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
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "commands/statistics.h"
#include "common/schema.h"
#include "format/result_set_format.h"
#include "reader/tsfile_reader.h"
#include "utils/storage_utils.h"

namespace tsfile_cli {
namespace {

const char* count_column_category_name(common::ColumnCategory category) {
    switch (category) {
        case common::ColumnCategory::TAG:
            return "TAG";
        case common::ColumnCategory::ATTRIBUTE:
            return "ATTRIBUTE";
        case common::ColumnCategory::TIME:
            return "TIME";
        case common::ColumnCategory::FIELD:
        default:
            return "FIELD";
    }
}

struct CountColumn {
    std::string name;
    common::TSDataType type;
    common::ColumnCategory category;
    long long non_null_count = 0;
};

struct TableCountSummary {
    std::string table_name;
    std::vector<CountColumn> columns;
    long long row_count = 0;
    long long min_time = 0;
    long long max_time = 0;
    bool has_time = false;
    std::set<std::string> entity_keys;
};

bool selected_for_count(const ParsedArgs& args, const std::string& name) {
    if (args.measurements.empty()) {
        return true;
    }
    for (const std::string& requested : args.measurements) {
        if (storage::to_lower(requested) == name) {
            return true;
        }
    }
    return false;
}

std::string entity_part(bool is_null, const std::string& value) {
    std::ostringstream ss;
    if (is_null) {
        ss << "N:";
    } else {
        ss << "V:" << value.size() << ":" << value;
    }
    return ss.str();
}

int collect_table_count(const ParsedArgs& args, storage::TsFileReader& reader,
                        TableCountSummary& summary, std::ostream& err,
                        bool require_all_measurements) {
    std::string table_name = storage::to_lower(args.table);
    std::shared_ptr<storage::TableSchema> schema =
        reader.get_table_schema(table_name);
    if (!schema) {
        err << "Error: table '" << args.table << "' does not exist\n";
        return kExitUsage;
    }
    summary.table_name = schema->get_table_name();

    auto categories = schema->get_column_categories();
    auto measurements = schema->get_measurement_schemas();
    std::vector<std::string> query_columns;
    std::set<std::string> known_columns;
    std::set<std::string> count_columns;
    std::map<std::string, uint32_t> result_column_indexes;
    std::vector<uint32_t> tag_result_indexes;
    for (size_t i = 0; i < measurements.size(); ++i) {
        if (!measurements[i]) {
            continue;
        }
        CountColumn c;
        c.name = measurements[i]->measurement_name_;
        c.type = measurements[i]->data_type_;
        c.category = i < categories.size() ? categories[i]
                                           : common::ColumnCategory::FIELD;
        known_columns.insert(c.name);
        if (c.category == common::ColumnCategory::TAG ||
            c.category == common::ColumnCategory::FIELD) {
            count_columns.insert(c.name);
            query_columns.push_back(c.name);
            const uint32_t result_index =
                static_cast<uint32_t>(query_columns.size() + 1);
            result_column_indexes[c.name] = result_index;
            if (c.category == common::ColumnCategory::TAG) {
                tag_result_indexes.push_back(result_index);
            }
        }
        if (count_columns.find(c.name) != count_columns.end() &&
            selected_for_count(args, c.name)) {
            summary.columns.push_back(c);
        }
    }
    if (require_all_measurements) {
        for (const std::string& requested : args.measurements) {
            if (known_columns.find(storage::to_lower(requested)) ==
                known_columns.end()) {
                err << "Error: column '" << requested
                    << "' does not exist in table " << summary.table_name
                    << "\n";
                return kExitUsage;
            }
            if (count_columns.find(storage::to_lower(requested)) ==
                count_columns.end()) {
                err << "Error: count accepts TAG or FIELD columns only; '"
                    << requested << "' has another category\n";
                return kExitUsage;
            }
        }
    }

    storage::ResultSet* rs = nullptr;
    int qret = reader.query(summary.table_name, query_columns,
                            std::numeric_limits<int64_t>::min(),
                            std::numeric_limits<int64_t>::max(), rs);
    if (qret != 0 || rs == nullptr) {
        // A schema-only table has no device index yet.  It is still a valid
        // empty table and its count is defined as zero rows/entities; do not
        // turn that state into a misleading "table does not exist" error.
        if (qret == common::E_TABLE_NOT_EXIST &&
            reader.get_all_devices(summary.table_name).empty()) {
            return kExitOk;
        }
        err << "Error: count query failed: " << error_code_message(qret)
            << "\n";
        if (rs != nullptr) {
            reader.destroy_query_data_set(rs);
        }
        return kExitFile;
    }

    auto meta = rs->get_metadata();
    std::vector<common::TSDataType> result_types;
    for (uint32_t i = 1; i <= meta->get_column_count(); ++i) {
        result_types.push_back(meta->get_column_type(i));
    }

    bool has_next = false;
    int code = common::E_OK;
    while ((code = rs->next(has_next)) == common::E_OK && has_next) {
        int64_t time = rs->get_value<int64_t>(1);
        if (!summary.has_time) {
            summary.min_time = time;
            summary.max_time = time;
            summary.has_time = true;
        } else {
            summary.min_time = std::min<long long>(summary.min_time, time);
            summary.max_time = std::max<long long>(summary.max_time, time);
        }
        ++summary.row_count;

        std::string entity_key;
        if (tag_result_indexes.empty()) {
            entity_key = "zero-tag";
        } else {
            for (uint32_t col : tag_result_indexes) {
                bool null = rs->is_null(col);
                entity_key += entity_part(
                    null, null
                              ? std::string()
                              : cell_to_string(rs, col, result_types[col - 1]));
                entity_key += "|";
            }
        }
        summary.entity_keys.insert(entity_key);

        for (CountColumn& out_col : summary.columns) {
            auto result_index = result_column_indexes.find(out_col.name);
            if (result_index != result_column_indexes.end() &&
                !rs->is_null(result_index->second)) {
                ++out_col.non_null_count;
            }
        }
    }
    reader.destroy_query_data_set(rs);
    if (code != common::E_OK) {
        err << "Error: failed to scan count rows: " << error_code_message(code)
            << "\n";
        return kExitFile;
    }
    return kExitOk;
}

}  // namespace

int cmd_count(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& err) {
    RowWriter w(
        out, fmt,
        {"model", "object", "column", "category", "row_count", "entity_count",
         "non_null_count", "null_count", "min_time", "max_time", "time_source"},
        {common::STRING, common::STRING, common::STRING, common::STRING,
         common::INT64, common::INT64, common::INT64, common::INT64,
         common::INT64, common::INT64, common::STRING},
        args.no_header);

    if (is_table_model(args, reader)) {
        const std::vector<std::shared_ptr<storage::TableSchema>> all_schemas =
            reader.get_all_table_schemas();
        if (args.table.empty()) {
            for (const std::string& requested : args.measurements) {
                const std::string canonical = storage::to_lower(requested);
                bool found = false;
                bool found_other = false;
                for (const auto& schema : all_schemas) {
                    if (!schema) {
                        continue;
                    }
                    const int index = schema->find_column_index(canonical);
                    if (index < 0) {
                        continue;
                    }
                    const auto categories = schema->get_column_categories();
                    const common::ColumnCategory category =
                        static_cast<size_t>(index) < categories.size()
                            ? categories[static_cast<size_t>(index)]
                            : common::ColumnCategory::FIELD;
                    if (category == common::ColumnCategory::TAG ||
                        category == common::ColumnCategory::FIELD) {
                        found = true;
                    } else {
                        found_other = true;
                    }
                }
                if (!found) {
                    if (found_other) {
                        err << "Error: count accepts TAG or FIELD columns "
                               "only; '"
                            << requested << "' has another category\n";
                    } else {
                        err << "Error: column '" << requested
                            << "' does not exist in the file\n";
                    }
                    return kExitUsage;
                }
            }
        }
        std::vector<ParsedArgs> scopes;
        if (!args.table.empty()) {
            scopes.push_back(args);
        } else {
            for (const auto& schema : all_schemas) {
                if (schema) {
                    ParsedArgs scope = args;
                    scope.table = schema->get_table_name();
                    scopes.push_back(scope);
                }
            }
        }
        std::vector<TableCountSummary> summaries(scopes.size());
        for (size_t i = 0; i < scopes.size(); ++i) {
            int ret = collect_table_count(scopes[i], reader, summaries[i], err,
                                          !args.table.empty());
            if (ret != kExitOk) {
                return ret;
            }
        }
        for (const TableCountSummary& summary : summaries) {
            for (const CountColumn& c : summary.columns) {
                long long null_count = summary.row_count - c.non_null_count;
                std::vector<std::string> cells = {
                    "table",
                    summary.table_name,
                    c.name,
                    count_column_category_name(c.category),
                    std::to_string(summary.row_count),
                    std::to_string(summary.has_time ? summary.entity_keys.size()
                                                    : 0),
                    std::to_string(c.non_null_count),
                    std::to_string(null_count),
                    summary.has_time ? std::to_string(summary.min_time) : "",
                    summary.has_time ? std::to_string(summary.max_time) : "",
                    summary.has_time ? "scan" : ""};
                if (!w.write(cells, {false, false, false, false, false, false,
                                     false, false, !summary.has_time,
                                     !summary.has_time, !summary.has_time})) {
                    err << "Error: failed to write output\n";
                    return kExitRuntime;
                }
            }
        }
        if (!w.finish()) {
            err << "Error: failed to write output\n";
            return kExitRuntime;
        }
        return kExitOk;
    }

    std::vector<SeriesStatRow> rows;
    int collect_ret = collect_series_stats(args, reader, rows, err);
    if (collect_ret != kExitOk) {
        return collect_ret;
    }
    for (const SeriesStatRow& row : rows) {
        const long long null_count = row.row_count - row.count;
        if (!w.write(
                {"tree", row.target, row.measurement, "FIELD",
                 std::to_string(row.row_count), "",
                 row.has_statistic ? std::to_string(row.count) : "",
                 row.has_statistic ? std::to_string(null_count) : "",
                 row.has_statistic ? std::to_string(row.start_time) : "",
                 row.has_statistic ? std::to_string(row.end_time) : "",
                 row.has_statistic ? "statistics" : ""},
                {false, false, false, false, false, true, !row.has_statistic,
                 !row.has_statistic, !row.has_statistic, !row.has_statistic,
                 !row.has_statistic})) {
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
