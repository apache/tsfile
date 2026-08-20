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
#include <iomanip>
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

struct StatsColumn {
    std::string name;
    common::TSDataType type;
    common::ColumnCategory category;
};

struct FieldAccumulator {
    common::TSDataType type = common::INVALID_DATATYPE;
    long long non_null_count = 0;
    bool has_value = false;
    std::string min_value;
    std::string max_value;
    std::string first_value;
    std::string last_value;
    long double min_numeric = 0;
    long double max_numeric = 0;
    long double numeric_sum = 0;
    long long bool_sum = 0;
};

struct EntityAccumulator {
    std::vector<std::string> tag_values;
    std::vector<bool> tag_nulls;
    std::map<std::string, FieldAccumulator> fields;
    std::vector<std::string> field_order;
    long long row_count = 0;
    long long min_time = 0;
    long long max_time = 0;
    bool has_time = false;
};

const char* stats_column_category_name(common::ColumnCategory category) {
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

bool is_stats_numeric(common::TSDataType type) {
    return type == common::INT32 || type == common::INT64 ||
           type == common::FLOAT || type == common::DOUBLE ||
           type == common::DATE || type == common::TIMESTAMP;
}

long double numeric_value(storage::ResultSet* rs, uint32_t col,
                          common::TSDataType type) {
    switch (type) {
        case common::INT32:
            return rs->get_value<int32_t>(col);
        case common::INT64:
        case common::TIMESTAMP:
            return static_cast<long double>(rs->get_value<int64_t>(col));
        case common::FLOAT:
            return rs->get_value<float>(col);
        case common::DOUBLE:
            return rs->get_value<double>(col);
        default:
            return 0;
    }
}

std::string long_double_to_string(long double value) {
    std::ostringstream ss;
    ss << std::setprecision(std::numeric_limits<long double>::digits10)
       << value;
    return ss.str();
}

void update_field(FieldAccumulator& field, storage::ResultSet* rs, uint32_t col,
                  common::TSDataType type) {
    field.type = type;
    std::string value = cell_to_string(rs, col, type);
    ++field.non_null_count;
    if (!field.has_value) {
        field.has_value = true;
        field.min_value = value;
        field.max_value = value;
        field.first_value = value;
        field.last_value = value;
        if (is_stats_numeric(type)) {
            field.min_numeric = numeric_value(rs, col, type);
            field.max_numeric = field.min_numeric;
            field.numeric_sum = field.min_numeric;
        }
        if (type == common::BOOLEAN && rs->get_value<bool>(col)) {
            field.bool_sum = 1;
        }
        return;
    }

    field.last_value = value;
    if (is_stats_numeric(type)) {
        long double numeric = numeric_value(rs, col, type);
        if (numeric < field.min_numeric) {
            field.min_numeric = numeric;
            field.min_value = value;
        }
        if (numeric > field.max_numeric) {
            field.max_numeric = numeric;
            field.max_value = value;
        }
        field.numeric_sum += numeric;
        return;
    }
    if (type == common::BOOLEAN) {
        if (rs->get_value<bool>(col)) {
            ++field.bool_sum;
        }
        return;
    }
    if ((type == common::STRING || type == common::TEXT) &&
        value < field.min_value) {
        field.min_value = value;
    }
    if ((type == common::STRING || type == common::TEXT) &&
        value > field.max_value) {
        field.max_value = value;
    }
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

std::vector<bool> stats_value_nulls(common::TSDataType type,
                                    const FieldAccumulator& field) {
    if (!field.has_value || type == common::BLOB) {
        return {true, true, true, true, true};
    }
    if (type == common::BOOLEAN) {
        return {true, true, false, false, false};
    }
    if (type == common::TEXT) {
        return {true, true, false, false, true};
    }
    if (type == common::INT64 || type == common::DATE ||
        type == common::TIMESTAMP) {
        return {false, false, false, false, true};
    }
    if (type == common::STRING) {
        return {false, false, false, false, true};
    }
    return {false, false, false, false, false};
}

std::vector<std::string> stats_value_cells(common::TSDataType type,
                                           const FieldAccumulator& field) {
    if (!field.has_value || type == common::BLOB) {
        return {"", "", "", "", ""};
    }
    if (type == common::BOOLEAN) {
        return {"", "", field.first_value, field.last_value,
                std::to_string(field.bool_sum)};
    }
    if (type == common::TEXT) {
        return {"", "", field.first_value, field.last_value, ""};
    }
    if (type == common::INT64 || type == common::DATE ||
        type == common::TIMESTAMP || type == common::STRING) {
        return {field.min_value, field.max_value, field.first_value,
                field.last_value, ""};
    }
    return {field.min_value, field.max_value, field.first_value,
            field.last_value, long_double_to_string(field.numeric_sum)};
}

bool selected_for_stats(const ParsedArgs& args, const std::string& name) {
    return args.measurements.empty() ||
           std::find(args.measurements.begin(), args.measurements.end(),
                     name) != args.measurements.end();
}

int cmd_table_stats(const ParsedArgs& args, storage::TsFileReader& reader,
                    OutputFormat fmt, std::ostream& out, std::ostream& err) {
    std::string table_name = storage::to_lower(args.table);
    std::shared_ptr<storage::TableSchema> schema;
    if (!table_name.empty()) {
        schema = reader.get_table_schema(table_name);
    } else {
        auto schemas = reader.get_all_table_schemas();
        if (schemas.size() != 1) {
            err << "Error: stats requires -t/--table when the file contains "
                   "multiple tables\n";
            return kExitUsage;
        }
        schema = schemas.empty() ? nullptr : schemas[0];
    }
    if (!schema) {
        err << "Error: table '" << args.table << "' does not exist\n";
        return kExitUsage;
    }

    auto categories = schema->get_column_categories();
    auto measurements = schema->get_measurement_schemas();
    std::vector<StatsColumn> columns;
    std::vector<size_t> tag_indexes;
    std::vector<size_t> field_indexes;
    std::set<std::string> known_columns;
    std::set<std::string> field_names;
    std::vector<std::string> query_columns;
    for (size_t i = 0; i < measurements.size(); ++i) {
        if (!measurements[i]) {
            continue;
        }
        StatsColumn c;
        c.name = measurements[i]->measurement_name_;
        c.type = measurements[i]->data_type_;
        c.category = i < categories.size() ? categories[i]
                                           : common::ColumnCategory::FIELD;
        columns.push_back(c);
        known_columns.insert(c.name);
        query_columns.push_back(c.name);
        if (c.category == common::ColumnCategory::TAG) {
            tag_indexes.push_back(i);
        } else if (c.category == common::ColumnCategory::FIELD) {
            field_names.insert(c.name);
            if (selected_for_stats(args, c.name)) {
                field_indexes.push_back(i);
            }
        }
    }
    for (const std::string& requested : args.measurements) {
        if (known_columns.find(requested) == known_columns.end()) {
            err << "Error: FIELD '" << requested << "' does not exist in table "
                << schema->get_table_name() << "\n";
            return kExitUsage;
        }
        if (field_names.find(requested) == field_names.end()) {
            err << "Error: '" << requested
                << "' is a " << stats_column_category_name(common::ColumnCategory::TAG)
                << "; stats accepts FIELD columns only\n";
            return kExitUsage;
        }
    }

    storage::ResultSet* rs = nullptr;
    int qret = reader.query(schema->get_table_name(), query_columns,
                            std::numeric_limits<int64_t>::min(),
                            std::numeric_limits<int64_t>::max(), rs);
    if (qret != 0 || rs == nullptr) {
        err << "Error: stats query failed: " << error_code_message(qret)
            << "\n";
        if (rs != nullptr) {
            reader.destroy_query_data_set(rs);
        }
        return kExitRuntime;
    }
    auto meta = rs->get_metadata();
    std::vector<common::TSDataType> result_types;
    for (uint32_t i = 1; i <= meta->get_column_count(); ++i) {
        result_types.push_back(meta->get_column_type(i));
    }

    std::map<std::string, EntityAccumulator> entities;
    std::vector<std::string> entity_order;
    bool has_next = false;
    int code = common::E_OK;
    while ((code = rs->next(has_next)) == common::E_OK && has_next) {
        std::string key;
        std::vector<std::string> tag_values;
        std::vector<bool> tag_nulls;
        for (size_t idx : tag_indexes) {
            uint32_t col = static_cast<uint32_t>(idx + 2);
            bool null = rs->is_null(col);
            std::string value =
                null ? std::string()
                     : cell_to_string(rs, col, result_types[col - 1]);
            key += entity_part(null, value);
            key += "|";
            tag_values.push_back(value);
            tag_nulls.push_back(null);
        }
        if (key.empty()) {
            key = "zero-tag";
        }
        if (entities.find(key) == entities.end()) {
            EntityAccumulator entity;
            entity.tag_values = tag_values;
            entity.tag_nulls = tag_nulls;
            entities[key] = entity;
            entity_order.push_back(key);
        }
        EntityAccumulator& entity = entities[key];
        int64_t time = rs->get_value<int64_t>(1);
        if (!entity.has_time) {
            entity.min_time = time;
            entity.max_time = time;
            entity.has_time = true;
        } else {
            entity.min_time = std::min<long long>(entity.min_time, time);
            entity.max_time = std::max<long long>(entity.max_time, time);
        }
        ++entity.row_count;

        for (size_t idx : field_indexes) {
            uint32_t col = static_cast<uint32_t>(idx + 2);
            const StatsColumn& field = columns[idx];
            FieldAccumulator& acc = entity.fields[field.name];
            if (std::find(entity.field_order.begin(), entity.field_order.end(),
                          field.name) == entity.field_order.end()) {
                entity.field_order.push_back(field.name);
            }
            acc.type = field.type;
            if (!rs->is_null(col)) {
                update_field(acc, rs, col, field.type);
            }
        }
    }
    reader.destroy_query_data_set(rs);
    if (code != common::E_OK) {
        err << "Error: failed to scan stats rows: " << error_code_message(code)
            << "\n";
        return kExitRuntime;
    }

    std::vector<std::string> headers = {"model", "object"};
    std::vector<common::TSDataType> types = {common::STRING, common::STRING};
    for (size_t idx : tag_indexes) {
        headers.push_back("tag." + columns[idx].name);
        types.push_back(common::STRING);
    }
    const char* rest[] = {"field",          "data_type", "non_null_count",
                          "null_count",     "min_time",  "max_time",
                          "min",            "max",       "first",
                          "last",           "sum",       "stats_source"};
    for (const char* h : rest) {
        headers.push_back(h);
        types.push_back(common::STRING);
    }
    RowWriter w(out, fmt, headers, types, args.no_header);
    for (const std::string& key : entity_order) {
        const EntityAccumulator& entity = entities[key];
        for (size_t idx : field_indexes) {
            const StatsColumn& field = columns[idx];
            auto it = entity.fields.find(field.name);
            FieldAccumulator acc;
            if (it != entity.fields.end()) {
                acc = it->second;
            }
            long long null_count = entity.row_count - acc.non_null_count;
            std::vector<std::string> cells = {"table", schema->get_table_name()};
            std::vector<bool> nulls = {false, false};
            cells.insert(cells.end(), entity.tag_values.begin(),
                         entity.tag_values.end());
            nulls.insert(nulls.end(), entity.tag_nulls.begin(),
                         entity.tag_nulls.end());
            cells.push_back(field.name);
            cells.push_back(tsdatatype_name(field.type));
            cells.push_back(std::to_string(acc.non_null_count));
            cells.push_back(std::to_string(null_count));
            cells.push_back(entity.has_time ? std::to_string(entity.min_time)
                                            : "");
            cells.push_back(entity.has_time ? std::to_string(entity.max_time)
                                            : "");
            nulls.insert(nulls.end(),
                         {false, false, false, false, !entity.has_time,
                          !entity.has_time});

            std::vector<std::string> values = stats_value_cells(field.type, acc);
            std::vector<bool> value_nulls = stats_value_nulls(field.type, acc);
            cells.insert(cells.end(), values.begin(), values.end());
            nulls.insert(nulls.end(), value_nulls.begin(), value_nulls.end());
            cells.push_back(entity.has_time ? "scan" : "");
            nulls.push_back(!entity.has_time);
            w.write(cells, nulls);
        }
    }
    w.finish();
    return kExitOk;
}

}  // namespace

int cmd_stats(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& err) {
    if (is_table_model(args, reader)) {
        return cmd_table_stats(args, reader, fmt, out, err);
    }

    RowWriter w(out, fmt,
                {"model", "object", "field", "data_type", "non_null_count",
                 "null_count", "min_time", "max_time", "min", "max", "first",
                 "last", "sum", "stats_source"},
                {common::STRING, common::STRING, common::STRING, common::STRING,
                 common::INT64, common::INT64, common::INT64, common::INT64,
                 common::STRING, common::STRING, common::STRING,
                 common::STRING, common::STRING},
                args.no_header);

    std::vector<SeriesStatRow> rows = collect_series_stats(args, reader);
    for (const SeriesStatRow& row : rows) {
        std::vector<std::string> cells = {
            "tree", row.target, row.measurement, "", std::to_string(row.count),
            "0", row.count > 0 ? std::to_string(row.start_time) : "",
            row.count > 0 ? std::to_string(row.end_time) : ""};
        cells.insert(cells.end(), row.value_cells.values.begin(),
                     row.value_cells.values.end());
        cells.push_back(row.count > 0 ? "statistics" : "");

        std::vector<bool> nulls = {false, false, false, true, false, false,
                                   row.count == 0, row.count == 0};
        nulls.insert(nulls.end(), row.value_cells.is_null.begin(),
                     row.value_cells.is_null.end());
        nulls.push_back(row.count == 0);
        w.write(cells, nulls);
    }
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
