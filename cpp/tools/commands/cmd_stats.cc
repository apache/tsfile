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

struct FieldStatistic {
    long long count = 0;
    long long start_time = 0;
    long long end_time = 0;
    bool present = false;
    StatisticCells values;
};

struct EntityAccumulator {
    std::vector<std::string> tag_values;
    std::vector<bool> tag_nulls;
    std::map<std::string, FieldStatistic> fields;
    long long row_count = 0;
    bool has_timeline_statistic = false;
};

struct TableStatsSummary {
    std::shared_ptr<storage::TableSchema> schema;
    std::vector<StatsColumn> columns;
    std::vector<size_t> tag_indexes;
    std::vector<size_t> field_indexes;
    std::map<std::string, EntityAccumulator> entities;
    std::vector<std::string> entity_order;
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

bool capture_timeline_statistic(storage::ITimeseriesIndex* index,
                                EntityAccumulator& entity) {
    if (index == nullptr) {
        return false;
    }
    storage::Statistic* statistic = nullptr;
    auto* aligned = dynamic_cast<storage::AlignedTimeseriesIndex*>(index);
    if (aligned != nullptr && aligned->time_ts_idx_ != nullptr) {
        statistic = aligned->time_ts_idx_->get_statistic();
    }
    auto* multi = dynamic_cast<storage::MultiAlignedTimeseriesIndex*>(index);
    if (multi != nullptr && multi->time_ts_idx_ != nullptr) {
        statistic = multi->time_ts_idx_->get_statistic();
    }
    if (statistic != nullptr) {
        entity.row_count = statistic->get_count();
        entity.has_timeline_statistic = true;
        return true;
    }

    common::SimpleList<storage::ChunkMeta*>* chunk_metas =
        index->get_time_chunk_meta_list();
    if (chunk_metas == nullptr) {
        return false;
    }
    long long row_count = 0;
    bool found = false;
    for (auto it = chunk_metas->begin(); it != chunk_metas->end(); it++) {
        storage::ChunkMeta* chunk_meta = it.get();
        if (chunk_meta == nullptr || chunk_meta->statistic_ == nullptr) {
            continue;
        }
        row_count += chunk_meta->statistic_->get_count();
        found = true;
    }
    if (found) {
        entity.row_count = row_count;
        entity.has_timeline_statistic = true;
    }
    return found;
}

void capture_field_statistic(storage::TimeseriesIndex* index,
                             const std::set<std::string>& selected_fields,
                             EntityAccumulator& entity) {
    if (index == nullptr) {
        return;
    }
    const std::string name = index->get_measurement_name().to_std_string();
    if (selected_fields.find(name) == selected_fields.end()) {
        return;
    }
    storage::Statistic* statistic = index->get_statistic();
    if (statistic == nullptr) {
        return;
    }
    FieldStatistic field;
    field.count = statistic->get_count();
    field.start_time = statistic->start_time_;
    field.end_time = statistic->get_end_time();
    field.present = true;
    field.values = statistic_value_cells(statistic);
    entity.fields[name] = field;
}

bool selected_for_stats(const ParsedArgs& args, const std::string& name) {
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

int collect_table_stats(const ParsedArgs& args,
                        const std::shared_ptr<storage::TableSchema>& schema,
                        storage::TsFileReader& reader,
                        TableStatsSummary& summary, std::ostream& err,
                        bool require_all_measurements) {
    if (!schema) {
        err << "Error: table '" << args.table << "' does not exist\n";
        return kExitUsage;
    }
    summary.schema = schema;

    auto categories = schema->get_column_categories();
    auto measurements = schema->get_measurement_schemas();
    summary.columns.resize(measurements.size());
    std::set<std::string> known_columns;
    std::set<std::string> field_names;
    std::set<std::string> selected_fields;
    for (size_t i = 0; i < measurements.size(); ++i) {
        if (!measurements[i]) {
            continue;
        }
        StatsColumn c;
        c.name = measurements[i]->measurement_name_;
        c.type = measurements[i]->data_type_;
        c.category = i < categories.size() ? categories[i]
                                           : common::ColumnCategory::FIELD;
        summary.columns[i] = c;
        known_columns.insert(c.name);
        if (c.category == common::ColumnCategory::TAG) {
            summary.tag_indexes.push_back(i);
        } else if (c.category == common::ColumnCategory::FIELD) {
            field_names.insert(c.name);
            if (selected_for_stats(args, c.name)) {
                summary.field_indexes.push_back(i);
                selected_fields.insert(c.name);
            }
        }
    }
    if (require_all_measurements) {
        for (const std::string& requested : args.measurements) {
            const std::string canonical = storage::to_lower(requested);
            if (known_columns.find(canonical) == known_columns.end()) {
                err << "Error: FIELD '" << requested
                    << "' does not exist in table " << schema->get_table_name()
                    << "\n";
                return kExitUsage;
            }
            if (field_names.find(canonical) == field_names.end()) {
                common::ColumnCategory category = common::ColumnCategory::TAG;
                int index = schema->find_column_index(canonical);
                if (index >= 0 &&
                    static_cast<size_t>(index) < categories.size()) {
                    category = categories[static_cast<size_t>(index)];
                }
                err << "Error: '" << requested << "' is a "
                    << stats_column_category_name(category)
                    << "; stats accepts FIELD columns only\n";
                return kExitUsage;
            }
        }
    }

    std::vector<std::shared_ptr<storage::IDeviceID>> devices =
        reader.get_all_devices(schema->get_table_name());
    storage::DeviceTimeseriesMetadataMap metadata =
        reader.get_timeseries_metadata(devices);
    for (const auto& device : devices) {
        if (!device) {
            continue;
        }
        const std::string key = device->get_device_name();
        EntityAccumulator entity;
        const auto& segments = device->get_segments();
        for (size_t i = 0; i < summary.tag_indexes.size(); ++i) {
            const size_t segment_index = i + 1;
            const std::string* segment = segment_index < segments.size()
                                             ? segments[segment_index]
                                             : nullptr;
            entity.tag_values.push_back(segment == nullptr ? "" : *segment);
            entity.tag_nulls.push_back(segment == nullptr);
        }

        auto metadata_it = metadata.find(device);
        if (metadata_it != metadata.end()) {
            for (const auto& index_ptr : metadata_it->second) {
                storage::ITimeseriesIndex* index = index_ptr.get();
                if (!entity.has_timeline_statistic) {
                    capture_timeline_statistic(index, entity);
                }
                auto* multi =
                    dynamic_cast<storage::MultiAlignedTimeseriesIndex*>(index);
                if (multi != nullptr) {
                    for (storage::TimeseriesIndex* value_index :
                         multi->get_value_indices()) {
                        capture_field_statistic(value_index, selected_fields,
                                                entity);
                    }
                    continue;
                }
                auto* aligned =
                    dynamic_cast<storage::AlignedTimeseriesIndex*>(index);
                if (aligned != nullptr) {
                    capture_field_statistic(aligned->value_ts_idx_,
                                            selected_fields, entity);
                } else {
                    capture_field_statistic(
                        dynamic_cast<storage::TimeseriesIndex*>(index),
                        selected_fields, entity);
                }
            }
        }
        summary.entities[key] = entity;
        summary.entity_order.push_back(key);
    }

    return kExitOk;
}

int cmd_table_stats(const ParsedArgs& args, storage::TsFileReader& reader,
                    OutputFormat fmt, std::ostream& out, std::ostream& err) {
    std::vector<std::shared_ptr<storage::TableSchema>> schemas;
    if (!args.table.empty()) {
        schemas.push_back(
            reader.get_table_schema(storage::to_lower(args.table)));
    } else {
        schemas = reader.get_all_table_schemas();
    }
    if (schemas.empty() || !schemas[0]) {
        err << "Error: table '" << args.table << "' does not exist\n";
        return kExitUsage;
    }

    if (args.table.empty()) {
        for (const std::string& requested : args.measurements) {
            const std::string canonical = storage::to_lower(requested);
            bool found_field = false;
            common::ColumnCategory found_category =
                common::ColumnCategory::FIELD;
            bool found_other = false;
            for (const auto& schema : schemas) {
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
                if (category == common::ColumnCategory::FIELD) {
                    found_field = true;
                } else if (!found_other) {
                    found_category = category;
                    found_other = true;
                }
            }
            if (!found_field) {
                if (found_other) {
                    err << "Error: '" << requested << "' is a "
                        << stats_column_category_name(found_category)
                        << "; stats accepts FIELD columns only\n";
                } else {
                    err << "Error: FIELD '" << requested
                        << "' does not exist in the file\n";
                }
                return kExitUsage;
            }
        }
    }

    std::vector<TableStatsSummary> summaries(schemas.size());
    std::vector<std::string> union_tags;
    std::set<std::string> seen_tags;
    for (size_t i = 0; i < schemas.size(); ++i) {
        ParsedArgs scope = args;
        scope.table = schemas[i] ? schemas[i]->get_table_name() : "";
        int ret = collect_table_stats(scope, schemas[i], reader, summaries[i],
                                      err, !args.table.empty());
        if (ret != kExitOk) {
            return ret;
        }
        for (size_t tag_index : summaries[i].tag_indexes) {
            const std::string& tag = summaries[i].columns[tag_index].name;
            if (seen_tags.insert(tag).second) {
                union_tags.push_back(tag);
            }
        }
    }

    std::vector<std::string> headers = {"model", "object"};
    std::vector<common::TSDataType> types = {common::STRING, common::STRING};
    for (const std::string& tag : union_tags) {
        headers.push_back("tag." + tag);
        types.push_back(common::STRING);
    }
    const char* rest[] = {"field",      "data_type", "non_null_count",
                          "null_count", "min_time",  "max_time",
                          "min",        "max",       "first",
                          "last",       "sum",       "stats_source"};
    for (const char* h : rest) {
        headers.push_back(h);
        types.push_back(common::STRING);
    }
    RowWriter w(out, fmt, headers, types, args.no_header);
    for (const TableStatsSummary& summary : summaries) {
        std::map<std::string, size_t> local_tag_positions;
        for (size_t i = 0; i < summary.tag_indexes.size(); ++i) {
            local_tag_positions[summary.columns[summary.tag_indexes[i]].name] =
                i;
        }
        for (const std::string& key : summary.entity_order) {
            const EntityAccumulator& entity =
                summary.entities.find(key)->second;
            for (size_t idx : summary.field_indexes) {
                const StatsColumn& field = summary.columns[idx];
                auto it = entity.fields.find(field.name);
                FieldStatistic statistic;
                if (it != entity.fields.end()) {
                    statistic = it->second;
                }
                const bool has_statistic = statistic.present;
                const bool has_null_count =
                    has_statistic && entity.has_timeline_statistic;
                const long long null_count =
                    has_null_count ? entity.row_count - statistic.count : 0;
                std::vector<std::string> cells = {
                    "table", summary.schema->get_table_name()};
                std::vector<bool> nulls = {false, false};
                for (const std::string& tag : union_tags) {
                    auto position = local_tag_positions.find(tag);
                    if (position == local_tag_positions.end()) {
                        cells.push_back("");
                        nulls.push_back(true);
                    } else {
                        cells.push_back(entity.tag_values[position->second]);
                        nulls.push_back(entity.tag_nulls[position->second]);
                    }
                }
                cells.push_back(field.name);
                cells.push_back(tsdatatype_name(field.type));
                cells.push_back(has_statistic ? std::to_string(statistic.count)
                                              : "");
                cells.push_back(has_null_count ? std::to_string(null_count)
                                               : "");
                cells.push_back(
                    has_statistic ? std::to_string(statistic.start_time) : "");
                cells.push_back(
                    has_statistic ? std::to_string(statistic.end_time) : "");
                nulls.insert(nulls.end(),
                             {false, false, !has_statistic, !has_null_count,
                              !has_statistic, !has_statistic});

                StatisticCells values;
                values.values.assign(5, "");
                values.is_null.assign(5, true);
                if (has_statistic) {
                    values = statistic.values;
                }
                cells.insert(cells.end(), values.values.begin(),
                             values.values.end());
                nulls.insert(nulls.end(), values.is_null.begin(),
                             values.is_null.end());
                cells.push_back(has_statistic ? "statistics" : "");
                nulls.push_back(!has_statistic);
                std::vector<common::TSDataType> row_types(types.size(),
                                                          common::STRING);
                const size_t fixed = 2 + union_tags.size();
                row_types[fixed + 2] = common::INT64;
                row_types[fixed + 3] = common::INT64;
                row_types[fixed + 4] = common::INT64;
                row_types[fixed + 5] = common::INT64;
                for (size_t value = 0; value < 4; ++value) {
                    row_types[fixed + 6 + value] = field.type;
                }
                if (field.type == common::BOOLEAN ||
                    field.type == common::INT32 || field.type == common::DATE ||
                    field.type == common::INT64 ||
                    field.type == common::TIMESTAMP) {
                    row_types[fixed + 10] = common::INT64;
                } else if (field.type == common::FLOAT ||
                           field.type == common::DOUBLE) {
                    row_types[fixed + 10] = field.type;
                }
                if (!w.write(cells, nulls, row_types)) {
                    err << "Error: failed to write output\n";
                    return kExitRuntime;
                }
            }
        }
    }
    if (!w.finish()) {
        err << "Error: failed to write output\n";
        return kExitRuntime;
    }
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
                 common::STRING, common::STRING, common::STRING, common::STRING,
                 common::STRING, common::STRING},
                args.no_header);

    std::vector<SeriesStatRow> rows;
    int collect_ret = collect_series_stats(args, reader, rows, err);
    if (collect_ret != kExitOk) {
        return collect_ret;
    }
    for (const SeriesStatRow& row : rows) {
        const long long null_count = row.row_count - row.count;
        std::vector<std::string> cells = {
            "tree",
            row.target,
            row.measurement,
            tsdatatype_name(row.data_type),
            row.has_statistic ? std::to_string(row.count) : "",
            row.has_statistic ? std::to_string(null_count) : "",
            row.has_statistic ? std::to_string(row.start_time) : "",
            row.has_statistic ? std::to_string(row.end_time) : ""};
        cells.insert(cells.end(), row.value_cells.values.begin(),
                     row.value_cells.values.end());
        cells.push_back(row.has_statistic ? "statistics" : "");

        std::vector<bool> nulls = {false,
                                   false,
                                   false,
                                   false,
                                   !row.has_statistic,
                                   !row.has_statistic,
                                   !row.has_statistic,
                                   !row.has_statistic};
        nulls.insert(nulls.end(), row.value_cells.is_null.begin(),
                     row.value_cells.is_null.end());
        nulls.push_back(!row.has_statistic);
        std::vector<common::TSDataType> row_types = {
            common::STRING, common::STRING, common::STRING, common::STRING,
            common::INT64,  common::INT64,  common::INT64,  common::INT64,
            row.data_type,  row.data_type,  row.data_type,  row.data_type,
            common::STRING, common::STRING};
        if (row.data_type == common::BOOLEAN ||
            row.data_type == common::INT32 || row.data_type == common::DATE ||
            row.data_type == common::INT64 ||
            row.data_type == common::TIMESTAMP) {
            row_types[12] = common::INT64;
        } else if (row.data_type == common::FLOAT ||
                   row.data_type == common::DOUBLE) {
            row_types[12] = row.data_type;
        }
        if (!w.write(cells, nulls, row_types)) {
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
