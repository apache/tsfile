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
#include <cstdint>
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
    int64_t start_time = 0;
    int64_t end_time = 0;
    bool present = false;
    StatisticCells values;
};

struct EntityAccumulator {
    std::vector<std::string> tag_values;
    std::vector<bool> tag_nulls;
    std::map<std::string, FieldStatistic> fields;
    long long row_count = 0;
    int64_t start_time = 0;
    int64_t end_time = 0;
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
        entity.start_time = statistic->start_time_;
        entity.end_time = statistic->end_time_;
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
        if (!found || chunk_meta->statistic_->start_time_ < entity.start_time) {
            entity.start_time = chunk_meta->statistic_->start_time_;
        }
        if (!found || chunk_meta->statistic_->end_time_ > entity.end_time) {
            entity.end_time = chunk_meta->statistic_->end_time_;
        }
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
    // A value index is still emitted for an all-NULL FIELD, but its statistic
    // has count=0 and no meaningful value/time range.  Treat that as absent so
    // the caller can derive the entity timeline by scanning rows.
    if (statistic == nullptr || statistic->get_count() == 0) {
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

bool same_entity(const EntityAccumulator& entity,
                 const std::vector<std::string>& tag_values,
                 const std::vector<bool>& tag_nulls) {
    return entity.tag_values == tag_values && entity.tag_nulls == tag_nulls;
}

int scan_table_timelines(const std::string& table_name,
                         const std::vector<std::string>& query_columns,
                         const std::vector<uint32_t>& tag_result_indexes,
                         storage::TsFileReader& reader,
                         TableStatsSummary& summary) {
    storage::ResultSet* rs = nullptr;
    int ret = reader.query(table_name, query_columns,
                           std::numeric_limits<int64_t>::min(),
                           std::numeric_limits<int64_t>::max(), rs);
    if (ret != common::E_OK || rs == nullptr) {
        if (rs != nullptr) reader.destroy_query_data_set(rs);
        return ret == common::E_OK ? common::E_FILE_READ_ERR : ret;
    }

    // Replace metadata-only timeline values.  This is needed for a FIELD
    // whose value statistic has count=0: the time chunk remains authoritative
    // for row_count/min_time/max_time, but some older readers expose it as a
    // zero-valued statistic.
    for (const std::string& key : summary.entity_order) {
        auto it = summary.entities.find(key);
        if (it != summary.entities.end()) {
            it->second.row_count = 0;
            it->second.start_time = 0;
            it->second.end_time = 0;
            it->second.has_timeline_statistic = false;
        }
    }

    auto metadata = rs->get_metadata();
    std::vector<common::TSDataType> result_types;
    for (uint32_t i = 1; i <= metadata->get_column_count(); ++i) {
        result_types.push_back(metadata->get_column_type(i));
    }
    bool has_next = false;
    while ((ret = rs->next(has_next)) == common::E_OK && has_next) {
        const int64_t timestamp = rs->get_int64_at(1);
        std::vector<std::string> tag_values;
        std::vector<bool> tag_nulls;
        for (uint32_t index : tag_result_indexes) {
            const bool is_null = rs->is_null(index);
            tag_nulls.push_back(is_null);
            tag_values.push_back(is_null
                                     ? std::string()
                                     : cell_to_string(rs, index,
                                                      result_types[index - 1]));
        }

        EntityAccumulator* entity = nullptr;
        for (const std::string& key : summary.entity_order) {
            auto it = summary.entities.find(key);
            if (it != summary.entities.end() &&
                same_entity(it->second, tag_values, tag_nulls)) {
                entity = &it->second;
                break;
            }
        }
        if (entity == nullptr) {
            const std::string synthetic_key =
                table_name + "#scan#" + std::to_string(summary.entity_order.size());
            EntityAccumulator created;
            created.tag_values = tag_values;
            created.tag_nulls = tag_nulls;
            summary.entity_order.push_back(synthetic_key);
            auto inserted = summary.entities.emplace(synthetic_key, created);
            entity = &inserted.first->second;
        }
        if (!entity->has_timeline_statistic) {
            entity->start_time = timestamp;
            entity->end_time = timestamp;
            entity->has_timeline_statistic = true;
        } else {
            entity->start_time = std::min(entity->start_time, timestamp);
            entity->end_time = std::max(entity->end_time, timestamp);
        }
        ++entity->row_count;
    }
    reader.destroy_query_data_set(rs);
    return ret;
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

    bool missing_value_statistics = false;
    for (const std::string& key : summary.entity_order) {
        const auto it = summary.entities.find(key);
        if (it == summary.entities.end()) continue;
        for (size_t index : summary.field_indexes) {
            const std::string& field = summary.columns[index].name;
            if (it->second.fields.find(field) == it->second.fields.end()) {
                missing_value_statistics = true;
                break;
            }
        }
        if (missing_value_statistics) break;
    }
    if (missing_value_statistics && !devices.empty()) {
        std::vector<std::string> query_columns;
        std::vector<uint32_t> tag_result_indexes;
        for (size_t i = 0; i < measurements.size(); ++i) {
            if (i >= categories.size() || !measurements[i]) continue;
            if (categories[i] == common::ColumnCategory::TAG ||
                categories[i] == common::ColumnCategory::FIELD) {
                query_columns.push_back(measurements[i]->measurement_name_);
                if (categories[i] == common::ColumnCategory::TAG) {
                    tag_result_indexes.push_back(
                        static_cast<uint32_t>(query_columns.size() + 1));
                }
            }
        }
        const int scan_ret = scan_table_timelines(
            schema->get_table_name(), query_columns, tag_result_indexes, reader,
            summary);
        if (scan_ret != common::E_OK) {
            err << "Error: failed to scan statistics rows: "
                << error_code_message(scan_ret) << "\n";
            return kExitFile;
        }
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
                const bool has_value_statistic = statistic.present;
                // A timeline can exist even when every value in this FIELD is
                // NULL, so retain a concrete row/null count and time range.
                // Value aggregates remain NULL and are marked as scan-derived.
                const bool has_timeline = entity.has_timeline_statistic;
                const bool has_statistic =
                    has_value_statistic || has_timeline;
                const bool has_null_count =
                    has_timeline;
                const long long null_count =
                    has_null_count
                        ? entity.row_count -
                              (has_value_statistic ? statistic.count : 0)
                        : 0;
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
                // A value statistic always carries an exact non-null count,
                // even when the file does not expose a separate timeline
                // statistic.  The null count, however, is only knowable when
                // the entity timeline is available (or has been scanned).
                cells.push_back(
                    has_value_statistic
                        ? std::to_string(statistic.count)
                        : (has_timeline ? "0" : ""));
                cells.push_back(has_null_count ? std::to_string(null_count)
                                              : "");
                cells.push_back(
                    has_value_statistic
                        ? std::to_string(statistic.start_time)
                        : (has_timeline ? std::to_string(entity.start_time)
                                        : ""));
                cells.push_back(
                    has_value_statistic
                        ? std::to_string(statistic.end_time)
                        : (has_timeline ? std::to_string(entity.end_time) : ""));
                nulls.insert(nulls.end(), {false, false, !has_statistic,
                                           !has_null_count, !has_statistic,
                                           !has_statistic});

                StatisticCells values;
                values.values.assign(5, "");
                values.is_null.assign(5, true);
                if (has_value_statistic) {
                    values = statistic.values;
                }
                cells.insert(cells.end(), values.values.begin(),
                             values.values.end());
                nulls.insert(nulls.end(), values.is_null.begin(),
                             values.is_null.end());
                cells.push_back(has_value_statistic
                                    ? "statistics"
                                    : (has_timeline ? "scan" : ""));
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
