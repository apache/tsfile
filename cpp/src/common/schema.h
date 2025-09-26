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

#ifndef COMMON_SCHEMA_H
#define COMMON_SCHEMA_H

#include <writer/chunk_writer.h>

#include <algorithm>
#include <map>  // use unordered_map instead
#include <memory>
#include <string>
#include <unordered_map>

#include "common/db_common.h"
#include "writer/time_chunk_writer.h"
#include "writer/value_chunk_writer.h"

namespace storage {
class ChunkWriter;
class ValueChunkWriter;
class TimeChunkWriter;
}  // namespace storage

namespace storage {

/* schema information for one measurement */
struct MeasurementSchema {
    std::string measurement_name_;  // for example: "s1"
    common::TSDataType data_type_;
    common::TSEncoding encoding_;
    common::CompressionType compression_type_;
    storage::ChunkWriter *chunk_writer_;
    ValueChunkWriter *value_chunk_writer_;
    std::map<std::string, std::string> props_;

    MeasurementSchema()
        : measurement_name_(),
          data_type_(common::INVALID_DATATYPE),
          encoding_(common::INVALID_ENCODING),
          compression_type_(common::INVALID_COMPRESSION),
          chunk_writer_(nullptr),
          value_chunk_writer_(nullptr) {}

    MeasurementSchema(const std::string &measurement_name,
                      common::TSDataType data_type)
        : measurement_name_(measurement_name),
          data_type_(data_type),
          encoding_(common::get_value_encoder(data_type)),
          compression_type_(common::get_default_compressor()),
          chunk_writer_(nullptr),
          value_chunk_writer_(nullptr) {}

    MeasurementSchema(const std::string &measurement_name,
                      common::TSDataType data_type, common::TSEncoding encoding,
                      common::CompressionType compression_type)
        : measurement_name_(measurement_name),
          data_type_(data_type),
          encoding_(encoding),
          compression_type_(compression_type),
          chunk_writer_(nullptr),
          value_chunk_writer_(nullptr) {}

    ~MeasurementSchema() {
        if (chunk_writer_ != nullptr) {
            delete chunk_writer_;
            chunk_writer_ = nullptr;
        }
        if (value_chunk_writer_ != nullptr) {
            delete value_chunk_writer_;
            value_chunk_writer_ = nullptr;
        }
    }

    int serialize_to(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(
                common::SerializationUtil::write_str(measurement_name_, out))) {
        } else if (RET_FAIL(
                       common::SerializationUtil::write_ui8(data_type_, out))) {
        } else if (RET_FAIL(
                       common::SerializationUtil::write_ui8(encoding_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui8(
                       compression_type_, out))) {
        }
        if (ret == common::E_OK) {
            if (RET_FAIL(common::SerializationUtil::write_ui32(props_.size(),
                                                               out))) {
                for (const auto &prop : props_) {
                    if (RET_FAIL(common::SerializationUtil::write_str(
                            prop.first, out))) {
                    } else if (RET_FAIL(common::SerializationUtil::write_str(
                                   prop.second, out))) {
                    }
                    if (IS_FAIL(ret)) break;
                }
            }
        }
        return ret;
    }

    int deserialize_from(common::ByteStream &in) {
        int ret = common::E_OK;
        uint8_t data_type = common::TSDataType::INVALID_DATATYPE,
                encoding = common::TSEncoding::INVALID_ENCODING,
                compression_type = common::CompressionType::INVALID_COMPRESSION;
        if (RET_FAIL(
                common::SerializationUtil::read_str(measurement_name_, in))) {
        } else if (RET_FAIL(
                       common::SerializationUtil::read_ui8(data_type, in))) {
        } else if (RET_FAIL(
                       common::SerializationUtil::read_ui8(encoding, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui8(
                       compression_type, in))) {
        }
        data_type_ = static_cast<common::TSDataType>(data_type);
        encoding_ = static_cast<common::TSEncoding>(encoding);
        compression_type_ =
            static_cast<common::CompressionType>(compression_type);
        uint32_t props_size;
        if (ret == common::E_OK) {
            if (RET_FAIL(
                    common::SerializationUtil::read_ui32(props_size, in))) {
                for (uint32_t i = 0; i < props_.size(); ++i) {
                    std::string key, value;
                    if (RET_FAIL(
                            common::SerializationUtil::read_str(key, in))) {
                    } else if (RET_FAIL(common::SerializationUtil::read_str(
                                   value, in))) {
                    }
                    props_.insert(std::make_pair(key, value));
                    if (IS_FAIL(ret)) break;
                }
            }
        }
        return ret;
    }
};

typedef std::map<std::string, MeasurementSchema *> MeasurementSchemaMap;
typedef std::map<std::string, MeasurementSchema *>::iterator
    MeasurementSchemaMapIter;
typedef std::pair<MeasurementSchemaMapIter, bool>
    MeasurementSchemaMapInsertResult;

/* schema information for a device */
struct MeasurementSchemaGroup {
    // measurement_name -> MeasurementSchema
    MeasurementSchemaMap measurement_schema_map_;
    bool is_aligned_ = false;
    TimeChunkWriter *time_chunk_writer_ = nullptr;

    ~MeasurementSchemaGroup() {
        if (time_chunk_writer_ != nullptr) {
            delete time_chunk_writer_;
            time_chunk_writer_ = nullptr;
        }
    }
};

/**
 * @brief Represents the schema information for an entire table.
 *
 * This class holds the metadata necessary to describe how a specific table is
 * structured, including its name and the schemas of all its columns.
 */
class TableSchema {
   public:
    TableSchema() = default;

    /**
     * Constructs a TableSchema object with the given table name, column
     * schemas, and column categories.
     *
     * @param table_name The name of the table. Must be a non-empty string.
     *                   This name is used to identify the table within the
     * system.
     * @param column_schemas A vector containing ColumnSchema objects.
     *                       Each ColumnSchema defines the schema for one column
     * in the table.
     */
    TableSchema(const std::string &table_name,
                const std::vector<common::ColumnSchema> &column_schemas)
        : table_name_(table_name) {
        to_lowercase_inplace(table_name_);
        for (const common::ColumnSchema &column_schema : column_schemas) {
            column_schemas_.emplace_back(std::make_shared<MeasurementSchema>(
                column_schema.get_column_name(),
                column_schema.get_data_type()));
            column_categories_.emplace_back(
                column_schema.get_column_category());
        }
        int idx = 0;
        for (const auto &measurement_schema : column_schemas_) {
            to_lowercase_inplace(measurement_schema->measurement_name_);
            column_pos_index_.insert(
                std::make_pair(measurement_schema->measurement_name_, idx++));
        }
    }

    TableSchema(const std::string &table_name,
                const std::vector<MeasurementSchema *> &column_schemas,
                const std::vector<common::ColumnCategory> &column_categories)
        : table_name_(table_name), column_categories_(column_categories) {
        to_lowercase_inplace(table_name_);
        for (const auto column_schema : column_schemas) {
            if (column_schema != nullptr) {
                column_schemas_.emplace_back(
                    std::shared_ptr<MeasurementSchema>(column_schema));
            }
        }
        int idx = 0;
        for (const auto &measurement_schema : column_schemas_) {
            to_lowercase_inplace(measurement_schema->measurement_name_);
            column_pos_index_.insert(
                std::make_pair(measurement_schema->measurement_name_, idx++));
        }
    }

    TableSchema(TableSchema &&other) noexcept
        : table_name_(std::move(other.table_name_)),
          column_schemas_(std::move(other.column_schemas_)),
          column_categories_(std::move(other.column_categories_)) {}

    TableSchema(const TableSchema &other) noexcept
        : table_name_(other.table_name_),
          column_categories_(other.column_categories_) {
        for (const auto &column_schema : other.column_schemas_) {
            // Just call default construction
            column_schemas_.emplace_back(
                std::make_shared<MeasurementSchema>(*column_schema));
        }
        int idx = 0;
        for (const auto &measurement_schema : column_schemas_) {
            column_pos_index_.insert(
                std::make_pair(measurement_schema->measurement_name_, idx++));
        }
    }

    int serialize_to(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                column_schemas_.size(), out))) {
        } else {
            for (size_t i = 0; IS_SUCC(ret) && i < column_schemas_.size();
                 i++) {
                auto column_schema = column_schemas_[i];
                auto column_category = column_categories_[i];
                if (RET_FAIL(column_schema->serialize_to(out))) {
                } else if (RET_FAIL(common::SerializationUtil::write_i32(
                               static_cast<int32_t>(column_category), out))) {
                }
            }
        }
        return ret;
    }

    int deserialize(common::ByteStream &in) {
        int ret = common::E_OK;
        uint32_t num_columns;
        if (RET_FAIL(
                common::SerializationUtil::read_var_uint(num_columns, in))) {
        } else {
            for (size_t i = 0; IS_SUCC(ret) && i < num_columns; i++) {
                auto column_schema = std::make_shared<MeasurementSchema>();
                int32_t column_category = 0;
                if (RET_FAIL(column_schema->deserialize_from(in))) {
                } else if (RET_FAIL(common::SerializationUtil::read_i32(
                               column_category, in))) {
                }
                column_schemas_.emplace_back(column_schema);
                column_categories_.emplace_back(
                    static_cast<common::ColumnCategory>(column_category));
            }
        }
        return ret;
    }

    ~TableSchema() { column_schemas_.clear(); }

    const std::string &get_table_name() { return table_name_; }

    void set_table_name(const std::string &table_name) {
        table_name_ = table_name;
    }

    std::vector<std::string> get_measurement_names() const {
        std::vector<std::string> ret(column_schemas_.size());
        for (size_t i = 0; i < column_schemas_.size(); i++) {
            ret[i] = column_schemas_[i]->measurement_name_;
        }
        return ret;
    }

    int32_t get_columns_num() const { return column_schemas_.size(); }

    int find_column_index(const std::string &column_name) {
        std::string lower_case_column_name = to_lower(column_name);
        auto it = column_pos_index_.find(lower_case_column_name);
        if (it != column_pos_index_.end()) {
            return it->second;
        } else {
            int index = -1;
            for (size_t i = 0; i < column_schemas_.size(); ++i) {
                if (column_schemas_[i]->measurement_name_ ==
                    lower_case_column_name) {
                    index = static_cast<int>(i);
                    break;
                }
            }
            if (index != -1) {
                column_pos_index_[lower_case_column_name] = index;
            }
            return index;
        }
    }

    size_t get_column_pos_index_num() const { return column_pos_index_.size(); }

    void update(ChunkGroupMeta *chunk_group_meta) {
        for (auto iter = chunk_group_meta->chunk_meta_list_.begin();
             iter != chunk_group_meta->chunk_meta_list_.end(); iter++) {
            auto &chunk_meta = iter.get();
            if (chunk_meta->data_type_ == common::VECTOR) {
                continue;
            }
            int column_idx = find_column_index(
                chunk_meta->measurement_name_.to_std_string());
            if (column_idx == -1) {
                auto measurement_schema = std::make_shared<MeasurementSchema>(
                    chunk_meta->measurement_name_.to_std_string(),
                    chunk_meta->data_type_, chunk_meta->encoding_,
                    chunk_meta->compression_type_);
                column_schemas_.emplace_back(measurement_schema);
                column_categories_.emplace_back(common::ColumnCategory::FIELD);
                column_pos_index_.insert(std::make_pair(
                    chunk_meta->measurement_name_.to_std_string(),
                    column_schemas_.size() - 1));
            } else {
                auto origin_measurement_schema = column_schemas_.at(column_idx);
                if (origin_measurement_schema->data_type_ !=
                    chunk_meta->data_type_) {
                    origin_measurement_schema->data_type_ =
                        common::TSDataType::STRING;
                }
            }
        }
    }

    std::vector<common::TSDataType> get_data_types() const {
        std::vector<common::TSDataType> ret;
        for (const auto &measurement_schema : column_schemas_) {
            ret.emplace_back(measurement_schema->data_type_);
        }
        return ret;
    }

    std::vector<common::ColumnCategory> get_column_categories() const {
        return column_categories_;
    }

    std::vector<std::shared_ptr<MeasurementSchema> > get_measurement_schemas()
        const {
        return column_schemas_;
    }

    common::ColumnSchema get_column_schema(const std::string &column_name) {
        int column_idx = find_column_index(column_name);
        if (column_idx == -1) {
            return common::ColumnSchema();
        } else {
            return common::ColumnSchema(
                column_schemas_[column_idx]->measurement_name_,
                column_schemas_[column_idx]->data_type_,
                column_schemas_[column_idx]->compression_type_,
                column_schemas_[column_idx]->encoding_,
                column_categories_[column_idx]);
        }
    }

    int32_t find_id_column_order(const std::string &column_name) {
        std::string lower_case_column_name = to_lower(column_name);

        int column_order = 0;
        for (size_t i = 0; i < column_schemas_.size(); ++i) {
            if (column_schemas_[i]->measurement_name_ ==
                    lower_case_column_name &&
                column_categories_[i] == common::ColumnCategory::TAG) {
                return column_order;
            } else if (column_categories_[i] == common::ColumnCategory::TAG) {
                column_order++;
            }
        }
        return -1;
    }

   private:
    std::string table_name_;
    std::vector<std::shared_ptr<MeasurementSchema> > column_schemas_;
    std::vector<common::ColumnCategory> column_categories_;
    std::map<std::string, int> column_pos_index_;
};

struct Schema {
    typedef std::unordered_map<std::string, std::shared_ptr<TableSchema> >
        TableSchemasMap;
    TableSchemasMap table_schema_map_;

    void update_table_schema(ChunkGroupMeta *chunk_group_meta) {
        std::shared_ptr<IDeviceID> device_id = chunk_group_meta->device_id_;
        auto table_name = device_id->get_table_name();
        if (table_schema_map_.find(table_name) == table_schema_map_.end()) {
            table_schema_map_[table_name] = std::make_shared<TableSchema>();
        }
        table_schema_map_[table_name]->update(chunk_group_meta);
    }
    void register_table_schema(
        const std::shared_ptr<TableSchema> &table_schema) {
        table_schema_map_[table_schema->get_table_name()] = table_schema;
    }
};
}  // end namespace storage
#endif  // COMMON_SCHEMA_H
