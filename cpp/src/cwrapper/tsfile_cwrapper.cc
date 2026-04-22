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

#include "cwrapper/tsfile_cwrapper.h"

#include <file/write_file.h>
#include <reader/qds_without_timegenerator.h>
#include <unistd.h>
#include <writer/tsfile_table_writer.h>

#include <cstring>
#include <set>
#include <vector>

#include "common/device_id.h"
#include "common/statistic.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "reader/filter/tag_filter.h"
#include "reader/result_set.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

// Forward declarations for arrow namespace functions (defined in arrow_c.cc)
namespace arrow {
int TsBlockToArrowStruct(common::TsBlock& tsblock, ArrowArray* out_array,
                         ArrowSchema* out_schema);
int ArrowStructToTablet(const char* table_name, const ArrowArray* in_array,
                        const ArrowSchema* in_schema,
                        const storage::TableSchema* reg_schema,
                        storage::Tablet** out_tablet, int time_col_index);
}  // namespace arrow

#ifdef __cplusplus
extern "C" {
#endif

static bool is_init = false;

void init_tsfile_config() {
    if (!is_init) {
        common::init_common();
        is_init = true;
    }
}

uint8_t get_global_time_encoding() {
    return common::get_global_time_encoding();
}

uint8_t get_global_time_compression() {
    return common::get_global_time_compression();
}

uint8_t get_datatype_encoding(uint8_t data_type) {
    return common::get_datatype_encoding(data_type);
}

uint8_t get_global_compression() { return common::get_global_compression(); }

int set_global_time_encoding(uint8_t encoding) {
    return common::set_global_time_encoding(encoding);
}

int set_global_time_compression(uint8_t compression) {
    return common::set_global_time_compression(compression);
}

int set_datatype_encoding(uint8_t data_type, uint8_t encoding) {
    return common::set_datatype_encoding(data_type, encoding);
}

int set_global_compression(uint8_t compression) {
    return common::set_global_compression(compression);
}

WriteFile write_file_new(const char* pathname, ERRNO* err_code) {
    int ret;
    init_tsfile_config();

    if (access(pathname, F_OK) == 0) {
        *err_code = common::E_ALREADY_EXIST;
        return nullptr;
    }

    int flags = O_RDWR | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    mode_t mode = 0666;
    storage::WriteFile* file = new storage::WriteFile;
    ret = file->create(pathname, flags, mode);
    *err_code = ret;
    return file;
}

TsFileWriter tsfile_writer_new(WriteFile file, TableSchema* schema,
                               ERRNO* err_code) {
    if (schema->column_num == 0) {
        *err_code = common::E_INVALID_SCHEMA;
        return nullptr;
    }

    init_tsfile_config();
    std::vector<common::ColumnSchema> column_schemas;
    std::set<std::string> column_names;
    for (int i = 0; i < schema->column_num; i++) {
        ColumnSchema cur_schema = schema->column_schemas[i];
        if (column_names.find(cur_schema.column_name) != column_names.end()) {
            *err_code = common::E_INVALID_SCHEMA;
            return nullptr;
        }
        column_names.insert(cur_schema.column_name);
        if (cur_schema.column_category == TAG &&
            cur_schema.data_type != TS_DATATYPE_STRING) {
            *err_code = common::E_INVALID_SCHEMA;
            return nullptr;
        }
        column_schemas.emplace_back(
            cur_schema.column_name,
            static_cast<common::TSDataType>(cur_schema.data_type),
            static_cast<common::ColumnCategory>(cur_schema.column_category));
    }

    storage::TableSchema* table_schema =
        new storage::TableSchema(schema->table_name, column_schemas);
    auto table_writer = new storage::TsFileTableWriter(
        static_cast<storage::WriteFile*>(file), table_schema);
    delete table_schema;
    *err_code = common::E_OK;
    return table_writer;
}

TsFileWriter tsfile_writer_new_with_memory_threshold(WriteFile file,
                                                     TableSchema* schema,
                                                     uint64_t memory_threshold,
                                                     ERRNO* err_code) {
    if (schema->column_num == 0) {
        *err_code = common::E_INVALID_SCHEMA;
        return nullptr;
    }
    init_tsfile_config();
    std::vector<common::ColumnSchema> column_schemas;
    std::set<std::string> column_names;
    for (int i = 0; i < schema->column_num; i++) {
        ColumnSchema cur_schema = schema->column_schemas[i];
        if (column_names.find(cur_schema.column_name) == column_names.end()) {
            *err_code = common::E_INVALID_SCHEMA;
            return nullptr;
        }
        column_names.insert(cur_schema.column_name);
        column_schemas.emplace_back(
            cur_schema.column_name,
            static_cast<common::TSDataType>(cur_schema.data_type),
            static_cast<common::ColumnCategory>(cur_schema.column_category));
    }

    storage::TableSchema* table_schema =
        new storage::TableSchema(schema->table_name, column_schemas);

    auto table_writer = new storage::TsFileTableWriter(
        static_cast<storage::WriteFile*>(file), table_schema, memory_threshold);
    *err_code = common::E_OK;
    delete table_schema;
    return table_writer;
}

TsFileReader tsfile_reader_new(const char* pathname, ERRNO* err_code) {
    init_tsfile_config();
    auto reader = new storage::TsFileReader();
    int ret = reader->open(pathname);
    if (ret != common::E_OK) {
        *err_code = ret;
        delete reader;
        return nullptr;
    }
    return reader;
}

ERRNO tsfile_writer_close(TsFileWriter writer) {
    if (writer == nullptr) {
        return common::E_OK;
    }
    auto* w = static_cast<storage::TsFileTableWriter*>(writer);
    int ret = w->flush();
    if (ret != common::E_OK) {
        return ret;
    }
    ret = w->close();
    if (ret != common::E_OK) {
        return ret;
    }
    delete w;
    return ret;
}

ERRNO tsfile_reader_close(TsFileReader reader) {
    auto* ts_reader = static_cast<storage::TsFileReader*>(reader);
    delete ts_reader;
    return common::E_OK;
}

Tablet tablet_new(char** column_name_list, TSDataType* data_types,
                  uint32_t column_num, uint32_t max_rows) {
    std::vector<std::string> measurement_list;
    std::vector<common::TSDataType> data_type_list;
    for (uint32_t i = 0; i < column_num; i++) {
        measurement_list.emplace_back(storage::to_lower(column_name_list[i]));
        data_type_list.push_back(
            static_cast<common::TSDataType>(*(data_types + i)));
    }
    return new storage::Tablet(measurement_list, data_type_list, max_rows);
}

uint32_t tablet_get_cur_row_size(Tablet tablet) {
    return static_cast<storage::Tablet*>(tablet)->get_cur_row_size();
}

ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           Timestamp timestamp) {
    return static_cast<storage::Tablet*>(tablet)->add_timestamp(row_index,
                                                                timestamp);
}

#define TABLET_ADD_VALUE_BY_NAME_DEF(type)                                   \
    ERRNO tablet_add_value_by_name_##type(Tablet tablet, uint32_t row_index, \
                                          const char* column_name,           \
                                          const type value) {                \
        return static_cast<storage::Tablet*>(tablet)->add_value(             \
            row_index, storage::to_lower(column_name), value);               \
    }
TABLET_ADD_VALUE_BY_NAME_DEF(int32_t);
TABLET_ADD_VALUE_BY_NAME_DEF(int64_t);
TABLET_ADD_VALUE_BY_NAME_DEF(float);
TABLET_ADD_VALUE_BY_NAME_DEF(double);
TABLET_ADD_VALUE_BY_NAME_DEF(bool);

ERRNO tablet_add_value_by_name_string_with_len(Tablet tablet,
                                               uint32_t row_index,
                                               const char* column_name,
                                               const char* value,
                                               int value_len) {
    return static_cast<storage::Tablet*>(tablet)->add_value(
        row_index, storage::to_lower(column_name),
        common::String(value, value_len));
}

#define TABLE_ADD_VALUE_BY_INDEX_DEF(type)                                    \
    ERRNO tablet_add_value_by_index_##type(Tablet tablet, uint32_t row_index, \
                                           uint32_t column_index,             \
                                           const type value) {                \
        return static_cast<storage::Tablet*>(tablet)->add_value(              \
            row_index, column_index, value);                                  \
    }

ERRNO tablet_add_value_by_index_string_with_len(Tablet tablet,
                                                uint32_t row_index,
                                                uint32_t column_index,
                                                const char* value,
                                                int value_len) {
    return static_cast<storage::Tablet*>(tablet)->add_value(
        row_index, column_index, common::String(value, value_len));
}

TABLE_ADD_VALUE_BY_INDEX_DEF(int32_t);
TABLE_ADD_VALUE_BY_INDEX_DEF(int64_t);
TABLE_ADD_VALUE_BY_INDEX_DEF(float);
TABLE_ADD_VALUE_BY_INDEX_DEF(double);
TABLE_ADD_VALUE_BY_INDEX_DEF(bool);

// TsRecord API
TsRecord _ts_record_new(const char* device_id, Timestamp timestamp,
                        int timeseries_num) {
    auto* record = new storage::TsRecord(timestamp, device_id, timeseries_num);
    return record;
}

#define INSERT_DATA_INTO_TS_RECORD_BY_NAME_DEF(type)                 \
    ERRNO _insert_data_into_ts_record_by_name_##type(                \
        TsRecord data, const char* measurement_name, type value) {   \
        auto* record = (storage::TsRecord*)data;                     \
        storage::DataPoint point(measurement_name, value);           \
        if (record->points_.size() + 1 > record->points_.capacity()) \
            return common::E_BUF_NOT_ENOUGH;                         \
        record->points_.push_back(point);                            \
        return common::E_OK;                                         \
    }

ERRNO _insert_data_into_ts_record_by_name_string_with_len(
    TsRecord data, const char* measurement_name, const char* value,
    const uint32_t value_len) {
    auto* record = (storage::TsRecord*)data;
    if (record->points_.size() + 1 > record->points_.capacity())
        return common::E_BUF_NOT_ENOUGH;
    common::String str_value;
    str_value.dup_from(value, value_len, record->pa);
    record->add_point(measurement_name, str_value);
    return common::E_OK;
}

INSERT_DATA_INTO_TS_RECORD_BY_NAME_DEF(int32_t);
INSERT_DATA_INTO_TS_RECORD_BY_NAME_DEF(int64_t);
INSERT_DATA_INTO_TS_RECORD_BY_NAME_DEF(bool);
INSERT_DATA_INTO_TS_RECORD_BY_NAME_DEF(float);
INSERT_DATA_INTO_TS_RECORD_BY_NAME_DEF(double);
/*
TsFileWriter tsfile_writer_new_with_conf(const char *pathname,
                                     const mode_t flag, ERRNO *err_code,
                                     TsFileConf *conf) {
init_tsfile_config();
auto *writer = new storage::TsFileWriter();
const int ret = writer->open(pathname, O_CREAT | O_RDWR, flag);
if (ret != common::E_OK) {
    delete writer;
    *err_code = ret;
    return nullptr;
}
return writer;
}

*/
ERRNO tsfile_writer_write(TsFileWriter writer, Tablet tablet) {
    auto* w = static_cast<storage::TsFileTableWriter*>(writer);
    auto* tbl = static_cast<storage::Tablet*>(tablet);
    return w->write_table(*tbl);
}

// ERRNO tsfile_writer_flush_data(TsFileWriter writer) {
//     auto *w = static_cast<storage::TsFileWriter *>(writer);
//     return w->flush();
// }

// Query

ResultSet tsfile_query_table(TsFileReader reader, const char* table_name,
                             char** columns, uint32_t column_num,
                             Timestamp start_time, Timestamp end_time,
                             ERRNO* err_code) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    storage::ResultSet* table_result_set = nullptr;
    std::vector<std::string> column_names;
    for (uint32_t i = 0; i < column_num; i++) {
        column_names.emplace_back(columns[i]);
    }
    *err_code = r->query(table_name, column_names, start_time, end_time,
                         table_result_set);
    return table_result_set;
}

ResultSet tsfile_query_table_on_tree(TsFileReader reader, char** columns,
                                     uint32_t column_num, Timestamp start_time,
                                     Timestamp end_time, ERRNO* err_code) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    storage::ResultSet* table_result_set = nullptr;
    std::vector<std::string> column_names;
    for (uint32_t i = 0; i < column_num; i++) {
        column_names.emplace_back(columns[i]);
    }
    *err_code = r->query_table_on_tree(column_names, start_time, end_time,
                                       table_result_set);
    return table_result_set;
}

ResultSet tsfile_reader_query_tree_by_row(TsFileReader reader,
                                          char** device_ids, int device_ids_len,
                                          char** measurement_names,
                                          int measurement_names_len, int offset,
                                          int limit, ERRNO* err_code) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    storage::ResultSet* result_set = nullptr;

    std::vector<std::string> path_list;
    if (device_ids_len > 0 && measurement_names_len > 0) {
        path_list.reserve(static_cast<size_t>(device_ids_len) *
                          static_cast<size_t>(measurement_names_len));
    }

    for (int i = 0; i < device_ids_len; i++) {
        const char* device_id = device_ids[i];
        if (device_id == nullptr) {
            continue;
        }
        for (int j = 0; j < measurement_names_len; j++) {
            const char* measurement_name = measurement_names[j];
            if (measurement_name == nullptr) {
                continue;
            }
            path_list.emplace_back(std::string(device_id) + "." +
                                   std::string(measurement_name));
        }
    }

    *err_code = r->queryByRow(path_list, offset, limit, result_set);
    return result_set;
}

ResultSet tsfile_reader_query_table_by_row(
    TsFileReader reader, const char* table_name, char** column_names,
    int column_names_len, int offset, int limit, TagFilterHandle tag_filter,
    int batch_size, ERRNO* err_code) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    storage::ResultSet* result_set = nullptr;

    std::vector<std::string> columns;
    if (column_names_len > 0) {
        columns.reserve(static_cast<size_t>(column_names_len));
    }
    for (int i = 0; i < column_names_len; i++) {
        const char* name = column_names[i];
        columns.emplace_back(name == nullptr ? "" : std::string(name));
    }

    *err_code = r->queryByRow(
        table_name == nullptr ? "" : table_name, columns, offset, limit,
        result_set, static_cast<storage::Filter*>(tag_filter), batch_size);
    return result_set;
}

ResultSet tsfile_query_table_batch(TsFileReader reader, const char* table_name,
                                   char** columns, uint32_t column_num,
                                   Timestamp start_time, Timestamp end_time,
                                   TagFilterHandle tag_filter, int batch_size,
                                   ERRNO* err_code) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    storage::ResultSet* table_result_set = nullptr;
    std::vector<std::string> column_names;
    for (uint32_t i = 0; i < column_num; i++) {
        column_names.emplace_back(columns[i]);
    }
    *err_code = r->query(table_name, column_names, start_time, end_time,
                         table_result_set,
                         static_cast<storage::Filter*>(tag_filter), batch_size);
    return table_result_set;
}

bool tsfile_result_set_next(ResultSet result_set, ERRNO* err_code) {
    auto* r = static_cast<storage::ResultSet*>(result_set);
    bool has_next = true;
    int ret = common::E_OK;
    ret = r->next(has_next);
    *err_code = ret;
    if (ret != common::E_OK) {
        return false;
    }
    return has_next;
}

ERRNO tsfile_result_set_get_next_tsblock_as_arrow(ResultSet result_set,
                                                  ArrowArray* out_array,
                                                  ArrowSchema* out_schema) {
    if (result_set == nullptr || out_array == nullptr ||
        out_schema == nullptr) {
        return common::E_INVALID_ARG;
    }

    auto* r = static_cast<storage::ResultSet*>(result_set);
    auto* table_result_set = dynamic_cast<storage::TableResultSet*>(r);
    if (table_result_set == nullptr) {
        return common::E_INVALID_ARG;
    }

    common::TsBlock* tsblock = nullptr;
    int ret = table_result_set->get_next_tsblock(tsblock);
    if (ret != common::E_OK) {
        return ret;
    }

    if (tsblock == nullptr) {
        return common::E_NO_MORE_DATA;
    }

    ret = arrow::TsBlockToArrowStruct(*tsblock, out_array, out_schema);
    return ret;
}

#define TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(type)                          \
    type tsfile_result_set_get_value_by_name_##type(ResultSet result_set,      \
                                                    const char* column_name) { \
        auto* r = static_cast<storage::ResultSet*>(result_set);                \
        std::string column_name_(column_name);                                 \
        return r->get_value<type>(column_name_);                               \
    }

TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(bool);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(int32_t);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(int64_t);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(float);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(double);
char* tsfile_result_set_get_value_by_name_string(ResultSet result_set,
                                                 const char* column_name) {
    auto* r = static_cast<storage::ResultSet*>(result_set);
    std::string column_name_(column_name);
    common::String* ret = r->get_value<common::String*>(column_name_);
    // Caller should free return's char* 's space.
    char* dup = (char*)malloc(ret->len_ + 1);
    if (dup) {
        memcpy(dup, ret->buf_, ret->len_);
        dup[ret->len_] = '\0';
    }
    return dup;
}

#define TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(type)                        \
    type tsfile_result_set_get_value_by_index_##type(ResultSet result_set,    \
                                                     uint32_t column_index) { \
        auto* r = static_cast<storage::ResultSet*>(result_set);               \
        return r->get_value<type>(column_index);                              \
    }

TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(int32_t);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(int64_t);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(float);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(double);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(bool);

char* tsfile_result_set_get_value_by_index_string(ResultSet result_set,
                                                  uint32_t column_index) {
    auto* r = static_cast<storage::ResultSet*>(result_set);
    common::String* ret = r->get_value<common::String*>(column_index);
    // Caller should free return's char* 's space.
    char* dup = (char*)malloc(ret->len_ + 1);
    if (dup) {
        memcpy(dup, ret->buf_, ret->len_);
        dup[ret->len_] = '\0';
    }
    return dup;
}

bool tsfile_result_set_is_null_by_name(ResultSet result_set,
                                       const char* column_name) {
    auto* r = static_cast<storage::ResultSet*>(result_set);
    return r->is_null(column_name);
}

bool tsfile_result_set_is_null_by_index(const ResultSet result_set,
                                        const uint32_t column_index) {
    auto* r = static_cast<storage::ResultSet*>(result_set);
    return r->is_null(column_index);
}

ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set) {
    auto* r = static_cast<storage::ResultSet*>(result_set);
    if (result_set == NULL) {
        return ResultSetMetaData();
    }

    ResultSetMetaData meta_data;
    std::shared_ptr<storage::ResultSetMetadata> result_set_metadata =
        r->get_metadata();
    meta_data.column_num = result_set_metadata->get_column_count();
    meta_data.column_names =
        static_cast<char**>(malloc(meta_data.column_num * sizeof(char*)));
    meta_data.data_types = static_cast<TSDataType*>(
        malloc(meta_data.column_num * sizeof(TSDataType)));
    for (int i = 0; i < meta_data.column_num; i++) {
        meta_data.column_names[i] =
            strdup(result_set_metadata->get_column_name(i + 1).c_str());
        meta_data.data_types[i] = static_cast<TSDataType>(
            result_set_metadata->get_column_type(i + 1));
    }
    return meta_data;
}

char* tsfile_result_set_metadata_get_column_name(ResultSetMetaData result_set,
                                                 uint32_t column_index) {
    if (column_index > (uint32_t)result_set.column_num) {
        return nullptr;
    }
    return result_set.column_names[column_index - 1];
}

TSDataType tsfile_result_set_metadata_get_data_type(
    ResultSetMetaData result_set, uint32_t column_index) {
    if (column_index > (uint32_t)result_set.column_num) {
        return TS_DATATYPE_INVALID;
    }
    return result_set.data_types[column_index - 1];
}

int tsfile_result_set_metadata_get_column_num(ResultSetMetaData result_set) {
    return result_set.column_num;
}

TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                           const char* table_name) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    auto table_shcema = r->get_table_schema(table_name);
    TableSchema ret_schema;
    ret_schema.table_name = strdup(table_shcema->get_table_name().c_str());
    int column_num = table_shcema->get_columns_num();
    ret_schema.column_num = column_num;
    ret_schema.column_schemas =
        static_cast<ColumnSchema*>(malloc(sizeof(ColumnSchema) * column_num));
    for (int i = 0; i < column_num; i++) {
        auto column_schema = table_shcema->get_measurement_schemas()[i];
        ret_schema.column_schemas[i].column_name =
            strdup(column_schema->measurement_name_.c_str());
        ret_schema.column_schemas[i].data_type =
            static_cast<TSDataType>(column_schema->data_type_);
        ret_schema.column_schemas[i].column_category =
            static_cast<ColumnCategory>(
                table_shcema->get_column_categories()[i]);
    }
    return ret_schema;
}

TableSchema* tsfile_reader_get_all_table_schemas(TsFileReader reader,
                                                 uint32_t* size) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    auto table_schemas = r->get_all_table_schemas();
    size_t table_num = table_schemas.size();
    TableSchema* ret =
        static_cast<TableSchema*>(malloc(sizeof(TableSchema) * table_num));
    for (size_t i = 0; i < table_schemas.size(); i++) {
        ret[i].table_name = strdup(table_schemas[i]->get_table_name().c_str());
        int column_num = table_schemas[i]->get_columns_num();
        ret[i].column_num = column_num;
        ret[i].column_schemas = static_cast<ColumnSchema*>(
            malloc(column_num * sizeof(ColumnSchema)));
        auto column_schemas = table_schemas[i]->get_measurement_schemas();
        for (int j = 0; j < column_num; j++) {
            ret[i].column_schemas[j].column_name =
                strdup(column_schemas[j]->measurement_name_.c_str());
            ret[i].column_schemas[j].data_type =
                static_cast<TSDataType>(column_schemas[j]->data_type_);
            ret[i].column_schemas[j].column_category =
                static_cast<ColumnCategory>(
                    table_schemas[i]->get_column_categories()[j]);
        }
    }
    *size = table_num;
    return ret;
}

DeviceSchema* tsfile_reader_get_all_timeseries_schemas(TsFileReader reader,
                                                       uint32_t* size) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    auto device_ids = r->get_all_device_ids();
    if (size == nullptr) {
        return nullptr;
    }
    *size = static_cast<uint32_t>(device_ids.size());
    if (device_ids.empty()) {
        return nullptr;
    }

    DeviceSchema* device_schema = static_cast<DeviceSchema*>(
        malloc(sizeof(DeviceSchema) * device_ids.size()));
    if (device_schema == nullptr) {
        *size = 0;
        return nullptr;
    }

    size_t device_index = 0;
    for (const auto& device_id : device_ids) {
        DeviceSchema& cur_schema = device_schema[device_index++];
        std::string device_name =
            device_id == nullptr ? "" : device_id->get_device_name();
        cur_schema.device_name = strdup(device_name.c_str());
        cur_schema.timeseries_num = 0;
        cur_schema.timeseries_schema = nullptr;

        std::vector<storage::MeasurementSchema> schemas;
        int ret = r->get_timeseries_schema(device_id, schemas);
        if (ret != common::E_OK || schemas.empty()) {
            continue;
        }

        cur_schema.timeseries_num = static_cast<int>(schemas.size());
        cur_schema.timeseries_schema = static_cast<TimeseriesSchema*>(
            malloc(sizeof(TimeseriesSchema) * schemas.size()));
        for (size_t i = 0; i < schemas.size(); ++i) {
            const auto& measurement_schema = schemas[i];
            cur_schema.timeseries_schema[i].timeseries_name =
                strdup(measurement_schema.measurement_name_.c_str());
            cur_schema.timeseries_schema[i].data_type =
                static_cast<TSDataType>(measurement_schema.data_type_);
            cur_schema.timeseries_schema[i].encoding =
                static_cast<TSEncoding>(measurement_schema.encoding_);
            cur_schema.timeseries_schema[i].compression =
                static_cast<CompressionType>(
                    measurement_schema.compression_type_);
        }
    }
    return device_schema;
}

void tsfile_device_id_free_contents(DeviceID* d) {
    if (d == nullptr) {
        return;
    }
    free(d->path);
    d->path = nullptr;
    free(d->table_name);
    d->table_name = nullptr;
    if (d->segments != nullptr) {
        for (uint32_t k = 0; k < d->segment_count; k++) {
            free(d->segments[k]);
        }
        free(d->segments);
        d->segments = nullptr;
    }
    d->segment_count = 0;
}

namespace {

char* dup_common_string_to_cstr(const common::String& s) {
    if (s.buf_ == nullptr || s.len_ == 0) {
        return strdup("");
    }
    char* p = static_cast<char*>(malloc(static_cast<size_t>(s.len_) + 1U));
    if (p == nullptr) {
        return nullptr;
    }
    memcpy(p, s.buf_, static_cast<size_t>(s.len_));
    p[s.len_] = '\0';
    return p;
}

static TSDataType cpp_stat_type_to_c(common::TSDataType t) {
    return static_cast<TSDataType>(static_cast<uint8_t>(t));
}

void free_timeseries_statistic_heap(TimeseriesStatistic* s) {
    if (s == nullptr) {
        return;
    }
    TsFileStatisticBase* b = tsfile_statistic_base(s);
    if (!b->has_statistic) {
        return;
    }
    switch (b->type) {
        case TS_DATATYPE_STRING:
            free(s->u.string_s.str_min);
            s->u.string_s.str_min = nullptr;
            free(s->u.string_s.str_max);
            s->u.string_s.str_max = nullptr;
            free(s->u.string_s.str_first);
            s->u.string_s.str_first = nullptr;
            free(s->u.string_s.str_last);
            s->u.string_s.str_last = nullptr;
            break;
        case TS_DATATYPE_TEXT:
            free(s->u.text_s.str_first);
            s->u.text_s.str_first = nullptr;
            free(s->u.text_s.str_last);
            s->u.text_s.str_last = nullptr;
            break;
        default:
            break;
    }
}

void clear_timeseries_statistic(TimeseriesStatistic* s) {
    memset(s, 0, sizeof(*s));
    tsfile_statistic_base(s)->type = TS_DATATYPE_INVALID;
}

/**
 * Fills @p out from C++ Statistic. On allocation failure returns E_OOM and
 * clears/frees any partial string fields in @p out.
 */
int fill_timeseries_statistic(storage::Statistic* st,
                              TimeseriesStatistic* out) {
    clear_timeseries_statistic(out);
    if (st == nullptr) {
        return common::E_OK;
    }
    const common::TSDataType t = st->get_type();
    switch (t) {
        case common::BOOLEAN: {
            auto* bs = static_cast<storage::BooleanStatistic*>(st);
            TsFileBoolStatistic* p = &out->u.bool_s;
            p->base.has_statistic = true;
            p->base.type = cpp_stat_type_to_c(common::BOOLEAN);
            p->base.row_count = st->get_count();
            p->base.start_time = st->start_time_;
            p->base.end_time = st->get_end_time();
            p->sum = static_cast<double>(bs->sum_value_);
            p->first_bool = bs->first_value_;
            p->last_bool = bs->last_value_;
            break;
        }
        case common::INT32: {
            auto* is = static_cast<storage::Int32Statistic*>(st);
            TsFileIntStatistic* p = &out->u.int_s;
            p->base.has_statistic = true;
            p->base.type = cpp_stat_type_to_c(common::INT32);
            p->base.row_count = st->get_count();
            p->base.start_time = st->start_time_;
            p->base.end_time = st->get_end_time();
            p->sum = static_cast<double>(is->sum_value_);
            if (p->base.row_count > 0) {
                p->min_int64 = static_cast<int64_t>(is->min_value_);
                p->max_int64 = static_cast<int64_t>(is->max_value_);
                p->first_int64 = static_cast<int64_t>(is->first_value_);
                p->last_int64 = static_cast<int64_t>(is->last_value_);
            }
            break;
        }
        case common::DATE: {
            auto* is = static_cast<storage::Int32Statistic*>(st);
            TsFileIntStatistic* p = &out->u.int_s;
            p->base.has_statistic = true;
            p->base.type = cpp_stat_type_to_c(common::DATE);
            p->base.row_count = st->get_count();
            p->base.start_time = st->start_time_;
            p->base.end_time = st->get_end_time();
            p->sum = static_cast<double>(is->sum_value_);
            if (p->base.row_count > 0) {
                p->min_int64 = static_cast<int64_t>(is->min_value_);
                p->max_int64 = static_cast<int64_t>(is->max_value_);
                p->first_int64 = static_cast<int64_t>(is->first_value_);
                p->last_int64 = static_cast<int64_t>(is->last_value_);
            }
            break;
        }
        case common::INT64: {
            auto* ls = static_cast<storage::Int64Statistic*>(st);
            TsFileIntStatistic* p = &out->u.int_s;
            p->base.has_statistic = true;
            p->base.type = cpp_stat_type_to_c(common::INT64);
            p->base.row_count = st->get_count();
            p->base.start_time = st->start_time_;
            p->base.end_time = st->get_end_time();
            p->sum = ls->sum_value_;
            if (p->base.row_count > 0) {
                p->min_int64 = ls->min_value_;
                p->max_int64 = ls->max_value_;
                p->first_int64 = ls->first_value_;
                p->last_int64 = ls->last_value_;
            }
            break;
        }
        case common::TIMESTAMP: {
            auto* ls = static_cast<storage::Int64Statistic*>(st);
            TsFileIntStatistic* p = &out->u.int_s;
            p->base.has_statistic = true;
            p->base.type = cpp_stat_type_to_c(common::TIMESTAMP);
            p->base.row_count = st->get_count();
            p->base.start_time = st->start_time_;
            p->base.end_time = st->get_end_time();
            p->sum = ls->sum_value_;
            if (p->base.row_count > 0) {
                p->min_int64 = ls->min_value_;
                p->max_int64 = ls->max_value_;
                p->first_int64 = ls->first_value_;
                p->last_int64 = ls->last_value_;
            }
            break;
        }
        case common::FLOAT: {
            auto* fs = static_cast<storage::FloatStatistic*>(st);
            TsFileFloatStatistic* p = &out->u.float_s;
            p->base.has_statistic = true;
            p->base.type = cpp_stat_type_to_c(common::FLOAT);
            p->base.row_count = st->get_count();
            p->base.start_time = st->start_time_;
            p->base.end_time = st->get_end_time();
            p->sum = static_cast<double>(fs->sum_value_);
            if (p->base.row_count > 0) {
                p->min_float64 = static_cast<double>(fs->min_value_);
                p->max_float64 = static_cast<double>(fs->max_value_);
                p->first_float64 = static_cast<double>(fs->first_value_);
                p->last_float64 = static_cast<double>(fs->last_value_);
            }
            break;
        }
        case common::DOUBLE: {
            auto* ds = static_cast<storage::DoubleStatistic*>(st);
            TsFileFloatStatistic* p = &out->u.float_s;
            p->base.has_statistic = true;
            p->base.type = cpp_stat_type_to_c(common::DOUBLE);
            p->base.row_count = st->get_count();
            p->base.start_time = st->start_time_;
            p->base.end_time = st->get_end_time();
            p->sum = ds->sum_value_;
            if (p->base.row_count > 0) {
                p->min_float64 = ds->min_value_;
                p->max_float64 = ds->max_value_;
                p->first_float64 = ds->first_value_;
                p->last_float64 = ds->last_value_;
            }
            break;
        }
        case common::STRING: {
            auto* ss = static_cast<storage::StringStatistic*>(st);
            TsFileStringStatistic* p = &out->u.string_s;
            p->base.has_statistic = true;
            p->base.type = cpp_stat_type_to_c(common::STRING);
            p->base.row_count = st->get_count();
            p->base.start_time = st->start_time_;
            p->base.end_time = st->get_end_time();
            p->str_min = dup_common_string_to_cstr(ss->min_value_);
            if (p->str_min == nullptr) {
                free_timeseries_statistic_heap(out);
                clear_timeseries_statistic(out);
                return common::E_OOM;
            }
            p->str_max = dup_common_string_to_cstr(ss->max_value_);
            if (p->str_max == nullptr) {
                free_timeseries_statistic_heap(out);
                clear_timeseries_statistic(out);
                return common::E_OOM;
            }
            p->str_first = dup_common_string_to_cstr(ss->first_value_);
            if (p->str_first == nullptr) {
                free_timeseries_statistic_heap(out);
                clear_timeseries_statistic(out);
                return common::E_OOM;
            }
            p->str_last = dup_common_string_to_cstr(ss->last_value_);
            if (p->str_last == nullptr) {
                free_timeseries_statistic_heap(out);
                clear_timeseries_statistic(out);
                return common::E_OOM;
            }
            break;
        }
        case common::TEXT: {
            auto* ts = static_cast<storage::TextStatistic*>(st);
            TsFileTextStatistic* p = &out->u.text_s;
            p->base.has_statistic = true;
            p->base.type = cpp_stat_type_to_c(common::TEXT);
            p->base.row_count = st->get_count();
            p->base.start_time = st->start_time_;
            p->base.end_time = st->get_end_time();
            p->str_first = dup_common_string_to_cstr(ts->first_value_);
            if (p->str_first == nullptr) {
                free_timeseries_statistic_heap(out);
                clear_timeseries_statistic(out);
                return common::E_OOM;
            }
            p->str_last = dup_common_string_to_cstr(ts->last_value_);
            if (p->str_last == nullptr) {
                free_timeseries_statistic_heap(out);
                clear_timeseries_statistic(out);
                return common::E_OOM;
            }
            break;
        }
        default: {
            TsFileStatisticBase* b = tsfile_statistic_base(out);
            b->has_statistic = true;
            b->type = TS_DATATYPE_INVALID;
            b->row_count = st->get_count();
            b->start_time = st->start_time_;
            b->end_time = st->get_end_time();
            break;
        }
    }
    return common::E_OK;
}

int fill_timeline_statistic(storage::ITimeseriesIndex* idx,
                            TimeseriesStatistic* out) {
    clear_timeseries_statistic(out);
    if (idx == nullptr) {
        return common::E_OK;
    }

    auto* aligned_idx = dynamic_cast<storage::AlignedTimeseriesIndex*>(idx);
    if (aligned_idx != nullptr && aligned_idx->time_ts_idx_ != nullptr &&
        aligned_idx->time_ts_idx_->get_statistic() != nullptr) {
        auto* st = aligned_idx->time_ts_idx_->get_statistic();
        TsFileStatisticBase* b = tsfile_statistic_base(out);
        b->has_statistic = true;
        b->type = TS_DATATYPE_VECTOR;
        b->row_count = st->get_count();
        b->start_time = st->start_time_;
        b->end_time = st->get_end_time();
        return common::E_OK;
    }

    if (idx->get_statistic() != nullptr &&
        idx->get_time_chunk_meta_list() == nullptr) {
        auto* st = idx->get_statistic();
        TsFileStatisticBase* b = tsfile_statistic_base(out);
        b->has_statistic = true;
        b->type = TS_DATATYPE_VECTOR;
        b->row_count = st->get_count();
        b->start_time = st->start_time_;
        b->end_time = st->get_end_time();
        return common::E_OK;
    }

    auto* list = idx->get_time_chunk_meta_list();
    if (list == nullptr) {
        list = idx->get_chunk_meta_list();
    }
    if (list == nullptr) {
        return common::E_OK;
    }

    int64_t row_count = 0;
    int64_t start_time = 0;
    int64_t end_time = 0;
    bool has_statistic = false;
    for (auto it = list->begin(); it != list->end(); it++) {
        auto* chunk_meta = it.get();
        if (chunk_meta == nullptr || chunk_meta->statistic_ == nullptr ||
            chunk_meta->statistic_->count_ <= 0) {
            continue;
        }
        if (!has_statistic) {
            start_time = chunk_meta->statistic_->start_time_;
            end_time = chunk_meta->statistic_->end_time_;
            has_statistic = true;
        } else {
            start_time =
                std::min(start_time, chunk_meta->statistic_->start_time_);
            end_time = std::max(end_time, chunk_meta->statistic_->end_time_);
        }
        row_count += chunk_meta->statistic_->count_;
    }

    if (!has_statistic) {
        return common::E_OK;
    }

    TsFileStatisticBase* b = tsfile_statistic_base(out);
    b->has_statistic = true;
    b->type = TS_DATATYPE_VECTOR;
    b->row_count = row_count;
    b->start_time = start_time;
    b->end_time = end_time;
    return common::E_OK;
}

void free_device_timeseries_metadata_entries_partial(
    DeviceTimeseriesMetadataEntry* entries, size_t filled_count) {
    if (entries == nullptr) {
        return;
    }
    for (size_t i = 0; i < filled_count; i++) {
        tsfile_device_id_free_contents(&entries[i].device);
        if (entries[i].timeseries != nullptr) {
            for (uint32_t j = 0; j < entries[i].timeseries_count; j++) {
                free_timeseries_statistic_heap(
                    &entries[i].timeseries[j].statistic);
                free_timeseries_statistic_heap(
                    &entries[i].timeseries[j].timeline_statistic);
                free(entries[i].timeseries[j].measurement_name);
            }
            free(entries[i].timeseries);
            entries[i].timeseries = nullptr;
        }
    }
    free(entries);
}

/**
 * Copies path, table name, and segment strings from IDeviceID into heap
 * buffers. On failure, frees any partial allocations and returns E_OOM.
 */
int duplicate_ideviceid_to_device_fields(storage::IDeviceID* id,
                                         char** out_path, char** out_table_name,
                                         uint32_t* out_segment_count,
                                         char*** out_segments) {
    *out_path = nullptr;
    *out_table_name = nullptr;
    *out_segment_count = 0;
    *out_segments = nullptr;
    if (id == nullptr) {
        *out_path = strdup("");
        *out_table_name = strdup("");
        if (*out_path == nullptr || *out_table_name == nullptr) {
            free(*out_path);
            free(*out_table_name);
            *out_path = nullptr;
            *out_table_name = nullptr;
            return common::E_OOM;
        }
        return common::E_OK;
    }
    const std::string dname = id->get_device_name();
    *out_path = strdup(dname.c_str());
    if (*out_path == nullptr) {
        return common::E_OOM;
    }
    const std::string tname = id->get_table_name();
    *out_table_name = strdup(tname.c_str());
    if (*out_table_name == nullptr) {
        free(*out_path);
        *out_path = nullptr;
        return common::E_OOM;
    }
    const int n = id->segment_num();
    if (n <= 0) {
        return common::E_OK;
    }
    auto* seg_arr =
        static_cast<char**>(malloc(sizeof(char*) * static_cast<size_t>(n)));
    if (seg_arr == nullptr) {
        free(*out_table_name);
        *out_table_name = nullptr;
        free(*out_path);
        *out_path = nullptr;
        return common::E_OOM;
    }
    memset(seg_arr, 0, sizeof(char*) * static_cast<size_t>(n));
    const auto& segs = id->get_segments();
    for (int i = 0; i < n; i++) {
        const std::string* ps =
            (static_cast<size_t>(i) < segs.size()) ? segs[i] : nullptr;
        const char* lit = (ps != nullptr) ? ps->c_str() : "null";
        seg_arr[i] = strdup(lit);
        if (seg_arr[i] == nullptr) {
            for (int j = 0; j < i; j++) {
                free(seg_arr[j]);
            }
            free(seg_arr);
            free(*out_table_name);
            *out_table_name = nullptr;
            free(*out_path);
            *out_path = nullptr;
            return common::E_OOM;
        }
    }
    *out_segment_count = static_cast<uint32_t>(n);
    *out_segments = seg_arr;
    return common::E_OK;
}

int fill_device_id_from_ideviceid(storage::IDeviceID* id, DeviceID* out) {
    memset(out, 0, sizeof(*out));
    return duplicate_ideviceid_to_device_fields(
        id, &out->path, &out->table_name, &out->segment_count, &out->segments);
}

void clear_metadata_entry_device_only(DeviceTimeseriesMetadataEntry* e) {
    if (e == nullptr) {
        return;
    }
    tsfile_device_id_free_contents(&e->device);
}

ERRNO populate_c_metadata_map_from_cpp(
    storage::DeviceTimeseriesMetadataMap& cpp_map,
    DeviceTimeseriesMetadataMap* out_map) {
    if (cpp_map.empty()) {
        return common::E_OK;
    }
    const uint32_t dev_n = static_cast<uint32_t>(cpp_map.size());
    auto* entries = static_cast<DeviceTimeseriesMetadataEntry*>(
        malloc(sizeof(DeviceTimeseriesMetadataEntry) * dev_n));
    if (entries == nullptr) {
        return common::E_OOM;
    }
    memset(entries, 0, sizeof(DeviceTimeseriesMetadataEntry) * dev_n);
    size_t di = 0;
    for (const auto& kv : cpp_map) {
        DeviceTimeseriesMetadataEntry& e = entries[di];
        const int dup_rc = fill_device_id_from_ideviceid(
            kv.first ? kv.first.get() : nullptr, &e.device);
        if (dup_rc != common::E_OK) {
            free_device_timeseries_metadata_entries_partial(entries, di);
            return dup_rc;
        }
        const auto& vec = kv.second;
        uint32_t n_ts = 0;
        for (const auto& idx_nz : vec) {
            if (idx_nz != nullptr) {
                n_ts++;
            }
        }
        e.timeseries_count = n_ts;
        if (e.timeseries_count == 0) {
            e.timeseries = nullptr;
            di++;
            continue;
        }
        e.timeseries = static_cast<TimeseriesMetadata*>(
            malloc(sizeof(TimeseriesMetadata) * e.timeseries_count));
        if (e.timeseries == nullptr) {
            clear_metadata_entry_device_only(&e);
            free_device_timeseries_metadata_entries_partial(entries, di);
            return common::E_OOM;
        }
        memset(e.timeseries, 0,
               sizeof(TimeseriesMetadata) * e.timeseries_count);
        uint32_t slot = 0;
        for (const auto& idx : vec) {
            if (idx == nullptr) {
                continue;
            }
            TimeseriesMetadata& m = e.timeseries[slot];
            common::String mn = idx->get_measurement_name();
            m.measurement_name = strdup(mn.to_std_string().c_str());
            if (m.measurement_name == nullptr) {
                for (uint32_t u = 0; u < slot; u++) {
                    free_timeseries_statistic_heap(&e.timeseries[u].statistic);
                    free(e.timeseries[u].measurement_name);
                }
                free(e.timeseries);
                e.timeseries = nullptr;
                clear_metadata_entry_device_only(&e);
                free_device_timeseries_metadata_entries_partial(entries, di);
                return common::E_OOM;
            }
            auto* aligned_idx =
                dynamic_cast<storage::AlignedTimeseriesIndex*>(idx.get());
            if (aligned_idx != nullptr &&
                aligned_idx->value_ts_idx_ != nullptr) {
                m.data_type = static_cast<TSDataType>(
                    aligned_idx->value_ts_idx_->get_data_type());
            } else {
                m.data_type = static_cast<TSDataType>(idx->get_data_type());
            }
            storage::Statistic* st = idx->get_statistic();
            int32_t chunk_cnt = 0;
            auto* cl = aligned_idx != nullptr ? idx->get_value_chunk_meta_list()
                                              : idx->get_chunk_meta_list();
            if (cl != nullptr) {
                chunk_cnt = static_cast<int32_t>(cl->size());
            }
            m.chunk_meta_count = chunk_cnt;
            const int st_rc = fill_timeseries_statistic(st, &m.statistic);
            if (st_rc != common::E_OK) {
                for (uint32_t u = 0; u < slot; u++) {
                    free_timeseries_statistic_heap(&e.timeseries[u].statistic);
                    free_timeseries_statistic_heap(
                        &e.timeseries[u].timeline_statistic);
                    free(e.timeseries[u].measurement_name);
                }
                free_timeseries_statistic_heap(&m.statistic);
                free_timeseries_statistic_heap(&m.timeline_statistic);
                free(m.measurement_name);
                free(e.timeseries);
                e.timeseries = nullptr;
                clear_metadata_entry_device_only(&e);
                free_device_timeseries_metadata_entries_partial(entries, di);
                return st_rc;
            }
            const int timeline_st_rc =
                fill_timeline_statistic(idx.get(), &m.timeline_statistic);
            if (timeline_st_rc != common::E_OK) {
                for (uint32_t u = 0; u < slot; u++) {
                    free_timeseries_statistic_heap(&e.timeseries[u].statistic);
                    free_timeseries_statistic_heap(
                        &e.timeseries[u].timeline_statistic);
                    free(e.timeseries[u].measurement_name);
                }
                free_timeseries_statistic_heap(&m.statistic);
                free_timeseries_statistic_heap(&m.timeline_statistic);
                free(m.measurement_name);
                free(e.timeseries);
                e.timeseries = nullptr;
                clear_metadata_entry_device_only(&e);
                free_device_timeseries_metadata_entries_partial(entries, di);
                return timeline_st_rc;
            }
            slot++;
        }
        di++;
    }
    out_map->entries = entries;
    out_map->device_count = dev_n;
    return common::E_OK;
}

}  // namespace

void tsfile_free_device_id_array(DeviceID* devices, uint32_t length) {
    if (devices == nullptr) {
        return;
    }
    for (uint32_t i = 0; i < length; i++) {
        tsfile_device_id_free_contents(&devices[i]);
    }
    free(devices);
}

ERRNO tsfile_reader_get_all_devices(TsFileReader reader, DeviceID** out_devices,
                                    uint32_t* out_length) {
    if (reader == nullptr || out_devices == nullptr || out_length == nullptr) {
        return common::E_INVALID_ARG;
    }
    *out_devices = nullptr;
    *out_length = 0;
    auto* r = static_cast<storage::TsFileReader*>(reader);
    const auto ids = r->get_all_devices();
    if (ids.empty()) {
        return common::E_OK;
    }
    auto* arr = static_cast<DeviceID*>(malloc(sizeof(DeviceID) * ids.size()));
    if (arr == nullptr) {
        return common::E_OOM;
    }
    memset(arr, 0, sizeof(DeviceID) * ids.size());
    for (size_t i = 0; i < ids.size(); i++) {
        const int rc = fill_device_id_from_ideviceid(ids[i].get(), &arr[i]);
        if (rc != common::E_OK) {
            tsfile_free_device_id_array(arr, static_cast<uint32_t>(i));
            return rc;
        }
    }
    *out_devices = arr;
    *out_length = static_cast<uint32_t>(ids.size());
    return common::E_OK;
}

ERRNO tsfile_reader_get_timeseries_metadata_all(
    TsFileReader reader, DeviceTimeseriesMetadataMap* out_map) {
    if (reader == nullptr || out_map == nullptr) {
        return common::E_INVALID_ARG;
    }
    out_map->entries = nullptr;
    out_map->device_count = 0;
    auto* r = static_cast<storage::TsFileReader*>(reader);
    storage::DeviceTimeseriesMetadataMap cpp_map = r->get_timeseries_metadata();
    return populate_c_metadata_map_from_cpp(cpp_map, out_map);
}

ERRNO tsfile_reader_get_timeseries_metadata_for_devices(
    TsFileReader reader, const DeviceID* devices, uint32_t length,
    DeviceTimeseriesMetadataMap* out_map) {
    if (reader == nullptr || out_map == nullptr) {
        return common::E_INVALID_ARG;
    }
    out_map->entries = nullptr;
    out_map->device_count = 0;
    if (length == 0) {
        return common::E_OK;
    }
    if (devices == nullptr) {
        return common::E_INVALID_ARG;
    }
    for (uint32_t i = 0; i < length; i++) {
        if (devices[i].path == nullptr) {
            return common::E_INVALID_ARG;
        }
    }
    auto* r = static_cast<storage::TsFileReader*>(reader);
    std::vector<std::shared_ptr<storage::IDeviceID>> query_ids;
    query_ids.reserve(length);
    for (uint32_t i = 0; i < length; i++) {
        query_ids.push_back(std::make_shared<storage::StringArrayDeviceID>(
            std::string(devices[i].path)));
    }
    storage::DeviceTimeseriesMetadataMap cpp_map =
        r->get_timeseries_metadata(query_ids);
    return populate_c_metadata_map_from_cpp(cpp_map, out_map);
}

void tsfile_free_device_timeseries_metadata_map(
    DeviceTimeseriesMetadataMap* map) {
    if (map == nullptr) {
        return;
    }
    free_device_timeseries_metadata_entries_partial(map->entries,
                                                    map->device_count);
    map->entries = nullptr;
    map->device_count = 0;
}

// delete pointer
void _free_tsfile_ts_record(TsRecord* record) {
    if (*record != nullptr) {
        delete static_cast<storage::TsRecord*>(*record);
    }
    *record = nullptr;
}

void free_tablet(Tablet* tablet) {
    if (*tablet != nullptr) {
        delete static_cast<storage::Tablet*>(*tablet);
    }
    *tablet = nullptr;
}

void free_tsfile_result_set(ResultSet* result_set) {
    if (*result_set != nullptr) {
        delete static_cast<storage::ResultSet*>(*result_set);
    }
    *result_set = nullptr;
}

void free_result_set_meta_data(ResultSetMetaData result_set_meta_data) {
    for (int i = 0; i < result_set_meta_data.column_num; i++) {
        free(result_set_meta_data.column_names[i]);
    }
    free(result_set_meta_data.column_names);
    free(result_set_meta_data.data_types);
}

void free_device_schema(DeviceSchema schema) {
    free(schema.device_name);
    for (int i = 0; i < schema.timeseries_num; i++) {
        free_timeseries_schema(schema.timeseries_schema[i]);
    }
    free(schema.timeseries_schema);
}
void free_timeseries_schema(TimeseriesSchema schema) {
    free(schema.timeseries_name);
}
void free_table_schema(TableSchema schema) {
    free(schema.table_name);
    for (int i = 0; i < schema.column_num; i++) {
        free_column_schema(schema.column_schemas[i]);
    }
    if (schema.column_num > 0) {
        free(schema.column_schemas);
    }
}
void free_column_schema(ColumnSchema schema) { free(schema.column_name); }

void free_write_file(WriteFile* write_file) {
    auto f = static_cast<storage::WriteFile*>(*write_file);
    delete f;
    *write_file = nullptr;
}

// For Python API
TsFileWriter _tsfile_writer_new(const char* pathname, uint64_t memory_threshold,
                                ERRNO* err_code) {
    init_tsfile_config();
    auto writer = new storage::TsFileWriter();
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    int ret = writer->open(pathname, flags, 0644);
    common::g_config_value_.chunk_group_size_threshold_ = memory_threshold;
    if (ret != common::E_OK) {
        delete writer;
        *err_code = ret;
        return nullptr;
    }
    return writer;
}

Tablet _tablet_new_with_target_name(const char* device_id,
                                    char** column_name_list,
                                    TSDataType* data_types, int column_num,
                                    int max_rows) {
    std::vector<std::string> measurement_list;
    std::vector<common::TSDataType> data_type_list;
    for (int i = 0; i < column_num; i++) {
        measurement_list.emplace_back(column_name_list[i]);
        data_type_list.push_back(
            static_cast<common::TSDataType>(*(data_types + i)));
    }
    if (device_id != nullptr) {
        return new storage::Tablet(device_id, &measurement_list,
                                   &data_type_list, max_rows);
    } else {
        return new storage::Tablet(measurement_list, data_type_list, max_rows);
    }
}

ERRNO _tsfile_writer_register_table(TsFileWriter writer, TableSchema* schema) {
    std::vector<storage::MeasurementSchema*> measurement_schemas;
    std::vector<common::ColumnCategory> column_categories;
    measurement_schemas.resize(schema->column_num);
    for (int i = 0; i < schema->column_num; i++) {
        ColumnSchema* cur_schema = schema->column_schemas + i;
        measurement_schemas[i] = new storage::MeasurementSchema(
            cur_schema->column_name,
            static_cast<common::TSDataType>(cur_schema->data_type));
        column_categories.push_back(
            static_cast<common::ColumnCategory>(cur_schema->column_category));
    }
    auto tsfile_writer = static_cast<storage::TsFileWriter*>(writer);
    return tsfile_writer->register_table(std::make_shared<storage::TableSchema>(
        schema->table_name, measurement_schemas, column_categories));
}

ERRNO _tsfile_writer_register_timeseries(TsFileWriter writer,
                                         const char* device_id,
                                         const TimeseriesSchema* schema) {
    auto* w = static_cast<storage::TsFileWriter*>(writer);

    int ret = w->register_timeseries(
        device_id,
        storage::MeasurementSchema(
            schema->timeseries_name,
            static_cast<common::TSDataType>(schema->data_type),
            static_cast<common::TSEncoding>(schema->encoding),
            static_cast<common::CompressionType>(schema->compression)));
    return ret;
}

ERRNO _tsfile_writer_register_device(TsFileWriter writer,
                                     const device_schema* device_schema) {
    auto* w = static_cast<storage::TsFileWriter*>(writer);
    for (int column_id = 0; column_id < device_schema->timeseries_num;
         column_id++) {
        TimeseriesSchema schema = device_schema->timeseries_schema[column_id];
        const ERRNO ret = w->register_timeseries(
            device_schema->device_name,
            storage::MeasurementSchema(
                schema.timeseries_name,
                static_cast<common::TSDataType>(schema.data_type),
                static_cast<common::TSEncoding>(schema.encoding),
                static_cast<common::CompressionType>(schema.compression)));
        if (ret != common::E_OK) {
            return ret;
        }
    }
    return common::E_OK;
}

ERRNO _tsfile_writer_write_tablet(TsFileWriter writer, Tablet tablet) {
    auto* w = static_cast<storage::TsFileWriter*>(writer);
    const auto* tbl = static_cast<storage::Tablet*>(tablet);
    return w->write_tablet(*tbl);
}

ERRNO _tsfile_writer_write_table(TsFileWriter writer, Tablet tablet) {
    auto* w = static_cast<storage::TsFileWriter*>(writer);
    auto* tbl = static_cast<storage::Tablet*>(tablet);
    return w->write_table(*tbl);
}

ERRNO _tsfile_writer_write_arrow_table(TsFileWriter writer,
                                       const char* table_name,
                                       ArrowArray* array, ArrowSchema* schema,
                                       int time_col_index) {
    auto* w = static_cast<storage::TsFileWriter*>(writer);
    std::shared_ptr<storage::TableSchema> reg_schema =
        w->get_table_schema(table_name ? std::string(table_name) : "");
    storage::Tablet* tablet = nullptr;
    int ret = arrow::ArrowStructToTablet(
        table_name, array, schema, reg_schema.get(), &tablet, time_col_index);
    if (ret != common::E_OK) return ret;
    ret = w->write_table(*tablet);
    delete tablet;
    return ret;
}

ERRNO _tsfile_writer_write_ts_record(TsFileWriter writer, TsRecord data) {
    auto* w = static_cast<storage::TsFileWriter*>(writer);
    const storage::TsRecord* record = static_cast<storage::TsRecord*>(data);
    const int ret = w->write_record(*record);
    return ret;
}

ERRNO _tsfile_writer_close(TsFileWriter writer) {
    auto* w = static_cast<storage::TsFileWriter*>(writer);
    int ret = w->flush();
    if (ret != common::E_OK) {
        return ret;
    }
    ret = w->close();
    if (ret != common::E_OK) {
        return ret;
    }
    delete w;
    return ret;
}

ERRNO _tsfile_writer_flush(TsFileWriter writer) {
    auto* w = static_cast<storage::TsFileWriter*>(writer);
    return w->flush();
}

ResultSet _tsfile_reader_query_device(TsFileReader reader,
                                      const char* device_name,
                                      char** sensor_name, uint32_t sensor_num,
                                      Timestamp start_time, Timestamp end_time,
                                      ERRNO* err_code) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    std::vector<std::string> selected_paths;
    selected_paths.reserve(sensor_num);
    for (uint32_t i = 0; i < sensor_num; i++) {
        selected_paths.push_back(std::string(device_name) + "." +
                                 std::string(sensor_name[i]));
    }
    storage::ResultSet* qds = nullptr;
    *err_code = r->query(selected_paths, start_time, end_time, qds);
    return qds;
}

// ---------- Tag Filter API ----------

TagFilterHandle tsfile_tag_filter_create(TsFileReader reader,
                                         const char* table_name,
                                         const char* column_name,
                                         const char* value, TagFilterOp op,
                                         ERRNO* err_code) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    auto schema = r->get_table_schema(table_name);
    if (!schema) {
        *err_code = common::E_INVALID_ARG;
        return nullptr;
    }
    storage::TagFilterBuilder builder(schema.get());
    storage::Filter* filter = nullptr;
    switch (op) {
        case TAG_FILTER_EQ:
            filter = builder.eq(column_name, value);
            break;
        case TAG_FILTER_NEQ:
            filter = builder.neq(column_name, value);
            break;
        case TAG_FILTER_LT:
            filter = builder.lt(column_name, value);
            break;
        case TAG_FILTER_LTEQ:
            filter = builder.lteq(column_name, value);
            break;
        case TAG_FILTER_GT:
            filter = builder.gt(column_name, value);
            break;
        case TAG_FILTER_GTEQ:
            filter = builder.gteq(column_name, value);
            break;
        case TAG_FILTER_REGEXP:
            filter = builder.reg_exp(column_name, value);
            break;
        case TAG_FILTER_NOT_REGEXP:
            filter = builder.not_reg_exp(column_name, value);
            break;
        default:
            *err_code = common::E_INVALID_ARG;
            return nullptr;
    }
    *err_code = common::E_OK;
    return static_cast<void*>(filter);
}

TagFilterHandle tsfile_tag_filter_between(TsFileReader reader,
                                          const char* table_name,
                                          const char* column_name,
                                          const char* lower, const char* upper,
                                          bool is_not, ERRNO* err_code) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    auto schema = r->get_table_schema(table_name);
    if (!schema) {
        *err_code = common::E_INVALID_ARG;
        return nullptr;
    }
    storage::TagFilterBuilder builder(schema.get());
    storage::Filter* filter =
        is_not ? builder.not_between_and(column_name, lower, upper)
               : builder.between_and(column_name, lower, upper);
    *err_code = common::E_OK;
    return static_cast<void*>(filter);
}

TagFilterHandle tsfile_tag_filter_and(TagFilterHandle left,
                                      TagFilterHandle right) {
    return static_cast<void*>(storage::TagFilterBuilder::and_filter(
        static_cast<storage::Filter*>(left),
        static_cast<storage::Filter*>(right)));
}

TagFilterHandle tsfile_tag_filter_or(TagFilterHandle left,
                                     TagFilterHandle right) {
    return static_cast<void*>(storage::TagFilterBuilder::or_filter(
        static_cast<storage::Filter*>(left),
        static_cast<storage::Filter*>(right)));
}

TagFilterHandle tsfile_tag_filter_not(TagFilterHandle filter) {
    return static_cast<void*>(storage::TagFilterBuilder::not_filter(
        static_cast<storage::Filter*>(filter)));
}

void tsfile_tag_filter_free(TagFilterHandle filter) {
    delete static_cast<storage::Filter*>(filter);
}

ResultSet tsfile_query_table_with_tag_filter(
    TsFileReader reader, const char* table_name, char** columns,
    uint32_t column_num, Timestamp start_time, Timestamp end_time,
    TagFilterHandle tag_filter, int batch_size, ERRNO* err_code) {
    auto* r = static_cast<storage::TsFileReader*>(reader);
    storage::ResultSet* table_result_set = nullptr;
    std::vector<std::string> column_names;
    for (uint32_t i = 0; i < column_num; i++) {
        column_names.emplace_back(columns[i]);
    }
    *err_code = r->query(table_name, column_names, start_time, end_time,
                         table_result_set,
                         static_cast<storage::Filter*>(tag_filter), batch_size);
    return table_result_set;
}

#ifdef __cplusplus
}
#endif
