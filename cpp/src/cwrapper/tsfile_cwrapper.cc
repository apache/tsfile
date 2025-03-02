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

#include "common/tablet.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

#ifdef __cplusplus
extern "C" {
#endif

static bool is_init = false;

void init_tsfile_config() {
    if (!is_init) {
        common::init_config_value();
        is_init = true;
    }
}

WriteFile write_file_new(const char *pathname, ERRNO *err_code) {
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
    storage::WriteFile *file = new storage::WriteFile;
    ret = file->create(pathname, flags, mode);
    *err_code = ret;
    return file;
}

TsFileWriter tsfile_writer_new(WriteFile file, TableSchema *schema,
                               ERRNO *err_code) {
    init_tsfile_config();
    std::vector<common::ColumnSchema> column_schemas;
    for (int i = 0; i < schema->column_num; i++) {
        ColumnSchema cur_schema = schema->column_schemas[i];
        column_schemas.emplace_back(common::ColumnSchema(
            cur_schema.column_name,
            static_cast<common::TSDataType>(cur_schema.data_type),
            static_cast<common::CompressionType>(cur_schema.compression),
            static_cast<common::TSEncoding>(cur_schema.encoding),
            static_cast<common::ColumnCategory>(cur_schema.column_category)));
    }

    // There is no need to free table_schema.
    storage::TableSchema *table_schema =
        new storage::TableSchema(schema->table_name, column_schemas);
    *err_code = common::E_OK;
    auto table_writer = new storage::TsFileTableWriter(
        static_cast<storage::WriteFile *>(file), table_schema);
    delete table_schema;
    return table_writer;
}

TsFileReader tsfile_reader_new(const char *pathname, ERRNO *err_code) {
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
    auto *w = static_cast<storage::TsFileTableWriter *>(writer);
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
    auto *ts_reader = static_cast<storage::TsFileReader *>(reader);
    delete ts_reader;
    return common::E_OK;
}

Tablet tablet_new(char **column_name_list, TSDataType *data_types,
                  uint32_t column_num, uint32_t max_rows) {
    std::vector<std::string> measurement_list;
    std::vector<common::TSDataType> data_type_list;
    for (uint32_t i = 0; i < column_num; i++) {
        measurement_list.emplace_back(column_name_list[i]);
        data_type_list.push_back(
            static_cast<common::TSDataType>(*(data_types + i)));
    }
    return new storage::Tablet(measurement_list, data_type_list, max_rows);
}

uint32_t tablet_get_cur_row_size(Tablet tablet) {
    return static_cast<storage::Tablet *>(tablet)->get_cur_row_size();
}

ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           Timestamp timestamp) {
    return static_cast<storage::Tablet *>(tablet)->add_timestamp(row_index,
                                                                 timestamp);
}

#define TABLET_ADD_VALUE_BY_NAME_DEF(type)                                   \
    ERRNO tablet_add_value_by_name_##type(Tablet tablet, uint32_t row_index, \
                                          const char *column_name,           \
                                          const type value) {                \
        return static_cast<storage::Tablet *>(tablet)->add_value(            \
            row_index, column_name, value);                                  \
    }
TABLET_ADD_VALUE_BY_NAME_DEF(int32_t);
TABLET_ADD_VALUE_BY_NAME_DEF(int64_t);
TABLET_ADD_VALUE_BY_NAME_DEF(float);
TABLET_ADD_VALUE_BY_NAME_DEF(double);
TABLET_ADD_VALUE_BY_NAME_DEF(bool);

ERRNO tablet_add_value_by_name_string(Tablet tablet, uint32_t row_index,
                                      const char *column_name,
                                      const char *value) {
    return static_cast<storage::Tablet *>(tablet)->add_value(
        row_index, column_name, common::String(value));
}

#define TABLE_ADD_VALUE_BY_INDEX_DEF(type)                                    \
    ERRNO tablet_add_value_by_index_##type(Tablet tablet, uint32_t row_index, \
                                           uint32_t column_index,             \
                                           const type value) {                \
        return static_cast<storage::Tablet *>(tablet)->add_value(             \
            row_index, column_index, value);                                  \
    }

ERRNO tablet_add_value_by_index_string(Tablet tablet, uint32_t row_index,
                                       uint32_t column_index,
                                       const char *value) {
    return static_cast<storage::Tablet *>(tablet)->add_value(
        row_index, column_index, common::String(value));
}

TABLE_ADD_VALUE_BY_INDEX_DEF(int32_t);
TABLE_ADD_VALUE_BY_INDEX_DEF(int64_t);
TABLE_ADD_VALUE_BY_INDEX_DEF(float);
TABLE_ADD_VALUE_BY_INDEX_DEF(double);
TABLE_ADD_VALUE_BY_INDEX_DEF(bool);

// TsRecord API
TsRecord _ts_record_new(const char *device_id, Timestamp timestamp,
                        int timeseries_num) {
    auto *record = new storage::TsRecord(timestamp, device_id, timeseries_num);
    return record;
}

#define INSERT_DATA_INTO_TS_RECORD_BY_NAME_DEF(type)                 \
    ERRNO _insert_data_into_ts_record_by_name_##type(                \
        TsRecord data, const char *measurement_name, type value) {   \
        auto *record = (storage::TsRecord *)data;                    \
        storage::DataPoint point(measurement_name, value);           \
        if (record->points_.size() + 1 > record->points_.capacity()) \
            return common::E_BUF_NOT_ENOUGH;                         \
        record->points_.push_back(point);                            \
        return common::E_OK;                                         \
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
    auto *w = static_cast<storage::TsFileTableWriter *>(writer);
    auto *tbl = static_cast<storage::Tablet *>(tablet);
    return w->write_table(*tbl);
}

// ERRNO tsfile_writer_flush_data(TsFileWriter writer) {
//     auto *w = static_cast<storage::TsFileWriter *>(writer);
//     return w->flush();
// }

// Query

ResultSet tsfile_query_table(TsFileReader reader, const char *table_name,
                             char **columns, uint32_t column_num,
                             Timestamp start_time, Timestamp end_time,
                             ERRNO *err_code) {
    auto *r = static_cast<storage::TsFileReader *>(reader);
    storage::ResultSet *table_result_set = nullptr;
    std::vector<std::string> column_names;
    for (uint32_t i = 0; i < column_num; i++) {
        column_names.emplace_back(columns[i]);
    }
    *err_code = r->query(table_name, column_names, start_time, end_time,
                         table_result_set);
    return table_result_set;
}

bool tsfile_result_set_next(ResultSet result_set, ERRNO *err_code) {
    auto *r = static_cast<storage::TableResultSet *>(result_set);
    bool has_next = true;
    int ret = common::E_OK;
    ret = r->next(has_next);
    *err_code = ret;
    if (ret != common::E_OK) {
        return false;
    }
    return has_next;
}

#define TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(type)                          \
    type tsfile_result_set_get_value_by_name_##type(ResultSet result_set,      \
                                                    const char *column_name) { \
        auto *r = static_cast<storage::TableResultSet *>(result_set);          \
        return r->get_value<type>(column_name);                                \
    }

TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(bool);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(int32_t);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(int64_t);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(float);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME_DEF(double);
char *tsfile_result_set_get_value_by_name_string(ResultSet result_set,
                                                 const char *column_name) {
    auto *r = static_cast<storage::TableResultSet *>(result_set);
    common::String *ret = r->get_value<common::String *>(column_name);
    // Caller should free return's char* 's space.
    char *dup = (char *)malloc(ret->len_ + 1);
    if (dup) {
        memcpy(dup, ret->buf_, ret->len_);
        dup[ret->len_] = '\0';
    }
    return dup;
}

#define TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(type)                        \
    type tsfile_result_set_get_value_by_index_##type(ResultSet result_set,    \
                                                     uint32_t column_index) { \
        auto *r = static_cast<storage::TableResultSet *>(result_set);         \
        return r->get_value<type>(column_index);                              \
    }

TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(int32_t);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(int64_t);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(float);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(double);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX_DEF(bool);

char *tsfile_result_set_get_value_by_index_string(ResultSet result_set,
                                                  uint32_t column_index) {
    auto *r = static_cast<storage::TableResultSet *>(result_set);
    common::String *ret = r->get_value<common::String *>(column_index);
    // Caller should free return's char* 's space.
    char *dup = (char *)malloc(ret->len_ + 1);
    if (dup) {
        memcpy(dup, ret->buf_, ret->len_);
        dup[ret->len_] = '\0';
    }
    return dup;
}

bool tsfile_result_set_is_null_by_name(ResultSet result_set,
                                       const char *column_name) {
    auto *r = static_cast<storage::TableResultSet *>(result_set);
    return r->is_null(column_name);
}

bool tsfile_result_set_is_null_by_index(const ResultSet result_set,
                                        const uint32_t column_index) {
    auto *r = static_cast<storage::TableResultSet *>(result_set);
    return r->is_null(column_index);
}

ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set) {
    auto *r = static_cast<storage::TableResultSet *>(result_set);
    if (result_set == NULL) {
        return ResultSetMetaData();
    }

    ResultSetMetaData meta_data;
    std::shared_ptr<storage::ResultSetMetadata> result_set_metadata =
        r->get_metadata();
    meta_data.column_num = result_set_metadata->get_column_count();
    meta_data.column_names =
        static_cast<char **>(malloc(meta_data.column_num * sizeof(char *)));
    meta_data.data_types = static_cast<TSDataType *>(
        malloc(meta_data.column_num * sizeof(TSDataType)));
    for (int i = 0; i < meta_data.column_num; i++) {
        meta_data.column_names[i] =
            strdup(result_set_metadata->get_column_name(i + 1).c_str());
        meta_data.data_types[i] =
            static_cast<TSDataType>(result_set_metadata->get_column_type(i + 1));
    }
    return meta_data;
}

char *tsfile_result_set_metadata_get_column_name(ResultSetMetaData result_set,
                                                 uint32_t column_index) {
    if (column_index > result_set.column_num) {
        return nullptr;
    }
    return result_set.column_names[column_index - 1];
}

TSDataType tsfile_result_set_metadata_get_data_type(
    ResultSetMetaData result_set, uint32_t column_index) {
    if (column_index > result_set.column_num) {
        return TS_DATATYPE_INVALID;
    }
    return result_set.data_types[column_index - 1];
}

int tsfile_result_set_metadata_get_column_num(ResultSetMetaData result_set) {
    return result_set.column_num;
}

// TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
//                                            const char *table_name) {
//     // TODO: Implement get table schema with tsfile reader.
//     return TableSchema();
// }
//
// DeviceSchema tsfile_reader_get_device_schema(TsFileReader reader,
//                                              const char *device_id) {
//     auto *r = static_cast<storage::TsFileReader *>(reader);
//     std::vector<storage::MeasurementSchema> measurement_schemas;
//     r->get_timeseries_schema(
//         std::make_shared<storage::StringArrayDeviceID>(device_id),
//         measurement_schemas);
//     DeviceSchema schema;
//     schema.device_name = strdup(device_id);
//     schema.timeseries_num = measurement_schemas.size();
//     schema.timeseries_schema = static_cast<TimeseriesSchema *>(
//         malloc(sizeof(TimeseriesSchema) * schema.timeseries_num));
//     for (int i = 0; i < schema.timeseries_num; i++) {
//         schema.timeseries_schema[i].timeseries_name =
//             strdup(measurement_schemas[i].measurement_name_.c_str());
//         schema.timeseries_schema[i].data_type =
//             static_cast<TSDataType>(measurement_schemas[i].data_type_);
//         schema.timeseries_schema[i].compression =
//         static_cast<CompressionType>(
//             measurement_schemas[i].compression_type_);
//         schema.timeseries_schema[i].encoding =
//             static_cast<TSEncoding>(measurement_schemas[i].encoding_);
//     }
//     return schema;
// }

TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                           const char *table_name) {
    auto *r = static_cast<storage::TsFileReader *>(reader);
    auto table_shcema = r->get_table_schema(table_name);
    TableSchema ret_schema;
    ret_schema.table_name = strdup(table_shcema->get_table_name().c_str());
    int column_num = table_shcema->get_columns_num();
    ret_schema.column_num = column_num;
    ret_schema.column_schemas =
        static_cast<ColumnSchema *>(malloc(sizeof(ColumnSchema) * column_num));
    for (int i = 0; i < column_num; i++) {
        auto column_schema = table_shcema->get_measurement_schemas()[i];
        ret_schema.column_schemas[i].column_name =
            strdup(column_schema->measurement_name_.c_str());
        ret_schema.column_schemas[i].data_type =
            static_cast<TSDataType>(column_schema->data_type_);
        ret_schema.column_schemas[i].compression =
            static_cast<CompressionType>(column_schema->compression_type_);
        ret_schema.column_schemas[i].encoding =
            static_cast<TSEncoding>(column_schema->encoding_);
        ret_schema.column_schemas[i].column_category =
            static_cast<ColumnCategory>(
                table_shcema->get_column_categories()[i]);
    }
    return ret_schema;
}

TableSchema *tsfile_reader_get_all_table_schemas(TsFileReader reader,
                                                 uint32_t *size) {
    auto *r = static_cast<storage::TsFileReader *>(reader);
    auto table_schemas = r->get_all_table_schemas();
    size_t table_num = table_schemas.size();
    TableSchema *ret =
        static_cast<TableSchema *>(malloc(sizeof(TableSchema) * table_num));
    for (size_t i = 0; i < table_schemas.size(); i++) {
        ret[i].table_name = strdup(table_schemas[i]->get_table_name().c_str());
        int column_num = table_schemas[i]->get_columns_num();
        ret[i].column_num = column_num;
        ret[i].column_schemas = static_cast<ColumnSchema *>(
            malloc(column_num * sizeof(ColumnSchema)));
        auto column_schemas = table_schemas[i]->get_measurement_schemas();
        for (int j = 0; j < column_num; j++) {
            ret[i].column_schemas[j].column_name =
                strdup(column_schemas[j]->measurement_name_.c_str());
            ret[i].column_schemas[j].data_type =
                static_cast<TSDataType>(column_schemas[j]->data_type_);
            ret[i].column_schemas[j].encoding =
                static_cast<TSEncoding>(column_schemas[j]->encoding_);
            ret[i].column_schemas[j].compression = static_cast<CompressionType>(
                column_schemas[j]->compression_type_);
            ret[i].column_schemas[j].column_category =
                static_cast<ColumnCategory>(
                    table_schemas[i]->get_column_categories()[j]);
        }
    }
    *size = table_num;
    return ret;
}

// delete pointer
void _free_tsfile_ts_record(TsRecord *record) {
    if (*record != nullptr) {
        delete static_cast<storage::TsRecord *>(*record);
    }
    *record = nullptr;
}

void free_tablet(Tablet *tablet) {
    if (*tablet != nullptr) {
        delete static_cast<storage::Tablet *>(*tablet);
    }
    *tablet = nullptr;
}

void free_tsfile_result_set(ResultSet *result_set) {
    if (*result_set != nullptr) {
        delete static_cast<storage::ResultSet *>(*result_set);
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
    free(schema.column_schemas);
}
void free_column_schema(ColumnSchema schema) { free(schema.column_name); }

void free_write_file(WriteFile *write_file) {
    auto f = static_cast<storage::WriteFile *>(*write_file);
    delete f;
    *write_file = nullptr;
}

// For Python API
TsFileWriter _tsfile_writer_new(const char *pathname, ERRNO *err_code) {
    init_tsfile_config();
    auto writer = new storage::TsFileWriter();
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    int ret = writer->open(pathname, flags, 0644);
    if (ret != common::E_OK) {
        delete writer;
        *err_code = ret;
        return nullptr;
    }
    return writer;
}

Tablet _tablet_new_with_target_name(const char *device_id,
                                    char **column_name_list,
                                    TSDataType *data_types, int column_num,
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

ERRNO _tsfile_writer_register_table(TsFileWriter writer, TableSchema *schema) {
    std::vector<storage::MeasurementSchema *> measurement_schemas;
    std::vector<common::ColumnCategory> column_categories;
    measurement_schemas.resize(schema->column_num);
    for (int i = 0; i < schema->column_num; i++) {
        ColumnSchema *cur_schema = schema->column_schemas + i;
        measurement_schemas[i] = new storage::MeasurementSchema(
            cur_schema->column_name,
            static_cast<common::TSDataType>(cur_schema->data_type));
        column_categories.push_back(
            static_cast<common::ColumnCategory>(cur_schema->column_category));
    }
    auto tsfile_writer = static_cast<storage::TsFileWriter *>(writer);
    return tsfile_writer->register_table(std::make_shared<storage::TableSchema>(
        schema->table_name, measurement_schemas, column_categories));
}

ERRNO _tsfile_writer_register_timeseries(TsFileWriter writer,
                                         const char *device_id,
                                         const TimeseriesSchema *schema) {
    auto *w = static_cast<storage::TsFileWriter *>(writer);

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
                                     const device_schema *device_schema) {
    auto *w = static_cast<storage::TsFileWriter *>(writer);
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
    auto *w = static_cast<storage::TsFileWriter *>(writer);
    const auto *tbl = static_cast<storage::Tablet *>(tablet);
    return w->write_tablet(*tbl);
}

ERRNO _tsfile_writer_write_table(TsFileWriter writer, Tablet tablet) {
    auto *w = static_cast<storage::TsFileWriter *>(writer);
    auto *tbl = static_cast<storage::Tablet *>(tablet);
    return w->write_table(*tbl);
}

ERRNO _tsfile_writer_write_ts_record(TsFileWriter writer, TsRecord data) {
    auto *w = static_cast<storage::TsFileWriter *>(writer);
    const storage::TsRecord *record = static_cast<storage::TsRecord *>(data);
    const int ret = w->write_record(*record);
    return ret;
}

ERRNO _tsfile_writer_close(TsFileWriter writer) {
    auto *w = static_cast<storage::TsFileWriter *>(writer);
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

ResultSet _tsfile_reader_query_device(TsFileReader reader,
                                      const char *device_name,
                                      char **sensor_name, uint32_t sensor_num,
                                      Timestamp start_time, Timestamp end_time,
                                      ERRNO *err_code) {
    auto *r = static_cast<storage::TsFileReader *>(reader);
    std::vector<std::string> selected_paths;
    selected_paths.reserve(sensor_num);
    for (uint32_t i = 0; i < sensor_num; i++) {
        selected_paths.push_back(std::string(device_name) + "." +
                                 std::string(sensor_name[i]));
    }
    storage::ResultSet *qds = nullptr;
    *err_code = r->query(selected_paths, start_time, end_time, qds);
    return qds;
}
#ifdef __cplusplus
}
#endif