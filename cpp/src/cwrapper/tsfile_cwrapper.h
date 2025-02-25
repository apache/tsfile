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

#ifndef SRC_CWRAPPER_TSFILE_CWRAPPER_H_
#define SRC_CWRAPPER_TSFILE_CWRAPPER_H_

#include <cstdint>
#include <iostream>

typedef enum {
    TS_DATATYPE_BOOLEAN = 0,
    TS_DATATYPE_INT32 = 1,
    TS_DATATYPE_INT64 = 2,
    TS_DATATYPE_FLOAT = 3,
    TS_DATATYPE_DOUBLE = 4,
    TS_DATATYPE_TEXT = 5,
    TS_DATATYPE_VECTOR = 6,
    TS_DATATYPE_NULL_TYPE = 254,
    TS_DATATYPE_INVALID = 255
} TSDataType;

typedef enum {
    TS_ENCODING_PLAIN = 0,
    TS_ENCODING_DICTIONARY = 1,
    TS_ENCODING_RLE = 2,
    TS_ENCODING_DIFF = 3,
    TS_ENCODING_TS_2DIFF = 4,
    TS_ENCODING_BITMAP = 5,
    TS_ENCODING_GORILLA_V1 = 6,
    TS_ENCODING_REGULAR = 7,
    TS_ENCODING_GORILLA = 8,
    TS_ENCODING_ZIGZAG = 9,
    TS_ENCODING_FREQ = 10,
    TS_ENCODING_INVALID = 255
} TSEncoding;

typedef enum {
    TS_COMPRESSION_UNCOMPRESSED = 0,
    TS_COMPRESSION_SNAPPY = 1,
    TS_COMPRESSION_GZIP = 2,
    TS_COMPRESSION_LZO = 3,
    TS_COMPRESSION_SDT = 4,
    TS_COMPRESSION_PAA = 5,
    TS_COMPRESSION_PLA = 6,
    TS_COMPRESSION_LZ4 = 7,
    TS_COMPRESSION_INVALID = 255
} CompressionType;

typedef enum column_category { TAG = 0, FIELD = 1 } ColumnCategory;

typedef struct column_schema {
    char* column_name;
    TSDataType data_type;
    ColumnCategory column_category;
} ColumnSchema;

typedef struct table_schema {
    char* table_name;
    ColumnSchema* column_schemas;
    int column_num;
} TableSchema;

typedef struct timeseries_schema {
    char* timeseries_name;
    TSDataType data_type;
    TSEncoding encoding;
    CompressionType compression;
} TimeseriesSchema;

typedef struct device_schema {
    char* device_name;
    TimeseriesSchema* timeseries_schema;
    int timeseries_num;
} DeviceSchema;

typedef struct result_set_meta_data {
    char** column_names;
    TSDataType* data_types;
    int column_num;
} ResultSetMetaData;

typedef struct tsfile_conf {
    int mem_threshold_kb;
} TsFileConf;

typedef void* TsFileReader;
typedef void* TsFileWriter;

// just reuse Tablet from c++
typedef void* Tablet;
typedef void* TsRecord;

typedef void* ResultSet;

typedef int32_t ERRNO;
typedef int64_t timestamp;

#ifdef __cplusplus
extern "C" {
#endif

/*--------------------------Tablet API------------------------ */
Tablet tablet_new_with_device(const char* device_id, char** column_name_list,
                              TSDataType* data_types, int column_num,
                              int max_rows);

Tablet tablet_new(const char** column_name_list, TSDataType* data_types,
                  int column_num);

uint32_t tablet_get_cur_row_size(Tablet tablet);

ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           timestamp timestamp);

#define TABLET_ADD_VALUE_BY_NAME(type)                                        \
    ERRNO tablet_add_value_by_name_##type(Tablet* tablet, uint32_t row_index, \
                                          char* column_name, type value);

TABLET_ADD_VALUE_BY_NAME(int32_t);
TABLET_ADD_VALUE_BY_NAME(int64_t);
TABLET_ADD_VALUE_BY_NAME(float);
TABLET_ADD_VALUE_BY_NAME(double);
TABLET_ADD_VALUE_BY_NAME(bool);

#define TABLE_ADD_VALUE_BY_INDEX(type)                                        \
    ERRNO tablet_add_value_by_index_##type(Tablet tablet, uint32_t row_index, \
                                           uint32_t column_index, type value);

TABLE_ADD_VALUE_BY_INDEX(int32_t);
TABLE_ADD_VALUE_BY_INDEX(int64_t);
TABLE_ADD_VALUE_BY_INDEX(float);
TABLE_ADD_VALUE_BY_INDEX(double);
TABLE_ADD_VALUE_BY_INDEX(bool);

void* tablet_get_value(Tablet tablet, uint32_t row_index, uint32_t schema_index,
                       TSDataType* type);

/*--------------------------TsRecord API------------------------ */
TsRecord ts_record_new(const char* device_id, timestamp timestamp,
                       int timeseries_num);

#define INSERT_DATA_INTO_TS_RECORD_BY_NAME(type)     \
    ERRNO insert_data_into_ts_record_by_name_##type( \
        TsRecord data, const char* measurement_name, type value);

INSERT_DATA_INTO_TS_RECORD_BY_NAME(int32_t);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(int64_t);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(bool);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(float);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(double);

/*--------------------------TsFile Reader and Writer------------------------ */
TsFileReader tsfile_reader_new(const char* pathname, ERRNO* err_code);
TsFileWriter tsfile_writer_new(const char* pathname, ERRNO* err_code);
TsFileWriter tsfile_writer_new_with_conf(const char* pathname, mode_t flag,
                                         ERRNO* err_code, TsFileConf* conf);

ERRNO tsfile_writer_close(TsFileWriter writer);
ERRNO tsfile_reader_close(TsFileReader reader);

/*--------------------------TsFile Writer Register------------------------ */
ERRNO tsfile_writer_register_table(TsFileWriter writer, TableSchema* schema);
ERRNO tsfile_writer_register_timeseries(TsFileWriter writer,
                                        const char* device_id,
                                        const TimeseriesSchema* schema);
ERRNO tsfile_writer_register_device(TsFileWriter writer,
                                    const DeviceSchema* device_schema);

/*-------------------TsFile Writer write and flush data------------------ */
ERRNO tsfile_writer_write_tablet(TsFileWriter writer, Tablet tablet);
ERRNO tsfile_writer_write_ts_record(TsFileWriter writer, TsRecord record);
ERRNO tsfile_writer_flush_data(TsFileWriter writer);

/*-------------------TsFile reader query data------------------ */
ResultSet tsfile_reader_query_table(TsFileReader reader, const char* table_name,
                                    char** columns, uint32_t column_num,
                                    timestamp start_time, timestamp end_time);
ResultSet tsfile_reader_query_device(TsFileReader reader,
                                     const char* device_name,
                                     char** sensor_name, uint32_t sensor_num,
                                     timestamp start_time, timestamp end_time);
bool tsfile_result_set_has_next(ResultSet result_set);

#define TSFILE_RESULT_SET_GET_VALUE_BY_NAME(type)                         \
    type tsfile_result_set_get_value_by_name_##type(ResultSet result_set, \
                                                    const char* column_name)
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(bool);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(int32_t);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(int64_t);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(float);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(double);

#define TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(type)                         \
    type tsfile_result_set_get_value_by_index_##type(ResultSet result_set, \
                                                     uint32_t column_index);

TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(int32_t);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(int64_t);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(float);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(double);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(bool);

bool tsfile_result_set_is_null_by_name(ResultSet result_set,
                                       const char* column_name);

bool tsfile_result_set_is_null_by_index(ResultSet result_set,
                                        uint32_t column_index);

ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set);
char* tsfile_result_set_meta_get_column_name(ResultSetMetaData result_set,
                                             uint32_t column_index);
TSDataType tsfile_result_set_meta_get_data_type(ResultSetMetaData result_set,
                                                uint32_t column_index);
int tsfile_result_set_meta_get_column_num(ResultSetMetaData result_set);

// Desc table schema.
TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                           const char* table_name);
DeviceSchema tsfile_reader_get_device_schema(TsFileReader reader,
                                             const char* device_id);

TableSchema* tsfile_reader_get_all_table_schemas(TsFileReader reader,
                                                 uint32_t* schema_num);

// Close and free resource.
void free_tsfile_ts_record(TsRecord* record);
void free_tablet(Tablet* tablet);
void free_tsfile_result_set(ResultSet* result_set);
void free_result_set_meta_data(ResultSetMetaData result_set_meta_data);
void free_device_schema(DeviceSchema schema);
void free_timeseries_schema(TimeseriesSchema schema);
void free_table_schema(TableSchema schema);
void free_column_schema(ColumnSchema schema);

#ifdef __cplusplus
}
#endif
#endif  // SRC_CWRAPPER_TSFILE_CWRAPPER_H_
