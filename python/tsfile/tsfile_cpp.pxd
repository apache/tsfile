# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#cython: language_level=3
from libc.stdint cimport uint32_t, int32_t, int64_t, uint64_t, uint8_t

ctypedef int32_t ErrorCode

# import symbols from tsfile_cwrapper.h
cdef extern from "./tsfile_cwrapper.h":
    # common
    ctypedef int64_t timestamp

    # reader and writer etc
    ctypedef void * TsFileReader
    ctypedef void * TsFileWriter
    ctypedef void * Tablet
    ctypedef void * TsRecord
    ctypedef void * ResultSet

    # enum types
    ctypedef enum TSDataType:
        TS_DATATYPE_BOOLEAN = 0
        TS_DATATYPE_INT32 = 1
        TS_DATATYPE_INT64 = 2
        TS_DATATYPE_FLOAT = 3
        TS_DATATYPE_DOUBLE = 4
        TS_DATATYPE_TEXT = 5
        TS_DATATYPE_VECTOR = 6
        TS_DATATYPE_STRING = 11
        TS_DATATYPE_NULL_TYPE = 254
        TS_DATATYPE_INVALID = 255

    ctypedef enum TSEncoding:
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

    ctypedef enum CompressionType:
        TS_COMPRESSION_UNCOMPRESSED = 0,
        TS_COMPRESSION_SNAPPY = 1,
        TS_COMPRESSION_GZIP = 2,
        TS_COMPRESSION_LZO = 3,
        TS_COMPRESSION_SDT = 4,
        TS_COMPRESSION_PAA = 5,
        TS_COMPRESSION_PLA = 6,
        TS_COMPRESSION_LZ4 = 7,
        TS_COMPRESSION_INVALID = 255

    ctypedef enum ColumnCategory:
        TAG = 0,
        FIELD = 1

    # struct types
    ctypedef struct ColumnSchema:
        char * column_name
        TSDataType data_type
        ColumnCategory column_category

    ctypedef struct TableSchema:
        char * table_name
        ColumnSchema * column_schemas
        int column_num

    ctypedef struct TimeseriesSchema:
        char * timeseries_name
        TSDataType data_type
        TSEncoding encoding
        CompressionType compression

    ctypedef struct DeviceSchema:
        char * device_name
        TimeseriesSchema * timeseries_schema
        int timeseries_num

    ctypedef struct ResultSetMetaData:
        char** column_names
        TSDataType * data_types
        int column_num

    # Function Declarations

    # reader：new and close
    TsFileReader tsfile_reader_new(const char * pathname, ErrorCode * err_code);
    ErrorCode tsfile_reader_close(TsFileReader reader)

    # writer： new and close
    TsFileWriter _tsfile_writer_new(const char * pathname, uint64_t memory_threshold,
                                    ErrorCode * err_code);
    ErrorCode _tsfile_writer_close(TsFileWriter writer);

    # writer : flush
    ErrorCode _tsfile_writer_flush(TsFileWriter writer);

    # writer : register table, device and timeseries
    ErrorCode _tsfile_writer_register_table(TsFileWriter writer, TableSchema * schema);
    ErrorCode _tsfile_writer_register_timeseries(TsFileWriter writer,
                                                 const char * device_id,
                                                 const TimeseriesSchema * schema);
    ErrorCode _tsfile_writer_register_device(TsFileWriter writer,
                                             const DeviceSchema * device_schema);

    # writer : write tablet data and flush
    ErrorCode _tsfile_writer_write_tablet(TsFileWriter writer, Tablet tablet);
    ErrorCode _tsfile_writer_write_table(TsFileWriter writer, Tablet tablet);
    ErrorCode _tsfile_writer_write_ts_record(TsFileWriter writer, TsRecord record);
    # tablet : new and add timestamp/value into tablet 
    Tablet _tablet_new_with_target_name(const char * device_id,
                                        char** column_name_list,
                                        TSDataType * data_types,
                                        int column_num, int max_rows);

    Tablet tablet_new(const char** column_names, TSDataType * data_types, int column_num);

    ErrorCode tablet_add_timestamp(Tablet tablet, uint32_t row_index, int64_t timestamp);
    ErrorCode tablet_add_value_by_index_int64_t(Tablet tablet, uint32_t row_index, uint32_t column_index,
                                                int64_t value);
    ErrorCode tablet_add_value_by_index_int32_t(Tablet tablet, uint32_t row_index, uint32_t column_index,
                                                int32_t value);
    ErrorCode tablet_add_value_by_index_double(Tablet tablet, uint32_t row_index, uint32_t column_index, double value);
    ErrorCode tablet_add_value_by_index_float(Tablet tablet, uint32_t row_index, uint32_t column_index, float value);
    ErrorCode tablet_add_value_by_index_bool(Tablet tablet, uint32_t row_index, uint32_t column_index, bint value);
    ErrorCode tablet_add_value_by_index_string(Tablet tablet, uint32_t row_index,
                                               uint32_t column_index, const char * value);

    void free_tablet(Tablet * tablet);

    # row_record
    TsRecord _ts_record_new(const char * device_id, int64_t timestamp, int timeseries_num);
    ErrorCode _insert_data_into_ts_record_by_name_int32_t(TsRecord data, const char *measurement_name,
                                                          const int32_t value);
    ErrorCode _insert_data_into_ts_record_by_name_int64_t(TsRecord data, const char *measurement_name,
                                                          const int64_t value);
    ErrorCode _insert_data_into_ts_record_by_name_float(TsRecord data, const char *measurement_name, const float value);
    ErrorCode _insert_data_into_ts_record_by_name_double(TsRecord data, const char *measurement_name,
                                                         const double value);
    ErrorCode _insert_data_into_ts_record_by_name_bool(TsRecord data, const char *measurement_name, const  bint value);

    void _free_tsfile_ts_record(TsRecord * record);

    # resulSet : query data from tsfile reader
    ResultSet tsfile_query_table(TsFileReader reader,
                                 const char * table_name,
                                 const char** columns, uint32_t column_num,
                                 int64_t start_time, int64_t end_time, ErrorCode *err_code)
    ResultSet _tsfile_reader_query_device(TsFileReader reader,
                                          const char *device_name,
                                          char ** sensor_name, uint32_t sensor_num,
                                          int64_t start_time, int64_t end_time, ErrorCode *err_code)

    TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                               const char * table_name);

    TableSchema * tsfile_reader_get_all_table_schemas(TsFileReader reader,
                                                      uint32_t * size);

    # resultSet : get data from resultSet
    bint tsfile_result_set_next(ResultSet result_set, ErrorCode * err_code);
    bint tsfile_result_set_is_null_by_index(ResultSet result_set, uint32_t column_index);
    bint tsfile_result_set_is_null_by_name(ResultSet result_set, const char * column_name);
    void free_tsfile_result_set(ResultSet * result_set);

    int32_t tsfile_result_set_get_value_by_index_int32_t(ResultSet result_set, uint32_t column_index);
    int64_t tsfile_result_set_get_value_by_index_int64_t(ResultSet result_set, uint32_t column_index);
    bint tsfile_result_set_get_value_by_index_bool(ResultSet result_set, uint32_t column_index);
    float tsfile_result_set_get_value_by_index_float(ResultSet result_set, uint32_t column_index);
    double tsfile_result_set_get_value_by_index_double(ResultSet result_set, uint32_t column_index);
    char * tsfile_result_set_get_value_by_index_string(ResultSet result_set, uint32_t column_index);

    ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set);
    void free_result_set_meta_data(ResultSetMetaData result_set_meta_data);



cdef extern from "./common/config/config.h" namespace "common":
    cdef cppclass ConfigValue:
        uint32_t tsblock_mem_inc_step_size_
        uint32_t tsblock_max_memory_
        uint32_t page_writer_max_point_num_
        uint32_t page_writer_max_memory_bytes_
        uint32_t max_degree_of_index_node_
        double tsfile_index_bloom_filter_error_percent_
        uint8_t time_encoding_type_
        uint8_t time_data_type_
        uint8_t time_compress_type_
        int32_t chunk_group_size_threshold_
        int32_t record_count_for_next_mem_check_
        bint encrypt_flag_
        uint8_t boolean_encoding_type_;
        uint8_t int32_encoding_type_;
        uint8_t int64_encoding_type_;
        uint8_t float_encoding_type_;
        uint8_t double_encoding_type_;
        uint8_t string_encoding_type_;
        uint8_t default_compression_type_;

cdef extern from "./common/global.h" namespace "common":
    ConfigValue g_config_value_
    int set_datatype_encoding(uint8_t data_type, uint8_t encoding)
    int set_global_compression(uint8_t compression)
    int set_global_time_data_type(uint8_t data_type);
    int set_global_time_encoding(uint8_t encoding);
    int set_global_time_compression(uint8_t compression);
