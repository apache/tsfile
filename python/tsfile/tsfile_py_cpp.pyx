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

from .tsfile_cpp cimport *

from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.string cimport strdup
from cpython.exc cimport PyErr_SetObject
from cpython.unicode cimport PyUnicode_AsUTF8String, PyUnicode_AsUTF8
from cpython.bytes cimport PyBytes_AsString

from tsfile.exceptions import ERROR_MAPPING
from tsfile.schema import ResultSetMetaData as ResultSetMetaDataPy
from tsfile.schema import TSDataType as TSDataTypePy, TSEncoding as TSEncodingPy
from tsfile.schema import Compressor as CompressorPy, ColumnCategory as CategoryPy
from tsfile.schema import TableSchema as TableSchemaPy, ColumnSchema as ColumnSchemaPy

# check exception and set py exception object
cdef inline void check_error(int errcode, const char* context=NULL) except *:
    cdef:
        object exc_type
        object exc_instance

    if errcode == 0:
        return

    exc_type = ERROR_MAPPING.get(errcode)
    print(exc_type)
    exc_instance = exc_type(errcode, "")
    PyErr_SetObject(exc_type, exc_instance)

# convert from c to python
cdef object from_c_result_set_meta_data(ResultSetMetaData schema):
    column_list = []
    data_types = []
    column_num = schema.column_num

    for i in range(column_num):
        column_list.append(schema.column_names[i].decode('utf-8'))
        data_types.append(TSDataTypePy(schema.data_types[i]))
    result = ResultSetMetaDataPy(column_list, data_types)
    return result

cdef object from_c_column_schema(ColumnSchema schema):
    column_name = schema.column_name.decode('utf-8')
    data_type = TSDataTypePy(schema.data_type)
    category_type = CategoryPy(schema.column_category)
    return ColumnSchemaPy(column_name, data_type, category_type)

cdef object from_c_table_schema(TableSchema schema):
    cdef int i
    table_name = schema.table_name.decode('utf-8')
    columns = []
    for i in range(schema.column_num):
        columns.append(from_c_column_schema(schema.column_schemas[i]))
    free_c_table_schema(&schema)
    return TableSchemaPy(table_name, columns)



# Convert from python to c struct
cdef dict TS_DATA_TYPE_MAP = {
    TSDataTypePy.BOOLEAN: TSDataType.TS_DATATYPE_BOOLEAN,
    TSDataTypePy.INT32: TSDataType.TS_DATATYPE_INT32,
    TSDataTypePy.INT64: TSDataType.TS_DATATYPE_INT64,
    TSDataTypePy.FLOAT: TSDataType.TS_DATATYPE_FLOAT,
    TSDataTypePy.DOUBLE: TSDataType.TS_DATATYPE_DOUBLE,
    TSDataTypePy.TEXT: TSDataType.TS_DATATYPE_TEXT,
    TSDataTypePy.STRING: TSDataType.TS_DATATYPE_STRING
}

cdef dict TS_ENCODING_MAP = {
    TSEncodingPy.PLAIN: TSEncoding.TS_ENCODING_PLAIN,
    TSEncodingPy.DICTIONARY: TSEncoding.TS_ENCODING_DICTIONARY,
    TSEncodingPy.RLE: TSEncoding.TS_ENCODING_RLE,
    TSEncodingPy.DIFF: TSEncoding.TS_ENCODING_DIFF,
    TSEncodingPy.TS_2DIFF: TSEncoding.TS_ENCODING_TS_2DIFF,
    TSEncodingPy.BITMAP: TSEncoding.TS_ENCODING_BITMAP,
    TSEncodingPy.GORILLA_V1: TSEncoding.TS_ENCODING_GORILLA_V1,
    TSEncodingPy.REGULAR: TSEncoding.TS_ENCODING_REGULAR,
    TSEncodingPy.GORILLA: TSEncoding.TS_ENCODING_GORILLA,
    TSEncodingPy.ZIGZAG: TSEncoding.TS_ENCODING_ZIGZAG,
}

cdef dict COMPRESSION_TYPE_MAP = {
    CompressorPy.UNCOMPRESSED: CompressionType.TS_COMPRESSION_UNCOMPRESSED,
    CompressorPy.SNAPPY: CompressionType.TS_COMPRESSION_SNAPPY,
    CompressorPy.GZIP: CompressionType.TS_COMPRESSION_GZIP,
    CompressorPy.LZO: CompressionType.TS_COMPRESSION_LZO,
    CompressorPy.SDT: CompressionType.TS_COMPRESSION_SDT,
    CompressorPy.PAA: CompressionType.TS_COMPRESSION_PAA,
    CompressorPy.PLA: CompressionType.TS_COMPRESSION_PLA,
    CompressorPy.LZ4: CompressionType.TS_COMPRESSION_LZ4,
}

cdef dict CATEGORY_MAP = {
    CategoryPy.TAG: ColumnCategory.TAG,
    CategoryPy.FIELD: ColumnCategory.FIELD
}

cdef TSDataType to_c_data_type(object data_type):
    try:
        return TS_DATA_TYPE_MAP[data_type]
    except KeyError:
        raise ValueError(f"Unsupported Python TSDataType: {data_type}")

cdef ColumnCategory to_c_category_type(object category):
    try:
        return CATEGORY_MAP[category]
    except KeyError:
        raise ValueError(f"Unsupported Python Column Category: {category}")


cdef TSEncoding to_c_encoding_type(object encoding_type):
    try:
        return TS_ENCODING_MAP[encoding_type]
    except KeyError:
        raise ValueError(f"Unsupported Python TSEncoding: {encoding_type}")

cdef CompressionType to_c_compression_type(object compression_type):
    try:
        return COMPRESSION_TYPE_MAP[compression_type]
    except KeyError:
        raise ValueError(f"Unsupported Python Compressor: {compression_type}")

cdef TimeseriesSchema* to_c_timeseries_schema(object py_schema):
    cdef TimeseriesSchema* c_schema
    c_schema = <TimeseriesSchema *> malloc(sizeof(TimeseriesSchema))
    c_schema.timeseries_name = strdup(py_schema.timeseries_name.encode('utf-8'))
    if py_schema.data_type is not None:
        c_schema.data_type = to_c_data_type(py_schema.data_type)
    else:
        raise ValueError("data_type cannot be None")
    if py_schema.encoding_type is not None:
        c_schema.encoding = to_c_encoding_type(py_schema.encoding_type)
    else:
        raise ValueError("encoding_type cannot be None")
    if py_schema.compression_type is not None:
        c_schema.compression = to_c_compression_type(py_schema.compression_type)
    else:
        raise ValueError("compression_type cannot be None")
    return c_schema


cdef DeviceSchema* to_c_device_schema(object py_schema):
    cdef DeviceSchema* c_schema
    c_schema = <DeviceSchema *> malloc(sizeof(DeviceSchema))
    c_schema.device_name = strdup(py_schema.device_name.encode('utf-8'))
    c_schema.timeseries_num = len(py_schema.timeseries_list)
    c_schema.timeseries_schema = <TimeseriesSchema *> malloc(c_schema.timeseries_num * sizeof(TimeseriesSchema))
    for i in range(c_schema.timeseries_num):
        c_schema.timeseries_schema[i].timeseries_name = strdup(py_schema.timeseries_list[i].timeseries_name.encode('utf-8'))
        c_schema.timeseries_schema[i].data_type = to_c_data_type(py_schema.timeseries_list[i].data_type)
        c_schema.timeseries_schema[i].encoding = to_c_encoding_type(py_schema.timeseries_list[i].encoding_type)
        c_schema.timeseries_schema[i].compression = to_c_compression_type(py_schema.timeseries_list[i].compression_type)
    return c_schema

cdef ColumnSchema* to_c_column_schema(object py_schema):
    cdef ColumnSchema* c_schema
    c_schema = <ColumnSchema*> malloc(sizeof(ColumnSchema))
    c_schema.data_type = to_c_data_type(py_schema.data_type)
    c_schema.column_category = py_schema.category
    c_schema.column_name = strdup(py_schema.column_name.encode('utf-8'))
    return c_schema

cdef TableSchema* to_c_table_schema(object py_schema):
    cdef TableSchema* c_schema
    c_schema = <TableSchema *> malloc(sizeof(TableSchema))
    c_schema.table_name = strdup(py_schema.table_name.encode('utf-8'))
    c_schema.column_num = len(py_schema.columns)
    c_schema.column_schemas = <ColumnSchema *> malloc(c_schema.column_num * sizeof(ColumnSchema))
    for i in range(c_schema.column_num):
        c_schema.column_schemas[i].column_name = strdup(py_schema.columns[i].column_name.encode('utf-8'))
        c_schema.column_schemas[i].column_category = to_c_category_type(py_schema.columns[i].category)
        c_schema.column_schemas[i].data_type = to_c_data_type(py_schema.columns[i].data_type)
    return c_schema


cdef Tablet to_c_tablet(object tablet):
    cdef Tablet ctablet
    cdef int max_row_num
    cdef TSDataType data_type
    cdef int64_t timestamp
    cdef bytes device_id_bytes
    cdef const char * device_id_c
    cdef char** columns_names
    cdef TSDataType* column_types
    cdef bytes row_bytes
    cdef const char *row_str

    if tablet.get_target_name() is not None:
        device_id_bytes = PyUnicode_AsUTF8String(tablet.get_target_name())
        device_id_c = device_id_bytes
    else:
        device_id_c = NULL

    column_num = len(tablet.get_column_name_list())
    columns_names = <char**> malloc(sizeof(char *) * column_num)
    columns_types = <TSDataType *> malloc(sizeof(TSDataType) * column_num)
    for i in range(column_num):
        columns_names[i] = strdup(tablet.get_column_name_list()[i].encode('utf-8'))
        columns_types[i] = to_c_data_type(tablet.get_data_type_list()[i])

    max_row_num = tablet.get_max_row_num()

    ctablet = _tablet_new_with_target_name(device_id_c, columns_names, columns_types, column_num,
                                     max_row_num)
    free(columns_types)
    for i in range(column_num):
        free(columns_names[i])
    free(columns_names)

    for row in range(max_row_num):
        timestamp_py = tablet.get_timestamp_list()[row]
        if timestamp_py is None:
            continue
        timestamp = timestamp_py
        tablet_add_timestamp(ctablet, row, timestamp)

    for col in range(column_num):
        data_type = to_c_data_type(tablet.get_data_type_list()[col])
        value = tablet.get_value_list()[col]
        # BOOLEAN
        if data_type == TS_DATATYPE_BOOLEAN:
            for row in range(max_row_num):
                if value[row] is not None:
                    tablet_add_value_by_index_bool(ctablet, row, col, value[row])
        # INT32
        elif data_type == TS_DATATYPE_INT32:
            for row in range(max_row_num):
                if value[row] is not None:
                    tablet_add_value_by_index_int32_t(ctablet, row, col, value[row])

        # INT64
        elif data_type == TS_DATATYPE_INT64:
            for row in range(max_row_num):
                if value[row] is not None:
                    tablet_add_value_by_index_int64_t(ctablet, row, col, value[row])
        # FLOAT
        elif data_type == TS_DATATYPE_FLOAT:
            for row in range(max_row_num):
                if value[row] is not None:
                    tablet_add_value_by_index_float(ctablet, row, col, value[row])

        # DOUBLE
        elif data_type == TS_DATATYPE_DOUBLE:
            for row in range(max_row_num):
                if value[row] is not None:
                    tablet_add_value_by_index_double(ctablet, row, col, value[row])

        # STRING
        elif data_type == TS_DATATYPE_STRING:
            for row in range(max_row_num):
                if value[row] is not None:
                    py_value = value[row]
                    row_bytes = PyUnicode_AsUTF8String(py_value)
                    row_str = PyBytes_AsString(row_bytes)
                    tablet_add_value_by_index_string(ctablet, row, col, row_str)


    return ctablet


cdef TsRecord to_c_record(object row_record):
    cdef int field_num = row_record.get_fields_num()
    cdef int64_t timestamp = <int64_t>row_record.get_timestamp()
    cdef bytes device_id_bytes = PyUnicode_AsUTF8String(row_record.get_device_id())
    cdef const char* device_id = device_id_bytes
    cdef TsRecord record
    cdef int i
    cdef TSDataType data_type
    record = _ts_record_new(device_id, timestamp, field_num)
    for i in range(field_num):
        field = row_record.get_fields()[i]
        data_type = to_c_data_type(field.get_data_type())
        if data_type == TS_DATATYPE_BOOLEAN:
            _insert_data_into_ts_record_by_name_bool(record, PyUnicode_AsUTF8(field.get_field_name()), field.get_bool_value())
        elif data_type == TS_DATATYPE_INT32:
            _insert_data_into_ts_record_by_name_int32_t(record, PyUnicode_AsUTF8(field.get_field_name()), field.get_int_value())
        elif data_type == TS_DATATYPE_INT64:
            _insert_data_into_ts_record_by_name_int64_t(record, PyUnicode_AsUTF8(field.get_field_name()), field.get_long_value())
        elif data_type == TS_DATATYPE_DOUBLE:
            _insert_data_into_ts_record_by_name_double(record, PyUnicode_AsUTF8(field.get_field_name()), field.get_double_value())
        elif data_type == TS_DATATYPE_FLOAT:
            _insert_data_into_ts_record_by_name_float(record, PyUnicode_AsUTF8(field.get_field_name()), field.get_float_value())

    return record

# Free c structs' space
cdef void free_c_table_schema(TableSchema* c_schema):
    free(c_schema.table_name)
    for i in range(c_schema.column_num):
        free_c_column_schema(&(c_schema.column_schemas[i]))
    free(c_schema.column_schemas)

cdef void free_c_column_schema(ColumnSchema* c_schema):
    free(c_schema.column_name)

cdef void free_c_timeseries_schema(TimeseriesSchema* c_schema):
    free(c_schema.timeseries_name)

cdef void free_c_device_schema(DeviceSchema* c_schema):
    free(c_schema.device_name)
    for i in range(c_schema.timeseries_num):
        free_c_timeseries_schema(&(c_schema.timeseries_schema[i]))
    free(c_schema.timeseries_schema)

cdef void free_c_tablet(Tablet tablet):
    free_tablet(&tablet)

cdef void free_c_row_record(TsRecord record):
    _free_tsfile_ts_record(&record)

# Reader and writer new.
cdef TsFileWriter tsfile_writer_new_c(object pathname, uint64_t memory_threshold) except +:
    cdef ErrorCode errno = 0
    cdef TsFileWriter writer
    cdef bytes encoded_path = PyUnicode_AsUTF8String(pathname)
    cdef const char* c_path = encoded_path
    writer = _tsfile_writer_new(c_path, memory_threshold, &errno)
    check_error(errno)
    return writer

cdef TsFileReader tsfile_reader_new_c(object pathname) except +:
    cdef ErrorCode errno = 0
    cdef TsFileReader reader
    cdef bytes encoded_path = PyUnicode_AsUTF8String(pathname)
    cdef const char* c_path = encoded_path
    reader =  tsfile_reader_new(c_path, &errno)
    check_error(errno)
    return reader

cpdef object get_tsfile_config():
    return {
        "tsblock_mem_inc_step_size_": g_config_value_.tsblock_mem_inc_step_size_,
        "tsblock_max_memory_": g_config_value_.tsblock_max_memory_,
        "page_writer_max_point_num_": g_config_value_.page_writer_max_point_num_,
        "page_writer_max_memory_bytes_": g_config_value_.page_writer_max_memory_bytes_,
        "max_degree_of_index_node_": g_config_value_.max_degree_of_index_node_,
        "tsfile_index_bloom_filter_error_percent_": g_config_value_.tsfile_index_bloom_filter_error_percent_,
        "time_encoding_type_":TSEncodingPy(int(g_config_value_.time_encoding_type_)),
        "time_data_type_": TSDataTypePy(int(g_config_value_.time_data_type_)),
        "time_compress_type_": CompressorPy(int(g_config_value_.time_compress_type_)),
        "chunk_group_size_threshold_": g_config_value_.chunk_group_size_threshold_,
        "record_count_for_next_mem_check_":g_config_value_.record_count_for_next_mem_check_,
        "encrypt_flag_":g_config_value_.encrypt_flag_,
        "boolean_encoding_type_":TSEncodingPy(int(g_config_value_.boolean_encoding_type_)),
        "int32_encoding_type_": TSEncodingPy(int(g_config_value_.int32_encoding_type_)),
        "int64_encoding_type_": TSEncodingPy(int(g_config_value_.int64_encoding_type_)),
        "float_encoding_type_": TSEncodingPy(int(g_config_value_.float_encoding_type_)),
        "double_encoding_type_": TSEncodingPy(int(g_config_value_.double_encoding_type_)),
        "string_encoding_type_": TSEncodingPy(int(g_config_value_.string_encoding_type_)),
        "default_compression_type_": CompressorPy(int(g_config_value_.default_compression_type_)),
    }


cpdef void set_tsfile_config(dict new_config):
    if "tsblock_mem_inc_step_size_" in new_config:
        _check_uint32(new_config["tsblock_mem_inc_step_size_"])
        g_config_value_.tsblock_max_memory_ = new_config["tsblock_mem_inc_step_size_"]
    if "tsblock_max_memory_" in new_config:
        _check_uint32(new_config["tsblock_max_memory_"])
        g_config_value_.tsblock_max_memory_ = new_config["tsblock_max_memory_"]
    if "page_writer_max_point_num_" in new_config:
        _check_uint32(new_config["page_writer_max_point_num_"])
        g_config_value_.page_writer_max_point_num_ = new_config["page_writer_max_point_num_"]
    if "page_writer_max_memory_bytes_" in new_config:
        _check_uint32(new_config["page_writer_max_memory_bytes_"])
        g_config_value_.page_writer_max_memory_bytes_ = new_config["page_writer_max_memory_bytes_"]
    if "max_degree_of_index_node_" in new_config:
        _check_uint32(new_config["max_degree_of_index_node_"])
        g_config_value_.max_degree_of_index_node_ = new_config["max_degree_of_index_node_"]
    if "tsfile_index_bloom_filter_error_percent_" in new_config:
        _check_double(new_config["tsfile_index_bloom_filter_error_percent_"])
        g_config_value_.tsfile_index_bloom_filter_error_percent_ = new_config["tsfile_index_bloom_filter_error_percent_"]
    if "time_encoding_type_" in new_config:
        if not isinstance(new_config["time_encoding_type_"], TSEncodingPy):
            raise TypeError(f"Unsupported TSEncoding: {new_config['time_encoding_type_']}")
        code = set_global_time_encoding(<uint8_t>(new_config["time_encoding_type_"].value))
        check_error(code)
    if "time_data_type_" in new_config:
        if not isinstance(new_config["time_data_type_"], TSDataTypePy):
            raise TypeError(f"Unsupported TSDataType: {new_config['time_data_type_']}")
        code = set_global_time_data_type(<uint8_t>(new_config["time_data_type_"].value))
        check_error(code)
    if "time_compress_type_" in new_config:
        if not isinstance(new_config["time_compress_type_"], CompressorPy):
            raise TypeError(f"Unsupported Compressor: {new_config['time_compress_type_']}")
        code = set_global_time_compression(<uint8_t>(new_config["time_compress_type_"].value))
        check_error(code)
    if "chunk_group_size_threshold_" in new_config:
        _check_uint32(new_config["chunk_group_size_threshold_"])
        g_config_value_.chunk_group_size_threshold_ = new_config["chunk_group_size_threshold_"]
    if "record_count_for_next_mem_check_" in new_config:
        _check_uint32(new_config["record_count_for_next_mem_check_"])
        g_config_value_.record_count_for_next_mem_check_ = new_config["record_count_for_next_mem_check_"]
    if "encrypt_flag_" in new_config:
        _check_bool(new_config["encrypt_flag_"])
        g_config_value_.encrypt_flag_ = <bint>new_config["encrypt_flag_"]

    if "boolean_encoding_type_" in new_config:
        if not isinstance(new_config["boolean_encoding_type_"], TSEncodingPy):
            raise TypeError(f"Unsupported TSEncodingType: {new_config['boolean_encoding_type_']}")
        code = set_datatype_encoding(TSDataTypePy.BOOLEAN.value, new_config['boolean_encoding_type_'].value)
        check_error(code)
    if "int32_encoding_type_" in new_config:
        if not isinstance(new_config["int32_encoding_type_"], TSEncodingPy):
            raise TypeError(f"Unsupported TSEncodingType: {new_config['int32_encoding_type_']}")
        code = set_datatype_encoding(TSDataTypePy.INT32.value, new_config['int32_encoding_type_'].value)
        check_error(code)
    if "int64_encoding_type_" in new_config:
        if not isinstance(new_config["int64_encoding_type_"], TSEncodingPy):
            raise TypeError(f"Unsupported TSEncodingType: {new_config['int64_encoding_type_']}")
        code = set_datatype_encoding(TSDataTypePy.INT64.value, new_config['int64_encoding_type_'].value)
        check_error(code)
    if "float_encoding_type_" in new_config:
        if not isinstance(new_config["float_encoding_type_"], TSEncodingPy):
            raise TypeError(f"Unsupported TSEncodingType: {new_config['float_encoding_type_']}")
        code = set_datatype_encoding(TSDataTypePy.FLOAT.value, new_config['float_encoding_type_'].value)
        check_error(code)
    if "double_encoding_type_" in new_config:
        if not isinstance(new_config["double_encoding_type_"], TSEncodingPy):
            raise TypeError(f"Unsupported TSEncodingType: {new_config['double_encoding_type_']}")
        code = set_datatype_encoding(TSDataTypePy.DOUBLE.value, new_config['double_encoding_type_'].value)
        check_error(code)
    if "string_encoding_type_" in new_config:
        if not isinstance(new_config["string_encoding_type_"], TSEncodingPy):
            raise TypeError(f"Unsupported TSEncodingType: {new_config['string_encoding_type_']}")
        code = set_datatype_encoding(TSDataTypePy.STRING.value, new_config['string_encoding_type_'].value)
        check_error(code)
    if "default_compression_type_" in new_config:
        if not isinstance(new_config["default_compression_type_"], CompressorPy):
            raise TypeError(f"Unsupported CompressionType: {new_config['default_compression_type_']}")
        code = set_global_compression(new_config["default_compression_type_"].value)
        check_error(code)

cdef _check_uint32(value):
    if not isinstance(value, int) or value < 0 or value > 0xFFFFFFFF:
        raise TypeError(f"Expected uint32, got {type(value)}")

cdef _check_double(value):
    if not isinstance(value, (int, float)):
        raise TypeError(f"Expected float, got {type(value)}")

cdef _check_bool(value):
    if not isinstance(value, bool):
        raise TypeError(f"Expected bool, got {type(value)}")

# Register table and device
cdef ErrorCode tsfile_writer_register_device_py_cpp(TsFileWriter writer, DeviceSchema *schema):
    cdef ErrorCode errno
    errno = _tsfile_writer_register_device(writer, schema)
    return errno

cdef ErrorCode tsfile_writer_register_timeseries_py_cpp(TsFileWriter writer, object device_name,
                                                        TimeseriesSchema *schema):
    cdef ErrorCode errno
    cdef bytes encoded_device_name = PyUnicode_AsUTF8String(device_name)
    cdef const char* c_device_name = encoded_device_name
    errno = _tsfile_writer_register_timeseries(writer, c_device_name, schema)
    return errno

cdef ErrorCode tsfile_writer_register_table_py_cpp(TsFileWriter writer, TableSchema *schema):
    cdef ErrorCode errno
    errno = _tsfile_writer_register_table(writer, schema)
    return errno

cdef bint tsfile_result_set_is_null_by_name_c(ResultSet result_set, object name):
    cdef bytes encoded_name = PyUnicode_AsUTF8String(name)
    cdef const char* c_name = encoded_name
    return tsfile_result_set_is_null_by_name(result_set, c_name)

cdef ResultSet tsfile_reader_query_table_c(TsFileReader reader, object table_name, object column_list,
                                            int64_t start_time, int64_t end_time):
    cdef ResultSet result
    cdef int column_num = len(column_list)
    cdef bytes table_name_bytes = PyUnicode_AsUTF8String(table_name)
    cdef const char* table_name_c = table_name_bytes
    cdef char** columns = <char**> malloc(sizeof(char*) * column_num)
    cdef int i
    cdef ErrorCode code = 0
    if columns == NULL:
        raise MemoryError("Failed to allocate memory for columns")
    try:
        for i in range(column_num):
            columns[i] = strdup((<str>column_list[i]).encode('utf-8'))
            if columns[i] == NULL:
                raise MemoryError("Failed to allocate memory for column name")
        result = tsfile_query_table(reader, table_name_c, columns,  column_num, start_time, end_time, &code)
        check_error(code)
        return result
    finally:
        if columns != NULL:
            for i in range(column_num):
                free(<void*>columns[i])
                columns[i] = NULL
            free(<void*>columns)
            columns = NULL

cdef ResultSet tsfile_reader_query_paths_c(TsFileReader reader, object device_name, object sensor_list, int64_t start_time,
                                                      int64_t end_time):
    cdef ResultSet result
    cdef int path_num = len(sensor_list)
    cdef char** sensor_list_c = <char**> malloc(sizeof(char*) * path_num)
    cdef bytes device_name_bytes = PyUnicode_AsUTF8String(device_name)
    cdef const char* device_name_c = device_name_bytes
    cdef int i
    cdef ErrorCode code = 0
    if sensor_list_c == NULL:
        raise MemoryError("Failed to allocate memory for paths")
    try:
        for i in range(path_num):
            sensor_list_c[i] = strdup((<str>sensor_list[i]).encode('utf-8'))
            if sensor_list_c[i] == NULL:
                raise MemoryError("Failed to allocate memory for path")
        result = _tsfile_reader_query_device(reader, device_name_c, sensor_list_c, path_num, start_time, end_time, &code)
        check_error(code)
        return result
    finally:
        if sensor_list_c != NULL:
            for i in range(path_num):
                if sensor_list_c[i] != NULL:
                    free(<void*>sensor_list_c[i])
                    sensor_list_c[i] = NULL
            free(<void*>sensor_list_c)
            sensor_list_c = NULL

cdef object get_table_schema(TsFileReader reader, object table_name):
    cdef bytes table_name_bytes = PyUnicode_AsUTF8String(table_name)
    cdef const char* table_name_c = table_name_bytes
    cdef TableSchema schema = tsfile_reader_get_table_schema(reader, table_name_c)
    return from_c_table_schema(schema)

cdef object get_all_table_schema(TsFileReader reader):
    cdef uint32_t table_num = 0
    cdef TableSchema* schemas
    cdef int i

    table_schemas = {}
    schemas = tsfile_reader_get_all_table_schemas(reader, &table_num)
    for i in range(table_num):
        schema_py = from_c_table_schema(schemas[i])
        table_schemas.update([(schema_py.get_table_name(), schema_py)])
    free(schemas)
    return table_schemas

