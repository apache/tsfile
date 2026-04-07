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
from datetime import date as date_type
from .date_utils import parse_date_to_int
from .tsfile_cpp cimport *

import pandas as pd
import numpy as np

from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.string cimport strdup
from libc.string cimport memset
from cpython.exc cimport PyErr_SetObject
from cpython.unicode cimport PyUnicode_AsUTF8String, PyUnicode_AsUTF8, PyUnicode_AsUTF8AndSize
from cpython.bytes cimport PyBytes_AsString, PyBytes_AsStringAndSize

from tsfile.exceptions import ERROR_MAPPING, TypeMismatchError
from tsfile.schema import ResultSetMetaData as ResultSetMetaDataPy
from tsfile.schema import TSDataType as TSDataTypePy, TSEncoding as TSEncodingPy
from tsfile.schema import Compressor as CompressorPy, ColumnCategory as CategoryPy
from tsfile.schema import TableSchema as TableSchemaPy, ColumnSchema as ColumnSchemaPy
from tsfile.schema import DeviceSchema as DeviceSchemaPy, TimeseriesSchema as TimeseriesSchemaPy
from tsfile.schema import BoolTimeseriesStatistic as BoolTimeseriesStatisticPy
from tsfile.schema import DeviceID as DeviceIDPy
from tsfile.schema import DeviceTimeseriesMetadataGroup as DeviceTimeseriesMetadataGroupPy
from tsfile.schema import FloatTimeseriesStatistic as FloatTimeseriesStatisticPy
from tsfile.schema import IntTimeseriesStatistic as IntTimeseriesStatisticPy
from tsfile.schema import StringTimeseriesStatistic as StringTimeseriesStatisticPy
from tsfile.schema import TextTimeseriesStatistic as TextTimeseriesStatisticPy
from tsfile.schema import TimeseriesStatistic as TimeseriesStatisticPy
from tsfile.schema import TimeseriesMetadata as TimeseriesMetadataPy

# check exception and set py exception object
cdef inline void check_error(int errcode, const char * context=NULL) except*:
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

cdef object from_c_timeseries_schema(TimeseriesSchema schema):
    timeseries_name = schema.timeseries_name.decode('utf-8')
    data_type = TSDataTypePy(schema.data_type)
    encoding = TSEncodingPy(schema.encoding)
    compression = CompressorPy(schema.compression)
    return TimeseriesSchemaPy(timeseries_name, data_type, encoding, compression)

cdef object from_c_device_schema(DeviceSchema schema):
    cdef int i
    device_name = schema.device_name.decode('utf-8')
    timeseries = []
    for i in range(schema.timeseries_num):
        timeseries.append(from_c_timeseries_schema(schema.timeseries_schema[i]))
    free_c_device_schema(&schema)
    return DeviceSchemaPy(device_name, timeseries)

# Convert from python to c struct
cdef dict TS_DATA_TYPE_MAP = {
    TSDataTypePy.BOOLEAN: TSDataType.TS_DATATYPE_BOOLEAN,
    TSDataTypePy.INT32: TSDataType.TS_DATATYPE_INT32,
    TSDataTypePy.INT64: TSDataType.TS_DATATYPE_INT64,
    TSDataTypePy.FLOAT: TSDataType.TS_DATATYPE_FLOAT,
    TSDataTypePy.DOUBLE: TSDataType.TS_DATATYPE_DOUBLE,
    TSDataTypePy.DATE: TSDataType.TS_DATATYPE_DATE,
    TSDataTypePy.TEXT: TSDataType.TS_DATATYPE_TEXT,
    TSDataTypePy.STRING: TSDataType.TS_DATATYPE_STRING,
    TSDataTypePy.BLOB: TSDataType.TS_DATATYPE_BLOB,
    TSDataTypePy.TIMESTAMP: TSDataType.TS_DATATYPE_TIMESTAMP
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
    CategoryPy.FIELD: ColumnCategory.FIELD,
    CategoryPy.ATTRIBUTE: ColumnCategory.ATTRIBUTE,
    CategoryPy.TIME: ColumnCategory.TIME
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

cdef TimeseriesSchema * to_c_timeseries_schema(object py_schema):
    cdef TimeseriesSchema * c_schema
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

cdef DeviceSchema * to_c_device_schema(object py_schema):
    cdef DeviceSchema * c_schema
    c_schema = <DeviceSchema *> malloc(sizeof(DeviceSchema))
    c_schema.device_name = strdup(py_schema.device_name.encode('utf-8'))
    c_schema.timeseries_num = len(py_schema.timeseries_list)
    c_schema.timeseries_schema = <TimeseriesSchema *> malloc(c_schema.timeseries_num * sizeof(TimeseriesSchema))
    for i in range(c_schema.timeseries_num):
        c_schema.timeseries_schema[i].timeseries_name = strdup(
            py_schema.timeseries_list[i].timeseries_name.encode('utf-8'))
        c_schema.timeseries_schema[i].data_type = to_c_data_type(py_schema.timeseries_list[i].data_type)
        c_schema.timeseries_schema[i].encoding = to_c_encoding_type(py_schema.timeseries_list[i].encoding_type)
        c_schema.timeseries_schema[i].compression = to_c_compression_type(py_schema.timeseries_list[i].compression_type)
    return c_schema

cdef ColumnSchema * to_c_column_schema(object py_schema):
    cdef ColumnSchema * c_schema
    c_schema = <ColumnSchema *> malloc(sizeof(ColumnSchema))
    c_schema.data_type = to_c_data_type(py_schema.data_type)
    c_schema.column_category = py_schema.category
    c_schema.column_name = strdup(py_schema.column_name.encode('utf-8'))
    return c_schema

cdef TableSchema * to_c_table_schema(object py_schema):
    cdef TableSchema * c_schema
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
    cdef TSDataType * column_types
    cdef bytes row_bytes
    cdef char *raw_str
    cdef const char * str_ptr
    cdef Py_ssize_t raw_len

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
        elif data_type == TS_DATATYPE_INT64 or data_type == TS_DATATYPE_TIMESTAMP:
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

        elif data_type == TS_DATATYPE_DATE:
            for row in range(max_row_num):
                if value[row] is not None:
                    tablet_add_value_by_index_int32_t(ctablet, row, col, parse_date_to_int(value[row]))

        # STRING or TEXT
        elif data_type == TS_DATATYPE_STRING or data_type == TS_DATATYPE_TEXT:
            for row in range(max_row_num):
                if value[row] is not None:
                    py_value = value[row]
                    str_ptr = PyUnicode_AsUTF8AndSize(py_value, &raw_len)
                    tablet_add_value_by_index_string_with_len(ctablet, row, col, str_ptr, raw_len)

        elif data_type == TS_DATATYPE_BLOB:
            for row in range(max_row_num):
                if value[row] is not None:
                    PyBytes_AsStringAndSize(value[row], &raw_str, &raw_len)
                    tablet_add_value_by_index_string_with_len(ctablet, row, col, raw_str, raw_len)

    return ctablet

cdef Tablet dataframe_to_c_tablet(object target_name, object dataframe, object table_schema):
    cdef Tablet ctablet
    cdef int max_row_num
    cdef TSDataType data_type
    cdef int64_t timestamp
    cdef const char * device_id_c = NULL
    cdef char** columns_names
    cdef TSDataType * columns_types
    cdef char *raw_str
    cdef const char * str_ptr
    cdef Py_ssize_t raw_len
    cdef int column_num
    cdef int i, row
    cdef object value
    cdef object py_value
    cdef object value_bytes

    device_id_bytes = PyUnicode_AsUTF8String(target_name.lower())
    device_id_c = device_id_bytes
    df_columns = list(dataframe.columns)
    use_id_as_time = False

    time_column = table_schema.get_time_column()
    use_id_as_time = time_column is None
    time_column_name = None if time_column is None else time_column.get_column_name()

    data_columns = [col for col in df_columns if col != time_column_name]
    column_num = len(data_columns)

    if column_num == 0:
        raise ValueError("DataFrame must have at least one data column besides 'time'")

    max_row_num = len(dataframe)

    column_types_list = []
    for column in data_columns:
        data_type = table_schema.get_column(column).get_data_type()
        column_types_list.append(data_type)

    columns_names = <char**> malloc(sizeof(char *) * column_num)
    columns_types = <TSDataType *> malloc(sizeof(TSDataType) * column_num)

    for i in range(column_num):
        columns_names[i] = strdup(data_columns[i].lower().encode('utf-8'))
        columns_types[i] = column_types_list[i]

    ctablet = _tablet_new_with_target_name(device_id_c, columns_names, columns_types, column_num,
                         max_row_num)

    free(columns_types)
    for i in range(column_num):
        free(columns_names[i])
    free(columns_names)

    if use_id_as_time:
        for row in range(max_row_num):
            timestamp_py = dataframe.index[row]
            if pd.isna(timestamp_py):
                continue
            timestamp = <int64_t> timestamp_py
            tablet_add_timestamp(ctablet, row, timestamp)
    else:
        time_values = dataframe[time_column.get_column_name()].values
        for row in range(max_row_num):
            timestamp_py = time_values[row]
            if pd.isna(timestamp_py):
                continue
            timestamp = <int64_t> timestamp_py
            tablet_add_timestamp(ctablet, row, timestamp)

    for col in range(column_num):
        col_name = data_columns[col]
        data_type = column_types_list[col]
        column_values = dataframe[col_name].values

        # Per-column validation for object types (check first non-null value only)
        if data_type in (TS_DATATYPE_DATE, TS_DATATYPE_STRING, TS_DATATYPE_TEXT, TS_DATATYPE_BLOB):
            col_series = dataframe[col_name]
            first_valid_idx = col_series.first_valid_index()
            if first_valid_idx is not None:
                value = col_series[first_valid_idx]
                if data_type == TS_DATATYPE_DATE:
                    if not isinstance(value, date_type):
                        raise TypeMismatchError(context=
                            f"Column '{col_name}': expected DATE (datetime.date), "
                            f"got {type(value).__name__}: {value!r}"
                        )
                elif data_type in (TS_DATATYPE_STRING, TS_DATATYPE_TEXT):
                    if not isinstance(value, str):
                        raise TypeMismatchError(context=
                            f"Column '{col_name}': expected STRING/TEXT, "
                            f"got {type(value).__name__}: {value!r}"
                        )
                elif data_type == TS_DATATYPE_BLOB:
                    if not isinstance(value, bytes):
                        raise TypeMismatchError(context=
                            f"Column '{col_name}': expected BLOB (bytes or bytearray), "
                            f"got {type(value).__name__}: {value!r}"
                        )

        # BOOLEAN
        if data_type == TS_DATATYPE_BOOLEAN:
            for row in range(max_row_num):
                value = column_values[row]
                if not pd.isna(value):
                    tablet_add_value_by_index_bool(ctablet, row, col, <bint> value)
        # INT32
        elif data_type == TS_DATATYPE_INT32:
            for row in range(max_row_num):
                value = column_values[row]
                if not pd.isna(value):
                    tablet_add_value_by_index_int32_t(ctablet, row, col, <int32_t> value)
        # INT64
        elif data_type == TS_DATATYPE_INT64 or data_type == TS_DATATYPE_TIMESTAMP:
            for row in range(max_row_num):
                value = column_values[row]
                if not pd.isna(value):
                    tablet_add_value_by_index_int64_t(ctablet, row, col, <int64_t> value)
        # FLOAT
        elif data_type == TS_DATATYPE_FLOAT:
            for row in range(max_row_num):
                value = column_values[row]
                if not pd.isna(value):
                    tablet_add_value_by_index_float(ctablet, row, col, <float> value)
        # DOUBLE
        elif data_type == TS_DATATYPE_DOUBLE:
            for row in range(max_row_num):
                value = column_values[row]
                if not pd.isna(value):
                    tablet_add_value_by_index_double(ctablet, row, col, <double> value)
        # DATE (validated per-column above)
        elif data_type == TS_DATATYPE_DATE:
            for row in range(max_row_num):
                value = column_values[row]
                if not pd.isna(value):
                    tablet_add_value_by_index_int32_t(ctablet, row, col, parse_date_to_int(value))
        # STRING or TEXT (validated per-column above)
        elif data_type == TS_DATATYPE_STRING or data_type == TS_DATATYPE_TEXT:
            for row in range(max_row_num):
                value = column_values[row]
                if not pd.isna(value):
                    py_value = str(value)
                    str_ptr = PyUnicode_AsUTF8AndSize(py_value, &raw_len)
                    tablet_add_value_by_index_string_with_len(ctablet, row, col, str_ptr, raw_len)
        # BLOB (validated per-column above)
        elif data_type == TS_DATATYPE_BLOB:
            for row in range(max_row_num):
                value = column_values[row]
                if not pd.isna(value):
                    if isinstance(value, bytes):
                        PyBytes_AsStringAndSize(value, &raw_str, &raw_len)
                        tablet_add_value_by_index_string_with_len(ctablet, row, col, raw_str, raw_len)
                    else:
                        value_bytes = bytes(value)
                        PyBytes_AsStringAndSize(value_bytes, &raw_str, &raw_len)
                        tablet_add_value_by_index_string_with_len(ctablet, row, col, raw_str, raw_len)

    return ctablet

cdef TsRecord to_c_record(object row_record):
    cdef int field_num = row_record.get_fields_num()
    cdef int64_t timestamp = <int64_t> row_record.get_timestamp()
    cdef bytes device_id_bytes = PyUnicode_AsUTF8String(row_record.get_device_id())
    cdef const char * device_id = device_id_bytes
    cdef const char * str_ptr
    cdef char * blob_ptr
    cdef Py_ssize_t str_len
    cdef TsRecord record
    cdef int i
    cdef TSDataType data_type
    record = _ts_record_new(device_id, timestamp, field_num)
    for i in range(field_num):
        field = row_record.get_fields()[i]
        data_type = to_c_data_type(field.get_data_type())
        if data_type == TS_DATATYPE_BOOLEAN:
            _insert_data_into_ts_record_by_name_bool(record, PyUnicode_AsUTF8(field.get_field_name()),
                                                     field.get_bool_value())
        elif data_type == TS_DATATYPE_INT32 or data_type == TS_DATATYPE_DATE:
            _insert_data_into_ts_record_by_name_int32_t(record, PyUnicode_AsUTF8(field.get_field_name()),
                                                        field.get_int_value())
        elif data_type == TS_DATATYPE_INT64:
            _insert_data_into_ts_record_by_name_int64_t(record, PyUnicode_AsUTF8(field.get_field_name()),
                                                        field.get_long_value())
        elif data_type == TS_DATATYPE_TIMESTAMP:
            _insert_data_into_ts_record_by_name_int64_t(record, PyUnicode_AsUTF8(field.get_field_name()),
                                                        field.get_timestamp_value())
        elif data_type == TS_DATATYPE_DOUBLE:
            _insert_data_into_ts_record_by_name_double(record, PyUnicode_AsUTF8(field.get_field_name()),
                                                       field.get_double_value())
        elif data_type == TS_DATATYPE_FLOAT:
            _insert_data_into_ts_record_by_name_float(record, PyUnicode_AsUTF8(field.get_field_name()),
                                                      field.get_float_value())
        elif data_type == TS_DATATYPE_TEXT or data_type == TS_DATATYPE_STRING:
            str_ptr = PyUnicode_AsUTF8AndSize(field.get_string_value(), &str_len)
            _insert_data_into_ts_record_by_name_string_with_len(record, PyUnicode_AsUTF8(field.get_field_name()),
                                                                str_ptr, str_len)
        elif data_type == TS_DATATYPE_BLOB:
            if PyBytes_AsStringAndSize(field.get_string_value(), &blob_ptr, &str_len) < 0:
                raise ValueError("blob not legal")
            _insert_data_into_ts_record_by_name_string_with_len(record, PyUnicode_AsUTF8(field.get_field_name()),
                                                                <const char *> blob_ptr, <uint32_t> str_len)
    return record

# Free c structs' space
cdef void free_c_table_schema(TableSchema * c_schema):
    free(c_schema.table_name)
    for i in range(c_schema.column_num):
        free_c_column_schema(&(c_schema.column_schemas[i]))
    free(c_schema.column_schemas)

cdef void free_c_column_schema(ColumnSchema * c_schema):
    free(c_schema.column_name)

cdef void free_c_timeseries_schema(TimeseriesSchema * c_schema):
    free(c_schema.timeseries_name)

cdef void free_c_device_schema(DeviceSchema * c_schema):
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
    cdef const char * c_path = encoded_path
    writer = _tsfile_writer_new(c_path, memory_threshold, &errno)
    check_error(errno)
    return writer

cdef TsFileReader tsfile_reader_new_c(object pathname) except +:
    cdef ErrorCode errno = 0
    cdef TsFileReader reader
    cdef bytes encoded_path = PyUnicode_AsUTF8String(pathname)
    cdef const char * c_path = encoded_path
    reader = tsfile_reader_new(c_path, &errno)
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
        "time_encoding_type_": TSEncodingPy(int(g_config_value_.time_encoding_type_)),
        "time_data_type_": TSDataTypePy(int(g_config_value_.time_data_type_)),
        "time_compress_type_": CompressorPy(int(g_config_value_.time_compress_type_)),
        "chunk_group_size_threshold_": g_config_value_.chunk_group_size_threshold_,
        "record_count_for_next_mem_check_": g_config_value_.record_count_for_next_mem_check_,
        "encrypt_flag_": g_config_value_.encrypt_flag_,
        "boolean_encoding_type_": TSEncodingPy(int(g_config_value_.boolean_encoding_type_)),
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
        g_config_value_.tsfile_index_bloom_filter_error_percent_ = new_config[
            "tsfile_index_bloom_filter_error_percent_"]
    if "time_encoding_type_" in new_config:
        if not isinstance(new_config["time_encoding_type_"], TSEncodingPy):
            raise TypeError(f"Unsupported TSEncoding: {new_config['time_encoding_type_']}")
        code = set_global_time_encoding(<uint8_t> (new_config["time_encoding_type_"].value))
        check_error(code)
    if "time_data_type_" in new_config:
        if not isinstance(new_config["time_data_type_"], TSDataTypePy):
            raise TypeError(f"Unsupported TSDataType: {new_config['time_data_type_']}")
        code = set_global_time_data_type(<uint8_t> (new_config["time_data_type_"].value))
        check_error(code)
    if "time_compress_type_" in new_config:
        if not isinstance(new_config["time_compress_type_"], CompressorPy):
            raise TypeError(f"Unsupported Compressor: {new_config['time_compress_type_']}")
        code = set_global_time_compression(<uint8_t> (new_config["time_compress_type_"].value))
        check_error(code)
    if "chunk_group_size_threshold_" in new_config:
        _check_uint32(new_config["chunk_group_size_threshold_"])
        g_config_value_.chunk_group_size_threshold_ = new_config["chunk_group_size_threshold_"]
    if "record_count_for_next_mem_check_" in new_config:
        _check_uint32(new_config["record_count_for_next_mem_check_"])
        g_config_value_.record_count_for_next_mem_check_ = new_config["record_count_for_next_mem_check_"]
    if "encrypt_flag_" in new_config:
        _check_bool(new_config["encrypt_flag_"])
        g_config_value_.encrypt_flag_ = <bint> new_config["encrypt_flag_"]

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
    cdef const char * c_device_name = encoded_device_name
    errno = _tsfile_writer_register_timeseries(writer, c_device_name, schema)
    return errno

cdef ErrorCode tsfile_writer_register_table_py_cpp(TsFileWriter writer, TableSchema *schema):
    cdef ErrorCode errno
    errno = _tsfile_writer_register_table(writer, schema)
    return errno

cdef bint tsfile_result_set_is_null_by_name_c(ResultSet result_set, object name):
    cdef bytes encoded_name = PyUnicode_AsUTF8String(name)
    cdef const char * c_name = encoded_name
    return tsfile_result_set_is_null_by_name(result_set, c_name)

cdef ResultSet tsfile_reader_query_table_c(TsFileReader reader, object table_name, object column_list,
                                           int64_t start_time, int64_t end_time):
    cdef ResultSet result
    cdef int column_num = len(column_list)
    cdef bytes table_name_bytes = PyUnicode_AsUTF8String(table_name)
    cdef const char * table_name_c = table_name_bytes
    cdef char** columns = <char**> malloc(sizeof(char *) * column_num)
    cdef int i
    cdef ErrorCode code = 0
    if columns == NULL:
        raise MemoryError("Failed to allocate memory for columns")
    try:
        for i in range(column_num):
            columns[i] = strdup((<str> column_list[i]).encode('utf-8'))
            if columns[i] == NULL:
                raise MemoryError("Failed to allocate memory for column name")
        result = tsfile_query_table(reader, table_name_c, columns, column_num, start_time, end_time, &code)
        check_error(code)
        return result
    finally:
        if columns != NULL:
            for i in range(column_num):
                free(<void *> columns[i])
                columns[i] = NULL
            free(<void *> columns)
            columns = NULL

cdef ResultSet tsfile_reader_query_table_on_tree_c(TsFileReader reader, object column_list,
                                                   int64_t start_time, int64_t end_time):
    cdef ResultSet result
    cdef int column_num = len(column_list)
    cdef char** columns = <char**> malloc(sizeof(char *) * column_num)
    cdef int i
    cdef ErrorCode code = 0
    if columns == NULL:
        raise MemoryError("Failed to allocate memory for columns")
    try:
        for i in range(column_num):
            columns[i] = strdup((<str> column_list[i]).encode('utf-8'))
            if columns[i] == NULL:
                raise MemoryError("Failed to allocate memory for column name")
        result = tsfile_query_table_on_tree(reader, columns, column_num, start_time, end_time, &code)
        check_error(code)
        return result
    finally:
        if columns != NULL:
            for i in range(column_num):
                free(<void *> columns[i])
                columns[i] = NULL
            free(<void *> columns)
            columns = NULL

cdef ResultSet tsfile_reader_query_tree_by_row_c(TsFileReader reader,
                                                 object device_ids,
                                                 object measurement_names,
                                                 int offset, int limit):
    cdef ResultSet result
    cdef int device_num = len(device_ids)
    cdef int measurement_num = len(measurement_names)
    cdef char** device_ids_c = <char**> malloc(sizeof(char *) * device_num)
    cdef char** measurement_names_c = <char**> malloc(sizeof(char *) * measurement_num)
    cdef int i
    cdef int j
    cdef ErrorCode code = 0

    if device_ids_c == NULL or measurement_names_c == NULL:
        raise MemoryError("Failed to allocate memory for tree by-row query arrays")

    try:
        for i in range(device_num):
            device_ids_c[i] = strdup((<str> device_ids[i]).encode('utf-8'))
            if device_ids_c[i] == NULL:
                raise MemoryError("Failed to allocate memory for device id")
        for j in range(measurement_num):
            measurement_names_c[j] = strdup((<str> measurement_names[j]).encode('utf-8'))
            if measurement_names_c[j] == NULL:
                raise MemoryError("Failed to allocate memory for measurement name")

        result = tsfile_reader_query_tree_by_row(reader,
                                                  device_ids_c, device_num,
                                                  measurement_names_c, measurement_num,
                                                  offset, limit, &code)
        check_error(code)
        return result
    finally:
        if device_ids_c != NULL:
            for i in range(device_num):
                if device_ids_c[i] != NULL:
                    free(<void *> device_ids_c[i])
                    device_ids_c[i] = NULL
            free(<void *> device_ids_c)
            device_ids_c = NULL
        if measurement_names_c != NULL:
            for j in range(measurement_num):
                if measurement_names_c[j] != NULL:
                    free(<void *> measurement_names_c[j])
                    measurement_names_c[j] = NULL
            free(<void *> measurement_names_c)
            measurement_names_c = NULL

cdef ResultSet tsfile_reader_query_table_by_row_c(TsFileReader reader,
                                                   object table_name,
                                                   object column_list,
                                                   int offset, int limit):
    cdef ResultSet result
    cdef int column_num = len(column_list)
    cdef char** columns = <char**> malloc(sizeof(char *) * column_num)
    cdef int i
    cdef bytes table_name_bytes = PyUnicode_AsUTF8String(table_name)
    cdef const char * table_name_c = table_name_bytes
    cdef ErrorCode code = 0

    if columns == NULL:
        raise MemoryError("Failed to allocate memory for table by-row query columns")
    try:
        for i in range(column_num):
            columns[i] = strdup((<str> column_list[i]).encode('utf-8'))
            if columns[i] == NULL:
                raise MemoryError("Failed to allocate memory for column name")

        result = tsfile_reader_query_table_by_row(reader,
                                                   table_name_c, columns, column_num,
                                                   offset, limit, &code)
        check_error(code)
        return result
    finally:
        if columns != NULL:
            for i in range(column_num):
                if columns[i] != NULL:
                    free(<void *> columns[i])
                    columns[i] = NULL
            free(<void *> columns)
            columns = NULL

cdef ResultSet tsfile_reader_query_table_batch_c(TsFileReader reader, object table_name, object column_list,
                                                 int64_t start_time, int64_t end_time, int batch_size):
    cdef ResultSet result
    cdef int column_num = len(column_list)
    cdef bytes table_name_bytes = PyUnicode_AsUTF8String(table_name)
    cdef const char * table_name_c = table_name_bytes
    cdef char** columns = <char**> malloc(sizeof(char *) * column_num)
    cdef int i
    cdef ErrorCode code = 0
    if columns == NULL:
        raise MemoryError("Failed to allocate memory for columns")
    try:
        for i in range(column_num):
            columns[i] = strdup((<str> column_list[i]).encode('utf-8'))
            if columns[i] == NULL:
                raise MemoryError("Failed to allocate memory for column name")
        result = tsfile_query_table_batch(reader, table_name_c, columns,
                                          column_num, start_time, end_time,
                                          batch_size, &code)
        check_error(code)
        return result
    finally:
        if columns != NULL:
            for i in range(column_num):
                if columns[i] != NULL:
                    free(<void *> columns[i])
                    columns[i] = NULL
            free(<void *> columns)
            columns = NULL

cdef ResultSet tsfile_reader_query_paths_c(TsFileReader reader, object device_name, object sensor_list,
                                           int64_t start_time,
                                           int64_t end_time):
    cdef ResultSet result
    cdef int path_num = len(sensor_list)
    cdef char** sensor_list_c = <char**> malloc(sizeof(char *) * path_num)
    cdef bytes device_name_bytes = PyUnicode_AsUTF8String(device_name)
    cdef const char * device_name_c = device_name_bytes
    cdef int i
    cdef ErrorCode code = 0
    if sensor_list_c == NULL:
        raise MemoryError("Failed to allocate memory for paths")
    try:
        for i in range(path_num):
            sensor_list_c[i] = strdup((<str> sensor_list[i]).encode('utf-8'))
            if sensor_list_c[i] == NULL:
                raise MemoryError("Failed to allocate memory for path")
        result = _tsfile_reader_query_device(reader, device_name_c, sensor_list_c, path_num, start_time, end_time,
                                             &code)
        check_error(code)
        return result
    finally:
        if sensor_list_c != NULL:
            for i in range(path_num):
                if sensor_list_c[i] != NULL:
                    free(<void *> sensor_list_c[i])
                    sensor_list_c[i] = NULL
            free(<void *> sensor_list_c)
            sensor_list_c = NULL

cdef object get_table_schema(TsFileReader reader, object table_name):
    cdef bytes table_name_bytes = PyUnicode_AsUTF8String(table_name)
    cdef const char * table_name_c = table_name_bytes
    cdef TableSchema schema = tsfile_reader_get_table_schema(reader, table_name_c)
    return from_c_table_schema(schema)

cdef object get_all_table_schema(TsFileReader reader):
    cdef uint32_t table_num = 0
    cdef TableSchema * schemas
    cdef int i

    table_schemas = {}
    schemas = tsfile_reader_get_all_table_schemas(reader, &table_num)
    for i in range(table_num):
        schema_py = from_c_table_schema(schemas[i])
        table_schemas.update([(schema_py.get_table_name(), schema_py)])
    free(schemas)
    return table_schemas

cdef object get_all_timeseries_schema(TsFileReader reader):
    cdef uint32_t device_num = 0
    cdef DeviceSchema * schemas
    cdef int i

    device_schemas = {}
    schemas = tsfile_reader_get_all_timeseries_schemas(reader, &device_num)
    for i in range(device_num):
        schema_py = from_c_device_schema(schemas[i])
        device_schemas.update([(schema_py.get_device_name(), schema_py)])
    free(schemas)
    return device_schemas

cdef object _c_str_to_py_utf8_or_none(char* p):
    if p == NULL:
        return None
    return p.decode('utf-8')

cdef object timeseries_statistic_c_to_py(TimeseriesStatistic* s):
    cdef TsFileStatisticBase* b
    cdef TSDataType dt
    if s == NULL:
        return TimeseriesStatisticPy(False, 0, 0, 0)
    b = <TsFileStatisticBase*>&s.u
    if not b.has_statistic:
        return TimeseriesStatisticPy(
            False, int(b.row_count), int(b.start_time), int(b.end_time))
    dt = b.type
    if dt == TS_DATATYPE_INVALID:
        return TimeseriesStatisticPy(
            True, int(b.row_count), int(b.start_time), int(b.end_time))
    if (dt == TS_DATATYPE_INT32 or dt == TS_DATATYPE_DATE or
            dt == TS_DATATYPE_INT64 or dt == TS_DATATYPE_TIMESTAMP):
        return IntTimeseriesStatisticPy(
            True, int(b.row_count), int(b.start_time), int(b.end_time),
            float(s.u.int_s.sum),
            int(s.u.int_s.min_int64),
            int(s.u.int_s.max_int64),
            int(s.u.int_s.first_int64),
            int(s.u.int_s.last_int64),
        )
    if dt == TS_DATATYPE_FLOAT or dt == TS_DATATYPE_DOUBLE:
        return FloatTimeseriesStatisticPy(
            True, int(b.row_count), int(b.start_time), int(b.end_time),
            float(s.u.float_s.sum),
            float(s.u.float_s.min_float64),
            float(s.u.float_s.max_float64),
            float(s.u.float_s.first_float64),
            float(s.u.float_s.last_float64),
        )
    if dt == TS_DATATYPE_BOOLEAN:
        return BoolTimeseriesStatisticPy(
            True, int(b.row_count), int(b.start_time), int(b.end_time),
            float(s.u.bool_s.sum),
            bool(s.u.bool_s.first_bool),
            bool(s.u.bool_s.last_bool),
        )
    if dt == TS_DATATYPE_STRING:
        return StringTimeseriesStatisticPy(
            True, int(b.row_count), int(b.start_time), int(b.end_time),
            _c_str_to_py_utf8_or_none(s.u.string_s.str_min),
            _c_str_to_py_utf8_or_none(s.u.string_s.str_max),
            _c_str_to_py_utf8_or_none(s.u.string_s.str_first),
            _c_str_to_py_utf8_or_none(s.u.string_s.str_last),
        )
    if dt == TS_DATATYPE_TEXT:
        return TextTimeseriesStatisticPy(
            True, int(b.row_count), int(b.start_time), int(b.end_time),
            _c_str_to_py_utf8_or_none(s.u.text_s.str_first),
            _c_str_to_py_utf8_or_none(s.u.text_s.str_last),
        )
    return TimeseriesStatisticPy(
        True, int(b.row_count), int(b.start_time), int(b.end_time))

cdef object timeseries_metadata_c_to_py(TimeseriesMetadata* m):
    cdef str name_py
    if m == NULL or m.measurement_name == NULL:
        name_py = ""
    else:
        name_py = m.measurement_name.decode('utf-8')
    cdef object stat = timeseries_statistic_c_to_py(&m.statistic)
    return TimeseriesMetadataPy(
        name_py,
        TSDataTypePy(m.data_type),
        int(m.chunk_meta_count),
        stat,
    )

cdef tuple c_device_segments_to_tuple(char** segs, uint32_t n):
    cdef uint32_t i
    cdef list out = []
    for i in range(n):
        if segs == NULL or segs[i] == NULL:
            out.append(None)
        else:
            out.append(segs[i].decode('utf-8'))
    return tuple(out)

cdef dict device_timeseries_metadata_map_to_py(DeviceTimeseriesMetadataMap* mmap):
    cdef dict out = {}
    cdef uint32_t di, ti
    cdef char* p
    cdef char* tnp
    cdef object key
    cdef object table_py
    cdef tuple segs_py
    cdef list series
    for di in range(mmap.device_count):
        p = mmap.entries[di].device.path
        if p == NULL:
            key = None
        else:
            key = p.decode('utf-8')
        tnp = mmap.entries[di].device.table_name
        if tnp == NULL:
            table_py = None
        else:
            table_py = tnp.decode('utf-8')
        segs_py = c_device_segments_to_tuple(
            mmap.entries[di].device.segments,
            mmap.entries[di].device.segment_count)
        series = []
        for ti in range(mmap.entries[di].timeseries_count):
            series.append(
                timeseries_metadata_c_to_py(
                    &mmap.entries[di].timeseries[ti]))
        out[key] = DeviceTimeseriesMetadataGroupPy(
            table_py, segs_py, series)
    return out

cdef public api object reader_get_all_devices_c(TsFileReader reader):
    cdef DeviceID* arr = NULL
    cdef uint32_t n = 0
    cdef int err
    cdef list out = []
    cdef uint32_t i
    cdef object path_py
    cdef object tname_py
    cdef tuple segs_py
    err = tsfile_reader_get_all_devices(reader, &arr, &n)
    check_error(err)
    try:
        for i in range(n):
            if arr[i].path == NULL:
                path_py = None
            else:
                path_py = arr[i].path.decode('utf-8')
            if arr[i].table_name == NULL:
                tname_py = None
            else:
                tname_py = arr[i].table_name.decode('utf-8')
            segs_py = c_device_segments_to_tuple(arr[i].segments,
                                                 arr[i].segment_count)
            out.append(DeviceIDPy(path_py, tname_py, segs_py))
    finally:
        tsfile_free_device_id_array(arr, n)
    return out

cdef public api object reader_get_timeseries_metadata_c(TsFileReader reader,
                                                        object device_ids):
    cdef DeviceTimeseriesMetadataMap mmap
    cdef DeviceID* q = NULL
    cdef uint32_t qlen = 0
    cdef uint32_t i
    cdef int err
    cdef bytes bpath
    cdef const char* raw
    memset(&mmap, 0, sizeof(DeviceTimeseriesMetadataMap))
    if device_ids is None:
        err = tsfile_reader_get_timeseries_metadata_all(reader, &mmap)
        check_error(err)
    elif len(device_ids) == 0:
        err = tsfile_reader_get_timeseries_metadata_for_devices(
            reader, NULL, 0, &mmap)
        check_error(err)
    else:
        qlen = <uint32_t> len(device_ids)
        q = <DeviceID*> malloc(sizeof(DeviceID) * qlen)
        if q == NULL:
            raise MemoryError()
        memset(q, 0, sizeof(DeviceID) * qlen)
        try:
            for i in range(qlen):
                dev = device_ids[i]
                try:
                    path_s = dev.path
                except AttributeError:
                    path_s = str(dev)
                bpath = path_s.encode('utf-8')
                raw = PyBytes_AsString(bpath)
                q[i].path = strdup(raw)
                if q[i].path == NULL:
                    raise MemoryError()
            err = tsfile_reader_get_timeseries_metadata_for_devices(
                reader, q, qlen, &mmap)
            check_error(err)
        finally:
            for i in range(qlen):
                free(q[i].path)
            free(q)
    try:
        return device_timeseries_metadata_map_to_py(&mmap)
    finally:
        tsfile_free_device_timeseries_metadata_map(&mmap)
