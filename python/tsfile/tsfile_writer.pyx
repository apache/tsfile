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
import pandas as pd
import numpy as np

from cpython.unicode cimport PyUnicode_AsUTF8String

from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.string cimport strdup

from .tsfile_cpp cimport *
from .tsfile_py_cpp cimport *
import numpy as np
import pandas as pd
cimport numpy as cnp

from tsfile.row_record import RowRecord
from tsfile.schema import TimeseriesSchema as TimeseriesSchemaPy, DeviceSchema as DeviceSchemaPy
from tsfile.schema import TableSchema as TableSchemaPy
from tsfile.tablet import Tablet as TabletPy
from tsfile.constants import TSDataType as TSDataTypePy

_pandas_dtype_to_ts = {
    "bool": TSDataTypePy.BOOLEAN,
    "int32": TSDataTypePy.INT32,
    "int64": TSDataTypePy.INT64,
    "float32": TSDataTypePy.FLOAT,
    "float64": TSDataTypePy.DOUBLE,
    "string": TSDataTypePy.STRING
}

cdef bint is_compatible(object expected, object actual):
    if expected == actual:
        return True
    if expected == TSDataTypePy.INT64 and actual == TSDataTypePy.INT32:
        return True
    if expected == TSDataTypePy.DOUBLE and actual == TSDataTypePy.FLOAT:
        return True
    return False

cdef object convert_series(object series, object target):
    dtype_map = {
        TSDataTypePy.INT64: "int64",
        TSDataTypePy.INT32: "int32",
        TSDataTypePy.FLOAT: "float32",
        TSDataTypePy.DOUBLE: "float64",
        TSDataTypePy.BOOLEAN: "bool",
        TSDataTypePy.STRING: "str",
    }

    target_str = dtype_map.get(target)
    if str(series.dtype) == target_str:
        return series.to_numpy()
    return series.astype(target_str).to_numpy()
cdef encode_or_null(x):
    if pd.isna(x):
        return None
    return str(x).encode('utf-8')

cdef class CTablet:
    cdef Tablet tablet
    cdef object column_name
    cdef object data_type
    cdef object max_row_num
    cdef char** column_names
    cdef int column_num
    cdef TSDataType * column_data_types

    def __init__(self, column_name: list[str], data_types: list[TSDataTypePy], max_row_num: int = 1024):

        self.column_name = column_name
        self.data_type = data_types
        self.max_row_num = max_row_num
        self.column_num = len(column_name)
        if len(data_types) != self.column_num:
            raise ValueError("Length of column_name and data_types must be equal")
        self.column_names = <char**> malloc(sizeof(char*) * self.column_num)
        self.column_data_types = <TSDataType*> malloc(sizeof(TSDataType) * self.column_num)

        ind = 0
        for name, dtype in zip(column_name, data_types):
            self.column_names[ind] = strdup(name.encode('utf-8'))
            self.column_data_types[ind] = to_c_data_type(dtype)
            ind = ind + 1

    cpdef set_target_name(self, object target_name):
        cdef bytes device_id_bytes
        cdef char * device_id_c
        device_id_bytes = PyUnicode_AsUTF8String(target_name)
        device_id_c = device_id_bytes
        _tablet_set_target_name(self.tablet, device_id_c)
    cdef Tablet get_tablet(self):
        return self.tablet

    cdef init_c_tablet(self):
        if self.tablet != NULL:
            free_tablet(&self.tablet)
        self.tablet = tablet_new(self.column_names, self.column_data_types, self.column_num,  self.max_row_num)

    cpdef from_data_frame(self, data_frame: pd.DataFrame):
        cdef void * data_ptr
        cdef char * mask_ptr
        cdef size_t length
        cdef char** str_ptr
        cdef bytes item
        cdef int64_t* time_ptr
        cdef cnp.ndarray[cnp.int64_t, ndim=1] time_array
        if not isinstance(data_frame, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame")
        if data_frame.shape[1] != len(self.column_name) + 1:
            raise ValueError(f"DataFrame column count {data_frame.shape[1]} doesn't match expected {len(self.column_name) + 1}")

        if "time" not in data_frame.columns:
            raise ValueError("Missing required column: 'time'")
        if not pd.api.types.is_integer_dtype(data_frame["time"]):
            raise TypeError("Column 'time' must be of integer type")
        if data_frame["time"].dtype != np.int64:
            raise TypeError(f"Column 'time' must be int64, but got {data_frame['time'].dtype}")

        if self.max_row_num !=len(data_frame["time"]):
            raise ValueError(f"Time column length {len(data_frame['time'])} doesn't match expected {self.max_row_num}")

        self.init_c_tablet()
        time_array = data_frame["time"].to_numpy()
        time_ptr = <int64_t *>time_array.data
        _tablet_set_batch_timestamp(self.tablet, time_ptr)

        for ind, col_name in enumerate(self.column_name):
            if col_name not in data_frame.columns:
                raise KeyError(f"Column '{col_name}' missing from DataFrame")
            series = data_frame[col_name]
            dtype_str = str(series.dtype)
            if dtype_str not in _pandas_dtype_to_ts:
                raise TypeError(f"Unsupported pandas dtype {dtype_str} for column {col_name}")
            actual_ts_type = _pandas_dtype_to_ts[dtype_str]
            expected_ts_type = self.data_type[ind]
            if not is_compatible(expected_ts_type, actual_ts_type):
                raise TypeError(
                    f"Column '{col_name}' type mismatch: expected {expected_ts_type.name}, got {actual_ts_type.name}")

            if expected_ts_type == TSDataTypePy.STRING:
                str_ptr = <char**> malloc(sizeof(char*) * self.max_row_num)
                array = series.fillna("").astype(str).apply(encode_or_null).to_numpy(dtype=object)
                for i in range(self.max_row_num):
                    if array[i] is None:
                        str_ptr[i] = NULL
                    else:
                        str_ptr[i] = strdup(<char*>array[i])
                _tablet_set_batch_str(self.tablet, ind, str_ptr)
                for i in range(self.max_row_num):
                    if str_ptr[i] != NULL:
                        free(str_ptr[i])
                free(str_ptr)
            else:
                array = convert_series(series, expected_ts_type)
                mask = series.notna().to_numpy().astype(np.byte)
                data_ptr = <void*>cnp.PyArray_DATA(array)
                mask_ptr = <char*> cnp.PyArray_DATA(mask)
                _tablet_set_batch_data(self.tablet, ind, data_ptr, mask_ptr)

    def __dealloc__(self):
        if self.tablet != NULL:
            free_tablet(&self.tablet)
            self.tablet = NULL

        if self.column_names != NULL:
            for i in range(self.column_num):
                free(self.column_names[i])
            free(self.column_names)
            self.column_names = NULL

        if self.column_data_types != NULL:
            free(self.column_data_types)
            self.column_data_types = NULL

cdef class TsFileWriterPy:
    cdef TsFileWriter writer

    def __init__(self, pathname):
        self.writer = tsfile_writer_new_c(pathname)

    def register_timeseries(self, device_name : str, timeseries_schema : TimeseriesSchemaPy):
        """
        Register a timeseries with tsfile writer.
        device_name: device name of the timeseries
        timeseries_schema: measurement's name/datatype/encoding/compressor
        """
        cdef TimeseriesSchema* c_schema = to_c_timeseries_schema(timeseries_schema)
        cdef ErrorCode errno
        try:
            errno = tsfile_writer_register_timeseries_py_cpp(self.writer, device_name, c_schema)
            check_error(errno)
        finally:
            free_c_timeseries_schema(c_schema)

    def register_device(self, device_schema : DeviceSchemaPy):
        """
        Register a device with tsfile writer.
        device_schema: the device definition, including device_name, some measurements' schema.
        """
        cdef DeviceSchema* device_schema_c = to_c_device_schema(device_schema)
        cdef ErrorCode errno
        try:
            errno = tsfile_writer_register_device_py_cpp(self.writer, device_schema_c)
            check_error(errno)
        finally:
            free_c_device_schema(device_schema_c)

    def register_table(self, table_schema : TableSchemaPy):
        """
        Register a table with tsfile writer.
        table_schema: the table definition, include table_name, columns' schema.
        """
        cdef TableSchema* c_schema = to_c_table_schema(table_schema)
        cdef ErrorCode errno
        try:
            errno = tsfile_writer_register_table_py_cpp(self.writer, c_schema)
            check_error(errno)
        finally:
            free_c_table_schema(c_schema)

    def write_tablet(self, tablet : TabletPy):
        """
        Write a tablet into tsfile with tsfile writer.
        tablet: data collection to be inserted
        Currently used for writing data into a device.
        """
        cdef Tablet ctablet = to_c_tablet(tablet)
        cdef ErrorCode errno
        try:
            errno = _tsfile_writer_write_tablet(self.writer, ctablet)
            check_error(errno)
        finally:
            free_c_tablet(ctablet)

    def write_row_record(self, record : RowRecord):
        """
        Write a record into tsfile with tsfile writer.
        record: timestamp and data collection
        Currently used for writing a row data into a device.
        """
        cdef TsRecord record_c = to_c_record(record)
        cdef ErrorCode errno
        try:
            errno = _tsfile_writer_write_ts_record(self.writer, record_c)
            check_error(errno)
        finally:
            free_c_row_record(record_c)

    def write_table(self, tablet : TabletPy):
        """
        Write a tablet data into a table in tsfile writer.
        Currently used for writing data into table.
        """
        cdef Tablet ctablet = to_c_tablet(tablet)
        cdef ErrorCode errno
        try:
            errno = _tsfile_writer_write_table(self.writer, ctablet)
            check_error(errno)
        finally:
            free_c_tablet(ctablet)


    def write_ctablet(self, tablet: CTablet):
        cdef ErrorCode errno
        errno = _tsfile_writer_write_table(self.writer, tablet.get_tablet())
        check_error(errno)


    cpdef close(self):
        """
        Flush data and Close tsfile writer.
        """
        cdef ErrorCode errno
        if self.writer == NULL:
            return
        errno = _tsfile_writer_close(self.writer)
        check_error(errno)
        self.writer = NULL

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

