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
from libc.string cimport strcpy
from libc.stdlib cimport malloc, free
import pandas as pd
from cpython.bytes cimport PyBytes_AsString
cimport numpy as cnp
import numpy as np
from .tsfile cimport *

TIMESTAMP_STR = "Time"
TS_TYPE_INT32 = 1 << 8
TS_TYPE_BOOLEAN = 1 << 9
TS_TYPE_FLOAT = 1 << 10
TS_TYPE_DOUBLE = 1 << 11
TS_TYPE_INT64 = 1 << 12
TS_TYPE_TEXT = 1 << 13

type_mapping = {
    'int32': TS_TYPE_INT32,
    'bool': TS_TYPE_BOOLEAN,
    'float32': TS_TYPE_FLOAT,
    'float64': TS_TYPE_DOUBLE,
    'int64': TS_TYPE_INT64
}

cdef class tsfile_reader:
    
    cdef CTsFileReader reader
    cdef QueryDataRet ret
    cdef int batch_size
    

    def __init__(self, pathname, table_name, columns, start_time=None, end_time=None, batch_size=None):
        self.open_reader(pathname)
        self.query_data_ret(table_name, columns, start_time, end_time)

        if batch_size is not None:
            self.batch_size = batch_size
        else:
            self.batch_size = -1

    cdef open_reader(self, pathname):
        cdef ErrorCode err_code
        err_code = 0
        self.reader = ts_reader_open(pathname.encode('utf-8'), &err_code)
        if (err_code != 0):
            raise Exception("Failed to open tsfile: %s, %s" %( pathname, err_code))
    
    cdef query_data_ret(self, table_name, columns, start_time = None, end_time=None):
        cdef bytes py_table_name
        cdef char** c_columns
        py_table_name = table_name.encode('utf-8')
        c_table_name = PyBytes_AsString(py_table_name)
        if isinstance(columns, str):
            columns = [columns]
        
        c_columns = <char**>malloc(len(columns) * sizeof(char*))
        if not c_columns:
            raise MemoryError("Failed to allocate memory for columns")

        for i in range(len(columns)):
            c_columns[i] = <char*>malloc(len(columns[i]) + 1)
            if not c_columns[i]:
                for j in range(i):
                    free(c_columns[j])
                free(c_columns)
                raise MemoryError("Failed to allocate memory for columns")
            column_binary = columns[i].encode('utf-8')
            column = PyBytes_AsString(column_binary)
            strcpy(c_columns[i], column)
        # query data from tsfile
        if start_time is not None or end_time is not None:
            if start_time is None:
                start_time = -1
            if end_time is None:
                end_time = -1
            self.ret = ts_reader_begin_end(self.reader, c_table_name, c_columns, len(columns), start_time, end_time)
        else:
            self.ret = ts_reader_read(self.reader, table_name.encode('utf-8'), c_columns, len(columns))
        
        
    def read_tsfile(self):
        # open tsfile to read
        res = pd.DataFrame()
        if self.batch_size == -1:
            self.batch_size = 1024
            while True:
                chunk = self.get_next_dataframe()
                if chunk is not None:
                    res = pd.concat([res, chunk])
                else:
                    break
        else:
            res = self.get_next_dataframe()
        self.free_resources()
        return res

    def __iter__(self):
        return self
    
    def __next__(self):
        res = self.get_next_dataframe()
        if res is None:
            raise StopIteration
        return res

    def get_next_dataframe(self):
        cdef:
            DataResult* result
            ColumnSchema* schema = NULL
            cnp.ndarray[cnp.int64_t, ndim=1, mode='c'] np_array_i64
            cnp.ndarray[cnp.int32_t, ndim=1, mode='c'] np_array_i32
            cnp.ndarray[cnp.float32_t, ndim=1, mode='c'] np_array_float
            cnp.ndarray[cnp.float64_t, ndim=1, mode='c'] np_array_double
            cnp.ndarray[bint, ndim=1, mode='c'] np_array_bool
            cnp.npy_intp length 
            bint has_null
            bytes pystr
            str py_string  
        
        res = {}
        column_order = []

        # Time column will be the first column
        column_order.append(TIMESTAMP_STR)

        for i in range(self.ret.column_num):
            pystr = self.ret.column_names[i]
            py_string = pystr.decode('utf-8')
            column_order.append(py_string)
            res[py_string] = []
        
        res[TIMESTAMP_STR] = []

        if self.ret.data == NULL:
            return None

        result = ts_next(self.ret, self.batch_size)

        # there is no data meet our requirement
        if result.column_schema == NULL:
            # free memory
            if (destory_tablet(result) != 0):
                raise Exception("Failed to destroy tablet")
            return None

        # time column
        length = result.cur_num + 1
        cdef cnp.ndarray[cnp.int64_t, ndim=1, mode='c'] data_array = \
            cnp.PyArray_SimpleNewFromData(1, &length, cnp.NPY_INT64, result.times)
        res[TIMESTAMP_STR] = np.array(data_array, dtype = np.int64)
        
        for i in range(result.column_num):

            # column name
            schema = result.column_schema[i]
            pystr = schema.name
            column_name = pystr.decode('utf-8')

            # column bitmap
            is_not_null = np.empty(length, dtype = bool)
            bool_ptr = <char*> result.bitmap[i]
            has_null = False
            for j in range(length):
                is_not_null[j] = bool_ptr[j]  != 0
                if bool_ptr[j]  == 0 and ~has_null:
                    has_null = True

        
            if schema.column_def == TS_TYPE_INT32:
                np_array_i32 = cnp.PyArray_SimpleNewFromData(1, &length, cnp.NPY_INT32, result.value[i])
                arr = np.array(np_array_i32, dtype = np.int32)

            elif schema.column_def == TS_TYPE_BOOLEAN:
                arr_bool_ = np.empty(length, dtype=np.bool_)
                bool_ptr = <char*> result.value[i]
                for j in range(length):
                    arr_bool_[j] = bool_ptr[j] != 0
                arr = np.array(arr_bool_, dtype = np.bool_)

            elif schema.column_def  == TS_TYPE_FLOAT:
                np_array_float = cnp.PyArray_SimpleNewFromData(1, &length, cnp.NPY_FLOAT32, result.value[i])
                arr = np.array(np_array_float, dtype = np.float32)
                arr = np.where(is_not_null, arr, np.nan)
                res[column_name]=arr
                continue

            elif schema.column_def == TS_TYPE_DOUBLE:
                np_array_double = cnp.PyArray_SimpleNewFromData(1, &length, cnp.NPY_FLOAT64, result.value[i])
                arr= np.array(np_array_double, dtype = np.float64)
                arr = np.where(is_not_null, arr, np.nan)
                res[column_name]=arr
                continue

            elif schema.column_def == TS_TYPE_INT64:
                np_array_i64 = cnp.PyArray_SimpleNewFromData(1, &length, cnp.NPY_INT64, result.value[i])
                arr = np.array(np_array_i64, dtype = np.int64)
            else:
                raise Exception("UnSupport column type")
            
            if has_null:
                tmp_array = np.full(length, np.nan, np.float64)
                tmp_array[is_not_null] = arr[is_not_null]
                if schema.column_def == TS_TYPE_INT32:
                    arr = pd.Series(tmp_array).astype('Int32')
                elif schema.column_def == TS_TYPE_BOOLEAN:
                    arr = pd.Series(tmp_array).astype(np.bool_)
                elif schema.column_def == TS_TYPE_INT64:
                    arr = pd.Series(tmp_array).astype('Int64')

            res[column_name] = arr
        if (destory_tablet(result) != 0):
            raise Exception("Failed to destroy tablet")
        return pd.DataFrame(res, columns = column_order)

    def __dealloc__(self):
        self.free_resources()

    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        self.free_resources()

    cdef free_resources(self):
        if self.reader:
            if ts_reader_close(self.reader) != 0 :
                raise Exception("Failed to close tsfile")
        if self.ret:
            if destory_query_dataret(self.ret) != 0:
                raise Exception("Failed to free query data ret")
        self.reader = NULL
        self.ret = NULL
        self.batch_size = -1

cdef class tsfile_writer:
    cdef CTsFileWriter writer
    cdef TsFileRowData row_data

    def __init__(self, pathname):
        self.open_writer(pathname)


    cdef open_writer(self, pathname):
        cdef ErrorCode err_code
        err_code = 0
        self.writer = ts_writer_open(pathname.encode('utf-8'), &err_code)
        if (err_code != 0):
            raise Exception("Failed to open tsfile: %s, %s" %( pathname, err_code))
    
    def resister_timeseries(self, table_name, column_name, data_type):
        cdef char* c_columns
        cdef bytes py_table_name
        cdef ColumnSchema schema
        cdef bytes encoded_column_name = column_name.encode('utf-8')
        py_table_name = table_name.encode('utf-8')
        c_table_name = PyBytes_AsString(py_table_name)
        schema.name = encoded_column_name
        schema.column_def = data_type
        if tsfile_register_table_column(self.writer, c_table_name, &schema) != 0:
            raise Exception("Failed to register timeseries")
    cdef create_row_data(self, table_name, time, column_length):
        self.row_data = create_tsfile_row(table_name.encode('utf-8'), time, column_length)
    def write_into_row_data(self, column_name, value, type):
        cdef char* c_column_name = PyBytes_AsString(column_name.encode('utf-8'))
        if type == TS_TYPE_INT32:
            insert_data_into_tsfile_row_int32(self.row_data, c_column_name, value)
        elif type == TS_TYPE_BOOLEAN:
            insert_data_into_tsfile_row_boolean(self.row_data, c_column_name, value)
        elif type == TS_TYPE_FLOAT:
            insert_data_into_tsfile_row_float(self.row_data, c_column_name, value)
        elif type == TS_TYPE_DOUBLE:
            insert_data_into_tsfile_row_double(self.row_data, c_column_name, value)
        elif type == TS_TYPE_INT64:
            insert_data_into_tsfile_row_int64(self.row_data, c_column_name,  value)
        else:
            raise TypeError("Unknown column type")
    def write_tsfile(self, table_name, df):
        column_names = df.columns.tolist()
        column_types = df.dtypes
        column_ctypes = []
        for i in range(len(column_names)):
            column_type = column_types[i].name
            if column_type in type_mapping:
                column_ctypes.append(type_mapping[column_type])
            else:
                raise TypeError("Unknown column type")
            
            if (column_names[i] != TIMESTAMP_STR):
                self.resister_timeseries(table_name, column_names[i], column_ctypes[i])

                
        for i in range(len(df)):
            time = df.iloc[i][TIMESTAMP_STR]
            self.create_row_data(table_name, time, len(column_names))
            for j in range(1, len(column_names)):
                column_name = column_names[j]
                column_value = df.iloc[i][column_name]
                column_ctype = column_ctypes[j]
                self.write_into_row_data(column_name, column_value, column_ctype)
            if tsfile_write_row_data(self.writer, self.row_data) != 0:
                raise Exception("Failed to write row data")

        if tsfile_flush_data(self.writer) != 0:
            raise Exception("Failed to flush data")
        self.row_data = NULL
        self.__exit__(None, None, None)
    def __exit__(self, exc_type, exc_value, traceback):
        if self.writer != NULL:
            if ts_writer_close(self.writer) != 0:
                raise Exception("Failed to close tsfile")
        self.writer = NULL
