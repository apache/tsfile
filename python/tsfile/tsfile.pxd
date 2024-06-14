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
cdef extern from "./TsFile-cwrapper.h":
    # common
    ctypedef int ErrorCode
    ctypedef long long timestamp
    ctypedef long long SchemaInfo



    # for read data from tsfile
    ctypedef void* CTsFileReader
    ctypedef void* TsFileRowData
    ctypedef void* QueryDataRetINTERNAL
    ctypedef void* TimeFilterExpression

    cdef struct query_data_ret:
        char** column_names
        int column_num
        QueryDataRetINTERNAL data

    ctypedef query_data_ret* QueryDataRet


    # for writer data to tsfile
    ctypedef void* CTsFileWriter
    cdef struct column_schema:
            char* name
            SchemaInfo column_def
    ctypedef column_schema ColumnSchema

    cdef struct TableSchema:
        char* table_name
        ColumnSchema** column_schema
        int column_num

    cdef struct Tablet:
        char* table_name
        ColumnSchema** column_schema
        int column_num
        timestamp* times
        bint** bitmap 
        void** value
        int cur_num
        int max_capacity
    
    ctypedef Tablet DataResult
    
    # Function Declarations
    # reader：tsfile reader
    CTsFileReader ts_reader_open(const char* path, ErrorCode* err_code)
    ErrorCode ts_reader_close(CTsFileReader reader)

    # writer：tsfile writer
    CTsFileWriter ts_writer_open(const char* path, ErrorCode* err_code)
    ErrorCode ts_writer_close(CTsFileWriter writer)


    # read tsfile data
    QueryDataRet ts_reader_begin_end(CTsFileReader reader, const char* table_name,
                                char** columns, int colum_num, timestamp start_time, timestamp end_time)
    QueryDataRet ts_reader_read(CTsFileReader reader, const char* table_name,
                                char** columns, int colum_num)
    DataResult* ts_next(QueryDataRet data, int expect_line_count)
    ErrorCode destory_query_dataret(QueryDataRet query_data_set)
    ErrorCode destory_tablet(Tablet* tablet)

    # writer tsfile data
    ErrorCode tsfile_register_table(CTsFileWriter writer, TableSchema* schema)
    ErrorCode tsfile_register_table_column(CTsFileWriter writer, const char* table_name, ColumnSchema* schema)
    TsFileRowData create_tsfile_row(const char* table_name, timestamp timestamp, int column_length)
    ErrorCode insert_data_into_tsfile_row_int32(TsFileRowData row_data, char* column_name, int value)
    ErrorCode insert_data_into_tsfile_row_int64(TsFileRowData row_data, char* column_name, long long value)
    ErrorCode insert_data_into_tsfile_row_float(TsFileRowData row_data,  char* column_name, float value)
    ErrorCode insert_data_into_tsfile_row_double(TsFileRowData row_data, char* column_name, double value)
    ErrorCode insert_data_into_tsfile_row_boolean(TsFileRowData row_data,  char* column_name, bint value)
    ErrorCode tsfile_write_row_data(CTsFileWriter writer, TsFileRowData data);
    ErrorCode tsfile_flush_data(CTsFileWriter writer)
    ErrorCode destory_tsfile_row(TsFileRowData data)



    