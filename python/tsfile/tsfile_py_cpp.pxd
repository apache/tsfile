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

cdef public api inline void check_error(int errcode, const char* context=NULL) except *
cdef public api object from_c_result_set_meta_data(ResultSetMetaData schema)
cdef public api object from_c_column_schema(ColumnSchema schema)
cdef public api object from_c_table_schema(TableSchema schema)
cdef public api TSDataType to_c_data_type(object data_type)
cdef public api TSEncoding to_c_encoding_type(object encoding_type)
cdef public api CompressionType to_c_compression_type(object compression_type)
cdef public api ColumnCategory to_c_category_type(object category)
cdef public api TimeseriesSchema* to_c_timeseries_schema(object py_schema)
cdef public api DeviceSchema* to_c_device_schema(object py_schema)
cdef public api ColumnSchema* to_c_column_schema(object py_schema)
cdef public api TableSchema* to_c_table_schema(object py_schema)
cdef public api Tablet to_c_tablet(object tablet)
cdef public api TsRecord to_c_record(object row_record)
cdef public api void free_c_table_schema(TableSchema* c_schema)
cdef public api void free_c_column_schema(ColumnSchema* c_schema)
cdef public api void free_c_timeseries_schema(TimeseriesSchema* c_schema)
cdef public api void free_c_device_schema(DeviceSchema* c_schema)
cdef public api void free_c_tablet(Tablet tablet)
cdef public api void free_c_row_record(TsRecord record)
cdef public api TsFileWriter tsfile_writer_new_c(object pathname, uint64_t memory_threshold) except +
cdef public api TsFileReader tsfile_reader_new_c(object pathname) except +
cdef public api ErrorCode tsfile_writer_register_device_py_cpp(TsFileWriter writer, DeviceSchema *schema)
cdef public api ErrorCode tsfile_writer_register_timeseries_py_cpp(TsFileWriter writer, object device_name,
                                                        TimeseriesSchema *schema)
cdef public api ErrorCode tsfile_writer_register_table_py_cpp(TsFileWriter writer, TableSchema *schema)
cdef public api bint tsfile_result_set_is_null_by_name_c(ResultSet result_set, object name)
cdef public api ResultSet tsfile_reader_query_table_c(TsFileReader reader, object table_name, object column_list,
                                            int64_t start_time, int64_t end_time)
cdef public api ResultSet tsfile_reader_query_paths_c(TsFileReader reader, object device_name, object sensor_list, int64_t start_time,
                                                      int64_t end_time)
cdef public api object get_table_schema(TsFileReader reader, object table_name)
cdef public api object get_all_table_schema(TsFileReader reader)
cpdef public api object get_tsfile_config()
cpdef public api void set_tsfile_config(dict new_config)