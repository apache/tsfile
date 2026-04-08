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

import weakref
from typing import List, Optional, Dict

import pandas as pd
from libc.string cimport strlen
from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.string cimport memset
import pyarrow as pa
from libc.stdint cimport INT64_MIN, INT64_MAX, uintptr_t

from tsfile.schema import TSDataType as TSDataTypePy
from tsfile.schema import DeviceID, DeviceTimeseriesMetadataGroup
from tsfile.tag_filter import ComparisonTagFilter, BetweenTagFilter, AndTagFilter, OrTagFilter, NotTagFilter
from .date_utils import parse_int_to_date
from .tsfile_cpp cimport *
from .tsfile_py_cpp cimport *

cdef class ResultSetPy:
    """
    Get data from a query result. When reader run a query, a query handler will return.
    If reader is closed, result set will not invalid anymore.
    """
    # a tag for enable weakref in cython.
    __pyx_allow_weakref__ = True
    cdef object __weakref__

    # ResultSet from C interface.
    cdef ResultSet result
    cdef object metadata

    # Tag filter handle owned by this result set (freed on close).
    cdef TagFilterHandle _tag_filter_handle

    # ResultSet is valid or not, if the reader is closed, valid will be False.
    cdef object valid
    # The reader
    cdef object tsfile_reader
    cdef object is_tree

    def __init__(self, tsfile_reader : TsFileReaderPy, is_tree: bint = False):
        self.metadata = None
        self.valid = True
        self.tsfile_reader = weakref.ref(tsfile_reader)
        self.is_tree = is_tree
        self._tag_filter_handle = NULL

    cdef init_c(self, ResultSet result, object device_name):
        """
        Init c symbols.
        """
        cdef ResultSetMetaData metadata_c
        self.result = result
        metadata_c = tsfile_result_set_get_metadata(self.result)
        self.metadata = from_c_result_set_meta_data(metadata_c)
        self.metadata.set_table_name(device_name)
        free_result_set_meta_data(metadata_c)

    def next(self):
        """
        Check and get next rows in query result.
        :return: boolean, true means get next rows.
        """
        cdef ErrorCode code = 0
        self.check_result_set_invalid()
        has_next = tsfile_result_set_next(self.result, &code)
        check_error(code)
        return has_next

    def get_result_column_info(self):
        """
        Get result set's columns info.
        :return: a dict contains column's name and datatype.
        """
        return {
            column_name: column_type
            for column_name, column_type in zip(
                self.metadata.column_list,
                self.metadata.data_types
            )
        }

    def read_data_frame(self, max_row_num : int = 1024):
        """
        :param max_row_num: default row num: 1024
        :return: a dataframe contains data from query result.
        """
        self.check_result_set_invalid()
        column_names = self.metadata.get_column_list()
        column_num = self.metadata.get_column_num()

        date_columns = [
            column_names[i]
            for i in range(column_num)
            if self.metadata.get_data_type(i + 1) == TSDataTypePy.DATE
        ]

        data_type = [self.metadata.get_data_type(i + 1).to_pandas_dtype() for i in range(column_num)]

        data_container = {
            column_name: [] for column_name in column_names
        }

        cur_line = 0

        # User may call result_set.next() before or not, so we just get current data.
        # if there is no data in result set, we just get a None list.
        row_data = [
            self.get_value_by_index(i + 1)
            for i in range(column_num)
        ]

        if not all(value is None for value in row_data):
            for column_name, value in zip(column_names, row_data):
                data_container[column_name].append(value)
            cur_line += 1

        while cur_line < max_row_num:
            if self.next():
                row_data = (
                    self.get_value_by_index(i + 1)
                    for i in range(column_num)
                )
                for column_name, value in zip(column_names, row_data):
                    data_container[column_name].append(value)
                cur_line += 1
            else:
                break

        df = pd.DataFrame(data_container)
        data_type_dict = {col: dtype for col, dtype in zip(column_names, data_type)}
        df = df.astype(data_type_dict)
        return df

    def read_arrow_batch(self):
        self.check_result_set_invalid()
        
        cdef ArrowArray arrow_array
        cdef ArrowSchema arrow_schema
        cdef ErrorCode code = 0
        cdef ErrorCode err_code = 0

        memset(&arrow_array, 0, sizeof(ArrowArray))
        memset(&arrow_schema, 0, sizeof(ArrowSchema))

        code = tsfile_result_set_get_next_tsblock_as_arrow(self.result, &arrow_array, &arrow_schema)

        if code == 21:  # E_NO_MORE_DATA
            return None
        if code != 0:
            check_error(code)

        if arrow_schema.release == NULL or arrow_array.release == NULL:
            raise RuntimeError("Arrow conversion returned invalid schema or array")

        try:
            schema_ptr = <uintptr_t>&arrow_schema
            array_ptr = <uintptr_t>&arrow_array
            batch = pa.RecordBatch._import_from_c(array_ptr, schema_ptr)
            table = pa.Table.from_batches([batch])
            return table
        except Exception as e:
            if arrow_array.release != NULL:
                arrow_array.release(&arrow_array)
            if arrow_schema.release != NULL:
                arrow_schema.release(&arrow_schema)
            raise e

    def get_value_by_index(self, index : int):
        """
        Get value by index from query result set.
        NOTE: index start from 1.
        """
        cdef char * string = NULL
        self.check_result_set_invalid()
        # Well when we check is null, id from 0, so there index -1.
        if tsfile_result_set_is_null_by_index(self.result, index):
            return None
        data_type = self.metadata.get_data_type(index)
        if data_type == TSDataTypePy.INT32:
            return tsfile_result_set_get_value_by_index_int32_t(self.result, index)
        elif data_type == TSDataTypePy.DATE:
            return parse_int_to_date(tsfile_result_set_get_value_by_index_int64_t(self.result, index))
        elif data_type == TSDataTypePy.INT64 or data_type == TSDataTypePy.TIMESTAMP:
            return tsfile_result_set_get_value_by_index_int64_t(self.result, index)
        elif data_type == TSDataTypePy.FLOAT:
            return tsfile_result_set_get_value_by_index_float(self.result, index)
        elif data_type == TSDataTypePy.DOUBLE:
            return tsfile_result_set_get_value_by_index_double(self.result, index)
        elif data_type == TSDataTypePy.BOOLEAN:
            return tsfile_result_set_get_value_by_index_bool(self.result, index)
        elif data_type == TSDataTypePy.STRING or data_type == TSDataTypePy.TEXT:
            try:
                string = tsfile_result_set_get_value_by_index_string(self.result, index)
                if string == NULL:
                    return None
                return string.decode('utf-8')
            finally:
                pass
        elif data_type == TSDataTypePy.BLOB:
            try:
                string = tsfile_result_set_get_value_by_index_string(self.result, index)
                if string == NULL:
                    return None
                return PyBytes_FromStringAndSize(string, strlen(string))
            finally:
                pass

    def get_value_by_name(self, column_name : str):
        """
        Get value by name from query result set.
        """
        self.check_result_set_invalid()
        if tsfile_result_set_is_null_by_name_c(self.result, column_name.lower()):
            return None
        # get index in metadata, metadata ind from 0.
        ind = self.metadata.get_column_name_index(column_name.lower(), self.is_tree)
        return self.get_value_by_index(ind)

    def get_metadata(self):
        return self.metadata
    def is_null_by_index(self, index : int):
        """
        Checks whether the field at the specified index in the result set is null.

        This method queries the underlying result set to determine if the value
        at the given column index position represents a null value.

        Index start from 1.
        """
        self.check_result_set_invalid()
        if index > (len(self.metadata.column_list) + 1) or index < 1:
            raise IndexError(
                f"Column index {index} out of range (column count: {len(self.metadata.column_list)})"
            )
        return tsfile_result_set_is_null_by_index(self.result, index)

    def is_null_by_name(self, name : str):
        """
        Checks whether the field with the specified column name in the result set is null.
        """
        self.check_result_set_invalid()
        ind = self.metadata.get_column_name_index(name, self.is_tree)
        return self.is_null_by_index(ind)

    def check_result_set_invalid(self):
        if not self.valid:
            raise Exception("Invalid result set. TsFile Reader not exists")

    def get_result_set_valid(self):
        return self.valid

    def close(self):
        """
        Close result set, free C resource.
        :return:
        """
        if self.result != NULL:
            free_tsfile_result_set(&self.result)

        if self._tag_filter_handle != NULL:
            tsfile_tag_filter_free(self._tag_filter_handle)
            self._tag_filter_handle = NULL

        if self.tsfile_reader is not None:
            reader = self.tsfile_reader()
            if reader is not None:
                reader.notify_result_set_discard(self)

        self.result = NULL
        self.valid = False

    def set_invalid_result_set(self):
        self.valid = False
        self.close()

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

cdef class TsFileReaderPy:
    """
    Cython wrapper class for interacting with TsFileReader C implementation.

    Provides a Pythonic interface to read and query time series data from TsFiles.
    """

    __pyx_allow_weakref__ = True
    cdef object __weakref__

    cdef TsFileReader reader
    cdef object activate_result_set_list

    def __init__(self, pathname):
        """
        Initialize a TsFile reader for the specified file path.
        """
        self.init_reader(pathname)
        self.activate_result_set_list = weakref.WeakSet()

    cdef init_reader(self, pathname):
        self.reader = tsfile_reader_new_c(pathname)

    def query_table(self, table_name : str, column_names : List[str],
                    start_time : int = INT64_MIN, end_time : int = INT64_MAX,
                    tag_filter = None, batch_size : int = 0) -> ResultSetPy:
        """
        Execute a time range query on specified table and columns.
        :param tag_filter: Optional TagFilter to filter by TAG column values.
        :param batch_size: <= 0 for row-by-row mode; > 0 for batch (TsBlock) mode.
        :return: query result handler.
        """
        cdef ResultSet result
        cdef TagFilterHandle c_tag_filter = NULL
        if tag_filter is not None:
            c_tag_filter = self._build_c_tag_filter(table_name.lower(), tag_filter)
        if batch_size <= 0:
            result = tsfile_reader_query_table_with_tag_filter_c(
                self.reader, table_name.lower(),
                [column_name.lower() for column_name in column_names],
                start_time, end_time, c_tag_filter, batch_size)
        else:
            result = tsfile_reader_query_table_batch_c(
                self.reader, table_name.lower(),
                [column_name.lower() for column_name in column_names],
                start_time, end_time, c_tag_filter, batch_size)
        pyresult = ResultSetPy(self)
        pyresult._tag_filter_handle = c_tag_filter
        pyresult.init_c(result, table_name)
        self.activate_result_set_list.add(pyresult)
        return pyresult

    cdef TagFilterHandle _build_c_tag_filter(self, str table_name, object tag_filter):
        """Recursively build C TagFilterHandle from Python TagFilter tree."""
        cdef ErrorCode code = 0
        cdef TagFilterHandle handle = NULL
        cdef bytes table_bytes
        cdef bytes col_bytes
        cdef bytes val_bytes
        cdef bytes lower_bytes
        cdef bytes upper_bytes

        if isinstance(tag_filter, ComparisonTagFilter):
            table_bytes = table_name.encode('utf-8')
            col_bytes = tag_filter.column_name.encode('utf-8')
            val_bytes = tag_filter.value.encode('utf-8')
            handle = tsfile_tag_filter_create(
                self.reader, <const char*>table_bytes,
                <const char*>col_bytes, <const char*>val_bytes,
                <TagFilterOp>tag_filter.op, &code)
            check_error(code)
            return handle
        elif isinstance(tag_filter, BetweenTagFilter):
            table_bytes = table_name.encode('utf-8')
            col_bytes = tag_filter.column_name.encode('utf-8')
            lower_bytes = tag_filter.lower.encode('utf-8')
            upper_bytes = tag_filter.upper.encode('utf-8')
            handle = tsfile_tag_filter_between(
                self.reader, <const char*>table_bytes,
                <const char*>col_bytes, <const char*>lower_bytes,
                <const char*>upper_bytes, tag_filter.is_not, &code)
            check_error(code)
            return handle
        elif isinstance(tag_filter, AndTagFilter):
            left = self._build_c_tag_filter(table_name, tag_filter.left)
            right = self._build_c_tag_filter(table_name, tag_filter.right)
            return tsfile_tag_filter_and(left, right)
        elif isinstance(tag_filter, OrTagFilter):
            left = self._build_c_tag_filter(table_name, tag_filter.left)
            right = self._build_c_tag_filter(table_name, tag_filter.right)
            return tsfile_tag_filter_or(left, right)
        elif isinstance(tag_filter, NotTagFilter):
            inner = self._build_c_tag_filter(table_name, tag_filter.filter)
            return tsfile_tag_filter_not(inner)
        else:
            raise TypeError(f"Unknown tag filter type: {type(tag_filter)}")
    def query_table_on_tree(self, column_names : List[str],
                            start_time : int = INT64_MIN, end_time : int = INT64_MAX) -> ResultSetPy:
        """
        Execute a time range query on specified columns on tree structure.
        :return: query result handler.
        """
        cdef ResultSet result;
        ## No need to convert column names to lowercase, as measurement names in the tree model are case-sensitive.
        result = tsfile_reader_query_table_on_tree_c(self.reader, column_names, start_time, end_time)
        pyresult = ResultSetPy(self, True)
        pyresult.init_c(result, "root")
        self.activate_result_set_list.add(pyresult)
        return pyresult

    def query_tree_by_row(self, device_ids : List[str], measurement_names : List[str],
                           offset : int = 0, limit : int = -1) -> ResultSetPy:
        """
        Execute tree-model query by row with offset/limit.
        """
        if len(device_ids) == 0:
            raise ValueError("device_ids must not be empty")
        if len(measurement_names) == 0:
            raise ValueError("measurement_names must not be empty")

        cdef ResultSet result
        result = tsfile_reader_query_tree_by_row_c(self.reader, device_ids,
                                                     measurement_names, offset, limit)
        pyresult = ResultSetPy(self, True)
        pyresult.init_c(result, device_ids[0])
        self.activate_result_set_list.add(pyresult)
        return pyresult

    def query_table_by_row(self, table_name : str, column_names : List[str],
                             offset : int = 0, limit : int = -1,
                             tag_filter = None, batch_size : int = 0
                             ) -> ResultSetPy:
        """
        Execute table-model query by row with offset/limit.
        """
        cdef ResultSet result
        cdef TagFilterHandle c_tag_filter = NULL
        if tag_filter is not None:
            c_tag_filter = self._build_c_tag_filter(table_name.lower(), tag_filter)
        result = tsfile_reader_query_table_by_row_c(self.reader, table_name.lower(),
                                                      [column_name.lower() for column_name in column_names],
                                                      offset, limit, c_tag_filter, batch_size)
        pyresult = ResultSetPy(self)
        pyresult.init_c(result, table_name)
        self.activate_result_set_list.add(pyresult)
        return pyresult

    def query_timeseries(self, device_name : str, sensor_list : List[str], start_time : int = 0,
                         end_time : int = 0) -> ResultSetPy:
        """
        Execute a time range query on a specify device.
        """
        cdef ResultSet result;
        result = tsfile_reader_query_paths_c(self.reader, device_name, sensor_list, start_time, end_time)
        pyresult = ResultSetPy(self, True)
        pyresult.init_c(result, device_name)
        self.activate_result_set_list.add(pyresult)
        return pyresult

    def notify_result_set_discard(self, result_set: ResultSetPy):
        """
        Remove activate result set from activate_result_set_list, called when a result set close.
        :param result_set:
        :return:
        """
        self.activate_result_set_list.discard(result_set)

    def get_table_schema(self, table_name : str):
        """
        Get table's schema with specify table name.
        """
        return get_table_schema(self.reader, table_name)

    def get_all_table_schemas(self):
        """
        Get all tables schemas
        """
        return get_all_table_schema(self.reader)

    def get_all_timeseries_schemas(self):
        """
        Get all timeseries schemas
        """
        return get_all_timeseries_schema(self.reader)

    def get_all_devices(self) -> List[DeviceID]:
        """
        Return all devices (path, table name, segments) as
        :class:`tsfile.schema.DeviceID`. NULL C fields become None.
        """
        return reader_get_all_devices_c(self.reader)

    def get_timeseries_metadata(
            self, device_ids: Optional[List] = None
    ) -> Dict[str, DeviceTimeseriesMetadataGroup]:
        """
        Return map device path -> :class:`tsfile.schema.DeviceTimeseriesMetadataGroup`
        (table name, segments, and list of :class:`tsfile.schema.TimeseriesMetadata`).

        ``device_ids is None``: all devices. ``device_ids == []``: empty map.
        Non-empty list restricts to those devices (only existing devices appear).
        """
        return reader_get_timeseries_metadata_c(self.reader, device_ids)

    def close(self):
        """
        Close TsFile Reader, if reader has result sets, invalid them.
        """
        if self.reader == NULL:
            return
        # result_set_bak to avoid runtime error.
        result_set_bak = list(self.activate_result_set_list)
        for result_set in result_set_bak:
            result_set.set_invalid_result_set()

        cdef ErrorCode err_code
        err_code = tsfile_reader_close(self.reader)
        check_error(err_code)
        self.reader = NULL

    def get_active_query_result(self):
        return self.activate_result_set_list

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
