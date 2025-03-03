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
from .tsfile_py_cpp cimport *

from tsfile.row_record import RowRecord
from tsfile.schema import TimeseriesSchema as TimeseriesSchemaPy, DeviceSchema as DeviceSchemaPy
from tsfile.schema import TableSchema as TableSchemaPy
from tsfile.tablet import Tablet as TabletPy

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

