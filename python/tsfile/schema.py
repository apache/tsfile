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
from typing import List

from .constants import TSDataType, ColumnCategory, TSEncoding, Compressor


class TimeseriesSchema:
    timeseries_name = None
    data_type = None
    encoding_type = None
    compression_type = None

    def __init__(self, timeseries_name: str, data_type: TSDataType, encoding_type: TSEncoding = TSEncoding.PLAIN,
                 compression_type: Compressor = Compressor.UNCOMPRESSED):
        self.timeseries_name = timeseries_name
        self.data_type = data_type
        self.encoding_type = encoding_type
        self.compression_type = compression_type

    def get_timeseries_name(self):
        return self.timeseries_name

    def get_data_type(self):
        return self.data_type

    def get_encoding_type(self):
        return self.encoding_type

    def get_compression_type(self):
        return self.compression_type


class DeviceSchema:
    device_name = None
    timeseries_list = None

    def __init__(self, device_name: str, timeseries_list: List[TimeseriesSchema]):
        self.device_name = device_name
        self.timeseries_list = timeseries_list

    def get_device_name(self):
        return self.device_name

    def get_timeseries_list(self):
        return self.timeseries_list


class ColumnSchema:
    column_name = None
    data_type = None

    def __init__(self, column_name: str, data_type: TSDataType,  category: ColumnCategory = ColumnCategory.FIELD):
        self.column_name = column_name
        self.data_type = data_type
        self.category = category

    def get_column_name(self):
        return self.column_name

    def get_data_type(self):
        return self.data_type

    def get_category(self):
        return self.category


class TableSchema:
    table_name = None
    columns = None

    def __init__(self, table_name: str, columns: List[ColumnSchema]):
        self.table_name = table_name
        self.columns = columns

    def get_table_name(self):
        return self.table_name

    def get_columns(self):
        return self.columns


class ResultSetMetaData:
    column_list = None
    data_types = None
    table_name = None

    def __init__(self, column_list: List[str], data_types: List[TSDataType]):
        self.column_list = column_list
        self.data_types = data_types

    def set_table_name(self, table_name: str):
        self.table_name = table_name

    def get_data_type(self, column_index: int) -> TSDataType:
        return self.data_types[column_index]

    def get_column_name(self, column_index: int) -> str:
        return self.column_list[column_index]

    def get_column_name_index(self, column_name: str) -> int:
        return self.column_list.index(self.table_name + "." + column_name)

    def get_column_num(self):
        return len(self.column_list)

    def get_column_list(self):
        return self.column_list

    def get_data_type_list(self):
        return self.data_types
