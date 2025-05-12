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
    """
    Metadata schema for a time series (name, data type, encoding, compression).
    """

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
    """Represents a device entity containing multiple time series."""

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
    """Defines schema for a table column (name, datatype, category)."""

    column_name = None
    data_type = None

    def __init__(self, column_name: str, data_type: TSDataType, category: ColumnCategory = ColumnCategory.FIELD):
        if column_name is None or len(column_name) == 0:
            raise ValueError("Column name cannot be None")
        self.column_name = column_name.lower()
        if data_type is None:
            raise ValueError("Data type cannot be None")
        self.data_type = data_type
        self.category = category

    def __repr__(self) -> str:
        return f"ColumnSchema({self.column_name}, {self.data_type.name}, {self.category.name})"

    def get_column_name(self):
        return self.column_name

    def get_data_type(self):
        return self.data_type

    def get_category(self):
        return self.category


class TableSchema:
    """Schema definition for a table structure."""
    table_name = None
    columns = None

    def __init__(self, table_name: str, columns: List[ColumnSchema]):
        if table_name is None or len(table_name) == 0:
            raise ValueError("Table name cannot be None")
        self.table_name = table_name.lower()
        if len(columns) == 0:
            raise ValueError("Columns cannot be empty")
        self.columns = columns

    def get_table_name(self):
        return self.table_name

    def get_columns(self):
        return self.columns

    def __repr__(self) -> str:
        return f"TableSchema({self.table_name}, {self.columns})"


class ResultSetMetaData:
    """Metadata container for query result sets (columns, types, table name)."""
    column_list = None
    data_types = None
    table_name = None

    def __init__(self, column_list: List[str], data_types: List[TSDataType]):
        self.column_list = column_list
        self.data_types = data_types

    def set_table_name(self, table_name: str):
        self.table_name = table_name

    def get_data_type(self, column_index: int) -> TSDataType:
        if column_index < 1 or column_index > len(self.column_list):
            raise OverflowError
        return self.data_types[column_index - 1]

    def get_column_name(self, column_index: int) -> str:
        if column_index < 1 or column_index > len(self.column_list):
            raise OverflowError
        return self.column_list[column_index - 1]

    def get_column_name_index(self, column_name: str, is_tree: bool = False) -> int:
        """
        For Tree model, column is full path, column_name means sensor_name.
        For Table model, column is just column name.
        """
        if is_tree:
            return self.column_list.index(self.table_name + "." + column_name) + 1
        else:
            return self.column_list.index(column_name) + 1

    def get_column_num(self):
        return len(self.column_list)

    def get_column_list(self):
        return self.column_list

    def get_data_type_list(self):
        return self.data_types
