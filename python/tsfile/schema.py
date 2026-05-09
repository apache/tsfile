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
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from .exceptions import TypeMismatchError
from .constants import TSDataType, ColumnCategory, TSEncoding, Compressor


@dataclass(frozen=True)
class TimeseriesStatistic:
    """Common statistic fields from the C API (no type-specific payload)."""

    has_statistic: bool
    row_count: int
    start_time: int
    end_time: int


@dataclass(frozen=True)
class IntTimeseriesStatistic(TimeseriesStatistic):
    """INT32, DATE, INT64, TIMESTAMP chunk statistics."""

    sum: float
    min_int64: int
    max_int64: int
    first_int64: int
    last_int64: int


@dataclass(frozen=True)
class FloatTimeseriesStatistic(TimeseriesStatistic):
    """FLOAT, DOUBLE chunk statistics."""

    sum: float
    min_float64: float
    max_float64: float
    first_float64: float
    last_float64: float


@dataclass(frozen=True)
class BoolTimeseriesStatistic(TimeseriesStatistic):
    """BOOLEAN chunk statistics."""

    sum: float
    first_bool: bool
    last_bool: bool


@dataclass(frozen=True)
class StringTimeseriesStatistic(TimeseriesStatistic):
    """STRING: lexicographic min/max and time-ordered first/last."""

    str_min: Optional[str]
    str_max: Optional[str]
    str_first: Optional[str]
    str_last: Optional[str]


@dataclass(frozen=True)
class TextTimeseriesStatistic(TimeseriesStatistic):
    """TEXT: first/last only (no min/max)."""

    str_first: Optional[str]
    str_last: Optional[str]


TimeseriesStatisticType = Union[
    TimeseriesStatistic,
    IntTimeseriesStatistic,
    FloatTimeseriesStatistic,
    BoolTimeseriesStatistic,
    StringTimeseriesStatistic,
    TextTimeseriesStatistic,
]


@dataclass(frozen=True)
class TimeseriesMetadata:
    """Per-measurement metadata from get_timeseries_metadata (includes statistic when present)."""

    measurement_name: str
    data_type: TSDataType
    chunk_meta_count: int
    statistic: TimeseriesStatisticType
    timeline_statistic: TimeseriesStatisticType


@dataclass(frozen=True)
class DeviceID:
    """Device identity from the native reader (path, table name, segments). NULL C fields become None."""

    path: Optional[str]
    table_name: Optional[str]
    segments: Tuple[Optional[str], ...]


@dataclass(frozen=True)
class DeviceTimeseriesMetadataGroup:
    """One device's timeseries list plus table name and path segments (dict key is device path)."""

    table_name: Optional[str]
    segments: Tuple[Optional[str], ...]
    timeseries: List[TimeseriesMetadata]


class TimeseriesSchema:
    """
    Metadata schema for a time series (name, data type, encoding, compression).
    """

    timeseries_name = None
    data_type = None
    encoding_type = None
    compression_type = None

    def __init__(
        self,
        timeseries_name: str,
        data_type: TSDataType,
        encoding_type: TSEncoding = TSEncoding.PLAIN,
        compression_type: Compressor = Compressor.UNCOMPRESSED,
    ):
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

    def __repr__(self):
        return f"TimeseriesSchema({self.timeseries_name}, {self.data_type.name}, {self.encoding_type.name}, {self.compression_type.name})"


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

    def __repr__(self):
        return f"DeviceSchema({self.device_name}, {self.timeseries_list})"


class ColumnSchema:
    """Defines schema for a table column (name, datatype, category)."""

    column_name = None
    data_type = None

    def __init__(
        self,
        column_name: str,
        data_type: TSDataType,
        category: ColumnCategory = ColumnCategory.FIELD,
    ):
        if column_name is None or len(column_name) == 0:
            raise ValueError("Column name cannot be None")
        self.column_name = column_name.lower()
        if data_type is None:
            raise ValueError("Data type cannot be None")
        if category == ColumnCategory.TIME and data_type not in [
            TSDataType.INT64,
            TSDataType.TIMESTAMP,
        ]:
            raise TypeError(
                f"Time Column should have type : INT64/Timestamp,"
                f" but got {data_type}"
            )
        elif category == ColumnCategory.TAG and data_type not in [
            TSDataType.STRING,
            TSDataType.TEXT,
        ]:
            raise TypeMismatchError(context="Tag column should be string or text")
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
    time_column = None

    def __init__(self, table_name: str, columns: List[ColumnSchema]):
        if table_name is None or len(table_name) == 0:
            raise ValueError("Table name cannot be None")
        self.table_name = table_name.lower()
        if len(columns) == 0:
            raise ValueError("Columns cannot be empty")
        self.columns = columns
        for column in columns:
            if column.get_category() == ColumnCategory.TIME:
                if self.time_column is not None:
                    raise ValueError(
                        f"Table '{self.table_name}' cannot have multiple time columns: "
                        f"'{self.time_column.get_column_name()}' and '{column.get_column_name()}'"
                    )
                self.time_column = column

    def get_table_name(self):
        return self.table_name

    def get_columns(self):
        return self.columns

    def get_column(self, column_name: str):
        name_lower = column_name.lower()
        for col in self.columns:
            if col.get_column_name() == name_lower:
                return col
        return None

    def get_time_column(self):
        return self.time_column

    def get_column_names(self):
        return [name.get_column_name() for name in self.columns]

    def get_field_columns(self):
        return [
            column
            for column in self.columns
            if column.get_category() == ColumnCategory.FIELD
        ]

    def get_tag_columns(self):
        return [
            column
            for column in self.columns
            if column.get_category() == ColumnCategory.TAG
        ]

    def add_column(self, column: ColumnSchema):
        if column.get_category() == ColumnCategory.TIME:
            if self.time_column is not None:
                raise ValueError(
                    f"Table '{self.table_name}' cannot have multiple time columns: "
                    f"'{self.time_column.name}' and '{column.name}'"
                )
            self.time_column = column
        else:
            for col in self.columns:
                if col.get_column_name() == column.get_column_name():
                    raise ValueError(f"Duplicate column name {col.get_column_name()}")
        self.columns.append(column)

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

    def add_column_at(self, index: int, column_name: str, data_type: TSDataType):
        """Insert a column and its data type at the given position (0-based index)."""
        if index < 0 or index > len(self.column_list):
            raise IndexError(
                f"column index {index} out of range (0 to {len(self.column_list)})"
            )
        self.column_list.insert(index, column_name)
        self.data_types.insert(index, data_type)

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
