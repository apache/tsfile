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
import math
import struct
from enum import unique, IntEnum
from typing import List, Union

import numpy as np

from .date_utils import parse_date_to_int
from .constants import TSDataType, ColumnCategory


class Tablet(object):
    """
    A pre-allocated columnar data container for batch data with type constraints.

    Initializes:
    - column_name_list: Ordered names for data columns
    - type_list: TSDataType values specifying allowed types per column
    - max_row_num: Pre-allocated row capacity (default 1024)

    Creates timestamp buffer and typed data columns, with value range validation ranges
    for numeric types.
    """

    def __init__(self, column_name_list: list[str], type_list: list[TSDataType],
                 max_row_num: int = 1024):
        self.timestamp_list = [None for _ in range(max_row_num)]
        self.data_list: List[List[Union[int, float, bool, str, bytes, None]]] = [
            [None for _ in range(max_row_num)] for _ in range(len(column_name_list))
        ]
        self.target_name = None
        self.column_name_list = [column_name.lower() for column_name in column_name_list]
        self.type_list = type_list
        self.max_row_num = max_row_num

        self._type_ranges = {
            TSDataType.INT32: (-2 ** 31, 2 ** 31 - 1),
            TSDataType.INT64: (-2 ** 63, 2 ** 63 - 1),
            TSDataType.FLOAT: (np.finfo(np.float32).min, np.finfo(np.float32).max),
            TSDataType.DOUBLE: (np.finfo(np.float64).min, np.finfo(np.float64).max),
        }

    def _check_index(self, col_index: int, row_index: int):
        if not (0 <= col_index < len(self.column_name_list)):
            raise IndexError(f"column index {col_index} out of range [0, {len(self.column_name_list) - 1}]")

        if not (0 <= row_index < self.max_row_num):
            raise IndexError(f"Row index {row_index} out of range [0, {self.max_row_num - 1}]")

    def set_table_name(self, table_name: str):
        self.target_name = table_name

    def get_column_name_list(self):
        return self.column_name_list

    def get_data_type_list(self):
        return self.type_list

    def get_timestamp_list(self):
        return self.timestamp_list

    def get_target_name(self):
        return self.target_name

    def get_value_list(self):
        return self.data_list

    def get_max_row_num(self):
        return self.max_row_num

    def add_column(self, column_name: str, data_type: TSDataType):
        self.column_name_list.append(column_name)
        self.type_list.append(data_type)

    def remove_column(self, column_name: str):
        ind = self.column_name_list.index(column_name)
        self.column_name_list.remove(column_name)
        self.type_list.remove(self.type_list[ind])

    def set_timestamp_list(self, timestamp_list: list[int]):
        self.timestamp_list = timestamp_list

    def add_timestamp(self, row_index: int, timestamp: int):
        self.timestamp_list[row_index] = timestamp

    def _check_numeric_range(self, value: Union[int, float], data_type: TSDataType):
        if math.isnan(value) or math.isinf(value):
            if data_type == TSDataType.INT32 or data_type == TSDataType.INT64:
                raise ValueError(f"NaN/Inf is invalid for integer type {data_type.name}")
            else:
                return
        min_val, max_val = self._type_ranges[data_type]
        if not (min_val <= value <= max_val):
            raise OverflowError(f"data:{value} out of range ({min_val}, {max_val})")

    def add_value_by_name(self, column_name: str, row_index: int, value: Union[int, float, bool, str, bytes]):
        try:
            col_index = self.column_name_list.index(column_name.lower())
        except ValueError:
            raise ValueError(f"Column '{column_name}' does not exist") from None

        if not (0 <= row_index < self.max_row_num):
            raise IndexError(
                f"Row index {row_index} out of range [0, {self.max_row_num - 1}]"
            )

        expected_type = self.type_list[col_index]

        if not isinstance(value, expected_type.to_py_type()):
            raise TypeError(f"Expected {expected_type.to_py_type()} got {type(value)}")

        if expected_type in (TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE):
            self._check_numeric_range(value, expected_type)

        self.data_list[col_index][row_index] = value

    def add_value_by_index(self, col_index: int, row_index: int, value: Union[int, float, bool, str, bytes]):
        self._check_index(col_index, row_index)
        expected_type = self.type_list[col_index]
        if not isinstance(value, expected_type.to_py_type()):
            raise TypeError(f"Expected {expected_type.to_py_type()} got {type(value)}")

        if expected_type in (TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE):
            self._check_numeric_range(value, expected_type)

        self.data_list[col_index][row_index] = value

    def get_value_by_index(self, col_index: int, row_index: int):
        self._check_index(col_index, row_index)
        return self.data_list[col_index][row_index]

    def get_value_by_name(self, column_name: str, row_index: int):
        try:
            col_index = self.column_name_list.index(column_name)
        except ValueError:
            raise ValueError(f"Column '{column_name}' does not exist") from None
        if not (0 <= row_index < self.max_row_num):
            raise IndexError(
                f"Row index {row_index} out of range [0, {self.max_row_num - 1}]"
            )
        return self.data_list[col_index][row_index]

    def get_value_list_by_name(self, column_name: str):
        try:
            col_index = self.column_name_list.index(column_name)
        except ValueError:
            raise ValueError(f"Column '{column_name}' does not exist") from None
        return self.data_list[col_index]
