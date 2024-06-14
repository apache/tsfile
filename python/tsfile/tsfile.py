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
import os

from .tsfile_pywrapper import tsfile_reader, tsfile_writer
from typing import overload
from pandas import DataFrame

TIMESTAMP_STR = "Time"


# default case -> Dataframe
@overload
def read_tsfile(
    file_path: str,
    table_name: str,
    columns: list[str] | str,
) -> DataFrame: ...


# case with filter -> Dataframe
@overload
def read_tsfile(
    file_path: str,
    table_name: str,
    columns: list[str] | str,
    filter: str,
    start_time: int,
    end_time: int,
) -> DataFrame: ...


# chunksize = int -> Dataframe
@overload
def read_tsfile(
    file_path: str,
    table_name: str,
    columns: list[str] | str,
    chunksize: int,
) -> DataFrame: ...


@overload
def read_tsfile(
    file_path: str,
    table_name: str,
    columns: list[str] | str,
    filter: str,
    start_time: int,
    end_time: int,
    chunksize: int,
) -> DataFrame: ...


# iterator = True -> Iterator
@overload
def read_tsfile(
    file_path: str,
    table_name: str,
    columns: list[str] | str,
    iterator: bool,
    chunksize: int,
) -> tsfile_reader: ...


@overload
def read_tsfile(
    file_path: str,
    table_name: str,
    columns: list[str] | str,
    start_time: int,
    end_time: int,
    iterator: bool,
    chunksize: int,
) -> tsfile_reader: ...


def read_tsfile(
    file_path: str,
    table_name: str,
    columns: list[str] | str,
    start_time: int = None,
    end_time: int = None,
    chunksize: int = None,
    iterator: bool = False,
) -> DataFrame | tsfile_reader:
    if not os.path.exists(file_path):
            raise FileNotFoundError(f"File '{file_path}' does not exist")
    if os.path.getsize(file_path) == 0:
        raise ValueError(f"File '{file_path}' is empty")
    reader = tsfile_reader(
        file_path, table_name, columns, start_time, end_time, chunksize
    )
    if iterator:
        return reader
    else:
        return reader.read_tsfile()


def write_tsfile(
    file_path: str,
    table_name: str,
    data: DataFrame,
):
    if data.empty:
        return
    column_names = data.columns.tolist()
    column_types = data.dtypes

    if TIMESTAMP_STR not in column_names:
        raise AttributeError("Time column is missing")
    if column_types[TIMESTAMP_STR] != "int64":
        raise TypeError("Time column must be of type int64")
    allowed_types = {"int64", "int32", "bool", "float32", "float64"}

    for col, dtype in column_types.items():
        if dtype.name not in allowed_types:
            raise TypeError(
                f"Column '{col}' has an invalid type '{dtype}'."
            )

    writer = tsfile_writer(file_path)
    writer.write_tsfile(table_name, data)
