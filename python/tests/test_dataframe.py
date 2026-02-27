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
from datetime import date

import numpy as np
import pandas as pd
import pytest

from tsfile import ColumnSchema, TableSchema, TSDataType, TIME_COLUMN
from tsfile import TsFileTableWriter, ColumnCategory
from tsfile import to_dataframe
from tsfile.exceptions import ColumnNotExistError, TypeMismatchError


def convert_to_nullable_types(df):
    """
    Convert DataFrame columns to nullable types to match returned DataFrame from to_dataframe.
    This handles the fact that returned DataFrames use nullable types (Int64, Float64, etc.)
    to support Null values.
    """
    df = df.copy()
    for col in df.columns:
        dtype = df[col].dtype
        if dtype == 'int64':
            df[col] = df[col].astype('Int64')
        elif dtype == 'int32':
            df[col] = df[col].astype('Int32')
        elif dtype == 'float64':
            df[col] = df[col].astype('Float64')
        elif dtype == 'float32':
            df[col] = df[col].astype('Float32')
        elif dtype == 'bool':
            df[col] = df[col].astype('boolean')
    return df


def test_write_dataframe_basic():
    table = TableSchema("test_table",
                        [ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
                         ColumnSchema("value2", TSDataType.INT64, ColumnCategory.FIELD)])
    tsfile_path = "test_write_dataframe_basic.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        with TsFileTableWriter(tsfile_path, table) as writer:
            df = pd.DataFrame({
                'time': [i for i in range(100)],
                'device': [f"device{i}" for i in range(100)],
                'value': [i * 1.5 for i in range(100)],
                'value2': [i * 10 for i in range(100)]
            })
            writer.write_dataframe(df)

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values(TIME_COLUMN).reset_index(drop=True)
        df_sorted = convert_to_nullable_types(df.sort_values('time').reset_index(drop=True))
        assert df_read.shape == (100, 4)
        assert df_read[TIME_COLUMN].equals(df_sorted["time"])
        assert df_read["device"].equals(df_sorted["device"])
        assert df_read["value"].equals(df_sorted["value"])
        assert df_read["value2"].equals(df_sorted["value2"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_write_dataframe_with_index():
    table = TableSchema("test_table",
                        [ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD)])
    tsfile_path = "test_write_dataframe_index.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        with TsFileTableWriter(tsfile_path, table) as writer:
            df = pd.DataFrame({
                'device': [f"device{i}" for i in range(50)],
                'value': [i * 2.5 for i in range(50)]
            })
            df.index = [i * 10 for i in range(50)]  # Set index as timestamps
            writer.write_dataframe(df)
        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values(TIME_COLUMN).reset_index(drop=True)
        df_sorted = df.sort_index()
        df_sorted = convert_to_nullable_types(df_sorted.reset_index(drop=True))
        time_series = pd.Series(df.sort_index().index.values, dtype='Int64')
        assert df_read.shape == (50, 3)
        assert df_read[TIME_COLUMN].equals(time_series)
        assert df_read["device"].equals(df_sorted["device"])
        assert df_read["value"].equals(df_sorted["value"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_write_dataframe_case_insensitive():
    table = TableSchema("test_table",
                        [ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD)])
    tsfile_path = "test_write_dataframe_case.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        with TsFileTableWriter(tsfile_path, table) as writer:
            df = pd.DataFrame({
                'Time': [i for i in range(30)],  # Capital T
                'Device': [f"device{i}" for i in range(30)],  # Capital D
                'VALUE': [i * 3.0 for i in range(30)]  # All caps
            })
            writer.write_dataframe(df)

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values(TIME_COLUMN).reset_index(drop=True)
        df_sorted = convert_to_nullable_types(df.sort_values('Time').reset_index(drop=True))
        assert df_read.shape == (30, 3)
        assert df_read[TIME_COLUMN].equals(df_sorted["Time"])
        assert df_read["device"].equals(df_sorted["Device"])
        assert df_read["value"].equals(df_sorted["VALUE"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_write_dataframe_column_not_in_schema():
    table = TableSchema("test_table",
                        [ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD)])
    tsfile_path = "test_write_dataframe_extra_col.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        with TsFileTableWriter(tsfile_path, table) as writer:
            df = pd.DataFrame({
                'time': [i for i in range(10)],
                'device': [f"device{i}" for i in range(10)],
                'value': [i * 1.0 for i in range(10)],
                'extra_column': [i for i in range(10)]  # Not in schema
            })
            with pytest.raises(ColumnNotExistError):
                writer.write_dataframe(df)
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_write_dataframe_type_mismatch():
    table = TableSchema("test_table",
                        [ColumnSchema("value", TSDataType.STRING, ColumnCategory.FIELD)])
    tsfile_path = "test_write_dataframe_type_mismatch.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        with TsFileTableWriter(tsfile_path, table) as writer:
            df = pd.DataFrame({
                'time': [i for i in range(10)],
                'value': [i for i in range(10)]
            })
            with pytest.raises(TypeMismatchError) as exc_info:
                writer.write_dataframe(df)
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_write_dataframe_all_datatypes():
    table = TableSchema("test_table",
                        [ColumnSchema("bool_col", TSDataType.BOOLEAN, ColumnCategory.FIELD),
                         ColumnSchema("int32_col", TSDataType.INT32, ColumnCategory.FIELD),
                         ColumnSchema("int64_col", TSDataType.INT64, ColumnCategory.FIELD),
                         ColumnSchema("float_col", TSDataType.FLOAT, ColumnCategory.FIELD),
                         ColumnSchema("double_col", TSDataType.DOUBLE, ColumnCategory.FIELD),
                         ColumnSchema("string_col", TSDataType.STRING, ColumnCategory.FIELD),
                         ColumnSchema("blob_col", TSDataType.BLOB, ColumnCategory.FIELD),
                         ColumnSchema("text_col", TSDataType.TEXT, ColumnCategory.FIELD),
                         ColumnSchema("date_col", TSDataType.DATE, ColumnCategory.FIELD),
                         ColumnSchema("timestamp_col", TSDataType.TIMESTAMP, ColumnCategory.FIELD)])
    tsfile_path = "test_write_dataframe_all_types.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        with TsFileTableWriter(tsfile_path, table) as writer:
            df = pd.DataFrame({
                'time': [i for i in range(50)],
                'bool_col': [i % 2 == 0 for i in range(50)],
                'int32_col': pd.Series([i for i in range(50)], dtype='int32'),
                'int64_col': [i * 10 for i in range(50)],
                'float_col': pd.Series([i * 1.5 for i in range(50)], dtype='float32'),
                'double_col': [i * 2.5 for i in range(50)],
                'string_col': [f"str{i}" for i in range(50)],
                'blob_col': [f"blob{i}".encode('utf-8') for i in range(50)],
                'text_col': [f"text{i}" for i in range(50)],
                'date_col': [date(2025, i % 11 + 1, i % 20 + 1) for i in range(50)],
                'timestamp_col': [i for i in range(50)]
            })
            writer.write_dataframe(df)

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values(TIME_COLUMN).reset_index(drop=True)
        df_sorted = convert_to_nullable_types(df.sort_values('time').reset_index(drop=True))
        assert df_read.shape == (50, 11)
        assert df_read["bool_col"].equals(df_sorted["bool_col"])
        assert df_read["int32_col"].equals(df_sorted["int32_col"])
        assert df_read["int64_col"].equals(df_sorted["int64_col"])
        assert np.allclose(df_read["float_col"], df_sorted["float_col"])
        assert np.allclose(df_read["double_col"], df_sorted["double_col"])
        assert df_read["string_col"].equals(df_sorted["string_col"])
        assert df_read["blob_col"].equals(df_sorted["blob_col"])
        assert df_read["text_col"].equals(df_sorted["text_col"])
        assert df_read["date_col"].equals(df_sorted["date_col"])
        assert df_read["timestamp_col"].equals(df_sorted["timestamp_col"])
        for i in range(50):
            assert df_read["blob_col"].iloc[i] == df_sorted["blob_col"].iloc[i]
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_write_dataframe_schema_time_column():
    table = TableSchema("test_table",
                        [ColumnSchema("time", TSDataType.TIMESTAMP, ColumnCategory.TIME),
                         ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD)])
    tsfile_path = "test_write_dataframe_schema_time.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        with TsFileTableWriter(tsfile_path, table) as writer:
            df = pd.DataFrame({
                'time': [i * 100 for i in range(50)],
                'device': [f"device{i}" for i in range(50)],
                'value': [i * 1.5 for i in range(50)]
            })
            writer.write_dataframe(df)

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values(TIME_COLUMN).reset_index(drop=True)
        df_sorted = convert_to_nullable_types(df.sort_values('time').reset_index(drop=True))
        assert df_read.shape == (50, 3)
        assert df_read[TIME_COLUMN].equals(df_sorted[TIME_COLUMN])
        assert df_read["device"].equals(df_sorted["device"])
        assert df_read["value"].equals(df_sorted["value"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_write_dataframe_schema_time_and_dataframe_time():
    table = TableSchema("test_table",
                        [ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD)])
    tsfile_path = "test_write_dataframe_schema_and_df_time.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        with TsFileTableWriter(tsfile_path, table) as writer:
            df = pd.DataFrame({
                'Time': [i for i in range(30)],
                'device': [f"dev{i}" for i in range(30)],
                'value': [float(i) for i in range(30)]
            })
            writer.write_dataframe(df)

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values(TIME_COLUMN).reset_index(drop=True)
        df_sorted = convert_to_nullable_types(
            df.sort_values('Time').rename(columns=str.lower).reset_index(drop=True)
        )
        assert df_read.shape == (30, 3)
        assert df_read["time"].equals(df_sorted["time"])
        assert df_read["device"].equals(df_sorted["device"])
        assert df_read["value"].equals(df_sorted["value"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_write_dataframe_empty():
    table = TableSchema("test_table",
                        [ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD)])
    tsfile_path = "test_write_dataframe_empty.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        with TsFileTableWriter(tsfile_path, table) as writer:
            df = pd.DataFrame({
                'time': [],
                'value': []
            })
            with pytest.raises(ValueError):
                writer.write_dataframe(df)

    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)
