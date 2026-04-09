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

from tsfile import to_dataframe, TsFileReader, ColumnCategory, TIME_COLUMN
from tsfile.utils import dataframe_to_tsfile


def convert_to_nullable_types(df):
    df = df.copy()
    for col in df.columns:
        dtype = df[col].dtype
        if dtype == "int64":
            df[col] = df[col].astype("Int64")
        elif dtype == "int32":
            df[col] = df[col].astype("Int32")
        elif dtype == "float64":
            df[col] = df[col].astype("Float64")
        elif dtype == "float32":
            df[col] = df[col].astype("Float32")
        elif dtype == "bool":
            df[col] = df[col].astype("boolean")
    return df


def test_dataframe_to_tsfile_basic():
    tsfile_path = "test_dataframe_to_tsfile_basic.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {
                "time": [i for i in range(100)],
                "device": [f"device{i}" for i in range(100)],
                "value": [i * 1.5 for i in range(100)],
                "value2": [i * 10 for i in range(100)],
            }
        )

        dataframe_to_tsfile(df, tsfile_path, table_name="test_table")

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values("time").reset_index(drop=True)
        df_sorted = convert_to_nullable_types(
            df.sort_values("time").reset_index(drop=True)
        )

        assert df_read.shape == (100, 4)
        assert df_read["time"].equals(df_sorted["time"])
        assert df_read["device"].equals(df_sorted["device"])
        assert df_read["value"].equals(df_sorted["value"])
        assert df_read["value2"].equals(df_sorted["value2"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_default_table_name():
    tsfile_path = "test_dataframe_to_tsfile_default.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame({"time": [0, 1], "value": [1.0, 2.0]})
        dataframe_to_tsfile(df, tsfile_path)

        df_read = to_dataframe(tsfile_path, table_name="default_table")
        assert len(df_read) == 2
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_with_index():
    tsfile_path = "test_dataframe_to_tsfile_index.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {
                "device": [f"device{i}" for i in range(30)],
                "value": [i * 2.0 for i in range(30)],
            }
        )
        df.index = [i * 100 for i in range(30)]
        dataframe_to_tsfile(df, tsfile_path, table_name="test_table")

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values("time").reset_index(drop=True)
        time_expected = pd.Series(df.index.values, dtype="Int64")
        assert df_read.shape == (30, 3)
        assert df_read["time"].equals(time_expected)

        with TsFileReader(tsfile_path) as reader:
            table_schema = reader.get_table_schema("test_table")
            device_col = table_schema.get_column("device")
            assert device_col is not None
            assert device_col.get_category() == ColumnCategory.FIELD
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_custom_time_column():
    tsfile_path = "test_dataframe_to_tsfile_custom_time.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {
                "timestamp": [i for i in range(30)],
                "device": [f"device{i}" for i in range(30)],
                "value": [i * 3.0 for i in range(30)],
            }
        )

        dataframe_to_tsfile(
            df, tsfile_path, table_name="test_table", time_column="timestamp"
        )

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values("timestamp").reset_index(drop=True)
        df_sorted = convert_to_nullable_types(
            df.sort_values("timestamp").reset_index(drop=True)
        )

        assert df_read.shape == (30, 3)
        assert df_read["timestamp"].equals(df_sorted["timestamp"])
        assert df_read["device"].equals(df_sorted["device"])
        assert df_read["value"].equals(df_sorted["value"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_case_insensitive_time():
    tsfile_path = "test_dataframe_to_tsfile_case_time.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {"Time": [i for i in range(20)], "value": [i * 2.0 for i in range(20)]}
        )

        dataframe_to_tsfile(df, tsfile_path, table_name="test_table")

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        assert df_read.shape == (20, 2)
        assert df_read["time"].equals(pd.Series([i for i in range(20)], dtype="Int64"))
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_with_tag_columns():
    tsfile_path = "test_dataframe_to_tsfile_tags.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {
                "time": [i for i in range(20)],
                "device": [f"device{i}" for i in range(20)],
                "location": [f"loc{i % 5}" for i in range(20)],
                "value": [i * 1.5 for i in range(20)],
            }
        )

        dataframe_to_tsfile(
            df, tsfile_path, table_name="test_table", tag_column=["device", "location"]
        )

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values(TIME_COLUMN).reset_index(drop=True)
        df_sorted = convert_to_nullable_types(
            df.sort_values("time").reset_index(drop=True)
        )

        assert df_read.shape == (20, 4)
        assert df_read["device"].equals(df_sorted["device"])
        assert df_read["location"].equals(df_sorted["location"])
        assert df_read["value"].equals(df_sorted["value"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_tag_time_unsorted():
    tsfile_path = "test_dataframe_to_tsfile_tag_time_unsorted.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {
                "time": [30, 10, 20, 50, 40, 15, 25, 35, 5, 45],
                "device": [
                    "device1",
                    "device1",
                    "device1",
                    "device2",
                    "device2",
                    "device1",
                    "device1",
                    "device2",
                    "device1",
                    "device2",
                ],
                "value": [i * 1.5 for i in range(10)],
            }
        )

        dataframe_to_tsfile(
            df, tsfile_path, table_name="test_table", tag_column=["device"]
        )

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_expected = df.sort_values(by=["device", "time"]).reset_index(drop=True)
        df_expected = convert_to_nullable_types(df_expected)

        assert df_read.shape == (10, 3)
        assert df_read["device"].equals(df_expected["device"])
        assert df_read[TIME_COLUMN].equals(df_expected["time"])
        assert df_read["value"].equals(df_expected["value"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_all_datatypes():
    tsfile_path = "test_dataframe_to_tsfile_all_types.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {
                "time": [i for i in range(50)],
                "bool_col": [i % 2 == 0 for i in range(50)],
                "int32_col": pd.Series([i for i in range(50)], dtype="int32"),
                "int64_col": [i * 10 for i in range(50)],
                "float_col": pd.Series([i * 1.5 for i in range(50)], dtype="float32"),
                "double_col": [i * 2.5 for i in range(50)],
                "string_col": [f"str{i}" for i in range(50)],
                "blob_col": [f"blob{i}".encode("utf-8") for i in range(50)],
                "text_col": [f"text{i}" for i in range(50)],
                "date_col": [date(2025, i % 11 + 1, i % 20 + 1) for i in range(50)],
                "timestamp_col": [i for i in range(50)],
            }
        )

        dataframe_to_tsfile(df, tsfile_path, table_name="test_table")

        df_read = to_dataframe(tsfile_path, table_name="test_table")
        df_read = df_read.sort_values(TIME_COLUMN).reset_index(drop=True)
        df_sorted = convert_to_nullable_types(
            df.sort_values("time").reset_index(drop=True)
        )

        assert df_read.shape == (50, 11)
        assert df_read["bool_col"].equals(df_sorted["bool_col"])
        assert df_read["int32_col"].equals(df_sorted["int32_col"])
        assert df_read["int64_col"].equals(df_sorted["int64_col"])
        assert np.allclose(df_read["float_col"], df_sorted["float_col"])
        assert np.allclose(df_read["double_col"], df_sorted["double_col"])
        assert df_read["string_col"].equals(df_sorted["string_col"])
        assert df_read["text_col"].equals(df_sorted["text_col"])
        assert df_read["date_col"].equals(df_sorted["date_col"])
        assert df_read["timestamp_col"].equals(df_sorted["timestamp_col"])
        for i in range(50):
            assert df_read["blob_col"].iloc[i] == df_sorted["blob_col"].iloc[i]
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_empty_dataframe():
    tsfile_path = "test_dataframe_to_tsfile_empty.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame()

        with pytest.raises(ValueError, match="DataFrame cannot be None or empty"):
            dataframe_to_tsfile(df, tsfile_path)
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_no_data_columns():
    tsfile_path = "test_dataframe_to_tsfile_no_data.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame({"time": [i for i in range(10)]})

        with pytest.raises(
            ValueError, match="DataFrame must have at least one data column"
        ):
            dataframe_to_tsfile(df, tsfile_path)
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_only_time_column_raises():
    """Only time column (no FIELD/TAG columns) must raise ValueError."""
    tsfile_path = "test_only_time_column.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)
        df = pd.DataFrame({"time": [1, 2, 3]})
        with pytest.raises(
            ValueError, match="at least one data column besides the time column"
        ):
            dataframe_to_tsfile(df, tsfile_path)
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_time_column_not_found():
    tsfile_path = "test_dataframe_to_tsfile_time_err.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame({"time": [0, 1], "value": [1.0, 2.0]})
        with pytest.raises(ValueError, match="Time column 'timestamp' not found"):
            dataframe_to_tsfile(df, tsfile_path, time_column="timestamp")
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_invalid_time_column():
    tsfile_path = "test_dataframe_to_tsfile_invalid_time.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {"timestamp": [i for i in range(10)], "value": [i * 1.0 for i in range(10)]}
        )

        with pytest.raises(ValueError, match="Time column 'time' not found"):
            dataframe_to_tsfile(df, tsfile_path, time_column="time")
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_non_integer_time_column():
    tsfile_path = "test_dataframe_to_tsfile_non_int_time.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {
                "time": [f"time{i}" for i in range(10)],
                "value": [i * 1.0 for i in range(10)],
            }
        )

        with pytest.raises(TypeError, match="must be integer type"):
            dataframe_to_tsfile(df, tsfile_path)
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_tag_column_not_found():
    tsfile_path = "test_dataframe_to_tsfile_tag_err.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame({"time": [0, 1], "device": ["a", "b"], "value": [1.0, 2.0]})
        with pytest.raises(ValueError, match="Tag column 'invalid' not found"):
            dataframe_to_tsfile(df, tsfile_path, tag_column=["invalid"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)


def test_dataframe_to_tsfile_invalid_tag_column():
    tsfile_path = "test_dataframe_to_tsfile_invalid_tag.tsfile"
    try:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)

        df = pd.DataFrame(
            {"time": [i for i in range(10)], "value": [i * 1.0 for i in range(10)]}
        )

        with pytest.raises(ValueError, match="Tag column 'invalid' not found"):
            dataframe_to_tsfile(df, tsfile_path, tag_column=["invalid"])
    finally:
        if os.path.exists(tsfile_path):
            os.remove(tsfile_path)
