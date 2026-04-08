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
"""
Tests for write_arrow_batch: write PyArrow RecordBatch/Table to tsfile
and verify correctness by reading back.
"""

import os
from datetime import date

import numpy as np
import pandas as pd
import pytest

pa = pytest.importorskip("pyarrow", reason="pyarrow is not installed")

from tsfile import ColumnCategory, ColumnSchema, TableSchema, TSDataType, TsFileReader
from tsfile import TsFileTableWriter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_schema(table_name, extra_cols):
    """Build a TableSchema with a string TAG 'device' plus the given field cols."""
    return TableSchema(
        table_name,
        [ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG)] + extra_cols,
    )


def _read_all_arrow(file_path, table_name, columns, start=0, end=10**18, batch_size=4096):
    """Read all rows from file via read_arrow_batch and return as a pa.Table."""
    reader = TsFileReader(file_path)
    rs = reader.query_table(
        table_name=table_name,
        column_names=columns,
        start_time=start,
        end_time=end,
        batch_size=batch_size,
    )
    batches = []
    while True:
        batch = rs.read_arrow_batch()
        if batch is None:
            break
        batches.append(batch)
    rs.close()
    reader.close()
    if not batches:
        return pa.table({})
    return pa.concat_tables(batches)


# ---------------------------------------------------------------------------
# Basic write + read-back
# ---------------------------------------------------------------------------

def test_write_arrow_basic():
    """Write 1 000 rows via write_arrow_batch and verify count + values."""
    path = "test_write_arrow_basic.tsfile"
    table_name = "t"
    n = 1000

    schema = _make_schema(table_name, [
        ColumnSchema("value1", TSDataType.INT64, ColumnCategory.FIELD),
        ColumnSchema("value2", TSDataType.DOUBLE, ColumnCategory.FIELD),
    ])

    batch = pa.record_batch({
        "time":   pa.array(np.arange(n, dtype="int64"), type=pa.timestamp("ns")),
        "device": pa.array([f"d{i}" for i in range(n)], type=pa.string()),
        "value1": pa.array(np.arange(n, dtype="int64"), type=pa.int64()),
        "value2": pa.array(np.arange(n, dtype="float64") * 1.5, type=pa.float64()),
    })

    try:
        if os.path.exists(path):
            os.remove(path)
        with TsFileTableWriter(path, schema) as w:
            w.write_arrow_batch(batch)

        result = _read_all_arrow(path, table_name, ["device", "value1", "value2"])
        assert len(result) == n

        df = result.to_pandas().sort_values("time").reset_index(drop=True)
        assert list(df["value1"]) == list(range(n))
        assert all(abs(df["value2"].iloc[i] - i * 1.5) < 1e-9 for i in range(n))
    finally:
        if os.path.exists(path):
            os.remove(path)


# ---------------------------------------------------------------------------
# pa.Table input
# ---------------------------------------------------------------------------

def test_write_arrow_from_table():
    """write_arrow_batch should accept pa.Table (multi-chunk) as well."""
    path = "test_write_arrow_from_table.tsfile"
    table_name = "t"
    n = 500

    schema = _make_schema(table_name, [
        ColumnSchema("v", TSDataType.INT32, ColumnCategory.FIELD),
    ])

    tbl = pa.table({
        "time":   pa.array(np.arange(n, dtype="int64"), type=pa.timestamp("ns")),
        "device": pa.array(["dev"] * n, type=pa.string()),
        "v":      pa.array(np.arange(n, dtype="int32"), type=pa.int32()),
    })

    try:
        if os.path.exists(path):
            os.remove(path)
        with TsFileTableWriter(path, schema) as w:
            w.write_arrow_batch(tbl)

        result = _read_all_arrow(path, table_name, ["device", "v"])
        assert len(result) == n
        df = result.to_pandas().sort_values("time").reset_index(drop=True)
        assert list(df["v"]) == list(range(n))
    finally:
        if os.path.exists(path):
            os.remove(path)


# ---------------------------------------------------------------------------
# Multiple batches
# ---------------------------------------------------------------------------

def test_write_arrow_multiple_batches():
    """Write several batches sequentially and verify the total row count."""
    path = "test_write_arrow_multi.tsfile"
    table_name = "t"
    rows_per_batch = 300
    num_batches = 4
    total = rows_per_batch * num_batches

    schema = _make_schema(table_name, [
        ColumnSchema("v", TSDataType.INT64, ColumnCategory.FIELD),
    ])

    try:
        if os.path.exists(path):
            os.remove(path)
        with TsFileTableWriter(path, schema) as w:
            for b in range(num_batches):
                start_ts = b * rows_per_batch
                batch = pa.record_batch({
                    "time":   pa.array(
                        np.arange(start_ts, start_ts + rows_per_batch, dtype="int64"),
                        type=pa.timestamp("ns")),
                    "device": pa.array(["dev"] * rows_per_batch, type=pa.string()),
                    "v":      pa.array(
                        np.arange(start_ts, start_ts + rows_per_batch, dtype="int64"),
                        type=pa.int64()),
                })
                w.write_arrow_batch(batch)

        result = _read_all_arrow(path, table_name, ["device", "v"])
        assert len(result) == total
    finally:
        if os.path.exists(path):
            os.remove(path)


# ---------------------------------------------------------------------------
# All supported data types
# ---------------------------------------------------------------------------

def test_write_arrow_all_datatypes():
    """Write every supported data type and verify values read back correctly."""
    path = "test_write_arrow_all_types.tsfile"
    table_name = "t"
    n = 200

    schema = TableSchema(table_name, [
        ColumnSchema("tag",        TSDataType.STRING,  ColumnCategory.TAG),
        ColumnSchema("bool_col",   TSDataType.BOOLEAN, ColumnCategory.FIELD),
        ColumnSchema("int32_col",  TSDataType.INT32,   ColumnCategory.FIELD),
        ColumnSchema("int64_col",  TSDataType.INT64,   ColumnCategory.FIELD),
        ColumnSchema("float_col",  TSDataType.FLOAT,   ColumnCategory.FIELD),
        ColumnSchema("double_col", TSDataType.DOUBLE,  ColumnCategory.FIELD),
        ColumnSchema("str_col",    TSDataType.STRING,  ColumnCategory.FIELD),
        ColumnSchema("date_col",   TSDataType.DATE,    ColumnCategory.FIELD),
    ])

    dates_days = [
        (date(2025, 1, (i % 28) + 1) - date(1970, 1, 1)).days for i in range(n)
    ]

    batch = pa.record_batch({
        "time":       pa.array(np.arange(n, dtype="int64"), type=pa.timestamp("ns")),
        "tag":        pa.array([f"dev{i}" for i in range(n)], type=pa.string()),
        "bool_col":   pa.array([i % 2 == 0 for i in range(n)], type=pa.bool_()),
        "int32_col":  pa.array(np.arange(n, dtype="int32"), type=pa.int32()),
        "int64_col":  pa.array(np.arange(n, dtype="int64") * 10, type=pa.int64()),
        "float_col":  pa.array(np.arange(n, dtype="float32") * 0.5, type=pa.float32()),
        "double_col": pa.array(np.arange(n, dtype="float64") * 1.1, type=pa.float64()),
        "str_col":    pa.array([f"s{i}" for i in range(n)], type=pa.string()),
        "date_col":   pa.array(dates_days, type=pa.date32()),
    })

    try:
        if os.path.exists(path):
            os.remove(path)
        with TsFileTableWriter(path, schema) as w:
            w.write_arrow_batch(batch)

        result = _read_all_arrow(
            path, table_name,
            ["tag", "bool_col", "int32_col", "int64_col",
             "float_col", "double_col", "str_col", "date_col"],
        )
        assert len(result) == n
        df = result.to_pandas().sort_values("time").reset_index(drop=True)

        for col in ["tag", "bool_col", "int32_col", "int64_col",
                    "float_col", "double_col", "str_col", "date_col"]:
            assert col in df.columns, f"Column '{col}' missing from result"

        assert list(df["int32_col"]) == list(range(n))
        assert list(df["int64_col"]) == [i * 10 for i in range(n)]
        for i in range(n):
            assert df["bool_col"].iloc[i] == (i % 2 == 0)
            assert abs(df["double_col"].iloc[i] - i * 1.1) < 1e-9
            assert df["str_col"].iloc[i] == f"s{i}"
    finally:
        if os.path.exists(path):
            os.remove(path)


# ---------------------------------------------------------------------------
# Parity with write_dataframe
# ---------------------------------------------------------------------------

def test_write_arrow_parity_with_dataframe():
    """Data written via write_arrow_batch must match data written via write_dataframe."""
    arrow_path = "test_write_arrow_parity_arrow.tsfile"
    df_path = "test_write_arrow_parity_df.tsfile"
    table_name = "t"
    n = 500

    schema_arrow = TableSchema(table_name, [
        ColumnSchema("device", TSDataType.STRING,  ColumnCategory.TAG),
        ColumnSchema("v_i32",  TSDataType.INT32,   ColumnCategory.FIELD),
        ColumnSchema("v_f64",  TSDataType.DOUBLE,  ColumnCategory.FIELD),
        ColumnSchema("v_bool", TSDataType.BOOLEAN, ColumnCategory.FIELD),
        ColumnSchema("v_str",  TSDataType.STRING,  ColumnCategory.FIELD),
    ])
    schema_df = TableSchema(table_name, [
        ColumnSchema("device", TSDataType.STRING,  ColumnCategory.TAG),
        ColumnSchema("v_i32",  TSDataType.INT32,   ColumnCategory.FIELD),
        ColumnSchema("v_f64",  TSDataType.DOUBLE,  ColumnCategory.FIELD),
        ColumnSchema("v_bool", TSDataType.BOOLEAN, ColumnCategory.FIELD),
        ColumnSchema("v_str",  TSDataType.STRING,  ColumnCategory.FIELD),
    ])

    timestamps = np.arange(n, dtype="int64")
    v_i32  = np.arange(n, dtype="int32")
    v_f64  = np.arange(n, dtype="float64") * 2.5
    v_bool = np.array([i % 3 == 0 for i in range(n)])
    v_str  = [f"row{i}" for i in range(n)]
    device = ["dev"] * n

    batch = pa.record_batch({
        "time":   pa.array(timestamps, type=pa.timestamp("ns")),
        "device": pa.array(device, type=pa.string()),
        "v_i32":  pa.array(v_i32, type=pa.int32()),
        "v_f64":  pa.array(v_f64, type=pa.float64()),
        "v_bool": pa.array(v_bool, type=pa.bool_()),
        "v_str":  pa.array(v_str, type=pa.string()),
    })

    dataframe = pd.DataFrame({
        "time":   pd.Series(timestamps, dtype="int64"),
        "device": device,
        "v_i32":  pd.Series(v_i32, dtype="int32"),
        "v_f64":  pd.Series(v_f64, dtype="float64"),
        "v_bool": pd.Series(v_bool, dtype="bool"),
        "v_str":  v_str,
    })

    cols = ["device", "v_i32", "v_f64", "v_bool", "v_str"]

    try:
        for p in (arrow_path, df_path):
            if os.path.exists(p):
                os.remove(p)

        with TsFileTableWriter(arrow_path, schema_arrow) as w:
            w.write_arrow_batch(batch)
        with TsFileTableWriter(df_path, schema_df) as w:
            w.write_dataframe(dataframe)

        result_arrow = _read_all_arrow(arrow_path, table_name, cols).to_pandas()
        result_df    = _read_all_arrow(df_path,    table_name, cols).to_pandas()

        result_arrow = result_arrow.sort_values("time").reset_index(drop=True)
        result_df    = result_df.sort_values("time").reset_index(drop=True)

        assert len(result_arrow) == len(result_df) == n

        assert list(result_arrow["v_i32"])  == list(result_df["v_i32"])
        assert list(result_arrow["v_str"])  == list(result_df["v_str"])
        assert list(result_arrow["v_bool"]) == list(result_df["v_bool"])
        for i in range(n):
            assert abs(result_arrow["v_f64"].iloc[i] - result_df["v_f64"].iloc[i]) < 1e-9
    finally:
        for p in (arrow_path, df_path):
            if os.path.exists(p):
                os.remove(p)


# ---------------------------------------------------------------------------
# Large batch
# ---------------------------------------------------------------------------

def test_write_arrow_large_batch():
    """Write a single large batch (100 k rows) and verify row count."""
    path = "test_write_arrow_large.tsfile"
    table_name = "t"
    n = 100_000

    schema = _make_schema(table_name, [
        ColumnSchema("v", TSDataType.DOUBLE, ColumnCategory.FIELD),
    ])

    batch = pa.record_batch({
        "time":   pa.array(np.arange(n, dtype="int64"), type=pa.timestamp("ns")),
        "device": pa.array(["d"] * n, type=pa.string()),
        "v":      pa.array(np.random.rand(n), type=pa.float64()),
    })

    try:
        if os.path.exists(path):
            os.remove(path)
        with TsFileTableWriter(path, schema) as w:
            w.write_arrow_batch(batch)

        result = _read_all_arrow(path, table_name, ["device", "v"], batch_size=8192)
        assert len(result) == n
    finally:
        if os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    pytest.main([__file__, "-v", "-s"])
