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
Benchmark: write_arrow_batch vs write_dataframe.

Compares write throughput (rows/s) for:
  - Arrow path  : write_arrow_batch(pa.RecordBatch)
  - DataFrame path: write_dataframe(pd.DataFrame)

Run:
  python -m pytest tests/bench_write_arrow_vs_dataframe.py -v -s
  python tests/bench_write_arrow_vs_dataframe.py [row_count [batch_size]]
"""

import os
import sys
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    TSDataType,
    TsFileTableWriter,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_ROW_COUNT   = 100_000
DEFAULT_BATCH_SIZE  = 8_192
DEFAULT_ROUNDS      = 3

TABLE_NAME = "bench_table"
BENCH_FILE = "bench_write_arrow.tsfile"

SCHEMA = TableSchema(TABLE_NAME, [
    ColumnSchema("device", TSDataType.STRING,  ColumnCategory.TAG),
    ColumnSchema("v_i64",  TSDataType.INT64,   ColumnCategory.FIELD),
    ColumnSchema("v_f64",  TSDataType.DOUBLE,  ColumnCategory.FIELD),
    ColumnSchema("v_bool", TSDataType.BOOLEAN, ColumnCategory.FIELD),
    ColumnSchema("v_str",  TSDataType.STRING,  ColumnCategory.FIELD),
])


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def _make_numpy_data(row_count: int):
    ts     = np.arange(row_count, dtype="int64")
    v_i64  = np.arange(row_count, dtype="int64")
    v_f64  = np.arange(row_count, dtype="float64") * 1.5
    v_bool = (np.arange(row_count) % 2 == 0)
    v_str  = [f"s{i}" for i in range(row_count)]
    device = ["device0"] * row_count
    return ts, device, v_i64, v_f64, v_bool, v_str


def _make_arrow_batches(row_count: int, batch_size: int):
    ts, device, v_i64, v_f64, v_bool, v_str = _make_numpy_data(row_count)
    batches = []
    for start in range(0, row_count, batch_size):
        end = min(start + batch_size, row_count)
        batches.append(pa.record_batch({
            "time":   pa.array(ts[start:end],     type=pa.timestamp("ns")),
            "device": pa.array(device[start:end], type=pa.string()),
            "v_i64":  pa.array(v_i64[start:end],  type=pa.int64()),
            "v_f64":  pa.array(v_f64[start:end],  type=pa.float64()),
            "v_bool": pa.array(v_bool[start:end], type=pa.bool_()),
            "v_str":  pa.array(v_str[start:end],  type=pa.string()),
        }))
    return batches


def _make_dataframe_chunks(row_count: int, batch_size: int):
    ts, device, v_i64, v_f64, v_bool, v_str = _make_numpy_data(row_count)
    chunks = []
    for start in range(0, row_count, batch_size):
        end = min(start + batch_size, row_count)
        chunks.append(pd.DataFrame({
            "time":   pd.Series(ts[start:end], dtype="int64"),
            "device": device[start:end],
            "v_i64":  pd.Series(v_i64[start:end], dtype="int64"),
            "v_f64":  pd.Series(v_f64[start:end], dtype="float64"),
            "v_bool": pd.Series(v_bool[start:end], dtype="bool"),
            "v_str":  v_str[start:end],
        }))
    return chunks


# ---------------------------------------------------------------------------
# Benchmark runners
# ---------------------------------------------------------------------------

def _write_arrow(file_path: str, batches):
    schema = TableSchema(TABLE_NAME, [
        ColumnSchema("device", TSDataType.STRING,  ColumnCategory.TAG),
        ColumnSchema("v_i64",  TSDataType.INT64,   ColumnCategory.FIELD),
        ColumnSchema("v_f64",  TSDataType.DOUBLE,  ColumnCategory.FIELD),
        ColumnSchema("v_bool", TSDataType.BOOLEAN, ColumnCategory.FIELD),
        ColumnSchema("v_str",  TSDataType.STRING,  ColumnCategory.FIELD),
    ])
    with TsFileTableWriter(file_path, schema) as w:
        for batch in batches:
            w.write_arrow_batch(batch)


def _write_dataframe(file_path: str, chunks):
    schema = TableSchema(TABLE_NAME, [
        ColumnSchema("device", TSDataType.STRING,  ColumnCategory.TAG),
        ColumnSchema("v_i64",  TSDataType.INT64,   ColumnCategory.FIELD),
        ColumnSchema("v_f64",  TSDataType.DOUBLE,  ColumnCategory.FIELD),
        ColumnSchema("v_bool", TSDataType.BOOLEAN, ColumnCategory.FIELD),
        ColumnSchema("v_str",  TSDataType.STRING,  ColumnCategory.FIELD),
    ])
    with TsFileTableWriter(file_path, schema) as w:
        for chunk in chunks:
            w.write_dataframe(chunk)


def _run_timed(label: str, func, *args, rounds: int = DEFAULT_ROUNDS, row_count: int = 0):
    times = []
    for _ in range(rounds):
        if os.path.exists(BENCH_FILE):
            os.remove(BENCH_FILE)
        t0 = time.perf_counter()
        func(BENCH_FILE, *args)
        times.append(time.perf_counter() - t0)
    avg = sum(times) / len(times)
    best = min(times)
    rps = row_count / avg if avg > 0 else 0
    print(f"  {label:42s}  avg={avg:.3f}s  best={best:.3f}s  {rps:>10.0f} rows/s")
    return avg


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------

def run_benchmark(
    row_count: int = DEFAULT_ROW_COUNT,
    batch_size: int = DEFAULT_BATCH_SIZE,
    rounds: int = DEFAULT_ROUNDS,
):
    print()
    print(f"=== write benchmark: {row_count:,} rows, batch_size={batch_size}, rounds={rounds} ===")

    # Pre-build data once (exclude data-preparation time from timing)
    arrow_batches = _make_arrow_batches(row_count, batch_size)
    df_chunks     = _make_dataframe_chunks(row_count, batch_size)

    df_avg = _run_timed(
        "write_dataframe",
        _write_dataframe, df_chunks,
        rounds=rounds, row_count=row_count,
    )
    arrow_avg = _run_timed(
        "write_arrow_batch",
        _write_arrow, arrow_batches,
        rounds=rounds, row_count=row_count,
    )

    print()
    if arrow_avg > 0 and df_avg > 0:
        ratio = df_avg / arrow_avg
        if ratio >= 1.0:
            print(f"  Arrow is {ratio:.2f}x faster than DataFrame")
        else:
            print(f"  DataFrame is {1/ratio:.2f}x faster than Arrow")
    print()

    if os.path.exists(BENCH_FILE):
        os.remove(BENCH_FILE)

    return df_avg, arrow_avg


# ---------------------------------------------------------------------------
# Pytest entry points
# ---------------------------------------------------------------------------

def test_bench_write_arrow_small():
    """Quick sanity check with small data (5 k rows)."""
    run_benchmark(row_count=5_000, batch_size=1_024, rounds=2)


def test_bench_write_arrow_default():
    """Default benchmark (100 k rows)."""
    run_benchmark(
        row_count=DEFAULT_ROW_COUNT,
        batch_size=DEFAULT_BATCH_SIZE,
        rounds=DEFAULT_ROUNDS,
    )


def test_bench_write_arrow_large():
    """Large benchmark (1 M rows)."""
    run_benchmark(row_count=10_000_000, batch_size=32_384, rounds=3)


# ---------------------------------------------------------------------------
# Script entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    row_count  = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROW_COUNT
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_BATCH_SIZE
    run_benchmark(row_count=row_count, batch_size=batch_size)
