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
Benchmark: query_table_batch + read_arrow_batch vs query_table + read_data_frame.

Compares throughput and elapsed time when reading the same table data via
  - Arrow path: query_table_batch(batch_size=N) then read_arrow_batch() in a loop
  - DataFrame path: query_table() then result.next() + read_data_frame(N) in a loop

Run from project root or python/tests, e.g.:
  python -m pytest tests/bench_batch_arrow_vs_dataframe.py -v -s
  python tests/bench_batch_arrow_vs_dataframe.py   # if run as script
"""

import os
import sys
import time
from os import remove

import pandas as pd
import pyarrow as pa
import pytest

from tsfile import (
    ColumnSchema,
    ColumnCategory,
    TSDataType,
    TableSchema,
    TsFileReader,
    TsFileTableWriter,
)

# Default benchmark size
DEFAULT_ROW_COUNT = 50_000
DEFAULT_BATCH_SIZE = 4096
DEFAULT_TIMED_ROUNDS = 3

BENCH_FILE = "bench_arrow_vs_dataframe.tsfile"
TABLE_NAME = "bench_table"
COLUMNS = ["device", "value1", "value2"]


def _ensure_bench_tsfile(file_path: str, row_count: int) -> None:
    """Create tsfile with table data if not present. Uses DataFrame for fast data generation."""
    if os.path.exists(file_path):
        remove(file_path)
    # Build data with pandas/numpy (vectorized, much faster than row-by-row Tablet)
    import numpy as np

    df = pd.DataFrame(
        {
            "time": np.arange(row_count, dtype=np.int64),
            "device": pd.Series([f"device" for i in range(row_count)]),
            "value1": np.arange(0, row_count * 10, 10, dtype=np.int64),
            "value2": np.arange(row_count, dtype=np.float64) * 1.5,
        }
    )

    table = TableSchema(
        TABLE_NAME,
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value1", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("value2", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(file_path, table) as writer:
        writer.write_dataframe(df)


def _read_via_arrow(file_path: str, batch_size: int, end_time: int) -> int:
    """Read all rows using query_table_batch + read_arrow_batch. Returns total rows."""
    reader = TsFileReader(file_path)
    result_set = reader.query_table(
        table_name=TABLE_NAME,
        column_names=COLUMNS,
        start_time=0,
        end_time=end_time,
        batch_size=batch_size,
    )
    total_rows = 0
    try:
        while True:
            batch = result_set.read_arrow_batch()
            if batch is None:
                break
            total_rows += len(batch)
    finally:
        result_set.close()
        reader.close()
    return total_rows


def _read_via_dataframe(file_path: str, batch_size: int, end_time: int) -> int:
    """Read all rows using query_table + next + read_data_frame. Returns total rows."""
    reader = TsFileReader(file_path)
    result_set = reader.query_table(
        TABLE_NAME,
        COLUMNS,
        start_time=0,
        end_time=end_time,
    )
    total_rows = 0
    try:
        while result_set.next():
            df = result_set.read_data_frame(max_row_num=batch_size)
            if df is None or len(df) == 0:
                break
            total_rows += len(df)
    finally:
        result_set.close()
        reader.close()
    return total_rows


def _run_timed(name: str, func, *args, rounds: int = DEFAULT_TIMED_ROUNDS):
    times = []
    for _ in range(rounds):
        start = time.perf_counter()
        n = func(*args)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    avg = sum(times) / len(times)
    total_rows = n
    rows_per_sec = total_rows / avg if avg > 0 else 0
    print(
        f"  {name}: {avg:.3f}s avg ({min(times):.3f}s min)  rows={total_rows}  {rows_per_sec:.0f} rows/s"
    )
    return avg, total_rows


def run_benchmark(
    row_count: int = DEFAULT_ROW_COUNT,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timed_rounds: int = DEFAULT_TIMED_ROUNDS,
    file_path: str = BENCH_FILE,
):
    _ensure_bench_tsfile(file_path, row_count)
    end_time = row_count + 1

    print(
        f"Benchmark: {row_count} rows, batch_size={batch_size}, timed_rounds={timed_rounds}"
    )

    df_avg, df_rows = _run_timed(
        "query_table + read_data_frame",
        _read_via_dataframe,
        file_path,
        batch_size,
        end_time,
        rounds=timed_rounds,
    )

    arrow_avg, arrow_rows = _run_timed(
        "query_table_batch + read_arrow_batch",
        _read_via_arrow,
        file_path,
        batch_size,
        end_time,
        rounds=timed_rounds,
    )
    print()
    if df_avg > 0:
        speedup = arrow_avg / df_avg
        print(
            f"  Arrow vs DataFrame time ratio: {speedup:.2f}x ({'Arrow faster' if speedup < 1 else 'DataFrame faster'})"
        )
    assert df_rows == row_count, f"DataFrame path row count {df_rows} != {row_count}"
    assert arrow_rows == row_count, f"Arrow path row count {arrow_rows} != {row_count}"

    print()
    return df_avg, arrow_avg


def test_bench_arrow_vs_dataframe_default():
    """Run benchmark with default size (quick sanity check)."""
    run_benchmark(
        row_count=5_000,
        batch_size=1024,
        timed_rounds=2,
    )


def test_bench_arrow_vs_dataframe_medium():
    """Run benchmark with medium size."""
    run_benchmark(
        row_count=DEFAULT_ROW_COUNT,
        batch_size=DEFAULT_BATCH_SIZE,
        timed_rounds=DEFAULT_TIMED_ROUNDS,
    )


def test_bench_arrow_vs_dataframe_large():
    run_benchmark(
        row_count=2000_000,
        batch_size=8192,
        timed_rounds=3,
    )


if __name__ == "__main__":
    row_count = DEFAULT_ROW_COUNT
    batch_size = DEFAULT_BATCH_SIZE
    if len(sys.argv) > 1:
        row_count = int(sys.argv[1])
    if len(sys.argv) > 2:
        batch_size = int(sys.argv[2])
    run_benchmark(row_count=row_count, batch_size=batch_size)
    # Clean up bench file when run as script (optional)
    if os.path.exists(BENCH_FILE):
        os.remove(BENCH_FILE)
