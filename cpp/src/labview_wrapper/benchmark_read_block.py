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

"""Compare LabVIEW scalar-row reads with numeric TsBlock reads."""

from __future__ import annotations

import argparse
import ctypes
import math
import os
import statistics
import time
from pathlib import Path

from benchmark_block import bind, check, default_library, open_writer


def bind_reader(dll_path: Path) -> ctypes.CDLL:
    lib = bind(dll_path)
    handle = ctypes.c_uint64
    status = ctypes.c_int32
    lib.lv_tsfile_reader_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(handle)]
    lib.lv_tsfile_reader_open.restype = status
    lib.lv_tsfile_reader_close.argtypes = [handle]
    lib.lv_tsfile_reader_close.restype = status
    lib.lv_tsfile_query_table.argtypes = [
        handle,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_int64,
        ctypes.c_int64,
        ctypes.POINTER(handle),
    ]
    lib.lv_tsfile_query_table.restype = status
    lib.lv_tsfile_query_table_batch.argtypes = [
        handle,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_int64,
        ctypes.c_int64,
        ctypes.c_int32,
        ctypes.POINTER(handle),
    ]
    lib.lv_tsfile_query_table_batch.restype = status
    lib.lv_tsfile_rs_next.argtypes = [handle, ctypes.POINTER(status)]
    lib.lv_tsfile_rs_next.restype = ctypes.c_int32
    lib.lv_tsfile_rs_get_i64.argtypes = [handle, ctypes.c_uint32]
    lib.lv_tsfile_rs_get_i64.restype = ctypes.c_int64
    lib.lv_tsfile_rs_get_f64.argtypes = [handle, ctypes.c_uint32]
    lib.lv_tsfile_rs_get_f64.restype = ctypes.c_double
    lib.lv_tsfile_rs_read_block_f64.argtypes = [
        handle,
        ctypes.POINTER(ctypes.c_int64),
        ctypes.POINTER(ctypes.c_double),
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_int32,
        ctypes.c_int32,
        ctypes.POINTER(ctypes.c_int32),
    ]
    lib.lv_tsfile_rs_read_block_f64.restype = status
    lib.lv_tsfile_rs_free.argtypes = [handle]
    return lib


def create_input(lib, path: Path, rows: int, cols: int, columns: list[bytes]) -> None:
    timestamp_type = ctypes.c_int64 * rows
    data_type = ctypes.c_double * (rows * cols)
    timestamps = timestamp_type(*(1000 + row for row in range(rows)))
    values = data_type(
        *(
            math.sin(row * 0.01 + col * 0.1)
            for row in range(rows)
            for col in range(cols)
        )
    )
    writer = open_writer(lib, path, columns)
    check(
        lib.lv_tsfile_write_block_f64(writer, timestamps, values, rows, cols), "write"
    )
    check(lib.lv_tsfile_writer_flush(writer), "flush")
    check(lib.lv_tsfile_writer_close(writer), "close")


def open_reader(lib, path: Path) -> int:
    reader = ctypes.c_uint64()
    check(
        lib.lv_tsfile_reader_open(os.fsencode(path), ctypes.byref(reader)),
        "reader_open",
    )
    return reader.value


def read_rows(lib, path: Path, table: bytes, columns: bytes, cols: int):
    reader = open_reader(lib, path)
    result_set = ctypes.c_uint64()
    check(
        lib.lv_tsfile_query_table(
            reader, table, columns, 0, (1 << 63) - 1, ctypes.byref(result_set)
        ),
        "query_table",
    )
    started = time.perf_counter()
    row_count = 0
    checksum = 0.0
    error = ctypes.c_int32()
    while lib.lv_tsfile_rs_next(result_set, ctypes.byref(error)) == 1:
        checksum += int(lib.lv_tsfile_rs_get_i64(result_set, 0)) * 1e-12
        for col in range(cols):
            checksum += float(lib.lv_tsfile_rs_get_f64(result_set, col + 1))
        row_count += 1
    elapsed = time.perf_counter() - started
    check(error.value, "rs_next")
    lib.lv_tsfile_rs_free(result_set)
    check(lib.lv_tsfile_reader_close(reader), "reader_close")
    return row_count, checksum, elapsed


def read_blocks(
    lib, path: Path, table: bytes, columns: bytes, cols: int, batch_rows: int
):
    reader = open_reader(lib, path)
    result_set = ctypes.c_uint64()
    check(
        lib.lv_tsfile_query_table_batch(
            reader,
            table,
            columns,
            0,
            (1 << 63) - 1,
            batch_rows,
            ctypes.byref(result_set),
        ),
        "query_table_batch",
    )
    timestamps = (ctypes.c_int64 * batch_rows)()
    values = (ctypes.c_double * (batch_rows * cols))()
    copied = ctypes.c_int32()
    row_count = 0
    checksum = 0.0
    decode_seconds = 0.0
    host_copy_seconds = 0.0
    while True:
        started = time.perf_counter()
        check(
            lib.lv_tsfile_rs_read_block_f64(
                result_set,
                timestamps,
                values,
                None,
                batch_rows,
                cols,
                ctypes.byref(copied),
            ),
            "read_block_f64",
        )
        decode_seconds += time.perf_counter() - started
        if copied.value == 0:
            break
        started = time.perf_counter()
        for row in range(copied.value):
            checksum += int(timestamps[row]) * 1e-12
            offset = row * cols
            for col in range(cols):
                checksum += float(values[offset + col])
        host_copy_seconds += time.perf_counter() - started
        row_count += copied.value
    lib.lv_tsfile_rs_free(result_set)
    check(lib.lv_tsfile_reader_close(reader), "reader_close")
    return row_count, checksum, decode_seconds, host_copy_seconds


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dll", type=Path, default=default_library())
    parser.add_argument("--rows", type=int, default=150_000)
    parser.add_argument("--cols", type=int, default=9)
    parser.add_argument("--batch-rows", type=int, default=10_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, default=Path.cwd())
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if args.smoke:
        args.rows, args.cols, args.batch_rows, args.repeats = 100, 3, 64, 2
    if min(args.rows, args.cols, args.batch_rows, args.repeats) <= 0:
        parser.error("rows, cols, batch-rows, and repeats must be positive")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / "lv_read_benchmark.tsfile"
    table = b"vib"
    column_names = [f"ch_{col}".encode() for col in range(args.cols)]
    columns = b"\n".join(column_names)
    lib = bind_reader(args.dll)
    create_input(lib, path, args.rows, args.cols, column_names)

    warm_rows = read_rows(lib, path, table, columns, args.cols)
    warm_blocks = read_blocks(lib, path, table, columns, args.cols, args.batch_rows)
    if warm_rows[0] != warm_blocks[0] or not math.isclose(
        warm_rows[1], warm_blocks[1], rel_tol=1e-12, abs_tol=1e-9
    ):
        raise RuntimeError("warm-up row and block results differ")

    legacy_samples = []
    decode_samples = []
    copy_samples = []
    for _ in range(args.repeats):
        row_result = read_rows(lib, path, table, columns, args.cols)
        block_result = read_blocks(
            lib, path, table, columns, args.cols, args.batch_rows
        )
        if row_result[0] != args.rows or block_result[0] != args.rows:
            raise RuntimeError("unexpected result row count")
        if not math.isclose(
            row_result[1], block_result[1], rel_tol=1e-12, abs_tol=1e-9
        ):
            raise RuntimeError("row and block checksums differ")
        legacy_samples.append(row_result[2])
        decode_samples.append(block_result[2])
        copy_samples.append(block_result[3])

    result = {
        "legacy_row_seconds": statistics.median(legacy_samples),
        "batch_decode_seconds": statistics.median(decode_samples),
        "host_copy_seconds": statistics.median(copy_samples),
    }
    if args.smoke and any(value <= 0 for value in result.values()):
        raise RuntimeError(f"non-positive smoke phase: {result}")
    batch_total = result["batch_decode_seconds"] + result["host_copy_seconds"]
    print(
        f"rows={args.rows} cols={args.cols} batch_rows={args.batch_rows} "
        f"repeats={args.repeats}"
    )
    for key, value in result.items():
        print(f"{key}={value:.9f}")
    print(f"batch_total_seconds={batch_total:.9f}")
    print(f"batch_speedup={result['legacy_row_seconds'] / batch_total:.2f}x")
    print(f"total_rows={args.rows}")
    print(f"total_values={args.rows * args.cols}")


if __name__ == "__main__":
    main()
