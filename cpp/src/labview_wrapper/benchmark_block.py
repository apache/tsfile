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

"""Compare per-cell FFI calls with one-call LabVIEW block writes."""

from __future__ import annotations

import argparse
import ctypes
import math
import os
import statistics
import time
from pathlib import Path

LV_TYPE_DOUBLE = 4
LV_CAT_FIELD = 1
LV_ENCODING_GORILLA = 8
LV_COMPRESSION_LZ4 = 7


def bind(dll_path: Path) -> ctypes.CDLL:
    dll_directory_handles = []
    if os.name == "nt":
        search_dirs = [dll_path.parent.resolve()]
        search_dirs.extend(
            Path(entry)
            for entry in os.environ.get("PATH", "").split(os.pathsep)
            if entry
        )
        for directory in search_dirs:
            if directory.is_dir():
                dll_directory_handles.append(
                    os.add_dll_directory(str(directory.resolve()))
                )
    try:
        lib = ctypes.CDLL(str(dll_path.resolve()))
    finally:
        for handle in dll_directory_handles:
            handle.close()

    handle = ctypes.c_uint64
    status = ctypes.c_int32
    lib.lv_tsfile_set_global_compression.argtypes = [ctypes.c_uint8]
    lib.lv_tsfile_set_global_compression.restype = status
    lib.lv_tsfile_set_datatype_encoding.argtypes = [ctypes.c_uint8, ctypes.c_uint8]
    lib.lv_tsfile_set_datatype_encoding.restype = status
    lib.lv_tsfile_schema_builder_new.argtypes = [ctypes.c_char_p]
    lib.lv_tsfile_schema_builder_new.restype = handle
    lib.lv_tsfile_schema_builder_add_column.argtypes = [
        handle,
        ctypes.c_char_p,
        ctypes.c_uint8,
        ctypes.c_uint8,
    ]
    lib.lv_tsfile_schema_builder_add_column.restype = status
    lib.lv_tsfile_schema_builder_free.argtypes = [handle]

    lib.lv_tsfile_writer_open.argtypes = [
        ctypes.c_char_p,
        handle,
        ctypes.c_uint64,
        ctypes.POINTER(handle),
    ]
    lib.lv_tsfile_writer_open.restype = status
    lib.lv_tsfile_writer_write.argtypes = [handle, handle]
    lib.lv_tsfile_writer_write.restype = status
    lib.lv_tsfile_write_block_f64.argtypes = [
        handle,
        ctypes.POINTER(ctypes.c_int64),
        ctypes.POINTER(ctypes.c_double),
        ctypes.c_int32,
        ctypes.c_int32,
    ]
    lib.lv_tsfile_write_block_f64.restype = status
    lib.lv_tsfile_writer_close.argtypes = [handle]
    lib.lv_tsfile_writer_close.restype = status

    lib.lv_tsfile_tablet_new.argtypes = [ctypes.c_uint32]
    lib.lv_tsfile_tablet_new.restype = handle
    lib.lv_tsfile_tablet_add_column.argtypes = [
        handle,
        ctypes.c_char_p,
        ctypes.c_uint8,
    ]
    lib.lv_tsfile_tablet_add_column.restype = status
    lib.lv_tsfile_tablet_finalize_columns.argtypes = [handle]
    lib.lv_tsfile_tablet_finalize_columns.restype = status
    lib.lv_tsfile_tablet_set_timestamp.argtypes = [
        handle,
        ctypes.c_uint32,
        ctypes.c_int64,
    ]
    lib.lv_tsfile_tablet_set_timestamp.restype = status
    lib.lv_tsfile_tablet_set_f64.argtypes = [
        handle,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_double,
    ]
    lib.lv_tsfile_tablet_set_f64.restype = status
    lib.lv_tsfile_tablet_free.argtypes = [handle]
    return lib


def check(status: int, operation: str) -> None:
    if status != 0:
        raise RuntimeError(f"{operation} failed with status {status}")


def open_writer(lib: ctypes.CDLL, path: Path, columns: list[bytes]) -> ctypes.c_uint64:
    builder = lib.lv_tsfile_schema_builder_new(b"vib")
    if not builder:
        raise RuntimeError("schema builder creation failed")
    try:
        for name in columns:
            check(
                lib.lv_tsfile_schema_builder_add_column(
                    builder, name, LV_TYPE_DOUBLE, LV_CAT_FIELD
                ),
                "schema_builder_add_column",
            )
        path.unlink(missing_ok=True)
        writer = ctypes.c_uint64()
        check(
            lib.lv_tsfile_writer_open(
                os.fsencode(path), builder, 128 * 1024 * 1024, ctypes.byref(writer)
            ),
            "writer_open",
        )
        return writer
    finally:
        lib.lv_tsfile_schema_builder_free(builder)


def make_batches(rows: int, cols: int, count: int):
    timestamp_type = ctypes.c_int64 * rows
    data_type = ctypes.c_double * (rows * cols)
    batches = []
    for batch in range(count):
        start = batch * rows
        timestamps = timestamp_type(*(start + row for row in range(rows)))
        data = data_type(
            *(
                math.sin((start + row) * 0.01 + col * 0.1)
                for row in range(rows)
                for col in range(cols)
            )
        )
        batches.append((timestamps, data))
    return batches


def run_cell(
    lib: ctypes.CDLL,
    path: Path,
    columns: list[bytes],
    batches,
    rows: int,
    cols: int,
) -> float:
    started = time.perf_counter()
    writer = open_writer(lib, path, columns)
    for timestamps, data in batches:
        tablet = lib.lv_tsfile_tablet_new(rows)
        if not tablet:
            raise RuntimeError("tablet_new failed")
        try:
            for name in columns:
                check(
                    lib.lv_tsfile_tablet_add_column(tablet, name, LV_TYPE_DOUBLE),
                    "tablet_add_column",
                )
            check(
                lib.lv_tsfile_tablet_finalize_columns(tablet),
                "tablet_finalize_columns",
            )
            for row in range(rows):
                check(
                    lib.lv_tsfile_tablet_set_timestamp(tablet, row, timestamps[row]),
                    "tablet_set_timestamp",
                )
                row_offset = row * cols
                for col in range(cols):
                    check(
                        lib.lv_tsfile_tablet_set_f64(
                            tablet, row, col, data[row_offset + col]
                        ),
                        "tablet_set_f64",
                    )
            check(lib.lv_tsfile_writer_write(writer, tablet), "writer_write")
        finally:
            lib.lv_tsfile_tablet_free(tablet)
    check(lib.lv_tsfile_writer_close(writer), "writer_close")
    return time.perf_counter() - started


def run_block(
    lib: ctypes.CDLL,
    path: Path,
    columns: list[bytes],
    batches,
    rows: int,
    cols: int,
) -> float:
    started = time.perf_counter()
    writer = open_writer(lib, path, columns)
    for timestamps, data in batches:
        check(
            lib.lv_tsfile_write_block_f64(writer, timestamps, data, rows, cols),
            "write_block_f64",
        )
    check(lib.lv_tsfile_writer_close(writer), "writer_close")
    return time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dll", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=2_000)
    parser.add_argument("--cols", type=int, default=9)
    parser.add_argument("--batches", type=int, default=3)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, default=Path.cwd())
    args = parser.parse_args()
    if args.rows <= 0 or args.cols <= 0 or args.batches <= 0 or args.repeats <= 0:
        parser.error("rows, cols, batches, and repeats must be positive")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    lib = bind(args.dll)
    check(
        lib.lv_tsfile_set_global_compression(LV_COMPRESSION_LZ4),
        "set_global_compression(LZ4)",
    )
    check(
        lib.lv_tsfile_set_datatype_encoding(LV_TYPE_DOUBLE, LV_ENCODING_GORILLA),
        "set_datatype_encoding(DOUBLE, GORILLA)",
    )
    columns = [f"ch_{col}".encode() for col in range(args.cols)]
    batches = make_batches(args.rows, args.cols, args.batches)

    cell_path = args.output_dir / "lv_cell_benchmark.tsfile"
    block_path = args.output_dir / "lv_block_benchmark.tsfile"
    cell_samples = []
    block_samples = []
    for repeat in range(args.repeats):
        if repeat % 2 == 0:
            cell_samples.append(
                run_cell(lib, cell_path, columns, batches, args.rows, args.cols)
            )
            block_samples.append(
                run_block(lib, block_path, columns, batches, args.rows, args.cols)
            )
        else:
            block_samples.append(
                run_block(lib, block_path, columns, batches, args.rows, args.cols)
            )
            cell_samples.append(
                run_cell(lib, cell_path, columns, batches, args.rows, args.cols)
            )

    cell_seconds = statistics.median(cell_samples)
    block_seconds = statistics.median(block_samples)

    points = args.rows * args.cols * args.batches
    value_bytes = points * ctypes.sizeof(ctypes.c_double)
    cell_calls = args.batches * (args.rows * (args.cols + 1) + args.cols + 4)
    block_calls = args.batches
    print(
        f"rows={args.rows} cols={args.cols} batches={args.batches} "
        f"repeats={args.repeats} points={points} value_bytes={value_bytes}"
    )
    print("cell samples : " + ", ".join(f"{sample:.6f}s" for sample in cell_samples))
    print("block samples: " + ", ".join(f"{sample:.6f}s" for sample in block_samples))
    print(
        f"cell : {cell_seconds:.6f}s, {points / cell_seconds:,.0f} points/s, "
        f"{value_bytes / cell_seconds / (1024 * 1024):,.2f} MiB/s values, "
        f"~{cell_calls:,} FFI calls in batch loop, "
        f"{cell_path.stat().st_size:,} file bytes"
    )
    print(
        f"block: {block_seconds:.6f}s, {points / block_seconds:,.0f} points/s, "
        f"{value_bytes / block_seconds / (1024 * 1024):,.2f} MiB/s values, "
        f"{block_calls:,} FFI calls in batch loop, "
        f"{block_path.stat().st_size:,} file bytes"
    )
    print(f"speedup: {cell_seconds / block_seconds:.2f}x")


if __name__ == "__main__":
    main()
