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

"""Benchmark legacy per-cell writes against reusable numeric block writes."""

from __future__ import annotations

import argparse
import ctypes
import math
import os
import statistics
import sys
import time
from pathlib import Path

LV_TYPE_DOUBLE = 4
LV_CAT_FIELD = 1
LV_ENCODING_GORILLA = 8
LV_COMPRESSION_LZ4 = 7


def default_library() -> Path:
    name = {
        "darwin": "libtsfile_labview.dylib",
        "win32": "tsfile_labview.dll",
    }.get(sys.platform, "libtsfile_labview.so")
    return Path(__file__).resolve().parents[2] / "target" / "build" / "lib" / name


def bind(dll_path: Path) -> ctypes.CDLL:
    directory_handles = []
    if os.name == "nt":
        directory_handles.append(os.add_dll_directory(str(dll_path.parent.resolve())))
    try:
        lib = ctypes.CDLL(str(dll_path.resolve()))
    finally:
        for handle in directory_handles:
            handle.close()

    handle = ctypes.c_uint64
    status = ctypes.c_int32
    lib.lv_tsfile_set_global_compression.argtypes = [ctypes.c_uint8]
    lib.lv_tsfile_set_global_compression.restype = status
    lib.lv_tsfile_set_datatype_encoding.argtypes = [ctypes.c_uint8, ctypes.c_uint8]
    lib.lv_tsfile_set_datatype_encoding.restype = status
    lib.get_global_compression.restype = ctypes.c_uint8
    lib.get_datatype_encoding.argtypes = [ctypes.c_uint8]
    lib.get_datatype_encoding.restype = ctypes.c_uint8
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
    lib.lv_tsfile_writer_flush.argtypes = [handle]
    lib.lv_tsfile_writer_flush.restype = status
    lib.lv_tsfile_writer_close.argtypes = [handle]
    lib.lv_tsfile_writer_close.restype = status
    lib.lv_tsfile_tablet_new.argtypes = [ctypes.c_uint32]
    lib.lv_tsfile_tablet_new.restype = handle
    lib.lv_tsfile_tablet_add_column.argtypes = [handle, ctypes.c_char_p, ctypes.c_uint8]
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


def open_writer(lib: ctypes.CDLL, path: Path, columns: list[bytes]) -> int:
    path.unlink(missing_ok=True)
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
        writer = ctypes.c_uint64()
        check(
            lib.lv_tsfile_writer_open(
                os.fsencode(path), builder, 0, ctypes.byref(writer)
            ),
            "writer_open",
        )
        return writer.value
    finally:
        lib.lv_tsfile_schema_builder_free(builder)


def make_batches(rows: int, cols: int, count: int):
    started = time.perf_counter()
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
    return batches, time.perf_counter() - started


def run_cell(lib, path, columns, batches, rows, cols) -> float:
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
            check(lib.lv_tsfile_tablet_finalize_columns(tablet), "tablet_finalize")
            for row in range(rows):
                check(
                    lib.lv_tsfile_tablet_set_timestamp(tablet, row, timestamps[row]),
                    "tablet_set_timestamp",
                )
                for col in range(cols):
                    check(
                        lib.lv_tsfile_tablet_set_f64(
                            tablet, row, col, data[row * cols + col]
                        ),
                        "tablet_set_f64",
                    )
            check(lib.lv_tsfile_writer_write(writer, tablet), "writer_write")
        finally:
            lib.lv_tsfile_tablet_free(tablet)
    check(lib.lv_tsfile_writer_flush(writer), "writer_flush")
    check(lib.lv_tsfile_writer_close(writer), "writer_close")
    return time.perf_counter() - started


def run_block(lib, path, columns, batches, rows, cols) -> dict[str, float]:
    opened = time.perf_counter()
    writer = open_writer(lib, path, columns)
    open_seconds = time.perf_counter() - opened

    started = time.perf_counter()
    for timestamps, data in batches:
        check(
            lib.lv_tsfile_write_block_f64(writer, timestamps, data, rows, cols),
            "write_block_f64",
        )
    writer_seconds = time.perf_counter() - started

    started = time.perf_counter()
    check(lib.lv_tsfile_writer_flush(writer), "writer_flush")
    flush_seconds = time.perf_counter() - started

    started = time.perf_counter()
    check(lib.lv_tsfile_writer_close(writer), "writer_close")
    close_seconds = time.perf_counter() - started
    return {
        "open_seconds": open_seconds,
        "writer_seconds": writer_seconds,
        "flush_seconds": flush_seconds,
        "close_seconds": close_seconds,
    }


def median(samples: list[dict[str, float]], key: str) -> float:
    return statistics.median(sample[key] for sample in samples)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dll", type=Path, default=default_library())
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--cols", type=int, default=9)
    parser.add_argument("--batches", type=int, default=30)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, default=Path.cwd())
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if args.smoke:
        args.rows, args.cols, args.batches, args.repeats = 100, 3, 2, 2
    if min(args.rows, args.cols, args.batches, args.repeats) <= 0:
        parser.error("rows, cols, batches, and repeats must be positive")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    lib = bind(args.dll)
    check(lib.lv_tsfile_set_global_compression(LV_COMPRESSION_LZ4), "set LZ4")
    check(
        lib.lv_tsfile_set_datatype_encoding(LV_TYPE_DOUBLE, LV_ENCODING_GORILLA),
        "set GORILLA",
    )
    effective_compression = int(lib.get_global_compression())
    effective_encoding = int(lib.get_datatype_encoding(LV_TYPE_DOUBLE))
    if (
        effective_compression != LV_COMPRESSION_LZ4
        or effective_encoding != LV_ENCODING_GORILLA
    ):
        raise RuntimeError(
            "effective codec configuration differs from requested values"
        )

    columns = [f"ch_{col}".encode() for col in range(args.cols)]
    batches, materialize_seconds = make_batches(args.rows, args.cols, args.batches)
    cell_path = args.output_dir / "lv_cell_benchmark.tsfile"
    block_path = args.output_dir / "lv_block_benchmark.tsfile"
    warmup_path = args.output_dir / "lv_block_benchmark_warmup.tsfile"
    run_block(lib, warmup_path, columns, batches[:1], args.rows, args.cols)
    warmup_path.unlink(missing_ok=True)

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

    phases = {
        "materialize_seconds": materialize_seconds,
        "writer_seconds": median(block_samples, "writer_seconds"),
        "flush_seconds": median(block_samples, "flush_seconds"),
        "close_seconds": median(block_samples, "close_seconds"),
    }
    if args.smoke and any(value <= 0 for value in phases.values()):
        raise RuntimeError(f"non-positive smoke phase: {phases}")

    cell_seconds = statistics.median(cell_samples)
    block_seconds = sum(
        median(block_samples, key)
        for key in ("open_seconds", "writer_seconds", "flush_seconds", "close_seconds")
    )
    points = args.rows * args.cols * args.batches
    print(
        f"rows={args.rows} cols={args.cols} batches={args.batches} "
        f"repeats={args.repeats} points={points}"
    )
    print(
        f"effective_value_encoding={effective_encoding} "
        f"effective_compression={effective_compression}"
    )
    for key, value in phases.items():
        print(f"{key}={value:.9f}")
    print(f"legacy_cell_seconds={cell_seconds:.9f}")
    print(f"block_total_seconds={block_seconds:.9f}")
    print(f"block_speedup={cell_seconds / block_seconds:.2f}x")
    print(f"output_bytes={block_path.stat().st_size}")


if __name__ == "__main__":
    main()
