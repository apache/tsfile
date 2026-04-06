#!/usr/bin/env python3
"""
E6-1 (Python): TsFile vs Parquet -- End-to-End Real Dataset Benchmark

Python-side counterpart of dataset_bench.cpp.
Compares read performance of TsFile (via Python bindings + Arrow batch)
against PyArrow/Parquet.

Usage:
  python py_dataset_bench.py --dataset geolife \
      --data-dir ../datasets/prepared/geolife
  python py_dataset_bench.py --all --data-root ../datasets/prepared
"""

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

# ── Ensure the python/tsfile package is importable ──────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "../../../../python"))
if PYTHON_DIR not in sys.path:
    sys.path.insert(0, PYTHON_DIR)

from tsfile import (
    TsFileReader,
    TagFilter,
    TsFileWriter,
    TableSchema,
    ColumnSchema,
    ColumnCategory,
)
from tsfile.constants import TSDataType

# ── Dataset Descriptors ─────────────────────────────────────────────────────

@dataclass
class FieldDesc:
    name: str
    ts_type: TSDataType
    arrow_type: pa.DataType


@dataclass
class DatasetConfig:
    name: str
    table_name: str
    tag_names: List[str]
    fields: List[FieldDesc]


DATASETS = {
    "geolife": DatasetConfig(
        "geolife", "geolife", ["user_id"],
        [FieldDesc("latitude", TSDataType.DOUBLE, pa.float64()),
         FieldDesc("longitude", TSDataType.DOUBLE, pa.float64()),
         FieldDesc("altitude", TSDataType.DOUBLE, pa.float64())],
    ),
    "tdrive": DatasetConfig(
        "tdrive", "tdrive", ["taxi_id"],
        [FieldDesc("longitude", TSDataType.DOUBLE, pa.float64()),
         FieldDesc("latitude", TSDataType.DOUBLE, pa.float64())],
    ),
    "tsbs": DatasetConfig(
        "tsbs", "tsbs", ["name", "fleet", "driver"],
        [FieldDesc("latitude", TSDataType.DOUBLE, pa.float64()),
         FieldDesc("longitude", TSDataType.DOUBLE, pa.float64()),
         FieldDesc("elevation", TSDataType.DOUBLE, pa.float64()),
         FieldDesc("velocity", TSDataType.DOUBLE, pa.float64())],
    ),
}

# ── CSV Loader ──────────────────────────────────────────────────────────────

def load_csv(csv_path: str, cfg: DatasetConfig):
    """Return (timestamps, tags_list, fields_list) as column-oriented lists."""
    num_tags = len(cfg.tag_names)
    num_fields = len(cfg.fields)
    expected = 1 + num_tags + num_fields

    timestamps = []
    tags_cols = [[] for _ in range(num_tags)]
    field_cols = [[] for _ in range(num_fields)]

    with open(csv_path, "r") as f:
        next(f)  # skip header
        for line in f:
            parts = line.rstrip("\n").split(",")
            if len(parts) < expected:
                continue
            timestamps.append(int(parts[0]))
            for t in range(num_tags):
                tags_cols[t].append(parts[1 + t])
            for fi in range(num_fields):
                field_cols[fi].append(float(parts[1 + num_tags + fi]))
            if len(timestamps) % 5_000_000 == 0:
                print(f"  loaded {len(timestamps) // 1_000_000}M rows...")

    return timestamps, tags_cols, field_cols

# ── Write TsFile ────────────────────────────────────────────────────────────

def write_tsfile(path: str, cfg: DatasetConfig, timestamps, tags_cols, field_cols):
    """Write dataset to TsFile using write_arrow_batch (Arrow C Data Interface).

    Data is already sorted by (device, timestamp) in the CSV.  We detect
    device boundaries and write one Arrow batch per device chunk (up to
    BATCH_CAP rows each) so that TsFile can build per-device chunk groups.
    """
    from tsfile import TsFileTableWriter, TableSchema, ColumnSchema, ColumnCategory
    from tsfile.constants import TSDataType

    # Build schema
    col_schemas = [
        ColumnSchema("time", TSDataType.TIMESTAMP, ColumnCategory.TIME),
    ]
    for tag in cfg.tag_names:
        col_schemas.append(ColumnSchema(tag, TSDataType.STRING, ColumnCategory.TAG))
    for fd in cfg.fields:
        col_schemas.append(ColumnSchema(fd.name, fd.ts_type, ColumnCategory.FIELD))
    schema = TableSchema(cfg.table_name, col_schemas)

    # Arrow schema (matches column order: time, tags, fields)
    arrow_fields = [pa.field("time", pa.int64())]
    for tag in cfg.tag_names:
        arrow_fields.append(pa.field(tag, pa.utf8()))
    for fd in cfg.fields:
        arrow_fields.append(pa.field(fd.name, fd.arrow_type))
    arrow_schema = pa.schema(arrow_fields)

    n = len(timestamps)
    num_tags = len(cfg.tag_names)

    # Detect device boundaries: data sorted by (tags, timestamp)
    def device_key(i):
        return tuple(tags_cols[t][i] for t in range(num_tags))

    with TsFileTableWriter(path, schema) as writer:
        off = 0
        while off < n:
            cur_dev = device_key(off)
            dev_end = off + 1
            while dev_end < n and device_key(dev_end) == cur_dev:
                dev_end += 1

            # Write this device in batches
            doff = off
            while doff < dev_end:
                batch_end = min(doff + BATCH_CAP, dev_end)
                arrays = [pa.array(timestamps[doff:batch_end], type=pa.int64())]
                for tc in tags_cols:
                    arrays.append(pa.array(tc[doff:batch_end], type=pa.utf8()))
                for fc in field_cols:
                    arrays.append(pa.array(fc[doff:batch_end], type=pa.float64()))
                batch = pa.record_batch(arrays, schema=arrow_schema)
                writer.write_arrow_batch(batch)
                doff = batch_end
            off = dev_end

# ── Write Parquet ───────────────────────────────────────────────────────────

BATCH_CAP = 65536

def write_parquet(path: str, cfg: DatasetConfig, timestamps, tags_cols, field_cols):
    """Write dataset to Parquet with SNAPPY compression, 64K row groups."""
    arrow_fields = [pa.field("time", pa.int64())]
    for tag in cfg.tag_names:
        arrow_fields.append(pa.field(tag, pa.utf8()))
    for fd in cfg.fields:
        arrow_fields.append(pa.field(fd.name, fd.arrow_type))
    schema = pa.schema(arrow_fields)

    writer = pq.ParquetWriter(path, schema, compression="snappy")
    n = len(timestamps)
    off = 0
    while off < n:
        end = min(off + BATCH_CAP, n)
        arrays = [pa.array(timestamps[off:end], type=pa.int64())]
        for tc in tags_cols:
            arrays.append(pa.array(tc[off:end], type=pa.utf8()))
        for fc in field_cols:
            arrays.append(pa.array(fc[off:end], type=pa.float64()))
        batch = pa.record_batch(arrays, schema=schema)
        writer.write_batch(batch)
        off = end
    writer.close()

# ── Read Benchmarks ─────────────────────────────────────────────────────────

@dataclass
class BenchResult:
    dataset: str
    experiment: str
    engine: str
    params: str
    seconds: float
    result_rows: int
    rows_per_sec: float


results: List[BenchResult] = []


def record(ds, exp, engine, params, secs, rows):
    tput = rows / secs / 1e6 if secs > 0 else 0
    results.append(BenchResult(ds, exp, engine, params, secs, rows,
                               rows / secs if secs > 0 else 0))
    p = f"  [{params}]" if params else ""
    print(f"  {exp:<18s}{engine:<10s}{secs:.3f} s  {tput:.2f} M rows/s{p}")

# ── TsFile Reads ────────────────────────────────────────────────────────────

TSFILE_BATCH_SIZE = BATCH_CAP  # 65536, same as C++ kBatchSize

def tsfile_full_scan(path: str, cfg: DatasetConfig, ts_max: int) -> int:
    cols = cfg.tag_names + [f.name for f in cfg.fields]
    total = 0
    with TsFileReader(path) as reader:
        with reader.query_table_batch(cfg.table_name, cols,
                                      start_time=0, end_time=ts_max,
                                      batch_size=TSFILE_BATCH_SIZE) as rs:
            while True:
                batch = rs.read_arrow_batch()
                if batch is None:
                    break
                total += batch.num_rows
    return total


def tsfile_tag_filter(path: str, cfg: DatasetConfig, ts_max: int,
                      tag_name: str, tag_value: str) -> int:
    cols = cfg.tag_names + [f.name for f in cfg.fields]
    total = 0
    with TsFileReader(path) as reader:
        tf = TagFilter.eq(reader, cfg.table_name, tag_name, tag_value)
        with reader.query_table_batch(cfg.table_name, cols,
                                      start_time=0, end_time=ts_max,
                                      batch_size=TSFILE_BATCH_SIZE,
                                      tag_filter=tf) as rs:
            while True:
                batch = rs.read_arrow_batch()
                if batch is None:
                    break
                total += batch.num_rows
    return total


def tsfile_time_filter(path: str, cfg: DatasetConfig,
                       ts_start: int, ts_end: int) -> int:
    cols = cfg.tag_names + [f.name for f in cfg.fields]
    total = 0
    with TsFileReader(path) as reader:
        with reader.query_table_batch(cfg.table_name, cols,
                                      start_time=ts_start,
                                      end_time=ts_end,
                                      batch_size=TSFILE_BATCH_SIZE) as rs:
            while True:
                batch = rs.read_arrow_batch()
                if batch is None:
                    break
                total += batch.num_rows
    return total

def tsfile_tag_time_filter(path: str, cfg: DatasetConfig,
                           tag_name: str, tag_value: str,
                           ts_start: int, ts_end: int) -> int:
    cols = cfg.tag_names + [f.name for f in cfg.fields]
    total = 0
    with TsFileReader(path) as reader:
        tf = TagFilter.eq(reader, cfg.table_name, tag_name, tag_value)
        with reader.query_table_batch(cfg.table_name, cols,
                                      start_time=ts_start, end_time=ts_end,
                                      batch_size=TSFILE_BATCH_SIZE,
                                      tag_filter=tf) as rs:
            while True:
                batch = rs.read_arrow_batch()
                if batch is None:
                    break
                total += batch.num_rows
    return total


# ── Parquet Reads ───────────────────────────────────────────────────────────

def parquet_full_scan(path: str) -> int:
    pf = pq.ParquetFile(path, memory_map=False)
    total = 0
    for batch in pf.iter_batches():
        total += batch.num_rows
    return total


def parquet_tag_filter(path: str, tag_col: str, tag_value: str) -> int:
    """Parquet tag filter with row-group pruning via min/max statistics."""
    pf = pq.ParquetFile(path, memory_map=False)
    meta = pf.metadata
    schema = pf.schema_arrow

    tag_idx = schema.get_field_index(tag_col)
    matching_rgs = []
    for rg_i in range(meta.num_row_groups):
        rg = meta.row_group(rg_i)
        col_meta = rg.column(tag_idx)
        if col_meta.statistics is not None and col_meta.statistics.has_min_max:
            mn = col_meta.statistics.min
            mx = col_meta.statistics.max
            if tag_value < mn or tag_value > mx:
                continue
        matching_rgs.append(rg_i)

    total = 0
    for rg_i in matching_rgs:
        table = pf.read_row_group(rg_i)
        col = table.column(tag_col)
        mask = pa.compute.equal(col, tag_value)
        total += pa.compute.sum(mask.cast(pa.int64())).as_py()
    return total


def parquet_time_filter(path: str, ts_start: int, ts_end: int) -> int:
    """Parquet time filter with row-group pruning via min/max statistics."""
    pf = pq.ParquetFile(path, memory_map=False)
    meta = pf.metadata
    schema = pf.schema_arrow

    time_idx = schema.get_field_index("time")
    matching_rgs = []
    for rg_i in range(meta.num_row_groups):
        rg = meta.row_group(rg_i)
        col_meta = rg.column(time_idx)
        if col_meta.statistics is not None and col_meta.statistics.has_min_max:
            mn = col_meta.statistics.min
            mx = col_meta.statistics.max
            if mx < ts_start or mn >= ts_end:
                continue
        matching_rgs.append(rg_i)

    total = 0
    for rg_i in matching_rgs:
        table = pf.read_row_group(rg_i)
        time_col = table.column("time")
        mask = pa.compute.and_(
            pa.compute.greater_equal(time_col, ts_start),
            pa.compute.less(time_col, ts_end),
        )
        total += pa.compute.sum(mask.cast(pa.int64())).as_py()
    return total

def parquet_tag_time_filter(path: str, tag_col: str, tag_value: str,
                            ts_start: int, ts_end: int) -> int:
    pf = pq.ParquetFile(path, memory_map=False)
    meta = pf.metadata
    schema = pf.schema_arrow
    tag_idx = schema.get_field_index(tag_col)
    time_idx = schema.get_field_index("time")

    matching_rgs = []
    for rg_i in range(meta.num_row_groups):
        rg = meta.row_group(rg_i)
        # Tag pruning
        tag_meta = rg.column(tag_idx)
        if tag_meta.statistics is not None and tag_meta.statistics.has_min_max:
            if tag_value < tag_meta.statistics.min or tag_value > tag_meta.statistics.max:
                continue
        # Time pruning
        time_meta = rg.column(time_idx)
        if time_meta.statistics is not None and time_meta.statistics.has_min_max:
            if time_meta.statistics.max < ts_start or time_meta.statistics.min >= ts_end:
                continue
        matching_rgs.append(rg_i)

    total = 0
    for rg_i in matching_rgs:
        table = pf.read_row_group(rg_i)
        tag_mask = pa.compute.equal(table.column(tag_col), tag_value)
        time_col = table.column("time")
        time_mask = pa.compute.and_(
            pa.compute.greater_equal(time_col, ts_start),
            pa.compute.less(time_col, ts_end),
        )
        mask = pa.compute.and_(tag_mask, time_mask)
        total += pa.compute.sum(mask.cast(pa.int64())).as_py()
    return total


# ── Run Experiments ─────────────────────────────────────────────────────────

def run_experiments(cfg: DatasetConfig, ts_path: str, pq_path: str,
                    ts_min: int, ts_max: int,
                    sample_tag_name: str, sample_tag_value: str,
                    dev_ts_min: int = 0, dev_ts_max: int = 0):
    # 1. Full scan
    print("\n=== Full Scan ===")
    t0 = time.perf_counter()
    rows = tsfile_full_scan(ts_path, cfg, ts_max)
    record(cfg.name, "full_scan", "tsfile", "", time.perf_counter() - t0, rows)

    t0 = time.perf_counter()
    rows = parquet_full_scan(pq_path)
    record(cfg.name, "full_scan", "parquet", "", time.perf_counter() - t0, rows)

    # 2. Tag filter (single device)
    print(f'\n=== Tag Filter ===')
    print(f'  filter: {sample_tag_name}="{sample_tag_value}"')

    t0 = time.perf_counter()
    rows = tsfile_tag_filter(ts_path, cfg, ts_max, sample_tag_name, sample_tag_value)
    record(cfg.name, "tag_filter", "tsfile", sample_tag_value,
           time.perf_counter() - t0, rows)

    t0 = time.perf_counter()
    rows = parquet_tag_filter(pq_path, sample_tag_name, sample_tag_value)
    record(cfg.name, "tag_filter", "parquet", sample_tag_value,
           time.perf_counter() - t0, rows)

    # 3. Time filter at varying selectivity
    print("\n=== Time Filter ===")
    ts_range = ts_max - ts_min
    for sel in [0.10, 0.25, 0.50]:
        ts_end = ts_min + int(ts_range * sel)
        if ts_end <= ts_min:
            ts_end = ts_min + 1
        param = f"{int(sel * 100)}%"

        t0 = time.perf_counter()
        rows = tsfile_time_filter(ts_path, cfg, ts_min, ts_end)
        record(cfg.name, "time_filter", "tsfile", param,
               time.perf_counter() - t0, rows)

        t0 = time.perf_counter()
        rows = parquet_time_filter(pq_path, ts_min, ts_end)
        record(cfg.name, "time_filter", "parquet", param,
               time.perf_counter() - t0, rows)

    # 4. Combined tag + time filter (use device's own time range)
    print("\n=== Tag + Time Filter ===")
    dev_range = dev_ts_max - dev_ts_min
    for sel in [0.10, 0.25, 0.50]:
        ts_end = dev_ts_min + int(dev_range * sel)
        if ts_end <= dev_ts_min:
            ts_end = dev_ts_min + 1
        param = f"{sample_tag_value}+{int(sel * 100)}%"

        t0 = time.perf_counter()
        rows = tsfile_tag_time_filter(ts_path, cfg, sample_tag_name,
                                      sample_tag_value, dev_ts_min, ts_end)
        record(cfg.name, "tag_time_filter", "tsfile", param,
               time.perf_counter() - t0, rows)

        t0 = time.perf_counter()
        rows = parquet_tag_time_filter(pq_path, sample_tag_name,
                                       sample_tag_value, dev_ts_min, ts_end)
        record(cfg.name, "tag_time_filter", "parquet", param,
               time.perf_counter() - t0, rows)

# ── CSV Output ──────────────────────────────────────────────────────────────

def write_results_csv(path: str):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dataset", "experiment", "engine", "params",
                     "seconds", "result_rows", "rows_per_sec"])
        for r in results:
            w.writerow([r.dataset, r.experiment, r.engine, r.params,
                        f"{r.seconds:.6f}", r.result_rows,
                        int(r.rows_per_sec)])
    print(f"\nResults written to {path}")

# ── Main ────────────────────────────────────────────────────────────────────

def run_dataset(ds_name: str, data_dir: str):
    cfg = DATASETS[ds_name]
    csv_path = os.path.join(data_dir, "data_sorted.csv")

    print(f"\n{'=' * 40}")
    print(f"  Dataset: {cfg.name}")
    print(f"  Tags:    {len(cfg.tag_names)}")
    print(f"  Fields:  {len(cfg.fields)}")
    print(f"{'=' * 40}")

    # Load CSV
    print(f"Loading {csv_path}...")
    timestamps, tags_cols, field_cols = load_csv(csv_path, cfg)
    n = len(timestamps)
    print(f"Loaded {n} rows")
    if n == 0:
        print(f"No data for {ds_name}, skipping")
        return

    # Time range
    ts_min = min(timestamps)
    ts_max = max(timestamps) + 1  # exclusive

    # Sample device for tag filter
    sample_tag = cfg.tag_names[0]
    sample_val = tags_cols[0][0]

    # Compute sample device's own time range
    num_tags = len(cfg.tag_names)
    dev_end_idx = 1
    while dev_end_idx < n and all(
        tags_cols[t][dev_end_idx] == tags_cols[t][0] for t in range(num_tags)
    ):
        dev_end_idx += 1
    dev_timestamps = timestamps[:dev_end_idx]
    dev_ts_min = min(dev_timestamps)
    dev_ts_max = max(dev_timestamps) + 1
    print(f'  Sample device "{sample_val}": {dev_end_idx} rows, '
          f'ts range [{dev_ts_min}, {dev_ts_max})')

    # Write phase
    ts_path = f"{ds_name}_bench.tsfile"
    pq_path = f"{ds_name}_bench.parquet"

    # Remove old files if they exist
    for p in [ts_path, pq_path]:
        if os.path.exists(p):
            os.remove(p)

    print("\nWriting TsFile...")
    t0 = time.perf_counter()
    write_tsfile(ts_path, cfg, timestamps, tags_cols, field_cols)
    sec = time.perf_counter() - t0
    print(f"  TsFile write: {sec:.3f} s")
    record(cfg.name, "write", "tsfile", "", sec, n)

    print("Writing Parquet...")
    t0 = time.perf_counter()
    write_parquet(pq_path, cfg, timestamps, tags_cols, field_cols)
    sec = time.perf_counter() - t0
    print(f"  Parquet write: {sec:.3f} s")
    record(cfg.name, "write", "parquet", "", sec, n)

    # File sizes
    ts_size_bytes = os.path.getsize(ts_path)
    pq_size_bytes = os.path.getsize(pq_path)
    ts_size = ts_size_bytes / (1024 * 1024)
    pq_size = pq_size_bytes / (1024 * 1024)
    print(f"  TsFile size:  {ts_size:.1f} MB")
    print(f"  Parquet size: {pq_size:.1f} MB")
    record(cfg.name, "space_bytes", "tsfile", "", 0, ts_size_bytes)
    record(cfg.name, "space_bytes", "parquet", "", 0, pq_size_bytes)

    # Read benchmarks
    run_experiments(cfg, ts_path, pq_path, ts_min, ts_max,
                    sample_tag, sample_val, dev_ts_min, dev_ts_max)


def main():
    parser = argparse.ArgumentParser(
        description="E6-1 (Python): TsFile vs Parquet benchmark")
    parser.add_argument("--dataset", choices=list(DATASETS.keys()),
                        help="Single dataset to benchmark")
    parser.add_argument("--data-dir", help="Prepared data directory")
    parser.add_argument("--all", action="store_true",
                        help="Run all datasets")
    parser.add_argument("--data-root",
                        help="Root of prepared datasets (for --all)")
    parser.add_argument("--csv-out", default="py_vs_parquet_results.csv",
                        help="Output CSV path")
    args = parser.parse_args()

    if args.all:
        if not args.data_root:
            parser.error("--data-root required with --all")
        for ds in DATASETS:
            ds_dir = os.path.join(args.data_root, ds)
            if os.path.isdir(ds_dir):
                run_dataset(ds, ds_dir)
            else:
                print(f"Skipping {ds}: {ds_dir} not found")
    elif args.dataset:
        if not args.data_dir:
            parser.error("--data-dir required with --dataset")
        run_dataset(args.dataset, args.data_dir)
    else:
        parser.error("Specify --dataset or --all")

    write_results_csv(args.csv_out)


if __name__ == "__main__":
    main()
