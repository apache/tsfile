#!/usr/bin/env python3
#
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
# Write/seal benchmark for the SQLite + TsFile extension on a small device.
# It writes a fixed number of wide rows, seals when the hot area reaches a
# configurable row count, and records write/seal time, RSS, and file sizes.
# It does not emulate 1 Hz pacing, concurrent clients, or query load.

import argparse
import json
import math
import os
import shutil
import sqlite3
import sys
import threading
import time
from pathlib import Path


def read_vm_rss_kb():
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def read_io_bytes():
    try:
        with open("/proc/self/io") as f:
            values = {}
            for line in f:
                key, value = line.split(":", 1)
                values[key.strip()] = int(value.strip())
            return values
    except OSError:
        return {}


class RssSampler(threading.Thread):
    def __init__(self, interval=0.25, kill_mb=6144):
        super().__init__(daemon=True)
        self.interval = interval
        self.kill_kb = kill_mb * 1024
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.current_kb = 0
        self.peak_kb = 0
        self.samples = []
        self.exceeded = False
        self.started = time.monotonic()

    def run(self):
        while not self.stop_event.is_set():
            value = read_vm_rss_kb()
            with self.lock:
                self.current_kb = value
                self.peak_kb = max(self.peak_kb, value)
                self.samples.append(
                    (round(time.monotonic() - self.started, 3), value))
                if value > self.kill_kb:
                    self.exceeded = True
            self.stop_event.wait(self.interval)

    def stop(self):
        self.stop_event.set()

    def reset_peak(self):
        with self.lock:
            self.peak_kb = self.current_kb

    def snapshot(self):
        with self.lock:
            return {
                "current_kb": self.current_kb,
                "peak_kb": self.peak_kb,
                "exceeded": self.exceeded,
            }

    def summary(self):
        with self.lock:
            values = [v for _, v in self.samples]
            return {
                "samples": len(values),
                "mean_kb": int(sum(values) / len(values)) if values else 0,
                "peak_kb": max(values) if values else 0,
                "exceeded": self.exceeded,
            }


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * p / 100.0
    low = int(math.floor(index))
    high = int(math.ceil(index))
    if low == high:
        return ordered[low]
    return ordered[low] * (high - index) + ordered[high] * (index - low)


def file_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def dir_stats(directory):
    files = 0
    total = 0
    for root, _, names in os.walk(directory):
        for name in names:
            if not name.endswith(".tsfile"):
                continue
            path = os.path.join(root, name)
            files += 1
            total += file_size(path)
    return {"files": files, "bytes": total}


def quote_sql(value):
    return "'" + value.replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--devices", type=int, required=True)
    parser.add_argument("--measurements", type=int, required=True)
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--batch-rows", type=int, default=1000)
    parser.add_argument("--seal-rows", type=int, default=500_000)
    parser.add_argument("--seal-max-mb", type=int, default=1024)
    parser.add_argument("--db", required=True)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--ext", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--rss-kill-mb", type=int, default=6144)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.devices <= 0 or args.measurements <= 0:
        parser.error("devices/measurements must be positive")
    if args.rows <= 0 or args.batch_rows <= 0 or args.seal_rows <= 0:
        parser.error("rows/batch-rows/seal-rows must be positive")

    total_rows = args.rows
    rows_per_device = total_rows // args.devices
    actual_rows = rows_per_device * args.devices
    if rows_per_device <= 0:
        parser.error("rows must be >= devices")
    points_per_row = args.measurements
    actual_points = actual_rows * points_per_row

    work_dir = Path(args.db).parent
    if args.clean:
        if work_dir.exists():
            shutil.rmtree(work_dir)
        if Path(args.dir).exists():
            shutil.rmtree(args.dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    Path(args.dir).parent.mkdir(parents=True, exist_ok=True)

    if Path(args.db).exists():
        Path(args.db).unlink()
    for suffix in ("-wal", "-shm"):
        p = Path(str(args.db) + suffix)
        if p.exists():
            p.unlink()

    result = {
        "status": "running",
        "config": {
            "devices": args.devices,
            "measurements": args.measurements,
            "rows_target": total_rows,
            "rows_per_device": rows_per_device,
            "rows_actual": actual_rows,
            "points_actual": actual_points,
            "batch_rows": args.batch_rows,
            "seal_rows": args.seal_rows,
            "seal_max_mb": args.seal_max_mb,
            "db": args.db,
            "dir": args.dir,
            "ext": args.ext,
            "sqlite_version": sqlite3.sqlite_version,
            "python_version": sys.version.split()[0],
        },
        "seals": [],
        "batches": [],
        "errors": [],
    }

    sampler = RssSampler(kill_mb=args.rss_kill_mb)
    sampler.start()
    conn = None
    started = time.monotonic()
    io_before = read_io_bytes()

    try:
        conn = sqlite3.connect(args.db, isolation_level=None, timeout=60)
        conn.enable_load_extension(True)
        conn.load_extension(args.ext)
        conn.enable_load_extension(False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA cache_size=-65536")

        columns = ["time TIMESTAMP TIME", "device STRING TAG"]
        for m in range(args.measurements):
            columns.append("m%05d DOUBLE FIELD" % m)
        ddl = (
            "CREATE VIRTUAL TABLE sensor USING tsfile_hybrid("
            + ",".join(columns)
            + ",directory="
            + quote_sql(args.dir)
            + ",timestamp_precision='ms')"
        )
        created_at = time.monotonic()
        conn.execute(ddl)
        result["create_s"] = time.monotonic() - created_at
        result["columns"] = len(columns)

        placeholders = ",".join(["?"] * (2 + args.measurements))
        insert_sql = "INSERT INTO sensor VALUES(" + placeholders + ")"
        device_names = ["dev_%05d" % d for d in range(args.devices)]
        t0 = 1700000000000

        def make_row(rows_index, device_index):
            timestamp = t0 + rows_index * 1000
            values = []
            base = rows_index * 131 + device_index * 17
            for m in range(args.measurements):
                values.append(((base + (m + 1) * 7) % 100000) / 1000.0)
            return (timestamp, device_names[device_index]) + tuple(values)

        batch = []
        batch_latencies = []
        hot_rows = 0
        written_rows = 0
        seal_count = 0
        write_s = 0.0
        seal_s = 0.0

        def do_seal():
            nonlocal hot_rows, seal_count, seal_s
            before = sampler.snapshot()["current_kb"]
            sampler.reset_peak()
            seal_start = time.monotonic()
            cursor = conn.execute("SELECT tsfile_seal('main.sensor', ?)",
                                  (args.seal_max_mb,))
            sealed = cursor.fetchall()
            cursor.close()
            elapsed = time.monotonic() - seal_start
            seal_s += elapsed
            seal_count += 1
            after = sampler.snapshot()
            stats = dir_stats(args.dir)
            info = conn.execute(
                "SELECT hot_rows,file_count,device_count "
                "FROM tsfile_table_info('main.sensor')").fetchone()
            record = {
                "index": seal_count,
                "requested_rows": hot_rows,
                "returned": sealed[0][0] if sealed else None,
                "elapsed_s": elapsed,
                "rss_before_kb": before,
                "rss_peak_kb": after["peak_kb"],
                "tsfile_files": stats["files"],
                "tsfile_bytes": stats["bytes"],
                "hot_rows_after": info[0] if info else None,
                "file_count_after": info[1] if info else None,
            }
            result["seals"].append(record)
            if not info or info[0] != 0:
                raise RuntimeError("seal did not clear hot rows")
            hot_rows = 0
            if not args.quiet:
                print("seal %d rows=%d time=%.3fs peak_rss=%dMB files=%d"
                      % (seal_count, record["requested_rows"], elapsed,
                         after["peak_kb"] // 1024, stats["files"]),
                      flush=True)

        def write_batch(rows):
            nonlocal hot_rows, written_rows, write_s
            if not rows:
                return
            if sampler.snapshot()["exceeded"]:
                raise MemoryError("RSS watchdog exceeded")
            begin = time.monotonic()
            conn.execute("BEGIN")
            conn.executemany(insert_sql, rows)
            conn.execute("COMMIT")
            elapsed = time.monotonic() - begin
            write_s += elapsed
            batch_latencies.append(elapsed)
            batch_rows = len(rows)
            written_rows += batch_rows
            hot_rows += batch_rows
            result["batches"].append({
                "rows": batch_rows,
                "seconds": elapsed,
                "hot_rows": hot_rows,
            })
            if len(result["batches"]) % 100 == 0 and not args.quiet:
                print("rows=%d/%d write_s=%.1f rss=%dMB"
                      % (written_rows, actual_rows, write_s,
                         sampler.snapshot()["current_kb"] // 1024), flush=True)
            if hot_rows >= args.seal_rows:
                do_seal()

        write_started = time.monotonic()
        for r in range(rows_per_device):
            for d in range(args.devices):
                batch.append(make_row(r, d))
                if len(batch) >= args.batch_rows:
                    write_batch(batch)
                    batch = []
        if batch:
            write_batch(batch)
        wall_without_final_seal = time.monotonic() - write_started

        if hot_rows > 0:
            do_seal()

        table_info = conn.execute(
            "SELECT hot_rows,file_count,device_count,watermark "
            "FROM tsfile_table_info('main.sensor')").fetchone()
        device = device_names[0]
        device_count = conn.execute(
            "SELECT count(*) FROM sensor WHERE device=?", (device,)).fetchone()[0]
        sample_time = t0
        sample = conn.execute(
            "SELECT m00000 FROM sensor WHERE device=? AND time=?",
            (device, sample_time)).fetchone()
        expected_sample = ((0 + 0 + 7) % 100000) / 1000.0
        validation = {
            "device": device,
            "rows_per_device_expected": rows_per_device,
            "rows_per_device_actual": device_count,
            "sample_time": sample_time,
            "sample_value": sample[0] if sample else None,
            "sample_value_expected": expected_sample,
            "table_info": {
                "hot_rows": table_info[0] if table_info else None,
                "file_count": table_info[1] if table_info else None,
                "device_count": table_info[2] if table_info else None,
                "watermark": table_info[3] if table_info else None,
            },
        }
        result["validation"] = validation
        if device_count != rows_per_device:
            raise RuntimeError("device row count mismatch")
        if sample is None or abs(sample[0] - expected_sample) > 1e-9:
            raise RuntimeError("sample value mismatch")
        if table_info is None or table_info[0] != 0:
            raise RuntimeError("hot rows remain after final seal")

        total_s = time.monotonic() - started
        io_after = read_io_bytes()
        stats = dir_stats(args.dir)
        result.update({
            "status": "ok",
            "rows_written": written_rows,
            "points_written": written_rows * points_per_row,
            "write_s": write_s,
            "seal_s": seal_s,
            "wall_s": total_s,
            "wall_without_final_seal_s": wall_without_final_seal,
            "write_rows_per_s": written_rows / write_s if write_s else None,
            "write_points_per_s": (written_rows * points_per_row / write_s
                                   if write_s else None),
            "batch_p50_s": percentile(batch_latencies, 50),
            "batch_p95_s": percentile(batch_latencies, 95),
            "batch_p99_s": percentile(batch_latencies, 99),
            "seal_count": seal_count,
            "rss": sampler.summary(),
            "db_bytes": file_size(args.db),
            "wal_bytes": file_size(args.db + "-wal"),
            "tsfile_files": stats["files"],
            "tsfile_bytes": stats["bytes"],
            "io_write_bytes": (io_after.get("write_bytes", 0)
                               - io_before.get("write_bytes", 0)),
            "io_read_bytes": (io_after.get("read_bytes", 0)
                              - io_before.get("read_bytes", 0)),
        })
    except BaseException as exc:
        result["status"] = "failed"
        result["error"] = repr(exc)
        result["elapsed_s"] = time.monotonic() - started
        try:
            if conn is not None and conn.in_transaction:
                conn.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        sampler.stop()
        sampler.join(timeout=2)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        result.setdefault("rss", sampler.summary())
        result["rss_samples"] = sampler.samples
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        if not args.keep:
            if work_dir.exists():
                shutil.rmtree(work_dir)
            if Path(args.dir).exists():
                shutil.rmtree(args.dir)
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
