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

"""Query latency benchmark for the SQLite + TsFile extension on a small device.

The script writes one dataset, measures hot-only queries, seals it, measures
cold-only queries, then writes a small hot tail and measures mixed queries.
It intentionally uses one Python process and one connection; query latency
includes SQLite, the extension, result materialization, and Python fetching.
"""

import argparse
import json
import os
import shutil
import sqlite3
import statistics
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
                self.samples.append((round(time.monotonic() - self.started, 3), value))
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


def quote_sql(value):
    return "'" + value.replace("'", "''") + "'"


def percentile(sorted_values, p):
    if not sorted_values:
        return 0.0
    index = (len(sorted_values) - 1) * p
    lower = int(index)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = index - lower
    return sorted_values[lower] * (1 - fraction) + sorted_values[upper] * fraction


def make_row(row_index, device_index, measurements):
    timestamp = 1_700_000_000_000 + row_index * 1000
    base = row_index * 131 + device_index * 17
    values = [((base + (m + 1) * 7) % 100000) / 1000.0 for m in range(measurements)]
    return (timestamp, "dev_%05d" % device_index) + tuple(values)


def execute_query(conn, sampler, sql, params=(), fetch="all"):
    before = sampler.snapshot()["current_kb"]
    io_before = read_io_bytes()
    sampler.reset_peak()
    started = time.perf_counter()
    cursor = conn.execute(sql, params)
    result_rows = 0
    value = None
    if fetch == "one":
        row = cursor.fetchone()
        result_rows = 0 if row is None else 1
        value = None if row is None else row[0]
    else:
        rows = cursor.fetchall()
        result_rows = len(rows)
    elapsed = time.perf_counter() - started
    after = sampler.snapshot()
    io_after = read_io_bytes()
    return {
        "elapsed_s": elapsed,
        "result_rows": result_rows,
        "value": value,
        "rss_before_kb": before,
        "rss_peak_kb": after["peak_kb"],
        "rss_after_kb": after["current_kb"],
        "io_rchar": io_after.get("rchar", 0) - io_before.get("rchar", 0),
        "io_read_bytes": io_after.get("read_bytes", 0) - io_before.get("read_bytes", 0),
    }


def measure(
    conn,
    sampler,
    sql,
    params=(),
    fetch="all",
    iterations=5,
    expected_rows=None,
    expected_value=None,
):
    for _ in range(1):
        execute_query(conn, sampler, sql, params, fetch)
    samples = []
    for _ in range(iterations):
        samples.append(execute_query(conn, sampler, sql, params, fetch))
    elapsed = sorted(s["elapsed_s"] for s in samples)
    result = {
        "iterations": iterations,
        "p50_s": percentile(elapsed, 0.50),
        "p95_s": percentile(elapsed, 0.95),
        "p99_s": percentile(elapsed, 0.99),
        "min_s": elapsed[0],
        "max_s": elapsed[-1],
        "mean_s": statistics.mean(elapsed),
        "result_rows": samples[-1]["result_rows"],
        "rss_before_kb": samples[-1]["rss_before_kb"],
        "rss_peak_kb": max(s["rss_peak_kb"] for s in samples),
        "rss_after_kb": samples[-1]["rss_after_kb"],
        "io_rchar": sum(s["io_rchar"] for s in samples),
        "io_read_bytes": sum(s["io_read_bytes"] for s in samples),
    }
    if expected_rows is not None:
        result["expected_rows"] = expected_rows
        result["rows_match"] = result["result_rows"] == expected_rows
    if expected_value is not None:
        result["expected_value"] = expected_value
        result["value_match"] = (
            result["result_rows"] > 0
            and abs(float(samples[-1]["value"]) - expected_value) < 1e-9
        )
    return result


def seal_now(conn, sampler, seal_max_mb):
    before = sampler.snapshot()["current_kb"]
    io_before = read_io_bytes()
    sampler.reset_peak()
    started = time.perf_counter()
    sealed = conn.execute(
        "SELECT tsfile_seal('main.sensor', ?)", (seal_max_mb,)
    ).fetchone()[0]
    elapsed = time.perf_counter() - started
    after = sampler.snapshot()
    io_after = read_io_bytes()
    return {
        "sealed": sealed,
        "elapsed_s": elapsed,
        "rss_before_kb": before,
        "rss_peak_kb": after["peak_kb"],
        "rss_after_kb": after["current_kb"],
        "io_rchar": io_after.get("rchar", 0) - io_before.get("rchar", 0),
        "io_read_bytes": io_after.get("read_bytes", 0) - io_before.get("read_bytes", 0),
    }


def phase_queries(phase, config, quick=False):
    devices = config["devices"]
    measurements = config["measurements"]
    rows_per_device = config["rows_per_device"]
    total_rows = config["rows_actual"]
    hot_prefix = config["hot_prefix"]
    extra_hot = config["extra_hot"]
    t0 = 1_700_000_000_000
    device = "dev_00000"
    m0 = "m%05d" % 0

    if phase == "hot":
        total = hot_prefix
        point_time = t0
        point_name = "point"
    elif phase == "cold":
        total = total_rows
        point_time = t0
        point_name = "point"
    else:
        total = total_rows + extra_hot
        point_time = t0
        point_name = "point_cold"
    extra_per_device = extra_hot // devices
    tag_devices = ["dev_%05d" % i for i in range(min(10, devices))]
    tag_placeholders = ",".join(["?"] * len(tag_devices))
    tag_rows = len(tag_devices) * (
        rows_per_device + (extra_per_device if phase == "mixed" else 0)
    )

    def range_start(rows):
        if phase != "mixed":
            return t0
        return t0 + (total_rows - rows // 2) * 1000

    specs = [
        (
            point_name,
            "SELECT " + m0 + " FROM sensor WHERE device=? AND time=?",
            (device, point_time),
            "one",
            20,
            1,
            0.007,
        ),
        (
            "latest",
            "SELECT " + m0 + " FROM sensor WHERE device=? "
            "ORDER BY time DESC LIMIT 1",
            (device,),
            "one",
            20,
            1,
            None,
        ),
        (
            "range_1k",
            "SELECT time," + m0 + " FROM sensor WHERE time BETWEEN ? AND ? "
            "ORDER BY time",
            None,
            "all",
            15,
            1000,
            None,
        ),
        (
            "range_10k",
            "SELECT time," + m0 + " FROM sensor WHERE time BETWEEN ? AND ? "
            "ORDER BY time",
            None,
            "all",
            8,
            10000,
            None,
        ),
        (
            "range_100k",
            "SELECT time," + m0 + " FROM sensor WHERE time BETWEEN ? AND ? "
            "ORDER BY time",
            None,
            "all",
            4,
            100000,
            None,
        ),
        (
            "tag_10",
            "SELECT count(*),avg("
            + m0
            + ") FROM sensor WHERE device IN ("
            + tag_placeholders
            + ")",
            tuple(tag_devices),
            "one",
            8,
            None,
            tag_rows,
        ),
        (
            "field_gt99",
            "SELECT count(*) FROM sensor WHERE " + m0 + " > 99.0",
            (),
            "one",
            8,
            None,
            None,
        ),
        (
            "aggregate",
            "SELECT count(*),avg(" + m0 + ") FROM sensor",
            (),
            "one",
            2,
            None,
            total,
        ),
        (
            "limit1",
            "SELECT * FROM sensor LIMIT 1",
            (),
            "one",
            6,
            1,
            None,
        ),
    ]
    if quick:
        allowed = {
            "point",
            "point_cold",
            "latest",
            "range_1k",
            "range_10k",
            "range_100k",
        }
        specs = [spec for spec in specs if spec[0] in allowed]
    out = []
    for name, sql, params, fetch, iterations, expected_rows, expected_value in specs:
        if name.startswith("range_"):
            rows = expected_rows
            start = range_start(rows)
            end = start + (rows - 1) * 1000
            params = (start, end)
        out.append(
            (name, sql, params, fetch, iterations, expected_rows, expected_value)
        )
    if phase == "cold":
        out.append(
            (
                "metadata_count",
                "SELECT COALESCE(SUM(row_count),0) FROM " '"sensor_tsfile$segments"',
                (),
                "one",
                20,
                None,
                total,
            )
        )
    return out


def run_phase(conn, sampler, phase, config, quick=False):
    queries = {}
    for (
        name,
        sql,
        params,
        fetch,
        iterations,
        expected_rows,
        expected_value,
    ) in phase_queries(phase, config):
        queries[name] = measure(
            conn,
            sampler,
            sql,
            params,
            fetch,
            iterations,
            expected_rows,
            expected_value,
        )
        if not queries[name].get("rows_match", True):
            queries[name]["error"] = "unexpected result rows"
    return queries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--devices", type=int, default=500)
    parser.add_argument("--measurements", type=int, default=15)
    parser.add_argument("--rows", type=int, default=2_000_000)
    parser.add_argument("--hot-prefix", type=int, default=200_000)
    parser.add_argument("--extra-hot", type=int, default=50_000)
    parser.add_argument("--batch-rows", type=int, default=1000)
    parser.add_argument("--seal-rows", type=int, default=50_000)
    parser.add_argument("--seal-max-mb", type=int, default=1024)
    parser.add_argument("--db", required=True)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--ext", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--rss-kill-mb", type=int, default=6144)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.devices <= 0 or args.measurements <= 0:
        parser.error("devices/measurements must be positive")
    if args.rows <= 0 or args.hot_prefix <= 0 or args.extra_hot < 0:
        parser.error("rows/hot-prefix/extra-hot must be positive")
    rows_per_device = args.rows // args.devices
    actual_rows = rows_per_device * args.devices
    if rows_per_device <= 0:
        parser.error("rows must be >= devices")
    if args.hot_prefix > actual_rows:
        parser.error("hot-prefix must be <= rows")

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
        path = Path(str(args.db) + suffix)
        if path.exists():
            path.unlink()

    config = {
        "devices": args.devices,
        "measurements": args.measurements,
        "rows_target": args.rows,
        "rows_actual": actual_rows,
        "rows_per_device": rows_per_device,
        "hot_prefix": args.hot_prefix,
        "extra_hot": args.extra_hot,
        "batch_rows": args.batch_rows,
        "seal_rows": args.seal_rows,
        "seal_max_mb": args.seal_max_mb,
        "db": args.db,
        "dir": args.dir,
        "ext": args.ext,
        "sqlite_version": sqlite3.sqlite_version,
        "python_version": sys.version.split()[0],
    }
    result = {
        "status": "running",
        "config": config,
        "phases": {},
        "seals": [],
        "errors": [],
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    sampler = RssSampler(kill_mb=args.rss_kill_mb)
    sampler.start()
    conn = None
    started = time.monotonic()
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
        conn.execute(ddl)
        insert_sql = (
            "INSERT INTO sensor VALUES("
            + ",".join(["?"] * (2 + args.measurements))
            + ")"
        )

        write_s = 0.0
        hot_rows = 0
        written = 0

        def write_batch(start_index, count):
            nonlocal write_s, hot_rows, written
            batch = [
                make_row(
                    start_index + i, (start_index + i) % args.devices, args.measurements
                )
                for i in range(count)
            ]
            began = time.perf_counter()
            conn.execute("BEGIN")
            conn.executemany(insert_sql, batch)
            conn.execute("COMMIT")
            write_s += time.perf_counter() - began
            hot_rows += count
            written += count

        def seal_if_needed(force=False):
            nonlocal hot_rows
            if hot_rows == 0:
                return
            if force or hot_rows >= args.seal_rows:
                record = seal_now(conn, sampler, args.seal_max_mb)
                record["hot_rows"] = hot_rows
                result["seals"].append(record)
                hot_rows = 0

        # Hot phase: write a bounded prefix without sealing.
        for start in range(0, args.hot_prefix, args.batch_rows):
            write_batch(start, min(args.batch_rows, args.hot_prefix - start))
        result["phases"]["hot"] = {
            "rows": args.hot_prefix,
            "queries": run_phase(conn, sampler, "hot", config, args.quick),
        }

        # Seal the hot prefix, then write the rest with periodic seals.
        seal_if_needed(force=True)
        for start in range(args.hot_prefix, actual_rows, args.batch_rows):
            write_batch(start, min(args.batch_rows, actual_rows - start))
            seal_if_needed()
        seal_if_needed(force=True)
        result["phases"]["cold"] = {
            "rows": actual_rows,
            "queries": run_phase(conn, sampler, "cold", config, args.quick),
        }

        # Mixed phase: write a small hot tail without sealing.
        for start in range(actual_rows, actual_rows + args.extra_hot, args.batch_rows):
            write_batch(
                start, min(args.batch_rows, actual_rows + args.extra_hot - start)
            )
        result["phases"]["mixed"] = {
            "rows": actual_rows + args.extra_hot,
            "queries": run_phase(conn, sampler, "mixed", config, args.quick),
        }

        table_info = conn.execute(
            "SELECT hot_rows,file_count,device_count,watermark "
            "FROM tsfile_table_info('main.sensor')"
        ).fetchone()
        device_count = conn.execute(
            "SELECT count(*) FROM sensor WHERE device=?", ("dev_00000",)
        ).fetchone()[0]
        result["validation"] = {
            "hot_rows": table_info[0],
            "file_count": table_info[1],
            "device_count": table_info[2],
            "watermark": table_info[3],
            "rows_device_0": device_count,
            "rows_device_0_expected": rows_per_device
            + (args.extra_hot // args.devices),
        }
        result["write_s"] = write_s
        result["wall_s"] = time.monotonic() - started
        result["rss"] = {
            "mean_kb": (
                statistics.mean([s[1] for s in sampler.samples])
                if sampler.samples
                else 0
            ),
            "peak_kb": sampler.snapshot()["peak_kb"],
            "exceeded": sampler.snapshot()["exceeded"],
        }
        result["db_bytes"] = Path(args.db).stat().st_size
        result["wal_bytes"] = (
            Path(args.db + "-wal").stat().st_size
            if Path(args.db + "-wal").exists()
            else 0
        )
        result["tsfile_bytes"] = sum(
            p.stat().st_size for p in Path(args.dir).glob("*.tsfile")
        )
        result["tsfile_files"] = len(list(Path(args.dir).glob("*.tsfile")))
        result["status"] = "ok"
    except Exception as exc:
        result["status"] = "failed"
        result["errors"].append(repr(exc))
        raise
    finally:
        sampler.stop()
        if conn is not None:
            conn.close()
        Path(args.out).write_text(json.dumps(result, indent=1))
        if not args.keep:
            shutil.rmtree(work_dir, ignore_errors=True)
            shutil.rmtree(args.dir, ignore_errors=True)
        if not args.quiet:
            print(json.dumps(result, indent=1))


if __name__ == "__main__":
    main()
