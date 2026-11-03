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
# Summarize pi_write_seal_bench.py JSON results into a Markdown table.

import glob
import json
import sys


def fmt_bytes(value):
    if value is None:
        return "-"
    value = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return "%.1f%s" % (value, unit)
        value /= 1024
    return "-"


def main():
    pattern = sys.argv[1] if len(sys.argv) > 1 else "*.json"
    rows = []
    for path in sorted(glob.glob(pattern)):
        with open(path) as f:
            data = json.load(f)
        cfg = data.get("config", {})
        rows.append((
            cfg.get("devices"),
            cfg.get("measurements"),
            cfg.get("seal_rows"),
            data.get("status"),
            data.get("rows_written"),
            data.get("write_s"),
            data.get("write_rows_per_s"),
            data.get("write_points_per_s"),
            data.get("seal_count"),
            data.get("seal_s"),
            (data.get("rss") or {}).get("mean_kb"),
            (data.get("rss") or {}).get("peak_kb"),
            data.get("db_bytes"),
            data.get("tsfile_files"),
            data.get("tsfile_bytes"),
            data.get("error", ""),
        ))
    print("| devices | meas | seal_rows | status | rows | write_s | rows/s | points/s | seals | seal_s | rss_mean | rss_peak | db | tsfile | tsfile_bytes | error |")
    print("|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for row in rows:
        dev, meas, seal_rows, status, rows_written, write_s, rps, pps, seals, seal_s, rss_mean, rss_peak, db, files, tsbytes, error = row
        print("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            dev, meas, seal_rows, status, rows_written,
            "%.2f" % write_s if write_s is not None else "-",
            "%.0f" % rps if rps is not None else "-",
            "%.0f" % pps if pps is not None else "-",
            seals if seals is not None else "-",
            "%.2f" % seal_s if seal_s is not None else "-",
            "%.0fMB" % (rss_mean / 1024) if rss_mean else "-",
            "%.0fMB" % (rss_peak / 1024) if rss_peak else "-",
            fmt_bytes(db), files if files is not None else "-",
            fmt_bytes(tsbytes), error.replace("|", "/"),
        ))


if __name__ == "__main__":
    main()
