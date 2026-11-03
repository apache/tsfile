#!/usr/bin/env bash
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
# Orchestrates the Pi write/seal matrix. Run on the Pi after building
# tsfile_sqlite.so. Each configuration is a fresh process, database and
# segment directory.

set -u

SCRIPT="${SCRIPT:-/root/tsfile-bench/pi_write_seal_bench.py}"
EXT="${EXT:-/root/tsfile-src/cpp/build/pi/lib/tsfile_sqlite.so}"
LIB="${LIB:-$(dirname "$EXT")}"
OUT="${OUT:-/root/tsfile-bench/results}"
DATA="${DATA:-/root/tsfile-bench/data}"
ROWS="${ROWS:-2000000}"
BATCH_ROWS="${BATCH_ROWS:-1000}"
SEAL_MAX_MB="${SEAL_MAX_MB:-1024}"
RSS_KILL_MB="${RSS_KILL_MB:-6144}"
STAGE="${1:-all}"

mkdir -p "$OUT" "$DATA"
exec 9>/root/tsfile-bench/.matrix.lock
if ! flock -n 9; then
    echo "another matrix run is active" >&2
    exit 1
fi

{
    date -Is
    uname -a
    lscpu | sed -n '1,14p'
    free -m
    df -h /root
    python3 --version
    python3 -c 'import sqlite3; print("sqlite", sqlite3.sqlite_version, "threadsafety", sqlite3.threadsafety)'
    vcgencmd measure_temp 2>/dev/null || true
    vcgencmd get_throttled 2>/dev/null || true
    sha256sum "$EXT" 2>/dev/null || true
} > "$OUT/env-$(date +%Y%m%d-%H%M%S).txt" 2>&1

run_one() {
    local devices="$1"
    local measurements="$2"
    local rows="$3"
    local seal_rows="$4"
    local tag="$5"
    local id
    id="$(date +%Y%m%d-%H%M%S)-${tag}-D${devices}-M${measurements}-S${seal_rows}"
    local data="$DATA/$id"
    local out="$OUT/$id.json"
    rm -rf "$data"
    mkdir -p "$data"
    echo "=== $id rows=$rows ==="
    sync
    timeout $((3600 + 1800)) \
        env LD_LIBRARY_PATH="$LIB" \
        python3 -I -u "$SCRIPT" \
        --devices "$devices" \
        --measurements "$measurements" \
        --rows "$rows" \
        --batch-rows "$BATCH_ROWS" \
        --seal-rows "$seal_rows" \
        --seal-max-mb "$SEAL_MAX_MB" \
        --rss-kill-mb "$RSS_KILL_MB" \
        --db "$data/bench.db" \
        --dir "$data/segments" \
        --ext "$EXT" \
        --out "$out" \
        --clean
    local rc=$?
    echo "=== $id rc=$rc ==="
    if [ $rc -ne 0 ]; then
        echo "FAILED $id rc=$rc" | tee -a "$OUT/failures.txt"
    fi
}

run_smoke() {
    run_one 100 5 200000 20000 smoke
}

run_main() {
    for spec in 100:5 100:15 100:30 500:5 500:15 500:30 2000:5 2000:15 2000:30; do
        run_one "${spec%%:*}" "${spec##*:}" "$ROWS" 50000 main
    done
}

run_scan() {
    for seal in 20000 50000 100000; do
        run_one 500 15 "$ROWS" "$seal" scan
    done
}

case "$STAGE" in
    smoke) run_smoke ;;
    main) run_main ;;
    scan) run_scan ;;
    all)
        run_smoke
        run_main
        run_scan
        ;;
    *)
        echo "usage: $0 [smoke|main|scan|all]" >&2
        exit 2
        ;;
esac

echo "results: $OUT"
