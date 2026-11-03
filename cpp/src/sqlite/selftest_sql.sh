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
# Manual SQL smoke test for the SQLite + TsFile extension.
#
# Usage (from the repository root):
#   SQLITE=/opt/homebrew/opt/sqlite/bin/sqlite3 \
#   TSFILE_SQLITE_EXTENSION=build/develop/lib/tsfile_sqlite.dylib \
#   bash cpp/src/sqlite/selftest_sql.sh
#
# Environment variables:
#   SQLITE                    sqlite3 binary (default: sqlite3)
#   TSFILE_SQLITE_EXTENSION   extension path (default: build/develop/lib/...)
#   SELFTEST_DIR              output directory (default: a fresh mktemp dir)
#
# The script loads the extension, creates a writable table, exercises hot
# CRUD, seal, diagnostics, export, read-only tables, directory sources,
# compact and expected error paths, and prints one PASS/FAIL line per check.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

SQLITE="${SQLITE:-sqlite3}"
EXT="${TSFILE_SQLITE_EXTENSION:-}"
if [ -z "$EXT" ]; then
    if [ -f "$REPO_ROOT/build/develop/lib/tsfile_sqlite.dylib" ]; then
        EXT="$REPO_ROOT/build/develop/lib/tsfile_sqlite.dylib"
    elif [ -f "$REPO_ROOT/build/develop/lib/tsfile_sqlite.so" ]; then
        EXT="$REPO_ROOT/build/develop/lib/tsfile_sqlite.so"
    else
        echo "extension not found; set TSFILE_SQLITE_EXTENSION" >&2
        exit 2
    fi
fi
if [ ! -f "$EXT" ]; then
    echo "extension not found: $EXT" >&2
    exit 2
fi

TMP_ROOT="${TMPDIR:-/tmp}"
BASE="${SELFTEST_DIR:-$(mktemp -d "${TMP_ROOT%/}/tsfile-sqlite-selftest.XXXXXX")}"
DB="$BASE/test.db"
mkdir -p "$BASE"

PASS=0
FAIL=0

run() {
    "$SQLITE" -batch "$DB" -cmd ".load $EXT" "$1"
}

check() {
    local name="$1"
    local sql="$2"
    local expected="$3"
    local got
    got=$(run "$sql" 2>&1)
    if [ "$got" = "$expected" ]; then
        echo "PASS  $name  -> $got"
        PASS=$((PASS + 1))
    else
        echo "FAIL  $name  expected=[$expected] got=[$got]"
        FAIL=$((FAIL + 1))
    fi
}

expect_fail() {
    local name="$1"
    local sql="$2"
    local output
    output=$(run "$sql" 2>&1)
    local rc=$?
    if [ $rc -ne 0 ]; then
        echo "PASS  $name  (rejected: $(echo "$output" | head -1))"
        PASS=$((PASS + 1))
    else
        echo "FAIL  $name  expected rejection, got=[$output]"
        FAIL=$((FAIL + 1))
    fi
}

echo "sqlite=$SQLITE"
echo "extension=$EXT"
echo "workdir=$BASE"

if ! run "SELECT 1;" >/dev/null 2>&1; then
    echo "cannot load extension: $EXT" >&2
    exit 2
fi

run "CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
    time TIMESTAMP TIME,
    group_id STRING TAG,
    device_id STRING TAG,
    v1 DOUBLE FIELD,
    v2 INT64 FIELD,
    v3 FLOAT FIELD,
    v4 INT32 FIELD,
    v5 BOOLEAN FIELD,
    directory='$BASE/segments',
    timestamp_precision='ms');" >/dev/null
run "INSERT INTO sensor VALUES(1000,'g0','d1',21.5,1,1.5,10,1);" >/dev/null
check "initial row count" "SELECT count(*) FROM sensor;" "1"

run "INSERT INTO sensor VALUES
    (2000,'g0','d1',22.0,2,2.5,20,0),
    (3000,'g1','d2',19.0,3,3.5,30,1),
    (4000,'g1','d2',20.0,4,4.5,40,0);" >/dev/null
check "row count after insert" "SELECT count(*) FROM sensor;" "4"

run "UPDATE sensor SET v1=22.5 WHERE time=2000 AND device_id='d1';
     DELETE FROM sensor WHERE time=4000 AND device_id='d2';" >/dev/null
check "update/delete" "SELECT count(*) FROM sensor WHERE v1=22.5;" "1"
check "query after delete" "SELECT count(*) FROM sensor;" "3"
check "field filter hot" "SELECT count(*) FROM sensor WHERE v1>20;" "2"

check "seal return rows" "SELECT tsfile_seal('main.sensor',64);" "3"
check "query cold" "SELECT count(*) FROM sensor;" "3"
check "field filter cold" "SELECT count(*) FROM sensor WHERE v1>20;" "2"
check "time+tag+field pushdown" \
    "SELECT count(*) FROM sensor WHERE time>=1000 AND time<3000 AND group_id='g0' AND v1>20;" "2"

check "table_info hot_rows" "SELECT hot_rows FROM tsfile_table_info('main.sensor');" "0"
check "table_info device_count" "SELECT device_count FROM tsfile_table_info('main.sensor');" "2"
check "table_info file_count" "SELECT file_count FROM tsfile_table_info('main.sensor');" "1"
check "table_info watermark" "SELECT watermark FROM tsfile_table_info('main.sensor');" "2001"
check "device_info rows" "SELECT count(*) FROM tsfile_device_info('main.sensor');" "2"
check "device d1 watermark" \
    "SELECT watermark FROM tsfile_device_info('main.sensor') WHERE device_key='g0,d1';" "2001"
check "device d2 watermark" \
    "SELECT watermark FROM tsfile_device_info('main.sensor') WHERE device_key='g1,d2';" "3001"
check "schema column count" "SELECT count(*) FROM tsfile_schema('main.sensor');" "8"
check "schema time type" \
    "SELECT tsfile_type || ':' || category FROM tsfile_schema('main.sensor') WHERE name='time';" "TIMESTAMP:TIME"
check "schema tag type" \
    "SELECT tsfile_type || ':' || category FROM tsfile_schema('main.sensor') WHERE name='device_id';" "STRING:TAG"
check "schema field type" \
    "SELECT tsfile_type || ':' || category FROM tsfile_schema('main.sensor') WHERE name='v1';" "DOUBLE:FIELD"
check "schema order" \
    "SELECT group_concat(name, ',') FROM tsfile_schema('main.sensor');" \
    "time,group_id,device_id,v1,v2,v3,v4,v5"

check "export files" "SELECT tsfile_export('main.sensor','$BASE/export');" "1"
EXPORTED=$(find "$BASE/export" -maxdepth 1 -name '*.tsfile' | head -1)
check "exported file exists" \
    "SELECT CASE WHEN '$EXPORTED' <> '' THEN 1 ELSE 0 END;" "1"

run "CREATE VIRTUAL TABLE history USING tsfile_hybrid(file='$EXPORTED', source_table='sensor');" >/dev/null
check "readonly file query" "SELECT count(*) FROM history;" "3"
check "readonly file field filter" "SELECT count(*) FROM history WHERE v1>20;" "2"
expect_fail "readonly insert rejected" \
    "INSERT INTO history VALUES(5000,'g0','d1',1.0,1,1.0,1,1);"

run "CREATE VIRTUAL TABLE history_dir USING tsfile_hybrid(file='$BASE/segments', source_table='sensor');" >/dev/null
check "directory source query" "SELECT count(*) FROM history_dir;" "3"
check "readonly table_info mode" "SELECT mode FROM tsfile_table_info('main.history_dir');" "readonly"

check "compact no-op" "SELECT tsfile_compact('main.sensor',10,20);" "0"
check "verify sensor" \
    "SELECT count(*) FROM tsfile_verify('main.sensor') WHERE status='OK';" "1"
check "verify history" \
    "SELECT count(*) FROM tsfile_verify('main.history') WHERE status='OK';" "1"

expect_fail "duplicate key rejected" \
    "INSERT INTO sensor VALUES(1000,'g0','d1',9.0,9,9.0,9,1);"
expect_fail "watermark rejected" \
    "INSERT INTO sensor VALUES(1000,'g1','d2',9.0,9,9.0,9,1);"
expect_fail "readonly delete rejected" "DELETE FROM history WHERE time=1000;"
expect_fail "missing table rejected" "SELECT * FROM tsfile_schema('main.missing');"

echo "----------------------------------------"
echo "PASS=$PASS FAIL=$FAIL"
[ $FAIL -eq 0 ]
