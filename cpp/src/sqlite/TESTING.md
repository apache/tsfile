<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Building and testing the SQLite extension

This guide covers building `tsfile_sqlite` and running the automated and manual
tests. For SQL usage, see [USER_GUIDE.md](USER_GUIDE.md) or
[USER_GUIDE_EN.md](USER_GUIDE_EN.md). Test results are in
[SELF_TEST_REPORT.md](SELF_TEST_REPORT.md).

## 1. Requirements

- CMake 3.11 or newer and a C++11 compiler.
- An **extension-capable** SQLite installation (3.31 or newer), including
  `sqlite3ext.h`. Headers compiled with `SQLITE_OMIT_LOAD_EXTENSION` are rejected
  by the build.
- On macOS, Apple's system SQLite headers disable extension loading. Install
  Homebrew SQLite and point CMake at it.
- The shared TsFile library is required: configure with
  `-DTSFILE_BUILD_SHARED=ON`.

### macOS

```bash
brew install cmake ninja sqlite
```

Homebrew SQLite is usually under `/opt/homebrew/opt/sqlite` on Apple Silicon and
`/usr/local/opt/sqlite` on Intel.

### Ubuntu / Debian

```bash
sudo apt-get update
sudo apt-get install -y cmake ninja-build libsqlite3-dev pkg-config
```

Ubuntu's `libsqlite3-dev` enables extension loading, so no extra SQLite build is
needed. On other architectures such as Raspberry Pi aarch64, the same packages
are available from the distribution repositories.

## 2. Build

From the repository root:

```bash
cmake -S cpp -B build/sqlite \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON \
  -DSQLite3_INCLUDE_DIR=/opt/homebrew/opt/sqlite/include \
  -DSQLite3_LIBRARY=/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib

cmake --build build/sqlite --target tsfile_sqlite TsFile_Sqlite_Test -j 8
```

On Linux, omit the `SQLite3_INCLUDE_DIR` / `SQLite3_LIBRARY` overrides so CMake
uses the system package.

Outputs:

- `build/sqlite/lib/libtsfile.*`
- `build/sqlite/lib/tsfile_sqlite.so` on Linux, or
  `build/sqlite/lib/tsfile_sqlite.dylib` on macOS
- `build/sqlite/test/lib/TsFile_Sqlite_Test`

### Offline or minimal builds

If the build host cannot download optional dependencies, disable them:

```bash
cmake -S cpp -B build/sqlite \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON \
  -DENABLE_ANTLR4=OFF \
  -DENABLE_SNAPPY=OFF \
  -DENABLE_LZ4=OFF \
  -DENABLE_LZOKAY=OFF \
  -DENABLE_ZLIB=OFF \
  -DENABLE_ZSTD=OFF \
  -DENABLE_SIMD=OFF
```

## 3. Run the automated tests

macOS:

```bash
DYLD_LIBRARY_PATH=build/sqlite/lib \
  ./build/sqlite/test/lib/TsFile_Sqlite_Test
```

Linux:

```bash
LD_LIBRARY_PATH=build/sqlite/lib \
  ./build/sqlite/test/lib/TsFile_Sqlite_Test
```

The test binary locates the extension through the
`TSFILE_SQLITE_EXTENSION_PATH` compile definition set by CMake. Run a subset with
`--gtest_filter`, for example:

```bash
DYLD_LIBRARY_PATH=build/sqlite/lib \
  ./build/sqlite/test/lib/TsFile_Sqlite_Test \
  --gtest_filter='TsFileSqliteTest.Management*'
```

As of 2026-11-03 the suite contains 41 cases and passes on macOS 26.6.2 and
Ubuntu 24.04.

## 4. Load the extension manually

Use a SQLite build that supports extension loading. Apple's `/usr/bin/sqlite3`
does not provide `.load` in some macOS versions; use Homebrew SQLite:

```bash
/opt/homebrew/opt/sqlite/bin/sqlite3 :memory:
```

Then load the module and create a table:

```sql
.load ./build/sqlite/lib/tsfile_sqlite.dylib

CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  group_id STRING TAG,
  device_id STRING TAG,
  temperature DOUBLE FIELD,
  directory='/tmp/tsfile-sqlite-demo/segments',
  timestamp_precision='ms'
);

INSERT INTO sensor VALUES(1000,'g0','d1',21.5);
SELECT * FROM sensor;
SELECT tsfile_seal('main.sensor', 200);
SELECT * FROM tsfile_table_info('main.sensor');
SELECT * FROM tsfile_schema('main.sensor');
```

In an application using the SQLite C API, enable loading before opening the
extension:

```c
sqlite3_enable_load_extension(db, 1);
sqlite3_load_extension(db, "./build/sqlite/lib/tsfile_sqlite.dylib",
                       NULL, &error);
```

## 5. Manual smoke checks

The following sequence exercises the main user-facing paths. Expected values are
for the exact SQL shown.

```sql
-- Prepare
CREATE VIRTUAL TABLE smoke USING tsfile_hybrid(
  time TIMESTAMP TIME,
  group_id STRING TAG,
  device_id STRING TAG,
  v1 DOUBLE FIELD,
  v2 INT64 FIELD,
  v3 FLOAT FIELD,
  v4 INT32 FIELD,
  v5 BOOLEAN FIELD,
  directory='/tmp/tsfile-sqlite-smoke/segments',
  timestamp_precision='ms'
);
INSERT INTO smoke VALUES
  (1000,'g0','d1',21.5,1,1.5,10,1),
  (2000,'g0','d1',22.0,2,2.5,20,0),
  (3000,'g1','d2',19.0,3,3.5,30,1);

-- Hot writes
SELECT count(*) FROM smoke;                       -- 3
UPDATE smoke SET v1=22.5 WHERE time=2000;         -- changes 1 row
DELETE FROM smoke WHERE time=3000;                -- deletes 1 row
SELECT count(*) FROM smoke WHERE v1>20;           -- 2

-- Seal and cold reads
SELECT tsfile_seal('main.smoke', 64);             -- 2
SELECT count(*) FROM smoke;                       -- 2
SELECT count(*) FROM smoke WHERE v1>20;           -- 2
SELECT hot_rows, device_count, file_count
FROM tsfile_table_info('main.smoke');             -- 0 | 2 | 1
SELECT device_key, watermark, max_time
FROM tsfile_device_info('main.smoke');            -- g0,d1 | 2001 | 2000
SELECT cid, name, tsfile_type, category
FROM tsfile_schema('main.smoke');                 -- 8 rows

-- Export and re-read
SELECT tsfile_export('main.smoke', '/tmp/tsfile-sqlite-smoke/export');  -- 1
```

The exported directory keeps the sealed file name. Create a read-only table using
that file and `source_table='smoke'`; it should return both rows.

## 6. Troubleshooting

| Symptom | Check |
| --- | --- |
| CMake rejects SQLite | Headers define `SQLITE_OMIT_LOAD_EXTENSION`; use Homebrew SQLite or `libsqlite3-dev` |
| `.load` is an unknown command | Use an extension-capable `sqlite3` binary, such as Homebrew SQLite |
| Extension cannot find `libtsfile` | Set `DYLD_LIBRARY_PATH` (macOS) or `LD_LIBRARY_PATH` (Linux) to `build/sqlite/lib` |
| Build tries to download dependencies | Use the offline build flags above or provide the dependency archives through `TSFILE_DEPENDENCY_CACHE` |
| Seal memory is too high on a small device | The current seal chunk is 4M rows; lower the chunk size in `tsfile_sqlite.cc` for memory-constrained targets |
