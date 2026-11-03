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

# SQLite + TsFile extension

`tsfile_sqlite` is an experimental SQLite extension for querying TsFile table-model
files and maintaining writable tables with mutable SQLite hot rows and immutable
TsFile history.

Documentation:

- User manual: [English](USER_GUIDE_EN.md) |
  [Chinese](USER_GUIDE.md).
- Technical report: [English](TECHNICAL_GUIDE_EN.md) |
  [Chinese](TECHNICAL_GUIDE.md).
- Build and test guide: [TESTING.md](TESTING.md).
- Self-test report: [SELF_TEST_REPORT.md](SELF_TEST_REPORT.md).

Build from the repository root:

```bash
cmake -S cpp -B cpp/build/sqlite \
  -DBUILD_SQLITE_EXTENSION=ON -DTSFILE_BUILD_SHARED=ON -DBUILD_TEST=ON
cmake --build cpp/build/sqlite --target tsfile_sqlite TsFile_Sqlite_Test -j
ctest --test-dir cpp/build/sqlite/test -R '^TsFileSqliteTest$' --output-on-failure
```

Use SQLite 3.31 or newer with extension loading enabled. On macOS, select
extension-capable headers and libraries, for example by adding
`-DSQLite3_INCLUDE_DIR=/opt/homebrew/opt/sqlite/include` and
`-DSQLite3_LIBRARY=/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib` for Homebrew on
Apple Silicon. Deploy the extension next to `libtsfile` from the same build.

All three creation modes use `tsfile_hybrid`:

```sql
.load /absolute/path/to/tsfile_sqlite

-- Empty writable table: declare its schema and time unit.
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  temperature DOUBLE FIELD,
  directory='/absolute/path/to/sensor-segments',
  timestamp_precision='ms'
);

-- Existing history plus a writable hot area: infer the source schema.
CREATE VIRTUAL TABLE continued USING tsfile_hybrid(
  file='/archive/history.tsfile',
  source_table='sensor',
  directory='/absolute/path/to/continued-segments'
);

-- Existing history without a directory: query only.
CREATE VIRTUAL TABLE temp.history USING tsfile_hybrid(
  file='/archive/history.tsfile',
  source_table='sensor'
);

INSERT INTO sensor VALUES (1700000000000, 'device-1', 21.5);
SELECT * FROM sensor ORDER BY time;
SELECT tsfile_seal('main.sensor', 200);
SELECT tsfile_export('main.sensor', '/export/sensor-001');
SELECT tsfile_compact('main.sensor', 50, 200);
SELECT * FROM tsfile_table_info('main.sensor');
SELECT * FROM tsfile_device_info('main.sensor');
SELECT * FROM tsfile_schema('main.sensor');
SELECT * FROM tsfile_verify('main.sensor');
```

The writable external-file mode requires a known time unit; supply
`timestamp_precision='ms'`, `'us'`, or `'ns'` when the source has no precision
property. New rows must be no earlier than the device's own watermark: strictly
later than that device's maximum time in the selected source table. New device
keys are not constrained by other devices. External files stay unchanged. Actual
updates or deletes of cold rows fail with `SQLITE_READONLY` and roll back the
whole statement.

`tsfile_seal` takes a maximum output file size in MB, seals all current hot rows,
and returns the sealed row count. `tsfile_export` copies the table's already
sealed TsFile files without sealing, so call `seal` first when hot rows must be
exported. It must run outside an explicit transaction. `tsfile_compact` merges
sealed files smaller than the minimum size into outputs near the target size.
Hot rows and device watermarks are unchanged by compact.

This version replaces the prototype's `column=` syntax, hidden management columns,
and shadow-table layout. It does not automatically migrate prototype databases.
