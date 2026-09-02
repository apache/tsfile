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

`tsfile_sqlite` is an experimental SQLite loadable extension that keeps recent
rows in SQLite and seals older rows into immutable TsFile table-model segments.

Documentation:

- [User guide](USER_GUIDE.md): build, load, configure, query, seal, deploy, and
  operate the extension.
- [Technical guide](TECHNICAL_GUIDE.md): architecture, virtual-table callbacks,
  storage layout, query planning, and transaction/crash consistency.

Build it with:

```bash
cmake -S cpp -B cpp/build/sqlite \
  -DBUILD_SQLITE_EXTENSION=ON -DTSFILE_BUILD_SHARED=ON -DBUILD_TEST=ON
cmake --build cpp/build/sqlite --target tsfile_sqlite
```

The extension is emitted next to `libtsfile` in the build `lib` directory.
Load it and create a hybrid table with:

```sql
.load ./tsfile_sqlite

CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  directory='/absolute/path/to/segments',
  timestamp_precision='ms',
  column='time:TIMESTAMP:TIME',
  column='device:STRING:TAG',
  column='temperature:DOUBLE:FIELD'
);
```

Seal the half-open historical interval ending at `cutoff` with:

```sql
INSERT INTO sensor(_tsfile_command, _tsfile_cutoff)
VALUES ('seal', 1700000000000);
```

The generated `<table>_data`, `<table>_segments`, and `<table>_config` tables
are SQLite shadow tables. A seal is synchronous and participates in the
surrounding SQLite transaction. Data at or above the watermark remains mutable;
attempts to insert or modify older data return `SQLITE_CONSTRAINT`.

The supported field types are `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`,
`TEXT`, `STRING`, `BLOB`, `DATE`, and `TIMESTAMP`. The first column must be a
`TIMESTAMP:TIME`, TAG columns must be non-null `STRING`, and the TAG columns
together with time form the unique key. The directory is created on table
creation and must be dedicated to that logical table; dropping the virtual
table does not delete already exported segment files.
