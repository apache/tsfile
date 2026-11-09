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

# SQLite TsFile Extension

This guide is intended for data engineers and platform developers who need to read and write Apache TsFile in SQLite. `tsfile_sqlite` is a SQLite loadable extension that registers the `tsfile_hybrid` virtual table module. It combines SQLite hot data and TsFile cold data behind a single logical table: newly written rows stay in SQLite and can be updated or deleted, while sealed rows are written to TsFile and remain queryable but immutable.

**Remember three things:** load the extension with `.load`, create tables with `CREATE VIRTUAL TABLE ... USING tsfile_hybrid(...)`, and seal hot data with `tsfile_seal`. The current version supports local, Table-model TsFiles.

## Quick Start

### Load the Extension

Load the extension in the SQLite CLI or your application. It must be reloaded on every new connection:

```sql
.load /path/to/tsfile_sqlite
```

### Create a Writable Table and Write Rows

```sql
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  temperature DOUBLE FIELD,
  directory='/data/sensor-segments',
  timestamp_precision='ms'
);

INSERT INTO sensor VALUES
  (1000, 'd1', 21.5),
  (2000, 'd1', 22.0),
  (3000, 'd2', 19.0);
```

`time` stores millisecond timestamps, `device` identifies devices, and `temperature` stores measurements. `directory` is the exclusive directory used to seal this table's data; the extension creates it. At this point all three rows are still in SQLite and not yet sealed.

### Seal into TsFile

```sql
SELECT tsfile_seal('main.sensor', 200);
```

Returns the number of sealed rows. The argument is the size limit for a single output file, in MB. After sealing, the rows remain queryable but can no longer be modified.

### Query

```sql
SELECT time, device, temperature FROM sensor WHERE device = 'd1';
```

## Reading and Querying

### Open an Existing TsFile (Read-Only)

```sql
CREATE VIRTUAL TABLE history USING tsfile_hybrid(
  file='/data/export/sensor-0001.tsfile',
  source_table='sensor'
);

SELECT * FROM history;
```

`file` is a file path (or a directory), and `source_table` is the table name stored inside the TsFile. A table without `directory` is read-only and needs no column declarations.

### Continue Writing on Top of an Existing TsFile

```sql
CREATE VIRTUAL TABLE continued USING tsfile_hybrid(
  file='/data/export/sensor-0001.tsfile',
  source_table='sensor',
  directory='/data/continued-segments'
);

INSERT INTO continued VALUES (4000, 'd1', 23.0);
```

Historical rows are read from the TsFile, and new rows are written to the SQLite hot region. The write boundary is per device: a new time must be no earlier than that device's maximum time in the source file plus one. New devices are not constrained by old device boundaries.

### Filters and Pushdown

Time ranges, TAG equality conditions, and FIELD comparison conditions are pushed down to the TsFile read layer; expressions that cannot be pushed down are evaluated by SQLite over the result set.

Filtering by `device` or another device condition is the most effective pruning: data is organized by device, so a device filter can skip other devices' data, whereas a global time-range query may scan a large amount of data even when it returns few rows. Business queries should include a device condition whenever possible.

## Writing and Sealing

### Table Modes

- **Start from scratch**: declare columns plus an exclusive `directory` and a time precision; writable.
- **Read-only query**: `file=` without `directory`.
- **Continue writing**: `file=` plus `directory=`, appending on top of an existing TsFile.

### Hot Writes, Updates, and Deletes

```sql
BEGIN;
INSERT INTO sensor VALUES (4000, 'd2', 20.0);
UPDATE sensor SET temperature = 22.5 WHERE time = 2000 AND device = 'd1';
DELETE FROM sensor WHERE time = 3000;
COMMIT;
```

Rows that are not yet sealed can be modified. If an UPDATE or DELETE actually hits a sealed row, the whole statement fails.

### Seal

```sql
SELECT tsfile_seal('main.sensor', 200);
```

Seals all current hot rows and returns the number of sealed rows. Seal runs synchronously and can participate in an explicit transaction (`BEGIN IMMEDIATE; ... ROLLBACK/COMMIT;`). After sealing, each device's new data must be no earlier than that device's new maximum time plus one.

### Compact Cold Segments

```sql
SELECT tsfile_compact('main.sensor', 50, 200);
```

`50` is the small-file threshold and `200` is the target output size limit, both in MB. Only files smaller than the threshold are compacted; the target size cannot be smaller than the threshold.

### Export

```sql
SELECT tsfile_export('main.sensor', '/data/export');
```

Export copies sealed cold segments directly and does not copy hot-region data. Run seal first when you need to deliver current hot data. The output directory must be an absolute path that does not yet exist.

## Type Mapping and NULL

| Declared type | Written value |
| --- | --- |
| BOOLEAN | integer; 0 is false, non-zero is true |
| INT32, DATE | integer within int32 range |
| INT64, TIMESTAMP | integer within int64 range |
| FLOAT, DOUBLE | integer or decimal |
| STRING, TEXT | text |
| BLOB | binary value, preserving length and zero bytes |

TIME cannot be NULL. TAG and FIELD may be NULL; TAG must be STRING. New rows with the same time and TAG combination conflict. NULL, the empty string, and the text `'null'` are all distinct; use `IS NULL` to find NULLs.

## End-to-End Example

Collect from scratch, seal, export, and read back:

```sql
-- Create a table and write rows
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  temperature DOUBLE FIELD,
  directory='/tmp/sqlite-demo/segments',
  timestamp_precision='ms'
);
INSERT INTO sensor VALUES (1000,'d1',21.5), (2000,'d1',22.0), (3000,'d2',19.0);

-- Seal and export
SELECT tsfile_seal('main.sensor', 200);
SELECT tsfile_export('main.sensor', '/tmp/sqlite-demo/export');

-- Read back as read-only
CREATE VIRTUAL TABLE history USING tsfile_hybrid(
  file='/tmp/sqlite-demo/export', source_table='sensor'
);
SELECT time, device, temperature FROM history ORDER BY time;
```

## Current Limitations and Troubleshooting

### Before Writing

1. Confirm the table is writable (provides `directory`) or intended as read-only.

2. Confirm the TIME column is non-NULL and new times are no earlier than the device's watermark.

3. Confirm TAG columns are STRING and column names are not duplicated.

### Known Limitations

- The current version supports one Table-model table in one local TsFile file or directory.

- ASCII uppercase TAG/FIELD names may be lowercased in a read-only view; the TIME column name is preserved. SQLite identifiers are case-insensitive, but be aware when downstream systems map by exact measurement name.

- A FIELD column name whose UTF-8 byte length reaches 1018 may fail cold reads; keep it at or below 1017 bytes.

- Global time-range queries without a device condition can be slow; include a device condition whenever possible.

- A long read transaction that continues querying an old snapshot after a compact may report a missing or changed file; end and restart the transaction.

## Capability Quick Reference

| Task | Entry point |
| --- | --- |
| Load the extension | `.load /path/to/tsfile_sqlite` |
| Create a writable table | `CREATE VIRTUAL TABLE ... USING tsfile_hybrid(...)` |
| Query a TsFile read-only | `file=...` + `source_table=...` |
| Continue writing | `file=...` + `directory=...` |
| Seal | `tsfile_seal('main.t', max_mb)` |
| Compact cold segments | `tsfile_compact('main.t', small_mb, target_mb)` |
| Export | `tsfile_export('main.t', '/path')` |
| Inspect status | `tsfile_table_info` / `tsfile_device_info` / `tsfile_schema` |
| Verify files | `tsfile_verify('main.t')` |
