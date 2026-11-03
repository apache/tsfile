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

# SQLite + TsFile user manual

Use `tsfile_sqlite` to query existing TsFile data with SQL, or collect new data and
save it as TsFile. New rows stay in SQLite, where you can update or delete them.
Sealing moves them into TsFile: they remain queryable but can no longer be changed.

This manual walks through creating a table, writing and querying rows, exporting,
and reopening the exported data. It then covers daily operations and troubleshooting.
Examples use the SQLite CLI; applications can execute the same SQL.
[中文](USER_GUIDE.md) · [Technical report](TECHNICAL_GUIDE_EN.md)

## 1. Prepare your environment

Use Linux or macOS with SQLite 3.31 or newer and extension loading enabled. You
need `tsfile_sqlite` and the shared `libtsfile` from the same build. If you already
have them, proceed to the tutorial. This extension supports TsFile table-model files.

To build from source, run from the repository root:

```bash
cmake -S cpp -B cpp/build/sqlite \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON
cmake --build cpp/build/sqlite --target tsfile_sqlite -j
```

The libraries are produced in `cpp/build/sqlite/lib`. The extension is
`tsfile_sqlite.so` on Linux and `tsfile_sqlite.dylib` on macOS. Keep it alongside
`libtsfile` when deploying.

Apple SDK SQLite headers disable extension loading. For Homebrew SQLite, use this
configuration command, then the build command above:

```bash
cmake -S cpp -B cpp/build/sqlite \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON \
  -DSQLite3_INCLUDE_DIR="$(brew --prefix sqlite)/include" \
  -DSQLite3_LIBRARY="$(brew --prefix sqlite)/lib/libsqlite3.dylib"
```

The CLI must also support loading extensions. Start Homebrew's CLI with
`"$(brew --prefix sqlite)/bin/sqlite3"`.

## 2. Complete your first example

Follow these steps in order to create a SQLite database and a standalone TsFile.
Use a new directory for each run; the examples use `/tmp/tsfile-demo`. If it already
exists, choose another name and replace that path throughout the examples. Use
persistent storage for production data.

### Open a database and load the extension

In your terminal:

```bash
mkdir /tmp/tsfile-demo
sqlite3 /tmp/tsfile-demo/demo.db
```

In SQLite, substitute the absolute path to your built extension:

```sql
.load /absolute/path/to/tsfile_sqlite
.headers on
.mode column
```

Load the extension on every new connection. You can include its `.so` or `.dylib`
suffix in the path.

### Create a table and write rows

```sql
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  temperature DOUBLE FIELD,
  directory='/tmp/tsfile-demo/sensor-segments',
  timestamp_precision='ms'
);

INSERT INTO sensor VALUES
  (1000, 'd1', 21.5),
  (2000, 'd1', 22.0),
  (3000, 'd2', 19.0);
```

Here time is a millisecond timestamp, device identifies a device, and temperature
holds a measurement. The extension creates the dedicated directory used for future
sealed data; it must be absent or empty when the table is created. These three rows
are initially mutable SQLite data.

### Query and correct a value

```sql
SELECT time, device, temperature FROM sensor ORDER BY time;
```

Expected result:

```text
time  device  temperature
1000  d1      21.5
2000  d1      22.0
3000  d2      19.0
```

Correct the second measurement and read it back:

```sql
UPDATE sensor SET temperature=22.5 WHERE time=2000 AND device='d1';
SELECT temperature FROM sensor WHERE time=2000 AND device='d1';
```

The result is `22.5`. You can also use ordinary DELETE on rows that have not been sealed.

### Seal and export to TsFile

Export only copies sealed TsFiles; hot rows are not exported. Seal all current hot
rows first:

```sql
SELECT tsfile_seal('main.sensor', 200);
```

The result is `3`, the number of sealed rows. `200` is the maximum size of one
output file in MB. This small data set fits in one file. The extension generates
the file name, for example
`/tmp/tsfile-demo/sensor-segments/tsfilesensor-12345-0.tsfile`. Repeated seals
create more files, and one device may span multiple files.

Now export:

```sql
SELECT tsfile_export('main.sensor', '/tmp/tsfile-demo/export-001');
```

The result is `1`, the number of copied sealed files. The output directory keeps
the original file names. The copy is independent and readable by a standard
TsFile Reader. The original table still returns the same rows, but they are now
sealed and cannot be updated or deleted.

Check the remaining mutable rows and the per-device write boundaries:

```sql
SELECT hot_rows, watermark FROM tsfile_table_info('main.sensor');
SELECT device_key, watermark, max_time FROM tsfile_device_info('main.sensor');
```

The table-level result is `hot_rows=0`, `watermark=2001`. The table watermark is
the minimum across devices: d1's maximum is 2000, so new d1 rows need a timestamp
of at least 2001, while d2 needs at least 3001. In management calls,
`main.sensor` means sensor in the current main database; include the database
qualifier when naming a table.

### Query the exported file directly

List the output directory, then replace `<file>` below with the actual name:

```sql
CREATE VIRTUAL TABLE temp.history USING tsfile_hybrid(
  file='/tmp/tsfile-demo/export-001/<file>.tsfile',
  source_table='sensor'
);

SELECT time, device, temperature FROM history ORDER BY time;
```

The result contains the same three sealed rows, including the corrected `22.5`.
No directory was supplied, so history is query-only. The temp table disappears
when the connection closes; the file remains.

There are no column declarations here: source_table selects the file's sensor
table and the extension reads its schema. The SQLite name history can differ
from the table name inside the file.

### Continue writing from that file

```sql
CREATE VIRTUAL TABLE continued USING tsfile_hybrid(
  file='/tmp/tsfile-demo/export-001/<file>.tsfile',
  source_table='sensor',
  directory='/tmp/tsfile-demo/continued-segments'
);

INSERT INTO continued VALUES (4000, 'd1', 23.0);
SELECT time, device, temperature FROM continued ORDER BY time;
```

The query returns four rows. The first three are read from the original TsFile;
the new row at 4000 is mutable SQLite hot data. The source file and history's
query results remain unchanged.

Write boundaries are per device: d1's maximum in the source is 2000, so d1 can
write 4000; d2's maximum is 3000, so new d2 rows need a timestamp of at least
3001. A different device cannot backfill its own history gaps. Providing a
directory gives this table its own storage for subsequent sealing.

## 3. Use your own data

### Start collecting into an empty table

Adapt the sensor example with your column names, dedicated directory and actual
time unit. Declare each column as `name TYPE CATEGORY`: the first is
`TIMESTAMP TIME`, identifiers are `STRING TAG`, and measurements are FIELD columns.

You can have multiple TAG columns or none. For a single reading at each timestamp:

```sql
CREATE VIRTUAL TABLE readings USING tsfile_hybrid(
  time TIMESTAMP TIME,
  value DOUBLE FIELD,
  directory='/tmp/tsfile-demo/readings-segments',
  timestamp_precision='ms'
);
```

readings accepts one new row per timestamp; sensor distinguishes new rows by
`(device, time)`. Use double quotes for names containing spaces, such as
`"sensor value" DOUBLE FIELD`. Names cannot differ only by ASCII letter case.
Every column needs an explicit TIME, TAG or FIELD category.

### Open an existing TsFile

Confirm its absolute path and internal table name, then follow the history or
continued example. Omit directory for query-only use; provide a new dedicated
directory to append rows. Each creation selects one table in one file. It does
not guess the table name from the filename or include other tables automatically.

Do not add column declarations when opening a file. Inspect the inferred columns:

```sql
PRAGMA table_info(continued);
```

The tutorial shows `time INTEGER`, `device TEXT` and `temperature REAL`. Use these
names in your queries. Ordinary TsFile sources expose the implicit time column as
time; files generated by this extension also preserve a custom TIME column name.

A file without precision metadata can be queried with precision unknown. To
append, first determine its actual time unit and add `timestamp_precision='ms'`,
`'us'` or `'ns'` to the creation arguments. If the file already records a unit, it
is inherited; any explicit option must match.

Keep source paths and contents unchanged. New rows go into SQLite, without
modifying or appending to the external file. Replacing that file does not refresh
the registered table.

### Supply the right values and time unit

Timestamps are stored as integers without automatic conversion. If your input is
in seconds and the table uses ms, convert the input to milliseconds before writing.

| Declared type | Write value |
| --- | --- |
| BOOLEAN | Integer; zero is false, nonzero true; read as 0 or 1 |
| INT32, DATE | Integer within signed int32 range |
| INT64, TIMESTAMP | Integer within signed int64 range |
| FLOAT, DOUBLE | Integer or real number |
| STRING, TEXT | Text |
| BLOB | Binary value, preserving length and zero bytes |

TIME cannot be NULL. TAG and FIELD values can be NULL; TAG columns must be STRING.
New rows with the same complete TAG combination and time conflict. For uniqueness,
two NULLs in the same key position count as equal, so NULL cannot bypass the key
check. NULL, empty text and the literal text `'null'` are distinct. Use IS NULL
when querying a NULL value.

Existing duplicate rows in a source file are returned unchanged, without automatic
deduplication when the table is created.

## 4. Query, edit and seal during daily use

### Query with ordinary SQL

Query the logical table regardless of where rows are stored. Conditions, joins,
aggregates and sorting work through SQLite. After completing the tutorial:

```sql
SELECT device, avg(temperature) AS avg_temperature
FROM continued
WHERE time >= 1000 AND time < 5000
GROUP BY device
ORDER BY device;
```

Specify ORDER BY when order matters. Time ranges, TAG equality conditions and
FIELD comparisons are pushed into the TsFile read path and can reduce scanning
and memory use; expressions that cannot be pushed are evaluated by SQLite on the
result set. Do not use implicit rowids as persistent business keys; sealing and
later queries can change them.

### Write or correct a batch

```sql
BEGIN;
INSERT INTO continued VALUES (5000, 'd2', 20.0);
UPDATE continued SET temperature=23.5 WHERE time=4000 AND device='d1';
COMMIT;
```

Use ROLLBACK instead of COMMIT to cancel the batch. Hot writes also support
savepoints. Locate rows precisely using their time and TAG values.

If an UPDATE or DELETE actually targets any sealed row, the entire statement fails,
including changes to other hot rows. IGNORE and FAIL do not bypass this rule.
Reading cold data remains allowed.

### Seal the hot area early

When recent data no longer needs correction, seal it explicitly:

```sql
SELECT tsfile_seal('main.continued', 200);
```

The argument is the maximum size of one output file in MB. Seal processes all
current hot rows and returns the sealed row count; an empty hot area returns 0,
creates no file and changes no device boundary. The output is one or more complete
TsFiles, each no larger than the limit; one device may span files. If a single
row exceeds the limit, seal returns an error and leaves the hot rows unchanged.

After sealing, each device's new rows must be no earlier than that device's
maximum sealed time plus one; devices without hot rows keep their boundary.
Sealing does not change query results. Re-query
`tsfile_device_info('main.continued')` to see each device's watermark and
max_time.

Seal is synchronous and can participate in an explicit transaction:

```sql
BEGIN IMMEDIATE;
SELECT tsfile_seal('main.continued', 200);
ROLLBACK;
```

This rollback undoes the seal and makes the sealed rows mutable again. Use COMMIT
to retain it. A successful seal call inside a transaction is not durable until
that outer transaction commits.

### Merge sealed files

Repeated seals produce multiple files. Merge small files explicitly:

```sql
SELECT tsfile_compact('main.continued', 50, 200);
```

`50` is the small-file threshold and `200` is the target maximum file size, both
in MB. Only sealed files smaller than 50 MB participate; outputs aim near 200 MB.
Files already at least 50 MB (including earlier compact outputs) are not selected.
The target cannot be smaller than the threshold.

Compact only touches sealed files in the table's own directory, never external
source files. New files are registered atomically and old files are deleted after
the transaction commits; on failure the originals remain and no partial result is
published. Hot rows and device watermarks are unchanged. It returns the number of
output files, or 0 when there are not enough mergeable files.

## 5. Export and deliver data

Export directly copies the table's own sealed TsFiles. It does not seal, and it
does not copy hot rows. Seal first when current hot data must be delivered; run
compact first if file sizes are unsuitable:

```sql
SELECT tsfile_seal('main.continued', 200);
SELECT tsfile_export('main.continued', '/tmp/tsfile-demo/export-002');
```

Commit or roll back any current transaction first, then execute this SELECT on
its own. Invoke export and seal as standalone calls, outside row queries, views,
triggers or other expressions. A read-only table exports its registered source
files.

The output must be a new absolute directory whose parent exists. It keeps the
sealed files' names. A nonempty table returns the number of copied files; an empty
table creates an empty directory and returns 0. The internal table name and time
precision stay as they were when sealed. Tell recipients the table name so they
can select it with source_table.

Keep output outside the table's owned segment directory and do not let it contain
or replace a source file. It can be a sibling of an external file. Existing
output paths are never overwritten. Deleting an export does not affect the
original table. However, keep the export if another table references it, as
history does in the tutorial.

Export does not include hot rows. Finish corrections or seal them separately
before exporting with a new output directory.

### Recover from an export failure

Read the error and check hot_rows and watermark through tsfile_table_info:

| State reported by the error | Next step |
| --- | --- |
| Path validation failed | Fix paths, permissions or files and retry; the table and sealed files are unchanged |
| Copy or verification failed | The table and sealed files remain; no partial output is published; fix the issue and retry with a new directory |
| Complete output published but parent-directory sync failed | Inspect the target and files first; use another directory if exporting again |

An interrupted process may leave a temporary directory containing
`.tsfile-export-` in its name; do not treat it as a completed delivery. Export
saves table data, not the complete SQLite database, business configuration or
hot/cold state. Empty exports do not preserve a table definition. Export cannot
replace a complete application database backup.

## 6. Check status and troubleshoot

### Determine what can still be written

```sql
SELECT mode, timestamp_precision, device_count, watermark, source_max_time,
       hot_rows, file_count, directory, source_file, source_table
FROM tsfile_table_info('main.continued');

SELECT device_key, watermark, max_time, hot_rows
FROM tsfile_device_info('main.continued');
```

`tsfile_table_info` returns table state. hot_rows counts mutable rows, file_count
is the number of registered TsFiles, and device_count is the number of complete
TAG combinations (1 for a table without TAGs). watermark is the minimum across
devices; an empty table reports INT64_MIN and a read-only table reports NULL.
source_max_time is the maximum time across devices in the external source.

`tsfile_device_info` reports each device's watermark, max_time and mutable row
count. watermark is the minimum allowed time for new rows on that device;
max_time is that device's current maximum. This describes the current view; the
actual write result is authoritative.

To recreate a table or hand it to other TsFile tools, use `tsfile_schema` to see
the original column definitions:

```sql
SELECT cid, name, tsfile_type, category, sqlite_type, not_null
FROM tsfile_schema('main.continued');
```

It returns, in declaration order, each column's TsFile type (TIMESTAMP, STRING,
DOUBLE, INT32 and so on), category (TIME, TAG or FIELD), mapped SQLite type and
nullability. SQLite's `DESC` or `PRAGMA table_info` only shows the mapped
INTEGER/REAL/TEXT/BLOB declarations and cannot distinguish TAG from FIELD;
`tsfile_schema` supplies that information.

If a device reaches INT64_MAX, its watermark becomes NULL and that device cannot
accept later data; other devices can still write. Existing data remains readable
and exportable. Seal can seal a hot row at INT64_MAX and set that device's
watermark to NULL.

### An INSERT, UPDATE or DELETE fails

Check mode and each device's boundary first. A file table created without
directory is read-only; create another named table with a new directory to append
data. A write with no target rows may succeed as a no-op, which does not make a
read-only table writable.

For writable tables, a new time must be at least that device's own watermark. For
external history, this means strictly after that device's maximum in the source;
the device's own historical gaps cannot be backfilled. A new device is not
constrained by other devices' boundaries. Then check for a duplicate
`(all TAGs, time)` key, NULL TIME, wrong types or out-of-range values. For
UPDATE/DELETE, also confirm the target rows are not sealed.

### A source is missing or queries report file problems

```sql
SELECT path, status, detail FROM tsfile_verify('main.continued');
```

Use the reported paths to investigate:

| Status | Action |
| --- | --- |
| OK | The current file checks passed |
| MISSING | Check whether the file moved or was deleted; restore the original at its registered path |
| CORRUPT | The file cannot be opened or parsed; check read permissions and completeness |
| MISMATCH | The file differs from registration; investigate replacement and restore the original |
| UNREGISTERED | Investigate the unregistered TsFile's origin before treating it as data or deleting it |

Verify reports only. It does not repair, register or delete files, or inspect other
files beside an external source. It reads each registered file in full and
computes its content fingerprint, so large files add overhead; ordinary queries
compare only size, modification time and inode metadata.

### Loading or creating a table fails

For not authorized, check SQLite extension-loading support; applications must
enable loading on the connection first. For a missing libtsfile, verify that it
and the extension are from the same build and are deployed together.

For creation failures, check absolute paths, the file-internal table name and
precision. Use an empty or absent directory dedicated to this table, without
overlapping or nesting another table's directory. File-based creation has no
explicit column declarations; empty-table creation needs columns, directory and
precision.

### The saved creation SQL has no column definitions

sqlite_schema.sql stores the virtual-table creation SQL and does not expand
inferred columns. Use `PRAGMA table_info(table_name)` to inspect the actual schema.
On older SQLite versions, sqlite_master is the compatible name for sqlite_schema.

## 7. Reopen tables and manage their files

Reopen the same database and load the extension to use persistent tables again.
Their columns, hot rows and write boundaries are retained; do not repeat CREATE.
Temporary tables and their hot rows disappear when their connection closes, while
referenced and generated TsFile files remain.

Retain the SQLite database, external source files and each table's segment directory.
Do not move or rewrite files still referenced by a table. New files appearing in a
directory are not automatically added to query results.

The `.tsfile-owner` marker binds a writable directory to the database path and
table name. A copied or moved SQLite database cannot simply reuse the original
directory for writes; plan data and path handling before moving a deployment.
Do not modify internal tables containing `_tsfile$` to change paths or data.

Use DROP TABLE only when the logical table is no longer needed. It removes SQLite
hot rows and registration, while retaining external files, sealed files and the
directory marker. Check that no other tables reference these files before later
archiving or deleting them.

The current version does not automatically migrate prototype databases or support
changing columns or switching read-only tables to writable in place. Query results
are materialized in memory; export copies files and its cost scales with file
size; compact re-reads and rewrites the selected sealed files. Evaluate memory and
runtime with representative data before using large tables. Sealing, compaction
and export are caller-initiated; there is no background schedule.
