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

# DuckDB TsFile Extension

This guide is intended for data engineers, platform developers, and analytics developers who need to analyze or generate Apache TsFile data in DuckDB. The TsFile extension is available as a DuckDB Community Extension. It makes TsFile both a SQL data source and a `COPY` output format, providing a complete read-query-write workflow.

**Remember two things:** use `read_tsfile(path, table_name)` to read data, and use `COPY ... TO ... (FORMAT tsfile)` to write data. The current version supports local, Table-model TsFiles. Before writing, sort the data by the TAG columns and then by the time column.

## Quick Start

### Start DuckDB and Load the Extension

For regular use, install and load the DuckDB Community Extension:

```sql
INSTALL tsfile FROM community;
```

```sql
LOAD tsfile;
```

Install the Community Extension the first time you use it. After that, you only need to run `LOAD tsfile;` whenever you start DuckDB.

### Read Your First TsFile

```sql
SELECT *
FROM read_tsfile('/data/measurements.tsfile', 'sensors');
```

The first argument to `read_tsfile` is a local file path, and the second is the name of the table stored in the TsFile. Each call reads one Table-model table from one file.

## Reading and Querying

### Select Columns

Select the columns you need just as you would from a regular DuckDB table. Projection is pushed down to the TsFile scan whenever possible, reducing the amount of data read:

```sql
SELECT time, device_id, temperature
FROM read_tsfile('/data/measurements.tsfile', 'sensors')
LIMIT 10;
```

### Filter by Time

The global TsFile time axis is exposed in DuckDB as a `BIGINT`. The time unit is determined by the file or protocol; milliseconds are common. The following operators can be pushed down to the scanner: `=`, `<`, `<=`, `>`, `>=`, and `BETWEEN`.

```sql
SELECT time, device_id, temperature
FROM read_tsfile('/data/measurements.tsfile', 'sensors')
WHERE time BETWEEN 1700000000000 AND 1700003600000;
```

### Filter by TAG

String TAG columns support equality and range comparisons, null checks, and combinations using `AND` and `OR`. Supported operations include `=`, `!=`, `<`, `<=`, `>`, `>=`, inclusive `BETWEEN`, `IS NULL`, and `IS NOT NULL`.

```sql
SELECT time, device_id, temperature
FROM read_tsfile('/data/measurements.tsfile', 'sensors')
WHERE (device_id = 'device-01' OR device_id = 'device-02')
  AND time BETWEEN 1700000000000 AND 1700003600000;
```

FIELD filters and TAG expressions that cannot be pushed down are still evaluated by DuckDB after the scan. A `NOT (...)` expression in any form is not pushed down.

### Check Filter Pushdown

Use `EXPLAIN` to inspect the query plan. If pushdown succeeds, the `READ_TSFILE` node shows `Time Range` and `TAG Filter`:

```sql
EXPLAIN
SELECT time, device_id, temperature
FROM read_tsfile('/data/measurements.tsfile', 'sensors')
WHERE device_id = 'device-01'
  AND time >= 1700000000000;
```

## Writing TsFile

### Write with COPY

Writing uses DuckDB's standard `COPY` interface. Clean, cast, and sort the data in a subquery before writing it to TsFile:

```sql
COPY (
    SELECT time, device_id, temperature, humidity
    FROM measurements
    ORDER BY device_id, time
)
TO '/data/measurements.tsfile'
(
    FORMAT tsfile,
    TABLE_NAME sensors,
    TIME_COLUMN time,
    TAG_COLUMNS (device_id)
);
```

`TIME_COLUMN` identifies the time axis. Columns listed in `TAG_COLUMNS` are written as TAG columns, and all other columns are written as FIELD columns. Identifier syntax is recommended for `TABLE_NAME` and `TIME_COLUMN`; use double quotes when an identifier contains special characters.

### Write Options

|Option|Required|Description|
|---|---|---|
|`FORMAT tsfile`|Yes|Selects the TsFile `COPY` writer.|
|`TABLE_NAME`|No|The local table name in the output file. The default is `default_table`.|
|`TIME_COLUMN`|No|The input column used as the time axis. The default is `time`, and its type must be `BIGINT`.|
|`TAG_COLUMNS`|No|A list of TAG column names. If omitted, all non-time columns are written as FIELD columns.|
|`OVERWRITE true`|No|Use this option to replace an existing target. Keep the default temporary-file handling enabled.|

### TAG\_COLUMNS Constraints

`TAG_COLUMNS` accepts a list of column names, not a list of values:

```sql
TAG_COLUMNS (device_id, region)
```

- Each column must exist in the input query.

- Each column must have the `VARCHAR` type.

- A TAG column cannot also be the `TIME_COLUMN`.

- Column-name matching is case-insensitive.

If you do not need TAG columns, you can write only the time and FIELD columns:

```sql
COPY (
    SELECT time, temperature, humidity
    FROM measurements
    ORDER BY time
)
TO '/data/field-only.tsfile'
(
    FORMAT tsfile,
    TABLE_NAME sensors,
    TIME_COLUMN time
);
```

### Type Mapping and NULL Values

|DuckDB Type|TsFile Type|Notes|
|---|---|---|
|BOOLEAN|BOOLEAN|FIELD NULL values are preserved.|
|INTEGER|INT32|FIELD NULL values are preserved.|
|BIGINT|INT64|FIELD NULL values are preserved; this type is also used for the time axis.|
|FLOAT|FLOAT|FIELD NULL values are preserved.|
|DOUBLE|DOUBLE|FIELD NULL values are preserved.|
|VARCHAR|STRING|FIELD NULL values are preserved.|
|BLOB|BLOB|FIELD NULL values are preserved.|
|TIMESTAMP\_NS|TIMESTAMP|Read as DuckDB `TIMESTAMP_NS`.|

## End-to-End Example

The following workflow reads a time range for device `a` from an existing TsFile, renames columns, writes a new TsFile, and then reads it back for verification:

```sql
LOAD tsfile;

COPY (
    SELECT time,
           s0 AS device_id,
           s2 AS value,
           CAST(s8 AS VARCHAR) AS day
    FROM read_tsfile('test/data/simple_table_t1.tsfile', 'test')
    WHERE s0 = 'a'
      AND time BETWEEN 1760106022000 AND 1760106024000
    ORDER BY device_id, time
)
TO '/tmp/tsfile_subset.tsfile'
(
    FORMAT tsfile,
    TABLE_NAME subset,
    TIME_COLUMN time,
    TAG_COLUMNS (device_id)
);

SELECT time, device_id, value, day
FROM read_tsfile('/tmp/tsfile_subset.tsfile', 'subset')
ORDER BY device_id, time;
```

The query returns three rows with timestamps `1760106022000`, `1760106023000`, and `1760106024000`.

## Current Limitations and Troubleshooting

### Pre-Write Checklist

1. Confirm that `TIME_COLUMN` exists and has the `BIGINT` type.

2. Confirm that the TIME column contains no NULL values.

3. Confirm that the input is sorted by all TAG columns and then by the time column.

4. If the target file already exists, keep temporary-file handling enabled and set `OVERWRITE true`.

### Known Limitations

- The current version supports one Table-model table in one local TsFile.

- FIELD filters are not pushed down. A `NOT (...)` TAG expression in any form is not pushed down.

- Writing DATE FIELD columns is temporarily disabled to avoid conversions that depend on the local time zone. DATE columns in existing TsFiles can still be read. To write such a column, first cast it to `VARCHAR`.

- When writing directly with `USE_TMP_FILE false`, the target path must not already exist.

## Quick Reference

|Task|Entry Point|
|---|---|
|Read TsFile|`read_tsfile('/path/file.tsfile', 'table')`|
|Inspect columns and data|`SELECT ... FROM read_tsfile(...)`|
|Push down time and TAG conditions|Use supported comparisons and logical combinations in `WHERE`|
|Inspect pushdown|`EXPLAIN SELECT ...`|
|Write TsFile|`COPY (...) TO 'file.tsfile' (FORMAT tsfile, ...)`|
|Replace an existing file|Default temporary-file handling + `OVERWRITE true`|
