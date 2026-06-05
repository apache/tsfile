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

[English](./README.md) | [Chinese](./README-zh.md)
# TsFile Tools Manual
## Introduction

## Development

### Prerequisites

To build the Java version of TsFile Tools, you must have the following dependencies installed:

1. Java >= 1.8 (1.8, 11 to 17 are verified. Make sure the environment variable is set).
2. Maven >= 3.6 (if you are compiling TsFile from source).

### Build with Maven

```sh
mvn clean package -P with-java -DskipTests
```

### Install to local machine

```
mvn install -P with-java -DskipTests
```

## Schema Definition

### Parameters

| Parameter | Description | Required | Default |
|-----------|------------|----------|---------|
| table_name | Table name | Yes | |
| time_precision | Time precision (ms / us / ns / s) | No | ms |
| has_header | Whether CSV contains a header (true / false). Ignored for Parquet / Arrow. | No | true |
| separator | CSV delimiter (, / tab / ;). Ignored for Parquet / Arrow. | No | , |
| null_format | String value treated as null in CSV. Ignored for Parquet / Arrow (native null). | No | |
| tag_columns | Tag columns (device identifiers / primary key). Supports virtual columns with DEFAULT value. | No | |
| time_column | Time column name | Yes | |
| source_columns | Column definitions mapping to source file columns | Yes | |

> **Backward compatibility**: `id_columns` and `csv_columns` are still accepted as aliases for `tag_columns` and `source_columns`.

### Column Concepts

- **time_column**: Exactly one per table. Written as `time` column with type `TIMESTAMP` in TsFile.
- **tag_columns**: Device identifiers (composite primary key), 0 or more. Supports virtual columns not present in the source file via `DEFAULT` keyword.
  - **Data type is always `STRING`** and cannot be changed. Any type declared for a tag column in `source_columns` is ignored. We recommend writing tag columns in `source_columns` with the column name only (no type).
- **source_columns**: Maps every column in the source file by position (CSV) or by name (Parquet / Arrow). Use `SKIP` to ignore a column.
- **FIELD** (derived, not configured): All columns in `source_columns` that are not `time_column`, not in `tag_columns`, and not `SKIP`. These are the measurement columns whose values change over time.

> **Column name case**: TsFile table-model column and table names are case-insensitive and stored as lowercase. Regardless of whether you write `Time` / `TIME` / `time` in `import.schema`, the on-disk and read-back name is `time`.

### Schema Example

> Duplicate timestamps within the same device are not supported — rows sharing identical tag column values and the same timestamp will fail to write.

CSV file content:
```
Region,FactoryNumber,DeviceNumber,Model,MaintenanceCycle,Time,Temperature,Emission
hebei,1001,1,10,1,1,80.0,1000.0
hebei,1001,1,10,1,4,80.0,1000.0
hebei,1002,7,5,2,1,90.0,1200.0
```

Schema file (`import.schema`):
```
table_name=root.db1
time_precision=ms
has_header=true
separator=,
null_format=\N

tag_columns
Group DEFAULT Datang
Region
FactoryNumber
DeviceNumber

time_column=Time

source_columns
Region,
FactoryNumber,
DeviceNumber,
SKIP,
SKIP,
Time INT64,
Temperature FLOAT,
Emission DOUBLE,
```

In this example:
- `Group` is a virtual tag column (not in CSV) with default value `Datang`
- `Region`, `FactoryNumber`, `DeviceNumber` are tag columns read from CSV; their type is fixed as `STRING` and need not be declared
- `Model` and `MaintenanceCycle` are skipped via `SKIP`
- `Temperature` and `Emission` are automatically derived as FIELD columns

For Parquet / Arrow in schema mode, `source_columns` matches by column **name** instead of position. Named SKIP is also supported:
```
source_columns
Time INT64,
unused_col SKIP,
Temperature FLOAT,
Emission DOUBLE,
```

**Validation rules for Parquet / Arrow schema mode** (enforced — mismatches raise an error and the source file is moved to `--fail_dir`):

- **Column count must match exactly.** The number of entries in `source_columns` must equal the number of columns in the Parquet / Arrow file. Use `SKIP` for any file column you don't want to import.
- **Every name must exist in the source file.** Each non-SKIP column name and every named SKIP must resolve to an actual column in the file.
- **Unnamed `SKIP` is not allowed.** Because matching is by name, an unqualified `SKIP` cannot identify a column. Always use `columnName SKIP`.

## CLI Parameters

| Parameter | Description | Required | Default |
|-----------|------------|----------|---------|
| -s, --source | Input file or directory | Yes | |
| -t, --target | Output directory | Yes | |
| --schema | Schema file path. Omit for auto mode. | No | |
| --fail_dir | Directory for failed source files | No | failed |
| --format | Source format: csv / parquet / arrow. Auto-detected by file extension if omitted. | No | auto-detect |
| --table_name | Table name override (auto mode) | No | derived from filename |
| --time_precision | Time precision override (auto mode): ms / us / ns / s | No | ms |
| --separator | CSV delimiter (auto mode): , / tab / ; | No | , |
| -b, --block_size | CSV chunk size (e.g. 256M, 1G) | No | 256M |
| -tn, --thread_num | Thread count for parallel processing | No | 8 |

## Modes

### Schema Mode

Provide a `--schema` file to explicitly define column mapping, types, tags, and time column.

```sh
# CSV
csv2tsfile.sh --source ./data/csv --target ./output --fail_dir ./failed --schema ./schema/import.schema
csv2tsfile.bat --source .\data\csv --target .\output --fail_dir .\failed --schema .\schema\import.schema

# Parquet
parquet2tsfile.sh --source ./data/parquet --target ./output --fail_dir ./failed --schema ./schema/import.schema
parquet2tsfile.bat --source .\data\parquet --target .\output --fail_dir .\failed --schema .\schema\import.schema

# Arrow
arrow2tsfile.sh --source ./data/arrow --target ./output --fail_dir ./failed --schema ./schema/import.schema
arrow2tsfile.bat --source .\data\arrow --target .\output --fail_dir .\failed --schema .\schema\import.schema
```

### Auto Mode

Omit `--schema` to automatically infer column types and detect the time column.

**Auto mode rules:**
- **Time column**: must be named exactly `time` or `TIME` (case-sensitive, strict match).
  - Parquet / Arrow: if the source file contains multiple Timestamp-typed columns, only the one named `time` / `TIME` is selected as the time axis. The remaining Timestamp columns become FIELD columns and are stored as `INT64` (raw value preserved, TIMESTAMP semantic dropped). To keep them as `TIMESTAMP`, switch to schema mode and declare them explicitly.
- All other columns become FIELD (no tag inference)
- **CSV type inference** uses a 100-row sampling window per column. Each non-null cell is classified into a base type (BOOLEAN / INT64 / DOUBLE / STRING).
  - If only one base type appears across the sampled rows for a column, that type is used.
  - When **different base types appear in the same column**, the column is promoted: INT64 + DOUBLE → DOUBLE; any other mixed combination (including BOOLEAN with any numeric type) → STRING.
- Parquet / Arrow use native schema types directly
- **Default table name**: derived from the source filename (e.g. `sensor.csv` → table `sensor`). Sanitization rules applied in order:
  1. Strip the file extension (`.csv` / `.parquet` / `.arrow` / `.ipc` / `.feather`, or the last `.`-suffix as a fallback).
  2. Keep only ASCII letters (`a–z`, `A–Z`), digits (`0–9`), underscore (`_`), and dot (`.`). Every other character is replaced with `_`.
  3. Collapse consecutive `_` into a single `_`; strip leading and trailing `_`.
  4. If the result is empty, use a format-specific default: `csv_data` / `parquet_data` / `arrow_data`.
  5. If the result starts with a digit, prefix `t_` (TsFile table names cannot start with a digit).
- Default null tokens (CSV only): empty cell and `\N`

**Auto mode example:**

CSV file (`sensor.csv`):
```
time,temperature,humidity,status
1000,25.5,60.0,true
2000,26.1,55.3,false
3000,27.0,58.1,true
```

Auto mode infers:
```
table name:  sensor        (from filename)
time column: time
fields:      temperature DOUBLE, humidity DOUBLE, status BOOLEAN
tags:        (none)
```

**Commands:**
```sh
# CSV
csv2tsfile.sh --source ./data/csv --target ./output --fail_dir ./failed
csv2tsfile.bat --source .\data\csv --target .\output --fail_dir .\failed

# CSV with options
csv2tsfile.sh --source ./data/csv --target ./output --table_name my_table --separator tab --time_precision us

# Parquet
parquet2tsfile.sh --source ./data/parquet --target ./output --fail_dir ./failed
parquet2tsfile.bat --source .\data\parquet --target .\output --fail_dir .\failed

# Arrow (.arrow / .ipc / .feather)
arrow2tsfile.sh --source ./data/arrow --target ./output --fail_dir ./failed
arrow2tsfile.bat --source .\data\arrow --target .\output --fail_dir .\failed
```

### Output File Naming

- Single batch: `{source_basename}.tsfile`
- Multiple batches: `{source_basename}_1.tsfile`, `{source_basename}_2.tsfile`, ...
- Table name and output filename are independent — table name comes from schema or `--table_name`, filename comes from source file.

