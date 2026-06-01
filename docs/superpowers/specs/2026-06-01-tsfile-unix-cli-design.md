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

# Design: A Unix-philosophy command-line interface for TsFile (C++)

- **Date**: 2026-06-01
- **Module**: `cpp/`
- **Status**: Approved design, pending implementation plan

## Goal

Give TsFile a set of composable, pipeable command-line tools — in the Unix
tradition of small programs that read a file and write machine-parseable data
to stdout. The primary gap today is the **read / inspect / export** side: to
answer "what devices, measurements, schema, and data live in this `.tsfile`?"
a user must write code or read the raw byte layout via the Java
`TsFileSketchTool`. A single `tsfile` binary closes that gap and composes with
`awk`, `jq`, `sort`, and database import tools.

## Scope

**In scope (v1):** read-only inspection and export verbs — `ls`, `schema`,
`stats`, `head`, `cat`, `select` — shipped as one multi-call C++ binary.

**Out of scope (v1, possible follow-ups):** write/convert verbs (the Java
`tools` module already imports CSV/Parquet/Arrow → TsFile), a structure-dump
verb at parity with `TsFileSketchTool`, ISO time formatting, and splitting into
multiple `tsfile-*` binaries.

## Why C++

The user wants the most Unix-native form: a single self-contained static binary
with fast startup and no runtime dependency (unlike the JVM-based Java tools or
the Python binding). The C++ read path already exposes everything the verbs
need, so the engine does not change — the work is argument parsing, subcommand
dispatch, and output formatting.

## Existing building blocks (no engine changes needed)

`storage::TsFileReader` (`cpp/src/reader/tsfile_reader.h`) already provides:

| Need | API |
|---|---|
| list devices (tree) | `get_all_device_ids()` / `get_all_devices()` |
| list tables (table) | `get_all_table_schemas()` |
| per-device measurement schema | `get_timeseries_schema(device_id, &out)` |
| per-table schema | `get_table_schema(name)` |
| per-series statistics | `get_timeseries_metadata()` (carries `Statistics`) |
| rows with offset/limit pushdown | `queryByRow(...)` (tree & table overloads) |
| rows by time range / columns | `query(...)` (tree & table overloads) |
| row iteration + column metadata | `ResultSet` + `ResultSetMetadata` (`cpp/src/reader/result_set.h`) |

`result_set.h` also contains `print_table_result_set`, which already iterates
columns and rows and formats each value by `TSDataType` (INT32/INT64/FLOAT/
DOUBLE/BOOLEAN/TEXT/STRING). The `tsv`/`table` formatters extend this pattern;
`csv`/`json` reuse the same type-dispatch.

## Architecture

### Code location

A new `cpp/tools/` directory, parallel to `cpp/examples/` (which is the existing
template for "an executable that links `libtsfile`").

```
cpp/tools/
├── CMakeLists.txt
├── tsfile_cli.cc          # main: parse top level, dispatch subcommand
├── cli_args.h / cli_args.cc      # minimal hand-rolled option parser (no new deps)
├── output_format.h / output_format.cc   # csv / tsv / json(NDJSON) / table formatters
└── commands/
    ├── command.h          # subcommand interface: name(), run(args), help()
    ├── cmd_ls.cc   cmd_schema.cc   cmd_stats.cc
    └── cmd_head.cc cmd_cat.cc      cmd_select.cc
```

### CLI shape — single multi-call binary, git-style dispatch

```sh
tsfile <command> [options] <file.tsfile>
tsfile --help | --version
tsfile help <command>
```

Argument parsing is hand-rolled. The verbs are simple, the project targets
C++11, and a goal is to keep the binary free of new third-party/runtime
dependencies, consistent with the rest of the C++ module.

### Unix discipline (applies to every command)

- **Data goes to stdout; diagnostics, progress, and errors go to stderr.** This
  is what lets `tsfile cat f.tsfile | jq` work without log noise on stdout.
- **Exit codes are meaningful:** `0` success, `1` usage error, `2` cannot open
  / corrupted file, `3` query or runtime error.
- The library currently prints open errors to stdout (`ReadFile::open`,
  `cpp/src/file/read_file.cc:52`). Along the CLI path these must go to stderr so
  they do not corrupt piped output. (Small, contained fix.)

### Build / packaging

- New CMake option `BUILD_TOOLS` (default `ON`), producing
  `build/<type>/bin/tsfile`.
- `install(TARGETS tsfile ...)` so `make install` ships the binary.
- `build.sh` is left unchanged for v1 (it follows CMake defaults); revisit if a
  dedicated flag is wanted.

## Command surface (v1)

All verbs are read-only and backed by the existing reader API.

| Command | Purpose | Backed by |
|---|---|---|
| `ls` | list devices (tree) or tables (table), one name per line | `get_all_device_ids()` / `get_all_table_schemas()` |
| `schema` | per-measurement data type / encoding / compression | `get_timeseries_schema()` (tree) / `get_table_schema()` + `get_timeseries_metadata()` (table) |
| `stats` | per-series row count and time range | `get_timeseries_metadata()` (`Statistic`) |
| `head` | first N rows | `queryByRow(..., offset=0, limit=N)` |
| `cat` | all rows of a device/table | `query()` / `queryByRow(..., limit=-1)` |
| `select` | chosen columns + time range + limit/offset | `query(table, cols, start, end, ...)` / tree `query(paths, start, end)` |

### Common flags

| Flag | Meaning |
|---|---|
| `-f, --format csv\|tsv\|json\|table` | output format; default is TTY-adaptive (see below) |
| `-d, --device <id>` | scope to a device (tree model) |
| `-t, --table <name>` | scope to a table (table model) |
| `-m, --measurements s1,s2` | select columns |
| `-n, --limit N` | max rows (`head` is sugar for `--limit`) |
| `--offset N` | skip leading rows |
| `--start <ts>` / `--end <ts>` | time range; v1 accepts epoch milliseconds |
| `--no-header` | suppress the header row |
| `--model tree\|table` | force a model (override auto-detection) |
| `-h, --help` / `--version` | usage / version |

## Tree vs. table model handling

A `.tsfile` is written in one of two data models. The CLI auto-detects and
adapts:

- **Detection:** `get_all_table_schemas()` non-empty ⇒ **table** model; otherwise
  **tree** model. `--model` overrides for edge cases.
- **`ls`:** tree ⇒ one device ID per line; table ⇒ one table name per line.
  One item per line keeps it pipe-friendly; per-column detail lives in `schema`.
- **Column semantics differ** (tree: device path + measurement; table: table +
  columns), but **the time column is always column 1** in row output
  (`ResultSetMetadata` guarantees this).
- **`schema` field availability:** tree-model files expose data type, encoding,
  and compression per measurement (via `get_timeseries_schema`). Table-model
  files expose column name and data type (via `get_timeseries_metadata`), but
  `TableSchema` has no public encoding/compression getter, so those two columns
  are emitted blank for table-model files. The output keeps a uniform 5-column
  shape (`target, measurement, datatype, encoding, compression`) across models.

## Output formats

- **`table`** (human): aligned columns.
- **`tsv`** (pipe): tab-separated, header row first (unless `--no-header`).
- **TTY-adaptive default:** when stdout is a terminal, default to `table`; when
  piped or redirected, default to `tsv`. `--format` always overrides. This
  mirrors the behavior of `git` and `ls`.
- **`csv`:** RFC 4180 quoting (quote fields containing delimiter, quote, or
  newline; double embedded quotes).
- **`json`:** **NDJSON** — one JSON object per row, newline-delimited — chosen
  for streaming and `jq -c` friendliness over a single large array.
- **Null handling:** empty field in CSV/TSV; `null` in JSON.
- **Timestamps:** v1 emits the raw stored epoch (INT64). `--time-format iso` is
  a deliberate follow-up.

## Error handling & exit codes

| Exit | Condition |
|---|---|
| `0` | success |
| `1` | usage / argument error (unknown command, bad flag, missing file arg) |
| `2` | file cannot be opened or is corrupted (`E_FILE_OPEN_ERR`, `E_TSFILE_CORRUPTED`) |
| `3` | query / runtime error |

The reader returns integer error codes; the CLI maps open/corruption codes to
exit `2` and query failures to exit `3`. The stray stdout error print in
`ReadFile::open` is redirected to stderr along the CLI path.

## Testing

Google Test, under `cpp/test/tools/` mirroring `cpp/src` test conventions.

- **Unit:**
  - `cli_args` parsing (commands, flags, error cases).
  - Each formatter (`csv`, `tsv`, `json`/NDJSON, `table`) against a synthetic
    `ResultSet` / `ResultSetMetadata`, including null and quoting edge cases.
  - Model detection (table-schema-present ⇒ table; otherwise tree).
- **End-to-end:** in a temp directory, write a small `.tsfile` via the existing
  writer (or reuse `cpp/examples/test_cpp.tsfile`), run each command as a
  subprocess, and assert both stdout content and exit code. Fixtures are
  hermetic (generated under a temp dir, cleaned up).

## License header

Every new file (`.cc`, `.h`, `CMakeLists.txt`, this `.md`) carries the Apache
License 2.0 header in the comment style appropriate to the file type, per
repository convention.

## Open follow-ups (explicitly deferred, not v1)

- Structure-dump verb at parity with Java `TsFileSketchTool`.
- Write / convert verbs (Java `tools` already covers import).
- `--time-format iso`, and richer `select` predicates beyond a time range.
- Optional split into multiple `tsfile-*` binaries (coreutils-style).
