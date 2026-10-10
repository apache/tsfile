---
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
name: tsfile-cli
description: >-
  Use specifically for the project's C++ `tsfile-cli` in cpp/tools: inspect,
  preview, export, or create an Apache TsFile; report metadata or per-column
  counts; or use its explicit single-table CSV write command.
---

# tsfile-cli

Single pipe-friendly C++ binary to inspect, read, export, and create `.tsfile`
files. Source `cpp/tools/`. Result data goes to stdout except `export`,
`sketch -o`, and `write`; diagnostics go to stderr.

## Scope

Use this skill only for the C++ `tsfile-cli` binary. For Java, Python, C++, or C
SDK integration, schema design, Java CSV/Parquet/Arrow import, Java table
point-count metadata checks or backfill, and programmatic tree-model writes,
load the sibling `tsfile` skill at `../tsfile/SKILL.md`.

The names overlap but the semantics do not: `tsfile-cli count` is a read-only
per-column report, not the Java table point-count property tool. `tsfile-cli
write` is the C++ binary's narrow one-file/stream, one-table CSV import; it
does not replace the Java batch and format-aware import tools.

## Binary

- Name `tsfile-cli` (CMake target `tsfile_cli`). Check `PATH`,
  `cpp/build/*/bin/tsfile-cli`, or `cpp/target/build/bin/tsfile-cli`.
- Build only if missing: `cd cpp && bash build.sh -t=Debug`.
- Use `tsfile-cli <command> --help` for the installed binary's supported
  syntax and result fields before choosing flags.

## Read

`tsfile-cli <cmd> [opts] <file.tsfile>` · `tsfile-cli --help | --version | help`

| cmd | output | scans pages |
|---|---|---|
| `ls` | `model,object` rows for tree devices or table names | no |
| `schema` | `model,object,column,category,data_type,encoding,compression` | no |
| `meta` | `size_bytes,format_version,model` | no |
| `stats` | FIELD statistics, null counts, and `stats_source` | maybe |
| `count` | per-column row/entity/value counts; no summary row | maybe |
| `head` | first N rows (default 10, `-n`) | yes |
| `cat` | all matching rows (streamed; `table` format buffers) | yes |
| `sketch` | physical layout text with offsets and chunk/page details | no FIELD decode |
| `export` | writes one object file or a numbered multi-object directory | yes |

Inspect `ls`, `schema`, and `meta` before querying rows to identify the model,
objects, and columns. `stats` can scan when statistics are unavailable, and
table-model `count` scans rows; neither is guaranteed to be metadata-only.

For `head` and `cat`, omit `-d/-t` only when the selected model has exactly one
accessible object. Multi-object files require explicit scope. `schema`,
`stats`, and `count` visit every object when scope is omitted.

```text
opts: -f table|ndjson|csv  (default table, including pipes)
      -d <device> | -t <table>   (mutually exclusive)
      -m <column> repeat for projection or metadata filtering; no comma lists
      -n N · --offset N · --start <int64> · --end <int64> for head/cat/export
      --tag-filter C OP [V] for table TAG predicates
        OP=eq|neq|regexp (requires V) or is-null|not-null (no V)
      --tag-match all|any when two or more tag filters are present
```

- Row time bounds are inclusive. Table results include `time`, all TAG columns,
  and then selected FIELD columns; `-m` does not remove TAG columns.
- `stats` reports `non_null_count,null_count,min_time,max_time,min,max,first,last,sum`
  with model/object/FIELD identity and `stats_source`; table rows include
  `tag.<name>` columns after `object`.
- `count` reports `model,object,column,category,row_count,entity_count,non_null_count,null_count,min_time,max_time,time_source`.
- `ndjson` emits one JSON object per line. INT64/TIMESTAMP values are decimal
  strings; BOOLEAN/INT32/FLOAT/DOUBLE are bare values, and NULL/NaN/Inf become
  JSON `null`. CSV includes a header and quotes cells using RFC 4180 rules.
- The aligned `table` format buffers rows and renders control characters as
  visible escapes. Prefer `csv` or `ndjson` for large dumps and pipelines.

## Sketch and export

`sketch [-o <file>] [--force] <file.tsfile>` prints the physical layout to
stdout or an output file. It does not accept `-f` or object scope. `--force`
requires an output path and replaces only an existing regular file.

```text
export (-d <device> | -t <table>) --type table|ndjson|csv -o <file> [query options] [--force] <file.tsfile>
export (-d <device>... | -t <table>...) --type table|ndjson|csv --output-dir <dir> [query options] <file.tsfile>
```

`export` requires explicit scope and `--type`; `-f` does not select the export
type. It accepts the row query options above. Single-object export commits
the output file atomically; `--force` allows replacing a regular file.
Multi-object export repeats only devices or only tables, writes numbered
files and `_manifest.json`, and requires a new output directory.

## Write

`tsfile-cli write --table <name> (--tag <name> STRING)* (--field <name> <TYPE>)+ [--encoding <TYPE> <ENC>] [--compression <TYPE> <COMP>] (-i <input.csv>|--stdin) -o <out.tsfile> [-v]`

Imports rows into a **new table-model** file. The target must not already exist.
Input is strict CSV with a required `time` header; all other columns are declared
explicitly by `--tag` and `--field` — **no type inference**.

```text
TYPE  ∈ { BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, TIMESTAMP, DATE, BLOB }
```

- `--tag` declarations must use `STRING`; at least one `--field` is required.
- The CSV header must contain `time` and exactly the declared TAG/FIELD names;
  rows are mapped by header name rather than physical column order.
- `--encoding` and `--compression` may override the bound defaults by data type.
- `--stdin` or `-i <input.csv>` is required; TSV, `--columns`, `--no-header`, and
  `--header-match` are not supported.
- Unquoted `\N` denotes NULL. Empty STRING/TEXT cells are empty strings;
  quoted `"\N"` is literal text. `DATE` cells are `YYYY-MM-DD`, and
  `TIMESTAMP`/`time` use strict decimal int64 values.
- A failed import leaves no partial output; success is silent unless `-v` is used.
- **timestamps must be strictly increasing per device** (device = tag-column values); rows for
  different tags may interleave/reuse timestamps. Out-of-order input → error with line number.
- `-v` writes a post-commit summary and resolved column physical settings to stderr.

Tree-model / JSON / programmatic writes → C++ SDK `cpp/examples/cpp_examples/demo_write.cpp`
(`TsFileTableWriter`/`TsFileWriter` + `Tablet`); Java/Python writers under `java/`, `python/`.

## Example workflow

Set `B` to the discovered executable. Use new output paths for this example.

```sh
B=tsfile-cli
printf 'time,site,temperature,humidity\n0,north,21.5,40\n1,north,21.7,41\n' \
  | "$B" write --table sensors --tag site STRING \
      --field temperature DOUBLE --field humidity INT32 --stdin -o sensors.tsfile
"$B" ls -f ndjson sensors.tsfile
"$B" meta -f ndjson sensors.tsfile
"$B" schema -t sensors -f csv sensors.tsfile
"$B" stats -t sensors -m temperature -f csv sensors.tsfile
"$B" count -t sensors -f csv sensors.tsfile
"$B" head -t sensors -m temperature -m humidity -n 20 -f csv sensors.tsfile
"$B" cat -t sensors --tag-filter site eq north -m temperature -f ndjson sensors.tsfile
"$B" export -t sensors --type csv -o sensors.csv sensors.tsfile
"$B" sketch -o layout.txt sensors.tsfile
```

## Exit status

- `0`: success; `1`: usage/parameter error; `2`: TsFile/CSV input problem;
  `3`: query/runtime, target, or write/commit failure.
- On nonzero exit, produced stdout or files are not complete results.
- Shell redirection (`cat > file`) is not atomic; use `export` for atomic
  single-object output.
