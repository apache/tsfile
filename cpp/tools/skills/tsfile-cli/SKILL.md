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

Reference files:

- `references/commands.md`: command syntax and result fields.
- `references/errors.md`: exit-code and failure-handling rules.
- `references/examples.md`: reviewed command recipes.

## Scope

Use this skill only for the C++ `tsfile-cli` binary. Route Java
`csv2tsfile`/`parquet2tsfile`/`arrow2tsfile` and table point-count metadata
checks or backfill to the top-level `tsfile` skill when it is installed.

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
| `stats` | FIELD statistics with null counts and source | maybe |
| `count` | object/column counts; no summary row | maybe |
| `head` | first N rows (default 10, `-n`) | yes |
| `cat` | all matching rows (disk-staged) | yes |
| `sketch` | physical layout text from the bound `printSketch` behavior | no FIELD decode |
| `export` | writes one object file or a numbered multi-object directory | yes |

Inspect `ls`, `schema`, and `meta` before querying rows to identify the model,
objects, and columns. `stats` can scan when statistics are unavailable, and
table-model `count` scans rows; neither is guaranteed to be metadata-only.

For `head` and `cat`, omit `-d/-t` only when the selected model has exactly one
accessible object. Multi-object files require explicit scope. `schema`,
`stats`, and `count` visit every object when scope is omitted. `export` always
requires explicit scope.

```
opts: -f table|ndjson|csv  (default table, including pipes)
      -d <device> | -t <table>   (mutually exclusive)
      -m <column> repeat for projection or metadata filtering; no comma lists
      -n N · --offset N · --start <int64> · --end <int64> for head/cat/export
      --tag-filter C OP [V] where OP=eq|neq|regexp|is-null|not-null
      --tag-match all|any when two or more tag filters are present
ndjson=one object per line; INT64/TIMESTAMP strings, NULL/NaN/Inf→null · csv=RFC 4180 with header
exit: 0 ok · 1 usage · 2 file open/corrupt · 3 query/runtime
```

```sh
B=cpp/build/Debug/bin/tsfile-cli
$B meta data.tsfile; $B count -t table1 -f csv data.tsfile
$B cat -t table1 --tag-filter device eq dev_1 -m temp -f ndjson data.tsfile
$B head -t table1 -m temp --start 1700000000000 -n 10 -f csv data.tsfile
```

## Write

`tsfile-cli write --table <name> (--tag <name> STRING)* (--field <name> <TYPE>)+ [--encoding <TYPE> <ENC>] [--compression <TYPE> <COMP>] (-i <input.csv>|--stdin) -o <out.tsfile> [-v]`

Creates a new single-table, table-model file from strict CSV. The target must
not already exist; the command never creates tree-model TsFiles and never
modifies an existing TsFile.

```
TYPE  ∈ { BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, TIMESTAMP, DATE, BLOB }
ENC   ∈ bound TsFile encodings such as PLAIN, TS_2DIFF, GORILLA, DICTIONARY
COMP  ∈ UNCOMPRESSED, SNAPPY, GZIP, LZO, LZ4, ZSTD, LZMA2
```

- CSV must contain a unique header with `time` and exactly the declared TAG/FIELD names; rows are mapped by header name, not physical column order.
- `--encoding` and `--compression` are keyed by declared data type and apply to all declared columns of that type.
- Encoding overrides follow the SDK encoder/decoder factories, including ALP
  and GORILLA_V1 for FLOAT/DOUBLE, and RLE for BOOLEAN/FLOAT/DOUBLE.
  FLOAT/DOUBLE RLE uses the SDK's two-decimal scaling and may be lossy.
  ALP is C++/Python-only, not Java-compatible. LZMA2 requires an enabled build.
- CSV null and backslash rules: see `references/commands.md` before preparing input.
- `DATE` cells are `YYYY-MM-DD`; `TIMESTAMP` and row `time` use strict decimal int64.
- **timestamps must be strictly increasing per device** (device = tag-column values);
  different tags may interleave/reuse timestamps. The CLI has no duplicate-time
  opt-in; out-of-order input reports its line number.
- A failed import deletes its partial output; success is silent unless `-v` is used.
- `-v` writes one post-commit summary to stderr: `created model=table object=<table> rows=<n> output=<path>` plus one line per column with resolved physical settings.
- exit: `1` usage/parameter error · `2` CSV/input problem · `3` target/write/commit failure.

```sh
printf 'time,id1,s1\n0,dev,0\n1,dev,10\n' \
  | tsfile-cli write --table t1 --tag id1 STRING --field s1 INT64 --stdin -o out.tsfile
tsfile-cli count -f csv out.tsfile
```

Tree-model / JSON / programmatic writes → C++ SDK `cpp/examples/cpp_examples/demo_write.cpp`
(`TsFileTableWriter`/`TsFileWriter` + `Tablet`); Java/Python writers under `java/`, `python/`.

## Caveats

- Row output uses bounded buffers: stdout is disk-staged until reads succeed;
  `export` streams to an atomic temporary target. Allow disk space for the result.
  Aligned `table` output also spools. Nonzero exit means incomplete results.
- Table results always include all TAG columns before selected FIELD columns.
- `sketch` follows the bound TsFile `printSketch` text and does not support `-f`.
