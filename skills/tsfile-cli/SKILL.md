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
  preview, export, or sample an Apache TsFile; report metadata or per-series
  counts; or use its explicit single-table CSV write command.
---

# tsfile-cli

Single pipe-friendly C++ binary to inspect `.tsfile` files and create a new
table-model TsFile from CSV (TsFile's analogue of `parquet-cli`/`pqrs`).
Source `cpp/tools/`. Read data → stdout, diagnostics → stderr.

## Scope

Use this skill only for the C++ `tsfile-cli` binary. For Java, Python, C++, or C
SDK integration, schema design, Java CSV/Parquet/Arrow import, Java table
point-count metadata checks or backfill, and programmatic tree-model writes,
load the sibling `tsfile` skill at `../tsfile/SKILL.md`.

The names overlap but the semantics do not: `tsfile-cli count` is a read-only
per-series report, not the Java table point-count property tool. `tsfile-cli
write` is the C++ binary's narrow one-file/stream, one-table CSV import; it
does not replace the Java batch and format-aware import tools.

## Binary

- Name `tsfile-cli` (CMake target `tsfile_cli`). Find: `ls cpp/build/*/bin/tsfile-cli`.
- Build only if missing: `cd cpp && bash build.sh -t=Debug`.

## Read

`tsfile-cli <cmd> [opts] <file.tsfile>` · `tsfile-cli --help | --version | help`

| cmd | output | scans pages |
|---|---|---|
| `ls` | device (tree) / table (table) per line | no |
| `schema` | `target,measurement,datatype,encoding,compression` | no |
| `meta` | model, device/table/series counts, time range, size | no |
| `stats` | per-series `count,start,end,min,max,first,last,sum` | no |
| `count` | per-series counts + `total` row | no |
| `head` | first N rows (default 10, `-n`) | yes |
| `cat` | all matching rows (streamed; `table` format buffers) | yes |
| `sample` | reservoir sample (default 10, `-n` + `--seed`) | yes |

Prefer no-scan verbs (`ls/schema/meta/stats/count`) — cheap and never hit the page-decode caveat.

Table model + row verbs (`head/cat/sample`): without `-t`, only the **first** table is queried. Pass `-t <table>` to target a specific one (`count` covers all tables).

```
opts: -f csv|tsv|json|table  (default TTY→table, pipe→tsv)
      -d <device> | -t <table>   (mutually exclusive)
      -m a,b,c (projection) · -n N · --offset N · --start <ms> · --end <ms> (inclusive)
      --tag-filter C OP V · --tag-between C L U · --tag-not-between C L U (table TAG predicates)
      --seed N · --no-header · --model tree|table (else auto)
applies: -m → schema/stats/count/head/cat/sample · -d/-t → row cmds/schema/stats/count
         (-d needs tree model, -t needs table model in head/cat/sample/schema) · --offset ∉ sample
         tag filters → head/cat/sample table model; OP=eq|neq|lt|lteq|gt|gteq|regexp|not-regexp
json=NDJSON (num/bool bare, else quoted, null→null, NaN/Inf→null) · csv=RFC4180 · ts=raw epoch ms
exit: 0 ok · 1 usage · 2 file open/corrupt · 3 query/runtime
```

The aligned `table` format buffers rows. Prefer `csv`, `tsv`, or `json` for
large dumps and pipelines.

```sh
B=cpp/build/Debug/bin/tsfile-cli
$B meta data.tsfile; $B count -t table1 -f tsv data.tsfile
$B cat -t table1 --tag-filter device eq dev_1 -m temp -f tsv data.tsfile
$B cat -m temp --start 1700000000000 -f csv data.tsfile 2>/dev/null | head
```

## Write

`tsfile-cli write --table <name> (--tag <name> STRING)* (--field <name> <TYPE>)+ (-i <input.csv>|--stdin) -o <out.tsfile> [-v]`

Imports rows into a **new table-model** file. The target must not already exist.
Input is strict CSV with a required `time` header; all other columns are declared
explicitly by `--tag` and `--field` — **no type inference**.

```
TYPE  ∈ { BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, TIMESTAMP, DATE, BLOB }
```

- `--tag` declarations must use `STRING`; at least one `--field` is required.
- The CSV header must contain `time` and exactly the declared TAG/FIELD names.
- `--encoding` and `--compression` may override the bound defaults by data type.
- `--stdin` or `-i <input.csv>` is required; TSV, `--columns`, `--no-header`, and
  `--header-match` are not supported.
- Empty cells use the CSV null spelling; `DATE` cells are `YYYY-MM-DD`, and
  `TIMESTAMP`/`time` use strict decimal int64 values.
- A failed import leaves no partial output; success is silent unless `-v` is used.
- **timestamps must be strictly increasing per device** (device = tag-column values); rows for
  different tags may interleave/reuse timestamps. Out-of-order input → error with line number.
- a failed import deletes its partial output (no half-written `.tsfile` left behind).
- exit: `1` usage/parameter error · `2` CSV/input problem · `3` target/write/commit failure.

```sh
printf 'time,id1,s1\n0,dev,0\n1,dev,10\n' \
  | tsfile-cli write --table t1 --tag id1 STRING --field s1 INT64 --stdin -o out.tsfile
tsfile-cli count -f tsv out.tsfile        # -> t1.dev  s1  2
```

Tree-model / JSON / programmatic writes → C++ SDK `cpp/examples/cpp_examples/demo_write.cpp`
(`TsFileTableWriter`/`TsFileWriter` + `Tablet`); Java/Python writers under `java/`, `python/`.

## Caveats

- `head`/`cat`/`sample` decode pages → may abort (`decode_cur_time_page_data`, exit 134) on
  some aligned files incl. bundled `cpp/examples/test_cpp.tsfile`. Storage-engine/file issue,
  not a CLI bug; metadata verbs still work. Use a well-formed (e.g. self-written) file for rows.
- table-model `target` is derived from tag bytes → may show non-printable chars in `stats/count/schema`.
- `schema` lists all columns; `meta/stats/count` count only field series → `series_count` can be
  fewer than `schema` rows (not a bug).
