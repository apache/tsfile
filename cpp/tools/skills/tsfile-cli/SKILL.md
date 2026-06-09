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

---
name: tsfile-cli
description: Use when you need to inspect, preview, export, OR import an Apache TsFile (.tsfile) from the command line — list devices/tables, dump schema, read file/series metadata, count rows, sample/preview rows, or write CSV/TSV into a new .tsfile — via the project's C++ `tsfile-cli` in cpp/tools.
---

# tsfile-cli

Single pipe-friendly C++ binary to inspect **and** import `.tsfile` (TsFile's analogue of
`parquet-cli`/`pqrs`). Source `cpp/tools/`. Read data → stdout, diagnostics → stderr;
`write` imports CSV/TSV → a new file.

## Binary

- Name `tsfile-cli` (CMake target `tsfile_cli`). Find: `ls cpp/build/*/bin/tsfile-cli`.
- Build only if missing: `cd cpp && bash build.sh -t=Debug`.
- CMake ≥4 aborts on bundled ANTLR4 (`Policy CMP00xx ... OLD`) → add `--disable-antlr4`
  (reader/CLI don't use ANTLR4).

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
| `cat` | all matching rows (streamed) | yes |
| `sample` | reservoir sample (default 10, `-n` + `--seed`) | yes |

Prefer no-scan verbs (`ls/schema/meta/stats/count`) — cheap and never hit the page-decode caveat.

Table model + row verbs (`head/cat/sample/count`): without `-t`, only the **first** table is queried. Pass `-t <table>` to target a specific one.

```
opts: -f csv|tsv|json|table  (default TTY→table, pipe→tsv)
      -d <device> | -t <table>   (mutually exclusive)
      -m a,b,c (projection) · -n N · --offset N · --start <ms> · --end <ms> (inclusive)
      --seed N · --no-header · --model tree|table (else auto)
applies: -m → schema/head/cat/sample · -d/-t → row cmds/schema/stats/count · --offset ∉ sample
json=NDJSON (num/bool bare, else quoted, null→null) · csv=RFC4180 · ts=raw epoch ms
exit: 0 ok · 1 usage · 2 file open/corrupt · 3 query/runtime
```

```sh
B=cpp/build/Debug/bin/tsfile-cli
$B meta data.tsfile; $B count -t table1 -f tsv data.tsfile
$B cat -m temp --start 1700000000000 -f csv data.tsfile 2>/dev/null | head
```

## Write

`tsfile-cli write --table <name> --columns <spec> -o <out> [-f csv|tsv] [--no-header] [--header-match] [-v] [<input> | -]`

Imports rows into a **new table-model** file (overwritten). Input col 0 = timestamp
(epoch ms, int); remaining cols declared by `--columns` — **no type inference**.

```
spec  := col (',' col)*
col   := name ':' TYPE ':' ('tag' | 'field')          # TYPE + category case-insensitive
TYPE  ∈ { BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, TIMESTAMP, DATE, BLOB }
input := file | '-' | omitted                          # '-' or omitted = stdin
```

- `-o` required (overwritten, must differ from input); `-f` default csv (json/table → usage error).
- header: first line skipped by default · `--no-header` if none · `--header-match` validates
  header names vs `--columns` (mutually exclusive with `--no-header`).
- empty cell = null · `--table` is lower-cased · `DATE` cells are `YYYY-MM-DD`, `TIMESTAMP` epoch ms · each column stored with the engine default encoding/compression for its type · success **silent**, `-v` → echoes the resolved config + `wrote N rows to <out>` on stderr.
- **timestamps must be strictly increasing per device** (device = tag-column values); rows for
  different tags may interleave/reuse timestamps. Out-of-order input → error with line number.
- a failed import deletes its partial output (no half-written `.tsfile` left behind).
- exit: `1` usage (missing `--table`/`--columns`/`-o`, bad spec, dup column, read-only flag) · `2` IO open · `3` row (field-count / type / overflow / timestamp-order / header mismatch).

```sh
printf 'time,id1,s1\n0,dev,0\n1,dev,10\n' \
  | tsfile-cli write --table t1 --columns "id1:STRING:tag,s1:INT64:field" -o out.tsfile -
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
