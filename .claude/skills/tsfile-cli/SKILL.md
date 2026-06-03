---
name: tsfile-cli
description: Use when you need to inspect, preview, export, OR import an Apache TsFile (.tsfile) from the command line — listing devices/tables, dumping schema, reading file/series metadata, counting rows, sampling/previewing rows, or writing CSV/TSV rows into a new .tsfile — via the project's C++ `tsfile-cli` in cpp/tools.
---

# tsfile CLI

## Overview

`tsfile-cli` is a single, pipe-friendly C++ binary for inspecting **and** importing
`.tsfile` data without writing reader/writer code — the TsFile analogue of
`parquet-cli`/`pqrs`. Source: `cpp/tools/`. Read commands send data to **stdout** and
diagnostics to **stderr** (so they compose with `awk`, `jq`, `sort`); the `write` command
imports CSV/TSV into a new `.tsfile` (see **Writing** below).

## Locating / building the binary

The executable is named **`tsfile-cli`** (the CMake *target* is `tsfile_cli`). Look first,
build only if missing:

```sh
ls cpp/build/*/bin/tsfile-cli         # prebuilt? e.g. cpp/build/Debug/bin/tsfile-cli
cd cpp && bash build.sh -t=Debug      # build if absent (binary in build/Debug/bin/tsfile-cli)
```

If CMake ≥ 4 aborts configuring the bundled ANTLR4 (`Policy CMP00xx may not be set to
OLD`), add `--disable-antlr4` — the reader and CLI don't use ANTLR4:

```sh
cd cpp && bash build.sh -t=Debug --disable-antlr4
```

## Commands

```
tsfile-cli <command> [options] <file.tsfile>
tsfile-cli --help | --version | help <command>
```

| Command | Output | Scans data pages? |
|---|---|---|
| `ls` | one device (tree model) or table (table model) per line | no |
| `schema` | `target, measurement, datatype, encoding, compression` | no |
| `meta` | file summary: model, version, device/table/series counts, time range, bloom, size | no |
| `stats` | per-series `count, start_time, end_time, min, max, first, last, sum` | no |
| `count` | per-series row counts + `total` row (from statistics) | no |
| `head` | first N rows (default 10, `-n`) | yes |
| `cat` | all matching rows (streamed) | yes |
| `sample` | reproducible reservoir sample (default 10, `-n` + `--seed`) | yes |

Use the no-scan metadata verbs (`ls`/`schema`/`meta`/`stats`/`count`) first — they answer
most inspection questions cheaply and reliably.

## Shared options

| Option | Meaning | Applies to |
|---|---|---|
| `-f, --format csv\|tsv\|json\|table` | output format; auto = `table` on a TTY, `tsv` when piped | all |
| `-d, --device <id>` / `-t, --table <name>` | scope to one device / table (mutually exclusive) | row cmds, `schema`, `stats`, `count` |
| `-m, --measurements a,b,c` | column projection | `schema`, `head`, `cat`, `sample` |
| `-n, --limit N` / `--offset N` | row cap / skip (`--offset` invalid for `sample`) | `head`, `cat`, (`--offset`: not `sample`) |
| `--start <ms>` / `--end <ms>` | inclusive epoch-millisecond time range | `head`, `cat`, `sample` |
| `--seed N` | reproducible sampling seed (only valid for `sample`) | `sample` |
| `--no-header`, `--model tree\|table` | suppress header; force model (else auto-detected) | all |

`json` is NDJSON (one object per line); numbers/booleans bare, others quoted, `null` as
`null`. CSV follows RFC 4180. Timestamps are raw epoch milliseconds.

Exit codes: `0` ok · `1` usage/argument error · `2` file open/corrupt · `3` query/runtime.

## Examples

```sh
BIN=cpp/build/Debug/bin/tsfile-cli
$BIN ls -f tsv data.tsfile                          # namespaces, one per line
$BIN meta data.tsfile                               # quick file overview
$BIN count -t table1 -f tsv data.tsfile             # row counts, no page scan
$BIN cat -m temp,humidity --start 1700000000000 -f csv data.tsfile | head
$BIN sample -m temp -n 20 --seed 42 -f json data.tsfile | jq .
$BIN cat -f csv data.tsfile 2>/dev/null | awk -F, 'NR>1{n++} END{print n}'
```

## Known caveats

- **Row commands can abort on some files.** `head`/`cat`/`sample` decode data pages and
  may hit a reader assertion (`decode_cur_time_page_data`, `aligned_chunk_reader.cc`,
  exit 134) on certain aligned files — including the bundled `cpp/examples/test_cpp.tsfile`.
  This is a storage-engine/file issue, not a CLI bug; the metadata verbs still work on
  such files. For row data, use a well-formed file (e.g. one you wrote yourself).
- **Garbled `target` for table model.** A table-model device id is built from tag-column
  bytes, so `stats`/`count`/`schema` may print non-printable characters in `target`.
- **`schema` can list more columns than `meta`/`stats`/`count` report as series.** Tag/id
  columns show up in `schema` but aren't always counted as field series, so `series_count`
  and the `stats`/`count` rows may be fewer than the `schema` rows — not a discrepancy bug.
- **Build needs `--disable-antlr4` on CMake ≥ 4** (see above).

## Writing (`write`): import CSV/TSV → tsfile

`tsfile-cli write` imports rows into a **new table-model** `.tsfile` (output is overwritten).
The first input column is the timestamp (epoch ms); the rest are declared explicitly with
`--columns` — **no type inference**.

```
tsfile-cli write --table <name> --columns <spec> -o <out.tsfile> \
                 [-f csv|tsv] [--no-header] [--header-match] [-v] [<input> | -]
```

| Option | Meaning |
|---|---|
| `--table <name>` | output table name (lower-cased) |
| `--columns "id1:STRING:tag,s1:INT64:field"` | ordered data columns; category `tag\|field`; type ∈ BOOLEAN/INT32/INT64/FLOAT/DOUBLE/STRING/TEXT (case-insensitive) |
| `-o, --output <path>` | output `.tsfile` (required, overwritten) |
| `<input>` / `-` | input file, or `-`/omitted = **stdin** |
| `-f csv\|tsv` | input delimiter (default csv; `json`/`table` rejected) |
| `--no-header` / `--header-match` | input has no header / validate header names vs `--columns` |
| `-v, --verbose` | print `wrote N rows to <out>` to stderr (else **silent on success**) |

Empty cell = null. Exit codes: `1` usage (missing `--table`/`--columns`/`-o`, bad
`--columns`, read-only flags), `2` input/output open fail, `3` bad row (field count / type
/ header mismatch).

```sh
printf 'time,id1,s1\n0,dev,0\n1,dev,10\n' \
  | tsfile-cli write --table t1 --columns "id1:STRING:tag,s1:INT64:field" -o out.tsfile -
tsfile-cli count -f tsv out.tsfile          # -> t1.dev  s1  2
```

For **tree-model** writes, JSON input, or programmatic use, use the C++ SDK —
`cpp/examples/cpp_examples/demo_write.cpp` (`TsFileTableWriter`/`TsFileWriter` + `Tablet`);
Java/Python writers live under `java/`, `python/`.
