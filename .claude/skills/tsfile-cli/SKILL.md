---
name: tsfile-cli
description: Use when you need to inspect, preview, or export an Apache TsFile (.tsfile) from the command line — listing devices/tables, dumping schema, reading file/series metadata, counting rows, or sampling/previewing rows — via the project's read-only C++ `tsfile` CLI in cpp/tools.
---

# tsfile CLI

## Overview

`tsfile` is a single, read-only, pipe-friendly C++ binary for inspecting a `.tsfile`
without writing reader code — the TsFile analogue of `parquet-cli`/`pqrs`. Source:
`cpp/tools/`. Data goes to **stdout**, diagnostics/errors to **stderr**, so it composes
with `awk`, `jq`, `sort`, etc.

It is **read-only**: there is no write/convert verb (see [Writing](#writing-a-tsfile)).

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

## Writing a TsFile

The CLI does **not** write. Produce a `.tsfile` with the C++ SDK — see
`cpp/examples/cpp_examples/demo_write.cpp` (`TsFileTableWriter` / `TsFileWriter` +
`Tablet`), then inspect the result with this CLI. Java and Python writers exist under
`java/` and `python/`.
