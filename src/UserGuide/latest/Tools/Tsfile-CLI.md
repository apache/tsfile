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
# tsfile-cli

`tsfile-cli` is a single, pipe-friendly C++ command-line tool for inspecting
**and** importing Apache TsFile (`.tsfile`) files from the shell. Read commands print data to **stdout** and
diagnostics to **stderr**, so they compose with `awk`, `jq`, `sort`, and friends;
the `write` command imports CSV/TSV into a new `.tsfile`. It is built on the
public `TsFileReader` and `TsFileTableWriter` APIs.

## Building from source

The CLI is part of the C++ module. Build it with the Maven wrapper, which
downloads a pinned CMake and compiles the whole C++ module (the `libtsfile`
shared library + the `tsfile-cli` executable) for you.

**Prerequisites:** a JDK (8+) to run Maven, and a C++11 compiler (GCC / Clang).
The third-party C++ dependencies (Snappy, LZ4, LZOKAY, Zlib, …) are bundled under
`cpp/third_party/` and built automatically.

From the repository root:

```bash
./mvnw clean package -P with-cpp
```

This produces, under `cpp/target/build/`:

| Artifact | Path |
|---|---|
| CLI executable | `cpp/target/build/bin/tsfile-cli` |
| Shared library | `cpp/target/build/lib/libtsfile.so` (Linux) — `libtsfile.dylib` on macOS |

`tsfile-cli` is dynamically linked against `libtsfile`. Run it **in place** by its
full path and it finds the library automatically:

```bash
cpp/target/build/bin/tsfile-cli --version    # -> tsfile-cli (Apache TsFile C++) <version>
cpp/target/build/bin/tsfile-cli --help
```

To run the binary from **somewhere else** (e.g. after copying it out of the build
tree), the dynamic loader must be able to find `libtsfile.so`. Either point the
loader at the build's `lib/` directory, or copy the library to a standard
location:

```bash
# point the loader at the build's lib directory (Linux; macOS uses DYLD_LIBRARY_PATH)
export LD_LIBRARY_PATH=/path/to/cpp/target/build/lib:$LD_LIBRARY_PATH

# — or — copy the library to a system library path
sudo cp cpp/target/build/lib/libtsfile.so /usr/local/lib/ && sudo ldconfig
```

## Usage

```text
tsfile-cli <command> [options] <file.tsfile>
tsfile-cli --help | --version | help
```

Exit codes: `0` success, `1` usage/argument error, `2` file open/corrupt,
`3` query/runtime error.

### Reading

| Command | Description |
|---|---|
| `ls` | List devices (tree model) or tables (table model), one name per line |
| `schema` | Per-series `target, measurement, datatype, encoding, compression` |
| `meta` | File summary: model, device/table/series counts, time range, file size |
| `stats` | Per-series `count, start_time, end_time, min, max, first, last, sum` |
| `count` | Per-series row counts plus a `total` row (from statistics, no page scan) |
| `head` | First N rows (default 10; use `-n`) |
| `cat` | All matching rows, streamed (`table` format buffers to align columns) |
| `sample` | Reproducible reservoir sample (default 10; `-n`, `--seed`) |

The metadata commands (`ls` / `schema` / `meta` / `stats` / `count`) answer most
questions **without decoding data pages**.

Shared options:

| Option | Meaning |
|---|---|
| `-f, --format csv\|tsv\|json\|table` | Output format; defaults to `table` on a TTY, `tsv` when piped |
| `-d, --device <id>` / `-t, --table <name>` | Scope to one device / table (mutually exclusive) |
| `-m, --measurements a,b,c` | Column projection (`schema`, `stats`, `count`, `head`, `cat`, `sample`) |
| `-n, --limit N` / `--offset N` | Max rows / rows to skip (`head`, `cat`; `--offset` not valid for `sample`) |
| `--start <ms>` / `--end <ms>` | Inclusive epoch-millisecond time range (`head`, `cat`, `sample`) |
| `--seed N` | Reproducible sampling seed (`sample` only) |
| `--tag-filter C OP V` / `--tag-between C L U` / `--tag-not-between C L U` | Table TAG predicate for `head`, `cat`, `sample`; `OP` is `eq`, `neq`, `lt`, `lteq`, `gt`, `gteq`, `regexp`, or `not-regexp` |
| `--no-header` | Omit the header row |
| `--model tree\|table` | Force the model (otherwise auto-detected) |

`json` output is NDJSON (one object per line; numbers/booleans bare, other values
quoted, nulls as `null`; non-finite floats — NaN/Inf — become `null`). CSV output
follows RFC 4180. Timestamps are raw epoch milliseconds. The `table` format
buffers all rows in memory to align columns, so prefer `csv`/`tsv`/`json` when
dumping large files.

```bash
BIN=cpp/build/Debug/bin/tsfile-cli
$BIN ls -f tsv data.tsfile                          # list tables / devices
$BIN meta data.tsfile                               # quick file overview
$BIN count -t table1 -f tsv data.tsfile             # row counts, no page scan
$BIN cat -t table1 --tag-filter device eq dev_1 -m temp -f tsv data.tsfile
$BIN cat -m temp,humidity --start 1700000000000 -f csv data.tsfile | head
$BIN sample -m temp -n 20 --seed 42 -f json data.tsfile | jq .
```

> For a table-model file, the row commands (`head` / `cat` / `sample`) query the
> **first** table unless you pass `-t <table>`. `count` covers all tables.

### Writing (import)

`tsfile-cli write` imports CSV/TSV rows into a **new table-model** `.tsfile` (the
output is overwritten). The first input column is the timestamp (epoch
milliseconds); the remaining columns are declared explicitly with `--columns` —
there is no type inference.

Timestamps must be **strictly increasing per device**, where a device is
identified by its `tag` column values (rows that share the same tags form one
device's timeline). Rows for different tag combinations may freely interleave and
reuse timestamps. Out-of-order input is rejected with the offending line number,
and a failed import leaves no output file behind. `--output` must differ from the
input file.

```text
tsfile-cli write --table <name> --columns <spec> -o <out.tsfile> \
                 [-f csv|tsv] [--no-header] [--header-match] [-v] [<input> | -]
```

`--columns` is a comma-separated list of `name:TYPE:category`, where `category`
(case-insensitive) is `tag` or `field` and `TYPE` (case-insensitive) is one of
`BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, TIMESTAMP, DATE, BLOB` — for
example `--columns "id1:STRING:tag,s1:INT64:field"`. `DATE` cells are written as
`YYYY-MM-DD`; `TIMESTAMP` cells as epoch milliseconds. Each column is stored with
the engine's default encoding and compression for its type.

| Option | Meaning |
|---|---|
| `--table <name>` | Output table name (lower-cased) |
| `--columns <spec>` | Ordered data columns (excludes the leading timestamp column) |
| `-o, --output <path>` | Output `.tsfile` (required; overwritten) |
| `<input>` / `-` | Input file, or `-` / omitted for stdin |
| `-f csv\|tsv` | Input delimiter (default csv; `json` / `table` are rejected) |
| `--no-header` | Input has no header row (default: first line is a header and is skipped) |
| `--header-match` | Validate header names against `--columns` |
| `-v, --verbose` | Print `wrote N rows to <out>` to stderr (otherwise silent on success) |

An empty cell is written as null. The command is silent on success (Unix-style);
pass `-v` for a one-line summary.

```bash
# round-trip through a pipe
printf 'time,id1,s1\n0,dev,0\n1,dev,10\n' \
  | tsfile-cli write --table t1 --columns "id1:STRING:tag,s1:INT64:field" -o out.tsfile -
tsfile-cli count -f tsv out.tsfile          # -> t1.dev  s1  2
```

## Using the skill with an AI assistant

`cpp/tools/skills/tsfile-cli/SKILL.md` is a machine-readable reference that
documents how to drive `tsfile-cli`. AI coding assistants that support skills can
load it to help you inspect and import `.tsfile` files.
