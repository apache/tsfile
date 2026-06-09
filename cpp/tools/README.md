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

# tsfile-cli — TsFile Command-Line Tool

`tsfile-cli` is a single, pipe-friendly C++ command-line tool for inspecting **and**
importing Apache TsFile (`.tsfile`) files from the shell — the TsFile analogue of
`parquet-cli` / `pqrs`. Read commands print data to **stdout** and diagnostics to
**stderr**, so they compose with `awk`, `jq`, `sort`, and friends; the `write` command
imports CSV/TSV into a new `.tsfile`. It is built on the public `storage::TsFileReader`
and `storage::TsFileTableWriter` APIs and does not modify the storage engine.

## Building from source

The CLI is part of the C++ module and is built by default (CMake option `BUILD_TOOLS=ON`).
The CMake target is `tsfile_cli`; the produced executable is named `tsfile-cli`.

**Prerequisites:** a C++11 compiler (GCC / Clang / MSVC) and CMake ≥ 3.11. The third-party
dependencies (ANTLR4, Snappy, LZ4, LZOKAY, Zlib, GoogleTest) are bundled under
`cpp/third_party/` and built automatically — no separate install step needed.

Choose any one of the following.

**1. Build script (recommended).** From `cpp/`:

```bash
bash build.sh -t=Debug      # -> cpp/build/Debug/bin/tsfile-cli
bash build.sh               # Release (default) -> cpp/build/Release/bin/tsfile-cli
```

**2. Maven (builds the whole C++ module).** From the repository root:

```bash
./mvnw clean package -P with-cpp   # -> cpp/target/build/bin/tsfile-cli
```

**3. Plain CMake.** From `cpp/`:

```bash
mkdir -p build/Debug && cd build/Debug
cmake ../.. -DCMAKE_BUILD_TYPE=Debug
make -j tsfile_cli                 # -> build/Debug/bin/tsfile-cli
```

> **CMake 4.x note.** The bundled ANTLR4 runtime sets old CMake policies that CMake 4
> rejects (`Policy CMP00xx may not be set to OLD behavior`). The reader and CLI do not use
> ANTLR4, so disable it — `--disable-antlr4` for the build script, or `-DENABLE_ANTLR4=OFF`
> for plain CMake:
>
> ```bash
> bash build.sh -t=Debug --disable-antlr4
> ```

Verify the binary:

```bash
./build/Debug/bin/tsfile-cli --version    # -> tsfile-cli (Apache TsFile C++) <version>
./build/Debug/bin/tsfile-cli --help
```

The executable links the `tsfile` shared library built alongside it. To run it from
anywhere, either run it in place by its full path, or use CMake's install step
(`cmake --install .` / `make install`), which installs the binary to `<prefix>/bin` and
`libtsfile` to `<prefix>/lib`.

## Usage

```
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
| `cat` | All matching rows, streamed |
| `sample` | Reproducible reservoir sample (default 10; `-n`, `--seed`) |

The metadata commands (`ls` / `schema` / `meta` / `stats` / `count`) answer most questions
without decoding data pages.

Shared options:

| Option | Meaning |
|---|---|
| `-f, --format csv\|tsv\|json\|table` | Output format; defaults to `table` on a TTY, `tsv` when piped |
| `-d, --device <id>` / `-t, --table <name>` | Scope to one device / table (mutually exclusive) |
| `-m, --measurements a,b,c` | Column projection (`schema`, `head`, `cat`, `sample`) |
| `-n, --limit N` / `--offset N` | Max rows / rows to skip (`head`, `cat`; `--offset` not valid for `sample`) |
| `--start <ms>` / `--end <ms>` | Inclusive epoch-millisecond time range (`head`, `cat`, `sample`) |
| `--seed N` | Reproducible sampling seed (`sample` only) |
| `--no-header` | Omit the header row |
| `--model tree\|table` | Force the model (otherwise auto-detected) |

`json` output is NDJSON (one object per line; numbers/booleans bare, other values quoted,
nulls as `null`). CSV output follows RFC 4180. Timestamps are raw epoch milliseconds.

```bash
BIN=cpp/build/Debug/bin/tsfile-cli
$BIN ls -f tsv data.tsfile                          # list tables / devices
$BIN meta data.tsfile                               # quick file overview
$BIN count -t table1 -f tsv data.tsfile             # row counts, no page scan
$BIN cat -m temp,humidity --start 1700000000000 -f csv data.tsfile | head
$BIN sample -m temp -n 20 --seed 42 -f json data.tsfile | jq .
```

### Writing (import)

`tsfile-cli write` imports CSV/TSV rows into a **new table-model** `.tsfile` (the output is
overwritten). The first input column is the timestamp (epoch milliseconds); the remaining
columns are declared explicitly with `--columns` — there is no type inference.

Timestamps must be **strictly increasing per device**, where a device is identified by its
`tag` column values (rows that share the same tags form one device's timeline). Rows for
different tag combinations may freely interleave and reuse timestamps. Out-of-order input is
rejected with the offending line number, and a failed import leaves no output file behind.
`--output` must differ from the input file.

```
tsfile-cli write --table <name> --columns <spec> -o <out.tsfile> \
                 [-f csv|tsv] [--no-header] [--header-match] [-v] [<input> | -]
```

`--columns` is a comma-separated list of `name:TYPE:category`, where `category` (case-insensitive)
is `tag` or `field` and `TYPE` (case-insensitive) is one of `BOOLEAN, INT32, INT64, FLOAT, DOUBLE,
STRING, TEXT, TIMESTAMP, DATE, BLOB` — for example `--columns "id1:STRING:tag,s1:INT64:field"`.
`DATE` cells are written as `YYYY-MM-DD`; `TIMESTAMP` cells as epoch milliseconds. Each column is
stored with the engine's default encoding and compression for its type.

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

An empty cell is written as null. The command is silent on success (Unix-style); pass `-v`
for a one-line summary.

```bash
# round-trip through a pipe
printf 'time,id1,s1\n0,dev,0\n1,dev,10\n' \
  | tsfile-cli write --table t1 --columns "id1:STRING:tag,s1:INT64:field" -o out.tsfile -
tsfile-cli count -f tsv out.tsfile          # -> t1.dev  s1  2
```

For tree-model writes, JSON input, or programmatic use, use the C++ SDK directly — see
`cpp/examples/cpp_examples/demo_write.cpp` (`TsFileTableWriter` / `TsFileWriter` + `Tablet`).

## Using the skill with an AI assistant

`cpp/tools/skills/tsfile-cli/SKILL.md` is a machine-readable reference that teaches AI
coding assistants (e.g. Claude Code) how to drive `tsfile-cli` correctly. Such assistants
auto-discover skills from a `.claude/skills/` directory at session start, so "installing"
the skill just means placing it there — either project-level or user-level:

```bash
# project-level (this repository only)
mkdir -p .claude/skills/tsfile-cli
cp cpp/tools/skills/tsfile-cli/SKILL.md .claude/skills/tsfile-cli/SKILL.md

# or user-level (available in all your projects)
mkdir -p ~/.claude/skills/tsfile-cli
cp cpp/tools/skills/tsfile-cli/SKILL.md ~/.claude/skills/tsfile-cli/SKILL.md
```

> The installed `SKILL.md` must begin with its YAML front-matter (`--- … ---`) for the
> assistant to detect it. The in-repo copy carries an Apache license header comment above
> the front-matter; if discovery fails, delete that leading `<!-- … -->` block from the
> installed copy so `---` is the first line.

Start a new assistant session afterward. The skill then activates automatically when you
ask to inspect or import a `.tsfile`; you can also invoke it explicitly (e.g. "use the
tsfile-cli skill").

## Source layout

```text
cpp/tools/
├── tools_main.cc          # main(): forwards argv to run_cli
├── cli/                   # argument parsing, top-level dispatch, exit codes
├── format/                # csv/tsv/json/table output + CSV/TSV input parsing
├── commands/              # one file per command + shared row-query / statistics helpers
└── skills/tsfile-cli/     # model-facing skill reference (for AI assistants)
```
