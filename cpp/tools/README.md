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
imports CSV into a new `.tsfile`. It is built on the public `storage::TsFileReader`
and `storage::TsFileTableWriter` APIs and does not modify the storage engine.

## Building from source

The CLI is part of the C++ module and is built by default (CMake option `BUILD_TOOLS=ON`).
The CMake target is `tsfile_cli`; the produced executable is named `tsfile-cli`.

**Prerequisites:** a C++11 compiler (GCC / Clang / MSVC) and CMake ≥ 3.11. The
build resolves third-party dependencies automatically from compatible system
packages, verified source archives, or the remaining repository copies. No
separate install step is needed with the default dependency policy.

Choose any one of the following.

**1. Build script (recommended).** From `cpp/`:

```bash
bash build.sh -t=Debug      # -> cpp/build/Debug/bin/tsfile-cli
bash build.sh               # Release (default) -> cpp/build/Release/bin/tsfile-cli
bash build.sh install       # Release build, then run make install
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

Verify the binary:

```bash
./build/Debug/bin/tsfile-cli --version    # -> tsfile-cli (Apache TsFile C++) <version>
./build/Debug/bin/tsfile-cli --help
```

The executable links the `tsfile` shared library built alongside it. To run it from
anywhere, either run it in place by its full path, or explicitly install it with
`bash build.sh install`, `cmake --install .`, or `make install`. The install step places
the binary under `<prefix>/bin` and `libtsfile` under `<prefix>/lib`. The build script
does not install by default.

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
| `ls` | List selected-model objects as `model, object` rows |
| `schema` | List schema rows for devices or tables |
| `meta` | File summary: `size_bytes`, `format_version`, and `model` |
| `stats` | FIELD statistics with counts, null counts, time range, values, and source |
| `count` | Object/column counts; no synthetic summary row |
| `sketch` | Print the physical file sketch, optionally to `-o` |
| `head` | First N rows (default 10; use `-n`) |
| `cat` | All matching rows, streamed (`table` format buffers to align columns) |
| `export` | Export one object to `-o`, or multiple objects to `--output-dir`, using `--type` |

The metadata commands (`ls` / `schema` / `meta` / `stats` / `count`) answer most questions
without decoding data pages.

Shared options:

| Option | Meaning |
|---|---|
| `-f, --format table\|ndjson\|csv` | Output format; defaults to `table` |
| `-d, --device <id>` / `-t, --table <name>` | Scope to one device / table (mutually exclusive) |
| `-m, --measurements <name>` | Column projection; repeat once per column. For `stats`, only FIELD columns are valid |
| `-n, --limit N` / `--offset N` | Max rows / rows to skip (`head`, `cat`, `export`) |
| `--start <time>` / `--end <time>` | Inclusive raw int64 timestamp range (`head`, `cat`, `export`) |
| `--tag-filter C OP [V]` | Table TAG predicate for row reads; `OP` is `eq`, `neq`, `regexp`, `is-null`, or `not-null` |
| `--tag-match all\|any` | Required when more than one `--tag-filter` is supplied |

`ndjson` output emits one JSON object per line; numbers/booleans are bare, other values are
quoted, nulls are `null`, and non-finite floats become `null`. CSV output follows RFC 4180.
CSV nulls are unquoted `\N`; empty strings are quoted as `""`. Timestamps are raw int64
values. The `table` format uses a temporary spool to align columns with bounded
memory; prefer `csv`/`ndjson` when temporary disk use is undesirable. `sketch`
does not accept `--format`.

```bash
BIN=cpp/build/Debug/bin/tsfile-cli
$BIN ls -f csv data.tsfile                          # list tables / devices
$BIN meta data.tsfile                               # quick file overview
$BIN count -t table1 -f csv data.tsfile             # exact row/column counts
$BIN cat -t table1 --tag-filter device eq dev_1 -m temp -f csv data.tsfile
$BIN cat -m temp -m humidity --start 1700000000000 -f csv data.tsfile | head
$BIN export -t table1 --type csv -o table1.csv data.tsfile
```

### Writing (import)

`tsfile-cli write` imports strict CSV rows into a **new table-model** `.tsfile`.
It never creates a tree-model file. The CSV must contain a unique header with the
reserved `time` column plus exactly the TAG and FIELD names declared on the command line.
Data rows are mapped by header name, not by physical column order. There is no type
inference.

Timestamps must be **strictly increasing per device**, where a device is identified by its
`tag` column values (rows that share the same tags form one device's timeline). Rows for
different tag combinations may freely interleave and reuse timestamps. Out-of-order input is
rejected with the offending line number, and a failed import leaves no output file behind.
`--output` must not already exist and must differ from the input file.

```
tsfile-cli write --table <name> [--tag <name> STRING]... --field <name> <TYPE>... \
                 -o <out.tsfile> (--input <csv> | --stdin) \
                 [--encoding <TYPE> <ENC>]... [--compression <TYPE> <COMP>]... [-v]
```

`TYPE` is one of `BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, TIMESTAMP, DATE, BLOB`.
`DATE` cells are written as `YYYY-MM-DD`; `TIMESTAMP` cells as raw int64 timestamps.
By default each column uses the engine's default encoding and compression for its type.
`--encoding` and `--compression` override by canonical data type, applying to every declared
TAG/FIELD of that type; they do not target individual columns and do not affect `time`.

| Option | Meaning |
|---|---|
| `--table <name>` | Output table name (lower-cased) |
| `--tag <name> STRING` | Ordered TAG column; may be repeated |
| `--field <name> <TYPE>` | Ordered FIELD column; may be repeated |
| `--encoding <TYPE> <ENC>` | Override encoding for all declared columns of the data type |
| `--compression <TYPE> <COMP>` | Override compression for all declared columns of the data type |
| `-o, --output <path>` | Output `.tsfile` (required; must not already exist) |
| `-i, --input <path>` / `--stdin` | Choose exactly one CSV input source |
| `-v, --verbose` | Print a creation summary to stderr after commit (otherwise silent on success) |

CSV input uses RFC 4180 quoting with comma separators. A null value is unquoted `\N`;
an empty string is `""`. Header errors, unused or duplicate physical overrides, unknown
types, and incompatible encodings fail before data rows are read. Target-file problems
such as an existing output, a missing parent directory, or output equal to input return
exit code `3`.

The command is silent on success (Unix-style). With `-v`, it prints a post-commit summary
and the effective physical settings for each declared column.

```bash
# round-trip through a pipe
printf 'time,id1,s1\n0,dev,0\n1,dev,10\n' \
  | tsfile-cli write --table t1 --tag id1 STRING --field s1 INT64 -o out.tsfile --stdin
tsfile-cli count -f csv out.tsfile          # -> model,object,column,category,...
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
├── format/                # csv/ndjson/table output + CSV input parsing
├── commands/              # one file per command + shared row-query / statistics helpers
└── skills/tsfile-cli/     # model-facing skill reference (for AI assistants)
```
