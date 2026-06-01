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

# TsFile Unix-philosophy C++ CLI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a single `tsfile` C++ binary with read-only, pipe-friendly verbs (`ls`, `schema`, `stats`, `head`, `cat`, `select`) for inspecting and exporting `.tsfile` files.

**Architecture:** A new `cpp/tools/` directory builds an OBJECT library (`tsfile_cli_obj`) plus a thin `main`. The library is also linked into `TsFile_Test` for unit tests. All command output goes to an injected `std::ostream&` (data→stdout, diagnostics→stderr) so commands are testable in-process. Formatting is split into a pure layer (escaping/aligning/`RowWriter`, no reader dependency, heavily unit-tested) and a `ResultSet` pump layer (e2e-tested against a generated fixture). Everything is backed by the existing `storage::TsFileReader` API — the read engine is not modified.

**Tech Stack:** C++11, CMake (≥3.11), Google Test 1.12.1, clang-format (Google style). No new third-party/runtime dependencies (argument parsing is hand-rolled).

**Spec:** `docs/superpowers/specs/2026-06-01-tsfile-unix-cli-design.md`

---

## Conventions used in every task

- **License header:** every new file (`.h`, `.cc`, `CMakeLists.txt`) starts with the Apache 2.0 header. For `.h`/`.cc` use the `/* ... */` block form copied verbatim from any existing file (e.g. `cpp/src/file/read_file.h` lines 1-18). For `CMakeLists.txt` use the `#[[ ... ]]` form (see `cpp/examples/CMakeLists.txt` lines 1-18). The code blocks below omit the header for brevity — **prepend it to each new file.**
- **Namespace:** all CLI code lives in `namespace tsfile_cli`.
- **Formatting:** run `./mvnw spotless:apply` (or `clang-format`) before each commit; the build's `-Wall` must stay clean.
- **Build/run from** `cpp/`: `bash build.sh -t=Debug` produces `build/Debug/bin/tsfile` and `build/Debug/lib/TsFile_Test`.

## File structure (created by this plan)

```
cpp/tools/
├── CMakeLists.txt                 # OBJECT lib tsfile_cli_obj + executable tsfile
├── tools_main.cc                  # main(): forwards argv to run_cli
├── cli/
│   ├── exit_codes.h               # kExitOk/kExitUsage/kExitFile/kExitRuntime
│   ├── cli_args.h / cli_args.cc   # ParsedArgs + parse_args()
│   └── run_cli.h / run_cli.cc     # top-level dispatch, reader open, error→exit mapping
├── format/
│   ├── output_format.h / .cc      # pure: resolve_format, escapes, type names, RowWriter
│   └── result_set_format.h / .cc  # ResultSet pump: cell_to_string, write_result_set
└── commands/
    ├── commands.h                 # is_table_model + cmd_* declarations
    ├── cmd_ls.cc  cmd_schema.cc  cmd_stats.cc
    └── cmd_head.cc cmd_cat.cc    cmd_select.cc

cpp/test/tools/
├── cli_test_util.h                # writes a table-model fixture .tsfile to a temp path
├── cli_args_test.cc
├── output_format_test.cc
└── command_e2e_test.cc
```

Modified files:
- `cpp/CMakeLists.txt` — add `option(BUILD_TOOLS ...)` and `add_subdirectory(tools)`.
- `cpp/test/CMakeLists.txt` — glob `tools/*_test.cc`, link `tsfile_cli_obj`.
- `cpp/src/file/read_file.cc:52-55` — route open-error prints to `stderr`.

---

## Task sequencing

Tasks are ordered so each ends green and committable:

1. CMake scaffold + `main` + `run_cli` skeleton (`--version`/`--help`)
2. `parse_args` (cli_args)
3. Pure output formatting (`output_format`)
4. `ResultSet` pump (`result_set_format`)
5. Model detection + `cmd_ls`
6. `cmd_schema`
7. `cmd_stats`
8. `cmd_head` / `cmd_cat` / `cmd_select` (row data)
9. Library stderr fix + `install()` + full-suite run + manual tree-model verification

Detailed tasks follow in separate sections of this document (one task per `###` heading). Each is self-contained: exact files, complete code, exact commands, expected output.

---

### Task 1: CMake scaffold + `main` + `run_cli` skeleton

**Files:**
- Create: `cpp/tools/cli/exit_codes.h`
- Create: `cpp/tools/cli/run_cli.h`, `cpp/tools/cli/run_cli.cc`
- Create: `cpp/tools/tools_main.cc`
- Create: `cpp/tools/CMakeLists.txt`
- Modify: `cpp/CMakeLists.txt` (add option + subdir)
- Modify: `cpp/test/CMakeLists.txt` (glob tools tests, link object lib)
- Test: `cpp/test/tools/cli_args_test.cc` (skeleton-level: version/help)

- [ ] **Step 1: Write the failing test** — `cpp/test/tools/cli_args_test.cc`

```cpp
#include <gtest/gtest.h>
#include <sstream>
#include "cli/run_cli.h"

TEST(RunCliTest, VersionFlagPrintsVersionAndReturnsOk) {
  std::ostringstream out, err;
  int code = tsfile_cli::run_cli({"--version"}, out, err);
  EXPECT_EQ(code, 0);
  EXPECT_NE(out.str().find("tsfile"), std::string::npos);
  EXPECT_TRUE(err.str().empty());
}

TEST(RunCliTest, NoArgsPrintsUsageToErrAndReturnsUsageError) {
  std::ostringstream out, err;
  int code = tsfile_cli::run_cli({}, out, err);
  EXPECT_EQ(code, 1);
  EXPECT_NE(err.str().find("Usage"), std::string::npos);
}

TEST(RunCliTest, UnknownCommandIsUsageError) {
  std::ostringstream out, err;
  int code = tsfile_cli::run_cli({"frobnicate", "x.tsfile"}, out, err);
  EXPECT_EQ(code, 1);
  EXPECT_NE(err.str().find("Unknown command"), std::string::npos);
}
```

- [ ] **Step 2: Create `cpp/tools/cli/exit_codes.h`**

```cpp
#ifndef TSFILE_CLI_EXIT_CODES_H
#define TSFILE_CLI_EXIT_CODES_H
namespace tsfile_cli {
constexpr int kExitOk = 0;       // success
constexpr int kExitUsage = 1;    // bad arguments / unknown command
constexpr int kExitFile = 2;     // cannot open or corrupted file
constexpr int kExitRuntime = 3;  // query / runtime error
}  // namespace tsfile_cli
#endif  // TSFILE_CLI_EXIT_CODES_H
```

- [ ] **Step 3: Create `cpp/tools/cli/run_cli.h`**

```cpp
#ifndef TSFILE_CLI_RUN_CLI_H
#define TSFILE_CLI_RUN_CLI_H
#include <ostream>
#include <string>
#include <vector>
namespace tsfile_cli {
// Entry point used by main() and by tests. argv excludes the program name.
// Data is written to `out`, diagnostics/errors to `err`. Returns an exit code
// from exit_codes.h.
int run_cli(const std::vector<std::string>& args, std::ostream& out,
            std::ostream& err);
}  // namespace tsfile_cli
#endif  // TSFILE_CLI_RUN_CLI_H
```

- [ ] **Step 4: Create `cpp/tools/cli/run_cli.cc`** (skeleton — dispatch filled in later tasks)

```cpp
#include "cli/run_cli.h"

#include "cli/exit_codes.h"

#ifndef TSFILE_CLI_VERSION
#define TSFILE_CLI_VERSION "unknown"
#endif

namespace tsfile_cli {

namespace {
void print_usage(std::ostream& os) {
  os << "Usage: tsfile <command> [options] <file.tsfile>\n"
        "Commands: ls schema stats head cat select\n"
        "Run 'tsfile help <command>' for command options.\n";
}
}  // namespace

int run_cli(const std::vector<std::string>& args, std::ostream& out,
            std::ostream& err) {
  for (const std::string& a : args) {
    if (a == "--version") {
      out << "tsfile (Apache TsFile C++) " << TSFILE_CLI_VERSION << "\n";
      return kExitOk;
    }
  }
  if (args.empty() || args[0] == "--help" || args[0] == "-h" ||
      args[0] == "help") {
    print_usage(args.empty() ? err : out);
    return args.empty() ? kExitUsage : kExitOk;
  }

  const std::string& command = args[0];
  // Dispatch table is extended in Tasks 5-8.
  err << "Unknown command: " << command << "\n";
  print_usage(err);
  return kExitUsage;
}

}  // namespace tsfile_cli
```

- [ ] **Step 5: Create `cpp/tools/tools_main.cc`**

```cpp
#include <iostream>
#include <string>
#include <vector>

#include "cli/run_cli.h"

int main(int argc, char** argv) {
  std::vector<std::string> args(argv + 1, argv + argc);
  return tsfile_cli::run_cli(args, std::cout, std::cerr);
}
```

- [ ] **Step 6: Create `cpp/tools/CMakeLists.txt`**

```cmake
message("Running in tools directory")

# Sources for the CLI logic, excluding main(), compiled once as an OBJECT
# library so both the executable and the test target can reuse them.
file(GLOB_RECURSE TSFILE_CLI_SRCS
        "cli/*.cc"
        "format/*.cc"
        "commands/*.cc")

add_library(tsfile_cli_obj OBJECT ${TSFILE_CLI_SRCS})

# Headers: this dir (for "cli/..", "format/..", "commands/..") + the SDK src.
target_include_directories(tsfile_cli_obj PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/src)
if (ENABLE_ANTLR4)
    target_include_directories(tsfile_cli_obj PUBLIC
            ${PROJECT_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)
endif ()

target_compile_definitions(tsfile_cli_obj PRIVATE
        TSFILE_CLI_VERSION="${TsFile_CPP_VERSION}")

# The shipped binary. Target name differs from the `tsfile` library target to
# avoid a collision; OUTPUT_NAME makes the file `tsfile`.
add_executable(tsfile_cli tools_main.cc $<TARGET_OBJECTS:tsfile_cli_obj>)
target_include_directories(tsfile_cli PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(tsfile_cli tsfile)
set_target_properties(tsfile_cli PROPERTIES
        OUTPUT_NAME tsfile
        RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
```

- [ ] **Step 7: Modify `cpp/CMakeLists.txt`** — add the option after the other `option(...)` lines (near line 171) and the subdir before `add_subdirectory(test)` (so `tsfile_cli_obj` exists for the test target). Insert:

```cmake
option(BUILD_TOOLS "Build the tsfile command-line tools" ON)
message("cmake using: BUILD_TOOLS=${BUILD_TOOLS}")
```

and change the tail of the file from:

```cmake
add_subdirectory(src)
if (BUILD_TEST)
```

to:

```cmake
add_subdirectory(src)
if (BUILD_TOOLS)
    add_subdirectory(tools)
endif ()
if (BUILD_TEST)
```

- [ ] **Step 8: Modify `cpp/test/CMakeLists.txt`** — add tools test glob after the existing `file(GLOB_RECURSE TEST_SRCS ...)` block (after line 114):

```cmake
if (BUILD_TOOLS)
    file(GLOB_RECURSE TOOLS_TEST_SRCS "tools/*_test.cc")
    list(APPEND TEST_SRCS ${TOOLS_TEST_SRCS})
endif ()
```

and extend the test target's link + includes. Change:

```cmake
add_executable(TsFile_Test ${TEST_SRCS})
target_link_libraries(
        TsFile_Test
        GTest::gtest_main
        GTest::gmock
        tsfile
)
```

to:

```cmake
add_executable(TsFile_Test ${TEST_SRCS})
if (BUILD_TOOLS)
    target_include_directories(TsFile_Test PRIVATE ${CMAKE_SOURCE_DIR}/tools)
endif ()
target_link_libraries(
        TsFile_Test
        GTest::gtest_main
        GTest::gmock
        tsfile
)
if (BUILD_TOOLS)
    target_link_libraries(TsFile_Test tsfile_cli_obj)
endif ()
```

- [ ] **Step 9: Build and run the tests**

Run: `cd cpp && bash build.sh -t=Debug 2>&1 | tail -20`
Expected: build succeeds; `build/Debug/bin/tsfile` and `build/Debug/lib/TsFile_Test` exist.

Run: `cd cpp && ./build/Debug/lib/TsFile_Test --gtest_filter=RunCliTest.*`
Expected: 3 tests PASS.

Run: `cd cpp && ./build/Debug/bin/tsfile --version`
Expected: prints `tsfile (Apache TsFile C++) 2.2.1.dev` and exits 0.

- [ ] **Step 10: Commit**

```bash
git add cpp/tools cpp/test/tools/cli_args_test.cc cpp/CMakeLists.txt cpp/test/CMakeLists.txt
git commit -m "feat(cpp-tools): scaffold tsfile CLI binary with run_cli skeleton"
```

---

### Task 2: `parse_args` (cli_args)

**Files:**
- Create: `cpp/tools/cli/cli_args.h`, `cpp/tools/cli/cli_args.cc`
- Test: append to `cpp/test/tools/cli_args_test.cc`

- [ ] **Step 1: Write the failing tests** — append to `cpp/test/tools/cli_args_test.cc`

```cpp
#include "cli/cli_args.h"

TEST(ParseArgsTest, CommandAndFilePositional) {
  auto p = tsfile_cli::parse_args({"ls", "data.tsfile"});
  EXPECT_TRUE(p.error.empty());
  EXPECT_EQ(p.command, "ls");
  EXPECT_EQ(p.file, "data.tsfile");
}

TEST(ParseArgsTest, FormatFlagParsed) {
  auto p = tsfile_cli::parse_args({"cat", "-f", "json", "data.tsfile"});
  EXPECT_TRUE(p.error.empty());
  EXPECT_EQ(p.format, tsfile_cli::ParsedArgs::Format::kJson);
}

TEST(ParseArgsTest, MeasurementsSplitOnComma) {
  auto p = tsfile_cli::parse_args(
      {"select", "-m", "s1,s2,s3", "data.tsfile"});
  ASSERT_EQ(p.measurements.size(), 3u);
  EXPECT_EQ(p.measurements[1], "s2");
}

TEST(ParseArgsTest, LimitOffsetAndTimeRange) {
  auto p = tsfile_cli::parse_args(
      {"head", "-n", "5", "--offset", "2", "--start", "100", "--end", "200",
       "data.tsfile"});
  EXPECT_EQ(p.limit, 5);
  EXPECT_EQ(p.offset, 2);
  EXPECT_TRUE(p.has_start);
  EXPECT_EQ(p.start, 100);
  EXPECT_TRUE(p.has_end);
  EXPECT_EQ(p.end, 200);
}

TEST(ParseArgsTest, UnknownFlagIsError) {
  auto p = tsfile_cli::parse_args({"ls", "--bogus", "data.tsfile"});
  EXPECT_FALSE(p.error.empty());
}

TEST(ParseArgsTest, BadFormatValueIsError) {
  auto p = tsfile_cli::parse_args({"cat", "-f", "yaml", "data.tsfile"});
  EXPECT_FALSE(p.error.empty());
}

TEST(ParseArgsTest, MissingFileIsAllowedAtParseTime) {
  // File presence is validated by run_cli, not parse_args.
  auto p = tsfile_cli::parse_args({"ls"});
  EXPECT_TRUE(p.error.empty());
  EXPECT_EQ(p.command, "ls");
  EXPECT_TRUE(p.file.empty());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd cpp && ./build/Debug/lib/TsFile_Test --gtest_filter=ParseArgsTest.*`
Expected: compile failure (`cli_args.h` missing) — that counts as red.

- [ ] **Step 3: Create `cpp/tools/cli/cli_args.h`**

```cpp
#ifndef TSFILE_CLI_CLI_ARGS_H
#define TSFILE_CLI_CLI_ARGS_H
#include <climits>
#include <string>
#include <vector>
namespace tsfile_cli {
struct ParsedArgs {
  enum class Format { kAuto, kCsv, kTsv, kJson, kTable };
  std::string command;
  std::string file;
  std::string device;                  // -d / --device (tree model)
  std::string table;                   // -t / --table  (table model)
  std::vector<std::string> measurements;  // -m / --measurements (comma list)
  long long limit = -1;                // -n / --limit (<0 = unlimited)
  long long offset = 0;                // --offset
  long long start = LLONG_MIN;         // --start (epoch ms)
  long long end = LLONG_MAX;           // --end   (epoch ms)
  bool has_start = false;
  bool has_end = false;
  Format format = Format::kAuto;       // -f / --format
  bool no_header = false;              // --no-header
  std::string model;                   // --model "tree"|"table"|""
  bool help = false;
  bool version = false;
  std::string error;                   // non-empty => parse error message
};

// Parses args (program name already stripped). On bad input, returns a
// ParsedArgs whose .error is set; otherwise .error is empty. Does NOT validate
// that a file was supplied — run_cli does that per command.
ParsedArgs parse_args(const std::vector<std::string>& args);
}  // namespace tsfile_cli
#endif  // TSFILE_CLI_CLI_ARGS_H
```

- [ ] **Step 4: Create `cpp/tools/cli/cli_args.cc`**

```cpp
#include "cli/cli_args.h"

#include <cstdlib>
#include <sstream>

namespace tsfile_cli {

namespace {
std::vector<std::string> split_csv(const std::string& s) {
  std::vector<std::string> out;
  std::string item;
  std::istringstream iss(s);
  while (std::getline(iss, item, ',')) {
    if (!item.empty()) out.push_back(item);
  }
  return out;
}

bool parse_ll(const std::string& s, long long& out) {
  if (s.empty()) return false;
  char* endp = nullptr;
  long long v = std::strtoll(s.c_str(), &endp, 10);
  if (endp == nullptr || *endp != '\0') return false;
  out = v;
  return true;
}

bool parse_format(const std::string& s, ParsedArgs::Format& out) {
  if (s == "csv") out = ParsedArgs::Format::kCsv;
  else if (s == "tsv") out = ParsedArgs::Format::kTsv;
  else if (s == "json") out = ParsedArgs::Format::kJson;
  else if (s == "table") out = ParsedArgs::Format::kTable;
  else return false;
  return true;
}
}  // namespace

ParsedArgs parse_args(const std::vector<std::string>& args) {
  ParsedArgs p;
  if (args.empty()) return p;
  p.command = args[0];

  // Flags requiring a value; the lambda fetches the next token.
  size_t i = 1;
  auto need_value = [&](const std::string& flag, std::string& dst) -> bool {
    if (i + 1 >= args.size()) {
      p.error = "Missing value for " + flag;
      return false;
    }
    dst = args[++i];
    return true;
  };

  for (; i < args.size(); ++i) {
    const std::string& a = args[i];
    std::string val;
    if (a == "-f" || a == "--format") {
      if (!need_value(a, val)) return p;
      if (!parse_format(val, p.format)) {
        p.error = "Invalid format: " + val + " (use csv|tsv|json|table)";
        return p;
      }
    } else if (a == "-d" || a == "--device") {
      if (!need_value(a, p.device)) return p;
    } else if (a == "-t" || a == "--table") {
      if (!need_value(a, p.table)) return p;
    } else if (a == "-m" || a == "--measurements") {
      if (!need_value(a, val)) return p;
      p.measurements = split_csv(val);
    } else if (a == "-n" || a == "--limit") {
      if (!need_value(a, val)) return p;
      if (!parse_ll(val, p.limit)) { p.error = "Invalid --limit: " + val; return p; }
    } else if (a == "--offset") {
      if (!need_value(a, val)) return p;
      if (!parse_ll(val, p.offset)) { p.error = "Invalid --offset: " + val; return p; }
    } else if (a == "--start") {
      if (!need_value(a, val)) return p;
      if (!parse_ll(val, p.start)) { p.error = "Invalid --start: " + val; return p; }
      p.has_start = true;
    } else if (a == "--end") {
      if (!need_value(a, val)) return p;
      if (!parse_ll(val, p.end)) { p.error = "Invalid --end: " + val; return p; }
      p.has_end = true;
    } else if (a == "--model") {
      if (!need_value(a, val)) return p;
      if (val != "tree" && val != "table") {
        p.error = "Invalid --model: " + val + " (use tree|table)";
        return p;
      }
      p.model = val;
    } else if (a == "--no-header") {
      p.no_header = true;
    } else if (a == "-h" || a == "--help") {
      p.help = true;
    } else if (a == "--version") {
      p.version = true;
    } else if (!a.empty() && a[0] == '-') {
      p.error = "Unknown flag: " + a;
      return p;
    } else {
      // First bare token is the file path; extra positionals are an error.
      if (p.file.empty()) p.file = a;
      else { p.error = "Unexpected argument: " + a; return p; }
    }
  }
  return p;
}

}  // namespace tsfile_cli
```

- [ ] **Step 5: Build and run tests to verify they pass**

Run: `cd cpp && bash build.sh -t=Debug 2>&1 | tail -5 && ./build/Debug/lib/TsFile_Test --gtest_filter=ParseArgsTest.*:RunCliTest.*`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add cpp/tools/cli/cli_args.h cpp/tools/cli/cli_args.cc cpp/test/tools/cli_args_test.cc
git commit -m "feat(cpp-tools): add hand-rolled CLI argument parser"
```

---

### Task 3: Pure output formatting (`output_format`)

**Files:**
- Create: `cpp/tools/format/output_format.h`, `cpp/tools/format/output_format.cc`
- Test: `cpp/test/tools/output_format_test.cc`

This layer has **no dependency on the reader**: it operates on pre-stringified
cells plus a parallel vector of column types (used only to decide JSON quoting).

- [ ] **Step 1: Write the failing tests** — `cpp/test/tools/output_format_test.cc`

```cpp
#include <gtest/gtest.h>

#include <sstream>
#include <vector>

#include "common/db_common.h"
#include "format/output_format.h"

using tsfile_cli::OutputFormat;
using tsfile_cli::ParsedArgs;
using tsfile_cli::RowWriter;

TEST(ResolveFormatTest, AutoUsesTableOnTtyTsvOtherwise) {
  EXPECT_EQ(tsfile_cli::resolve_format(ParsedArgs::Format::kAuto, true),
            OutputFormat::kTable);
  EXPECT_EQ(tsfile_cli::resolve_format(ParsedArgs::Format::kAuto, false),
            OutputFormat::kTsv);
  EXPECT_EQ(tsfile_cli::resolve_format(ParsedArgs::Format::kJson, true),
            OutputFormat::kJson);
}

TEST(CsvEscapeTest, QuotesWhenSpecialCharsPresent) {
  EXPECT_EQ(tsfile_cli::csv_escape("plain"), "plain");
  EXPECT_EQ(tsfile_cli::csv_escape("a,b"), "\"a,b\"");
  EXPECT_EQ(tsfile_cli::csv_escape("she said \"hi\""),
            "\"she said \"\"hi\"\"\"");
  EXPECT_EQ(tsfile_cli::csv_escape("line\nbreak"), "\"line\nbreak\"");
}

TEST(JsonEscapeTest, EscapesQuotesBackslashAndControls) {
  EXPECT_EQ(tsfile_cli::json_escape("a\"b\\c"), "a\\\"b\\\\c");
  EXPECT_EQ(tsfile_cli::json_escape("tab\there"), "tab\\there");
}

TEST(TypeNameTest, KnownTypesMapToNames) {
  EXPECT_STREQ(tsfile_cli::tsdatatype_name(common::INT64), "INT64");
  EXPECT_STREQ(tsfile_cli::tsdatatype_name(common::STRING), "STRING");
  EXPECT_STREQ(tsfile_cli::tsdatatype_name(common::BOOLEAN), "BOOLEAN");
}

TEST(RowWriterTest, TsvWritesHeaderThenRows) {
  std::ostringstream out;
  RowWriter w(out, OutputFormat::kTsv, {"time", "s1"},
              {common::INT64, common::INT64}, /*no_header=*/false);
  w.write({"1", "10"}, {false, false});
  w.write({"2", ""}, {false, true});
  w.finish();
  EXPECT_EQ(out.str(), "time\ts1\n1\t10\n2\t\n");
}

TEST(RowWriterTest, NoHeaderSuppressesHeader) {
  std::ostringstream out;
  RowWriter w(out, OutputFormat::kTsv, {"name"}, {common::STRING}, true);
  w.write({"table1"}, {false});
  w.finish();
  EXPECT_EQ(out.str(), "table1\n");
}

TEST(RowWriterTest, CsvEscapesCells) {
  std::ostringstream out;
  RowWriter w(out, OutputFormat::kCsv, {"name"}, {common::STRING}, false);
  w.write({"a,b"}, {false});
  w.finish();
  EXPECT_EQ(out.str(), "name\n\"a,b\"\n");
}

TEST(RowWriterTest, JsonNumbersUnquotedStringsQuotedNullEmitted) {
  std::ostringstream out;
  RowWriter w(out, OutputFormat::kJson, {"time", "name"},
              {common::INT64, common::STRING}, false);
  w.write({"5", "dev1"}, {false, false});
  w.write({"6", ""}, {false, true});
  w.finish();
  EXPECT_EQ(out.str(),
            "{\"time\":5,\"name\":\"dev1\"}\n"
            "{\"time\":6,\"name\":null}\n");
}

TEST(RowWriterTest, TableAlignsColumns) {
  std::ostringstream out;
  RowWriter w(out, OutputFormat::kTable, {"name", "type"},
              {common::STRING, common::STRING}, false);
  w.write({"s1", "INT64"}, {false, false});
  w.write({"longname", "BOOLEAN"}, {false, false});
  w.finish();
  EXPECT_EQ(out.str(),
            "name      type\n"
            "s1        INT64\n"
            "longname  BOOLEAN\n");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd cpp && ./build/Debug/lib/TsFile_Test --gtest_filter=*Format*:RowWriterTest.*:*EscapeTest*:TypeNameTest.*`
Expected: compile failure (`format/output_format.h` missing) — red.

- [ ] **Step 3: Create `cpp/tools/format/output_format.h`**

```cpp
#ifndef TSFILE_CLI_OUTPUT_FORMAT_H
#define TSFILE_CLI_OUTPUT_FORMAT_H

#include <ostream>
#include <string>
#include <vector>

#include "cli/cli_args.h"
#include "common/db_common.h"

namespace tsfile_cli {

enum class OutputFormat { kCsv, kTsv, kJson, kTable };

// kAuto resolves to kTable on a TTY, kTsv otherwise. Other values pass through.
OutputFormat resolve_format(ParsedArgs::Format f, bool stdout_is_tty);

// Stable display name for every TSDataType value (does not assert).
const char* tsdatatype_name(common::TSDataType t);

std::string csv_escape(const std::string& field);
std::string json_escape(const std::string& s);

// Writes rows in the chosen format. Cells are pre-stringified; `types` is used
// only by the JSON formatter to decide whether a value is emitted bare
// (numeric/boolean) or quoted (everything else). For kTable, rows are buffered
// and flushed (column-aligned) by finish().
class RowWriter {
 public:
  RowWriter(std::ostream& out, OutputFormat fmt,
            std::vector<std::string> header,
            std::vector<common::TSDataType> types, bool no_header);
  void write(const std::vector<std::string>& cells,
             const std::vector<bool>& is_null);
  void finish();

 private:
  void ensure_header();          // streaming formats: lazily emit header
  bool is_numeric(size_t col) const;  // JSON: bare vs quoted

  std::ostream& out_;
  OutputFormat fmt_;
  std::vector<std::string> header_;
  std::vector<common::TSDataType> types_;
  bool no_header_;
  bool header_done_ = false;
  std::vector<std::vector<std::string>> rows_;        // kTable buffer
  std::vector<std::vector<bool>> rows_null_;          // kTable buffer
};

}  // namespace tsfile_cli
#endif  // TSFILE_CLI_OUTPUT_FORMAT_H
```

- [ ] **Step 4: Create `cpp/tools/format/output_format.cc`**

```cpp
#include "format/output_format.h"

#include <algorithm>
#include <sstream>

namespace tsfile_cli {

OutputFormat resolve_format(ParsedArgs::Format f, bool stdout_is_tty) {
  switch (f) {
    case ParsedArgs::Format::kCsv: return OutputFormat::kCsv;
    case ParsedArgs::Format::kTsv: return OutputFormat::kTsv;
    case ParsedArgs::Format::kJson: return OutputFormat::kJson;
    case ParsedArgs::Format::kTable: return OutputFormat::kTable;
    case ParsedArgs::Format::kAuto:
    default:
      return stdout_is_tty ? OutputFormat::kTable : OutputFormat::kTsv;
  }
}

const char* tsdatatype_name(common::TSDataType t) {
  switch (t) {
    case common::BOOLEAN: return "BOOLEAN";
    case common::INT32: return "INT32";
    case common::INT64: return "INT64";
    case common::FLOAT: return "FLOAT";
    case common::DOUBLE: return "DOUBLE";
    case common::TEXT: return "TEXT";
    case common::VECTOR: return "VECTOR";
    case common::TIMESTAMP: return "TIMESTAMP";
    case common::DATE: return "DATE";
    case common::BLOB: return "BLOB";
    case common::STRING: return "STRING";
    case common::NULL_TYPE: return "NULL";
    default: return "UNKNOWN";
  }
}

std::string csv_escape(const std::string& field) {
  bool needs_quote = field.find_first_of(",\"\n\r") != std::string::npos;
  if (!needs_quote) return field;
  std::string out = "\"";
  for (char c : field) {
    if (c == '"') out += "\"\"";
    else out += c;
  }
  out += "\"";
  return out;
}

std::string json_escape(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 2);
  for (unsigned char c : s) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\b': out += "\\b"; break;
      case '\f': out += "\\f"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (c < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else {
          out += static_cast<char>(c);
        }
    }
  }
  return out;
}

RowWriter::RowWriter(std::ostream& out, OutputFormat fmt,
                     std::vector<std::string> header,
                     std::vector<common::TSDataType> types, bool no_header)
    : out_(out),
      fmt_(fmt),
      header_(std::move(header)),
      types_(std::move(types)),
      no_header_(no_header) {}

bool RowWriter::is_numeric(size_t col) const {
  if (col >= types_.size()) return false;
  switch (types_[col]) {
    case common::BOOLEAN:
    case common::INT32:
    case common::INT64:
    case common::FLOAT:
    case common::DOUBLE:
    case common::TIMESTAMP:
      return true;
    default:
      return false;
  }
}

void RowWriter::ensure_header() {
  if (header_done_) return;
  header_done_ = true;
  if (no_header_) return;
  const char sep = (fmt_ == OutputFormat::kCsv) ? ',' : '\t';
  for (size_t i = 0; i < header_.size(); ++i) {
    if (i) out_ << sep;
    out_ << (fmt_ == OutputFormat::kCsv ? csv_escape(header_[i]) : header_[i]);
  }
  out_ << "\n";
}

void RowWriter::write(const std::vector<std::string>& cells,
                      const std::vector<bool>& is_null) {
  if (fmt_ == OutputFormat::kTable) {
    rows_.push_back(cells);
    rows_null_.push_back(is_null);
    return;
  }
  if (fmt_ == OutputFormat::kJson) {
    out_ << "{";
    for (size_t i = 0; i < header_.size(); ++i) {
      if (i) out_ << ",";
      out_ << "\"" << json_escape(header_[i]) << "\":";
      if (i < is_null.size() && is_null[i]) {
        out_ << "null";
      } else if (is_numeric(i)) {
        out_ << (i < cells.size() ? cells[i] : "null");
      } else {
        out_ << "\"" << json_escape(i < cells.size() ? cells[i] : "")
             << "\"";
      }
    }
    out_ << "}\n";
    return;
  }
  // csv / tsv
  ensure_header();
  const char sep = (fmt_ == OutputFormat::kCsv) ? ',' : '\t';
  for (size_t i = 0; i < cells.size(); ++i) {
    if (i) out_ << sep;
    bool null_cell = i < is_null.size() && is_null[i];
    if (null_cell) continue;  // empty field
    out_ << (fmt_ == OutputFormat::kCsv ? csv_escape(cells[i]) : cells[i]);
  }
  out_ << "\n";
}

void RowWriter::finish() {
  if (fmt_ != OutputFormat::kTable) return;
  const size_t ncols = header_.size();
  std::vector<size_t> width(ncols, 0);
  if (!no_header_) {
    for (size_t i = 0; i < ncols; ++i) width[i] = header_[i].size();
  }
  for (const auto& row : rows_) {
    for (size_t i = 0; i < ncols && i < row.size(); ++i) {
      width[i] = std::max(width[i], row[i].size());
    }
  }
  auto emit = [&](const std::vector<std::string>& cells,
                  const std::vector<bool>& nulls) {
    for (size_t i = 0; i < ncols; ++i) {
      std::string cell =
          (i < cells.size() && !(i < nulls.size() && nulls[i])) ? cells[i]
                                                                : "";
      out_ << cell;
      if (i + 1 < ncols) {
        out_ << std::string(width[i] - cell.size() + 2, ' ');
      }
    }
    out_ << "\n";
  };
  if (!no_header_) {
    std::vector<bool> no_nulls(ncols, false);
    emit(header_, no_nulls);
  }
  for (size_t r = 0; r < rows_.size(); ++r) emit(rows_[r], rows_null_[r]);
}

}  // namespace tsfile_cli
```

- [ ] **Step 5: Build and run tests to verify they pass**

Run: `cd cpp && bash build.sh -t=Debug 2>&1 | tail -5 && ./build/Debug/lib/TsFile_Test --gtest_filter=*Format*:RowWriterTest.*:*EscapeTest*:TypeNameTest.*`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add cpp/tools/format/output_format.h cpp/tools/format/output_format.cc cpp/test/tools/output_format_test.cc
git commit -m "feat(cpp-tools): add pure output formatters (csv/tsv/json/table)"
```

---

### Task 4: `ResultSet` pump (`result_set_format`)

**Files:**
- Create: `cpp/tools/format/result_set_format.h`, `cpp/tools/format/result_set_format.cc`

This layer converts a live `storage::ResultSet` into formatted rows. It is
exercised end-to-end by the command tests (Tasks 5-8); it has no standalone unit
test because constructing a `ResultSet` requires a real file. Keep the typed
extraction here and out of the pure layer.

- [ ] **Step 1: Create `cpp/tools/format/result_set_format.h`**

```cpp
#ifndef TSFILE_CLI_RESULT_SET_FORMAT_H
#define TSFILE_CLI_RESULT_SET_FORMAT_H

#include <ostream>
#include <string>

#include "common/db_common.h"
#include "format/output_format.h"
#include "reader/result_set.h"

namespace tsfile_cli {

// Stringifies one cell (column index is 1-based, per ResultSetMetadata).
// Caller must have checked is_null() first.
std::string cell_to_string(storage::ResultSet* rs, uint32_t col_index,
                           common::TSDataType type);

// Pumps every row of `rs` into `out` using `fmt`. Reads column names/types from
// the result set metadata. Returns 0 on success or a non-zero error code if the
// underlying ResultSet::next() fails.
int write_result_set(storage::ResultSet* rs, OutputFormat fmt, bool no_header,
                     std::ostream& out);

}  // namespace tsfile_cli
#endif  // TSFILE_CLI_RESULT_SET_FORMAT_H
```

- [ ] **Step 2: Create `cpp/tools/format/result_set_format.cc`**

```cpp
#include "format/result_set_format.h"

#include <cstdio>
#include <sstream>
#include <vector>

#include "utils/errno_define.h"  // common::E_OK

namespace tsfile_cli {

std::string cell_to_string(storage::ResultSet* rs, uint32_t i,
                           common::TSDataType type) {
  std::ostringstream ss;
  switch (type) {
    case common::BOOLEAN:
      return rs->get_value<bool>(i) ? "true" : "false";
    case common::INT32:
      ss << rs->get_value<int32_t>(i);
      return ss.str();
    case common::INT64:
    case common::TIMESTAMP:
      ss << rs->get_value<int64_t>(i);
      return ss.str();
    case common::FLOAT:
      ss << rs->get_value<float>(i);
      return ss.str();
    case common::DOUBLE:
      ss << rs->get_value<double>(i);
      return ss.str();
    case common::DATE: {
      std::tm d = rs->get_value<std::tm>(i);
      char buf[16];
      std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", d.tm_year + 1900,
                    d.tm_mon + 1, d.tm_mday);
      return buf;
    }
    case common::TEXT:
    case common::STRING:
    case common::BLOB: {
      common::String* s = rs->get_value<common::String*>(i);
      return s == nullptr ? std::string() : s->to_std_string();
    }
    default:
      return "<UNKNOWN>";
  }
}

int write_result_set(storage::ResultSet* rs, OutputFormat fmt, bool no_header,
                     std::ostream& out) {
  auto meta = rs->get_metadata();
  const uint32_t ncol = meta->get_column_count();
  std::vector<std::string> header;
  std::vector<common::TSDataType> types;
  header.reserve(ncol);
  types.reserve(ncol);
  for (uint32_t i = 1; i <= ncol; ++i) {
    header.push_back(meta->get_column_name(i));
    types.push_back(meta->get_column_type(i));
  }

  RowWriter writer(out, fmt, header, types, no_header);
  bool has_next = false;
  int code = common::E_OK;
  while ((code = rs->next(has_next)) == common::E_OK && has_next) {
    std::vector<std::string> cells(ncol);
    std::vector<bool> nulls(ncol, false);
    for (uint32_t i = 1; i <= ncol; ++i) {
      if (rs->is_null(i)) {
        nulls[i - 1] = true;
      } else {
        cells[i - 1] = cell_to_string(rs, i, types[i - 1]);
      }
    }
    writer.write(cells, nulls);
  }
  writer.finish();
  return code;
}

}  // namespace tsfile_cli
```

> **Note:** `common::E_OK` is defined in `cpp/src/utils/errno_define.h` (and is
> also pulled in transitively by `reader/result_set.h`). The explicit include
> above keeps the source self-documenting.

- [ ] **Step 3: Build to verify it compiles** (no test yet; covered in Task 5)

Run: `cd cpp && bash build.sh -t=Debug 2>&1 | tail -5`
Expected: build succeeds (the new `.cc` is picked up by the tools glob).

- [ ] **Step 4: Commit**

```bash
git add cpp/tools/format/result_set_format.h cpp/tools/format/result_set_format.cc
git commit -m "feat(cpp-tools): add ResultSet-to-rows pump layer"
```

---

### Task 5: Model detection + `cmd_ls` + reader-open dispatch

**Files:**
- Create: `cpp/tools/commands/commands.h`
- Create: `cpp/tools/commands/cmd_ls.cc`
- Replace: `cpp/tools/cli/run_cli.cc` (full dispatch + reader open)
- Create: `cpp/test/tools/cli_test_util.h`
- Create: `cpp/test/tools/command_e2e_test.cc`

- [ ] **Step 1: Create `cpp/tools/commands/commands.h`**

```cpp
#ifndef TSFILE_CLI_COMMANDS_H
#define TSFILE_CLI_COMMANDS_H

#include <ostream>

#include "cli/cli_args.h"
#include "format/output_format.h"

namespace storage {
class TsFileReader;
}

namespace tsfile_cli {

// Returns true if the file should be treated as table-model. Honors
// args.model ("tree"/"table"); otherwise detects via table schemas presence.
bool is_table_model(const ParsedArgs& args, storage::TsFileReader& reader);

// Every command writes data to `out`, diagnostics to `err`, and returns an
// exit code from exit_codes.h.
int cmd_ls(const ParsedArgs& args, storage::TsFileReader& reader,
           OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_schema(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_stats(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_head(const ParsedArgs& args, storage::TsFileReader& reader,
             OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_cat(const ParsedArgs& args, storage::TsFileReader& reader,
            OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_select(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err);

}  // namespace tsfile_cli
#endif  // TSFILE_CLI_COMMANDS_H
```

- [ ] **Step 2: Create `cpp/tools/commands/cmd_ls.cc`**

```cpp
#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

bool is_table_model(const ParsedArgs& args, storage::TsFileReader& reader) {
  if (args.model == "tree") return false;
  if (args.model == "table") return true;
  return !reader.get_all_table_schemas().empty();
}

int cmd_ls(const ParsedArgs& args, storage::TsFileReader& reader,
           OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
  std::vector<std::string> names;
  if (is_table_model(args, reader)) {
    for (auto& ts : reader.get_all_table_schemas()) {
      if (ts) names.push_back(ts->get_table_name());
    }
  } else {
    for (auto& dev : reader.get_all_device_ids()) {
      if (dev) names.push_back(dev->get_device_name());
    }
  }
  RowWriter w(out, fmt, {"name"}, {common::STRING}, args.no_header);
  for (const std::string& n : names) {
    w.write({n}, {false});
  }
  w.finish();
  return kExitOk;
}

}  // namespace tsfile_cli
```

- [ ] **Step 3: Replace `cpp/tools/cli/run_cli.cc`** with the full version

```cpp
#include "cli/run_cli.h"

#include <cstdio>
#include <set>

#include "cli/cli_args.h"
#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "format/output_format.h"
#include "reader/tsfile_reader.h"

#ifdef _WIN32
#include <io.h>
#define TSFILE_ISATTY _isatty
#define TSFILE_FILENO _fileno
#else
#include <unistd.h>
#define TSFILE_ISATTY isatty
#define TSFILE_FILENO fileno
#endif

#ifndef TSFILE_CLI_VERSION
#define TSFILE_CLI_VERSION "unknown"
#endif

namespace tsfile_cli {

namespace {
void print_usage(std::ostream& os) {
  os << "Usage: tsfile <command> [options] <file.tsfile>\n"
        "Commands:\n"
        "  ls       list devices (tree) or tables (table)\n"
        "  schema   per-measurement data type/encoding/compression\n"
        "  stats    per-series row count and time range\n"
        "  head     first N rows (use -n)\n"
        "  cat      all rows of a device/table\n"
        "  select   choose columns (-m), time range (--start/--end), "
        "limit/offset\n"
        "Options: -f/--format csv|tsv|json|table, -d/--device, -t/--table,\n"
        "         -m/--measurements a,b, -n/--limit, --offset, --start, --end,\n"
        "         --no-header, --model tree|table, -h/--help, --version\n";
}

bool is_known_command(const std::string& c) {
  static const std::set<std::string> kCmds = {"ls",   "schema", "stats",
                                              "head", "cat",    "select"};
  return kCmds.count(c) != 0;
}
}  // namespace

int run_cli(const std::vector<std::string>& args, std::ostream& out,
            std::ostream& err) {
  ParsedArgs p = parse_args(args);

  if (p.version || (!args.empty() && args[0] == "--version")) {
    out << "tsfile (Apache TsFile C++) " << TSFILE_CLI_VERSION << "\n";
    return kExitOk;
  }
  if (args.empty()) {
    print_usage(err);
    return kExitUsage;
  }
  if (p.command == "help" || p.command == "--help" || p.command == "-h" ||
      (p.help && p.file.empty() && !is_known_command(p.command))) {
    print_usage(out);
    return kExitOk;
  }
  if (!p.error.empty()) {
    err << "Error: " << p.error << "\n";
    print_usage(err);
    return kExitUsage;
  }
  if (!is_known_command(p.command)) {
    err << "Unknown command: " << p.command << "\n";
    print_usage(err);
    return kExitUsage;
  }
  if (p.file.empty()) {
    err << "Error: missing <file.tsfile> argument\n";
    return kExitUsage;
  }

  storage::libtsfile_init();
  storage::TsFileReader reader;
  int open_ret = reader.open(p.file);
  if (open_ret != 0) {
    err << "Error: cannot open or corrupted file: " << p.file << "\n";
    return kExitFile;
  }

  bool stdout_tty = TSFILE_ISATTY(TSFILE_FILENO(stdout)) != 0;
  OutputFormat fmt = resolve_format(p.format, stdout_tty);

  int code;
  if (p.command == "ls") {
    code = cmd_ls(p, reader, fmt, out, err);
  } else {
    // Filled in by Tasks 6-8 (schema/stats/head/cat/select).
    err << "Error: command not yet implemented: " << p.command << "\n";
    code = kExitUsage;
  }

  reader.close();
  return code;
}

}  // namespace tsfile_cli
```

- [ ] **Step 4: Create `cpp/test/tools/cli_test_util.h`** (table-model fixture writer)

```cpp
#ifndef TSFILE_CLI_TEST_UTIL_H
#define TSFILE_CLI_TEST_UTIL_H

#include <fcntl.h>

#include <string>
#include <writer/tsfile_table_writer.h>

namespace tsfile_cli_test {

// Writes a small table-model fixture and returns its path. Table "table1":
// TAG columns id1,id2 (STRING) + FIELD column s1 (INT64); 5 rows, ts=0..4,
// s1 = row*10.
inline std::string write_table_fixture(
    const std::string& path = "tsfile_cli_fixture.tsfile") {
  storage::libtsfile_init();
  std::string table_name = "table1";

  storage::WriteFile file;
  int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
  flags |= O_BINARY;
#endif
  file.create(path, flags, 0666);

  auto* schema = new storage::TableSchema(
      table_name,
      {
          common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED,
                               common::PLAIN, common::ColumnCategory::TAG),
          common::ColumnSchema("id2", common::STRING, common::UNCOMPRESSED,
                               common::PLAIN, common::ColumnCategory::TAG),
          common::ColumnSchema("s1", common::INT64, common::UNCOMPRESSED,
                               common::PLAIN, common::ColumnCategory::FIELD),
      });

  auto* writer = new storage::TsFileTableWriter(&file, schema);
  storage::Tablet tablet(
      table_name, {"id1", "id2", "s1"},
      {common::STRING, common::STRING, common::INT64},
      {common::ColumnCategory::TAG, common::ColumnCategory::TAG,
       common::ColumnCategory::FIELD},
      10);
  for (int row = 0; row < 5; ++row) {
    tablet.add_timestamp(row, static_cast<int64_t>(row));
    tablet.add_value(row, "id1", "id1_field_1");
    tablet.add_value(row, "id2", "id2_field_2");
    tablet.add_value(row, "s1", static_cast<int64_t>(row * 10));
  }
  writer->write_table(tablet);
  writer->flush();
  writer->close();

  delete writer;
  delete schema;
  return path;
}

}  // namespace tsfile_cli_test
#endif  // TSFILE_CLI_TEST_UTIL_H
```

> **If the fixture fails to compile** (a transitively-included type is missing),
> add the explicit header — `common/tablet.h` for `Tablet`, `file/write_file.h`
> for `WriteFile`, `common/schema.h` for `TableSchema`/`ColumnSchema`. The
> `examples/cpp_examples/demo_write.cpp` compiles with just the table-writer
> include, so start minimal.

- [ ] **Step 5: Create `cpp/test/tools/command_e2e_test.cc`**

```cpp
#include <gtest/gtest.h>

#include <cstdio>
#include <sstream>
#include <string>

#include "cli/run_cli.h"
#include "cli_test_util.h"

namespace {
struct Fixture {
  std::string path = tsfile_cli_test::write_table_fixture();
  ~Fixture() { std::remove(path.c_str()); }
};
}  // namespace

TEST(CliE2E, LsListsTableNameTsv) {
  Fixture f;
  std::ostringstream out, err;
  int code = tsfile_cli::run_cli({"ls", "-f", "tsv", f.path}, out, err);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(out.str(), "name\ntable1\n");
  EXPECT_TRUE(err.str().empty());
}

TEST(CliE2E, LsNoHeaderJustName) {
  Fixture f;
  std::ostringstream out, err;
  int code =
      tsfile_cli::run_cli({"ls", "-f", "tsv", "--no-header", f.path}, out, err);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(out.str(), "table1\n");
}

TEST(CliE2E, OpenMissingFileReturnsFileError) {
  std::ostringstream out, err;
  int code = tsfile_cli::run_cli({"ls", "definitely_missing.tsfile"}, out, err);
  EXPECT_EQ(code, 2);
  EXPECT_FALSE(err.str().empty());
}
```

- [ ] **Step 6: Build and run tests**

Run: `cd cpp && bash build.sh -t=Debug 2>&1 | tail -8 && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.*`
Expected: 3 tests PASS.

Run: `cd cpp && ./build/Debug/bin/tsfile ls -f tsv examples/test_cpp.tsfile`
Expected: prints `name` then `table1` (the bundled example is table-model).

- [ ] **Step 7: Commit**

```bash
git add cpp/tools/commands/commands.h cpp/tools/commands/cmd_ls.cc cpp/tools/cli/run_cli.cc cpp/test/tools/cli_test_util.h cpp/test/tools/command_e2e_test.cc
git commit -m "feat(cpp-tools): implement model detection and 'ls' command"
```

---

### Task 6: `cmd_schema` (+ encoding/compression name helpers)

**Files:**
- Modify: `cpp/tools/format/output_format.h` / `.cc` (add `tsencoding_name`, `compression_name`)
- Create: `cpp/tools/commands/cmd_schema.cc`
- Modify: `cpp/tools/cli/run_cli.cc` (dispatch `schema`)
- Modify: `cpp/test/tools/output_format_test.cc`, `cpp/test/tools/command_e2e_test.cc`

`schema` emits a uniform 5-column shape `target, measurement, datatype, encoding,
compression`. Name + type come from `get_timeseries_metadata()` (works for both
models). Encoding/compression are enriched from `get_timeseries_schema()` for
tree-model files; for table-model files those two columns are blank (no public
getter on `TableSchema`).

- [ ] **Step 1: Add failing unit tests** — append to `cpp/test/tools/output_format_test.cc`

```cpp
TEST(EncodingNameTest, KnownEncodings) {
  EXPECT_STREQ(tsfile_cli::tsencoding_name(common::PLAIN), "PLAIN");
  EXPECT_STREQ(tsfile_cli::tsencoding_name(common::TS_2DIFF), "TS_2DIFF");
  EXPECT_STREQ(tsfile_cli::tsencoding_name(common::SPRINTZ), "SPRINTZ");
}

TEST(CompressionNameTest, KnownCompressors) {
  EXPECT_STREQ(tsfile_cli::compression_name(common::UNCOMPRESSED),
               "UNCOMPRESSED");
  EXPECT_STREQ(tsfile_cli::compression_name(common::SNAPPY), "SNAPPY");
  EXPECT_STREQ(tsfile_cli::compression_name(common::LZ4), "LZ4");
}
```

- [ ] **Step 2: Add declarations** to `cpp/tools/format/output_format.h` (after `tsdatatype_name`)

```cpp
const char* tsencoding_name(common::TSEncoding e);
const char* compression_name(common::CompressionType c);
```

- [ ] **Step 3: Add definitions** to `cpp/tools/format/output_format.cc` (after `tsdatatype_name`)

```cpp
const char* tsencoding_name(common::TSEncoding e) {
  switch (e) {
    case common::PLAIN: return "PLAIN";
    case common::DICTIONARY: return "DICTIONARY";
    case common::RLE: return "RLE";
    case common::DIFF: return "DIFF";
    case common::TS_2DIFF: return "TS_2DIFF";
    case common::BITMAP: return "BITMAP";
    case common::GORILLA_V1: return "GORILLA_V1";
    case common::REGULAR: return "REGULAR";
    case common::GORILLA: return "GORILLA";
    case common::ZIGZAG: return "ZIGZAG";
    case common::FREQ: return "FREQ";
    case common::SPRINTZ: return "SPRINTZ";
    default: return "UNKNOWN";
  }
}

const char* compression_name(common::CompressionType c) {
  switch (c) {
    case common::UNCOMPRESSED: return "UNCOMPRESSED";
    case common::SNAPPY: return "SNAPPY";
    case common::GZIP: return "GZIP";
    case common::LZO: return "LZO";
    case common::SDT: return "SDT";
    case common::PAA: return "PAA";
    case common::PLA: return "PLA";
    case common::LZ4: return "LZ4";
    default: return "UNKNOWN";
  }
}
```

- [ ] **Step 4: Create `cpp/tools/commands/cmd_schema.cc`**

```cpp
#include <map>
#include <utility>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/schema.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int cmd_schema(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
  const bool table = is_table_model(args, reader);
  RowWriter w(out, fmt,
              {"target", "measurement", "datatype", "encoding", "compression"},
              {common::STRING, common::STRING, common::STRING, common::STRING,
               common::STRING},
              args.no_header);

  storage::DeviceTimeseriesMetadataMap meta = reader.get_timeseries_metadata();
  for (auto& kv : meta) {
    std::string target = kv.first ? kv.first->get_device_name() : "";

    // Tree-model enrichment: measurement -> (encoding, compression).
    std::map<std::string, std::pair<std::string, std::string>> enc_comp;
    if (!table && kv.first) {
      std::vector<storage::MeasurementSchema> ms;
      if (reader.get_timeseries_schema(kv.first, ms) == 0) {
        for (auto& m : ms) {
          enc_comp[m.measurement_name_] = {tsencoding_name(m.encoding_),
                                           compression_name(m.compression_type_)};
        }
      }
    }

    for (auto& ts : kv.second) {
      if (!ts) continue;
      std::string m = ts->get_measurement_name().to_std_string();
      std::string dt = tsdatatype_name(ts->get_data_type());
      std::string enc, comp;
      auto it = enc_comp.find(m);
      if (it != enc_comp.end()) {
        enc = it->second.first;
        comp = it->second.second;
      }
      w.write({target, m, dt, enc, comp},
              {false, false, false, enc.empty(), comp.empty()});
    }
  }
  w.finish();
  return kExitOk;
}

}  // namespace tsfile_cli
```

- [ ] **Step 5: Wire dispatch** — in `cpp/tools/cli/run_cli.cc`, replace the `ls`/else block:

```cpp
  int code;
  if (p.command == "ls") {
    code = cmd_ls(p, reader, fmt, out, err);
  } else {
```

with:

```cpp
  int code;
  if (p.command == "ls") {
    code = cmd_ls(p, reader, fmt, out, err);
  } else if (p.command == "schema") {
    code = cmd_schema(p, reader, fmt, out, err);
  } else {
```

- [ ] **Step 6: Add e2e test** — append to `cpp/test/tools/command_e2e_test.cc`

```cpp
TEST(CliE2E, SchemaShowsFieldColumnAndType) {
  Fixture f;
  std::ostringstream out, err;
  int code = tsfile_cli::run_cli({"schema", "-f", "tsv", f.path}, out, err);
  EXPECT_EQ(code, 0);
  EXPECT_NE(out.str().find(
                "target\tmeasurement\tdatatype\tencoding\tcompression"),
            std::string::npos);
  EXPECT_NE(out.str().find("s1"), std::string::npos);
  EXPECT_NE(out.str().find("INT64"), std::string::npos);
}
```

> **If `SchemaShowsFieldColumnAndType` shows no rows** (i.e.
> `get_timeseries_metadata()` returns empty for a table-model file in this build),
> fall back to deriving name+type from a zero-row probe:
> `reader.queryByRow(table_name, all_measurement_names, /*offset=*/0,
> /*limit=*/0, rs)` and read `rs->get_metadata()`. Keep the 5-column output
> shape; leave encoding/compression blank.

- [ ] **Step 7: Build and run tests**

Run: `cd cpp && bash build.sh -t=Debug 2>&1 | tail -5 && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.*:EncodingNameTest.*:CompressionNameTest.*`
Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add cpp/tools/format/output_format.h cpp/tools/format/output_format.cc cpp/tools/commands/cmd_schema.cc cpp/tools/cli/run_cli.cc cpp/test/tools/output_format_test.cc cpp/test/tools/command_e2e_test.cc
git commit -m "feat(cpp-tools): implement 'schema' command"
```

---

### Task 7: `cmd_stats`

**Files:**
- Create: `cpp/tools/commands/cmd_stats.cc`
- Modify: `cpp/tools/cli/run_cli.cc` (dispatch `stats`)
- Modify: `cpp/test/tools/command_e2e_test.cc`

`stats` emits `target, measurement, count, start_time, end_time` from each
series' `Statistic` (via `get_timeseries_metadata()`).

- [ ] **Step 1: Create `cpp/tools/commands/cmd_stats.cc`**

```cpp
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/statistic.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int cmd_stats(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
  RowWriter w(out, fmt,
              {"target", "measurement", "count", "start_time", "end_time"},
              {common::STRING, common::STRING, common::INT64, common::INT64,
               common::INT64},
              args.no_header);

  storage::DeviceTimeseriesMetadataMap meta = reader.get_timeseries_metadata();
  for (auto& kv : meta) {
    std::string target = kv.first ? kv.first->get_device_name() : "";
    for (auto& ts : kv.second) {
      if (!ts) continue;
      std::string m = ts->get_measurement_name().to_std_string();
      storage::Statistic* st = ts->get_statistic();
      if (st != nullptr) {
        w.write({target, m, std::to_string(st->get_count()),
                 std::to_string(st->start_time_),
                 std::to_string(st->end_time_)},
                {false, false, false, false, false});
      } else {
        w.write({target, m, "", "", ""},
                {false, false, true, true, true});
      }
    }
  }
  w.finish();
  return kExitOk;
}

}  // namespace tsfile_cli
```

- [ ] **Step 2: Wire dispatch** — in `cpp/tools/cli/run_cli.cc`, add a branch after the `schema` branch:

```cpp
  } else if (p.command == "stats") {
    code = cmd_stats(p, reader, fmt, out, err);
```

(Place it between the `schema` branch and the final `else`.)

- [ ] **Step 3: Add e2e test** — append to `cpp/test/tools/command_e2e_test.cc`

```cpp
TEST(CliE2E, StatsReportsCountAndTimeRange) {
  Fixture f;
  std::ostringstream out, err;
  int code = tsfile_cli::run_cli({"stats", "-f", "tsv", f.path}, out, err);
  EXPECT_EQ(code, 0);
  EXPECT_NE(out.str().find(
                "target\tmeasurement\tcount\tstart_time\tend_time"),
            std::string::npos);
  // s1 has 5 rows with timestamps 0..4.
  EXPECT_NE(out.str().find("s1\t5\t0\t4"), std::string::npos);
}
```

- [ ] **Step 4: Build and run tests**

Run: `cd cpp && bash build.sh -t=Debug 2>&1 | tail -5 && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.*`
Expected: all PASS (including the new `StatsReportsCountAndTimeRange`).

> **If `s1\t5\t0\t4` is not found**, print the raw output
> (`./build/Debug/bin/tsfile stats -f tsv examples/test_cpp.tsfile`) and adjust
> the substring to the actual whitespace/columns — the count(5) and range(0..4)
> values themselves are guaranteed by the fixture.

- [ ] **Step 5: Commit**

```bash
git add cpp/tools/commands/cmd_stats.cc cpp/tools/cli/run_cli.cc cpp/test/tools/command_e2e_test.cc
git commit -m "feat(cpp-tools): implement 'stats' command"
```

---

### Task 8: `cmd_head` / `cmd_cat` / `cmd_select` (row data)

**Files:**
- Modify: `cpp/tools/format/result_set_format.h` / `.cc` (add offset/limit)
- Modify: `cpp/tools/commands/commands.h` (declare `run_row_query`)
- Create: `cpp/tools/commands/row_query.cc`
- Create: `cpp/tools/commands/cmd_head.cc`, `cmd_cat.cc`, `cmd_select.cc`
- Modify: `cpp/tools/cli/run_cli.cc` (dispatch head/cat/select)
- Modify: `cpp/test/tools/command_e2e_test.cc`

All three row commands share `run_row_query`, which opens a `ResultSet` (time
range honored via `--start/--end`) and pumps it with client-side offset/limit.
`head` defaults `limit` to 10; `cat`/`select` use the parsed `--limit`
(default unlimited).

- [ ] **Step 1: Add offset/limit to `write_result_set`** — change the declaration in `cpp/tools/format/result_set_format.h`:

```cpp
int write_result_set(storage::ResultSet* rs, OutputFormat fmt, bool no_header,
                     std::ostream& out, long long offset = 0,
                     long long limit = -1);
```

and update the definition's loop in `cpp/tools/format/result_set_format.cc` (replace the existing `while` loop and the `RowWriter writer(...)` line onward):

```cpp
  RowWriter writer(out, fmt, header, types, no_header);
  bool has_next = false;
  int code = common::E_OK;
  long long skipped = 0, emitted = 0;
  while ((code = rs->next(has_next)) == common::E_OK && has_next) {
    if (skipped < offset) {
      ++skipped;
      continue;
    }
    if (limit >= 0 && emitted >= limit) break;
    std::vector<std::string> cells(ncol);
    std::vector<bool> nulls(ncol, false);
    for (uint32_t i = 1; i <= ncol; ++i) {
      if (rs->is_null(i)) {
        nulls[i - 1] = true;
      } else {
        cells[i - 1] = cell_to_string(rs, i, types[i - 1]);
      }
    }
    writer.write(cells, nulls);
    ++emitted;
  }
  writer.finish();
  return code;
```

- [ ] **Step 2: Declare `run_row_query`** in `cpp/tools/commands/commands.h` (before the `cmd_*` declarations):

```cpp
// Shared by head/cat/select: opens a row ResultSet (honoring --start/--end and
// --device/--table/--measurements) and writes it with client-side offset/limit.
int run_row_query(const ParsedArgs& args, storage::TsFileReader& reader,
                  OutputFormat fmt, std::ostream& out, std::ostream& err,
                  long long offset, long long limit);
```

- [ ] **Step 3: Create `cpp/tools/commands/row_query.cc`**

```cpp
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/device_id.h"
#include "common/schema.h"
#include "format/result_set_format.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int run_row_query(const ParsedArgs& args, storage::TsFileReader& reader,
                  OutputFormat fmt, std::ostream& out, std::ostream& err,
                  long long offset, long long limit) {
  const int64_t start =
      args.has_start ? static_cast<int64_t>(args.start)
                     : std::numeric_limits<int64_t>::min();
  const int64_t end = args.has_end ? static_cast<int64_t>(args.end)
                                   : std::numeric_limits<int64_t>::max();

  storage::ResultSet* rs = nullptr;
  int qret = 0;

  if (is_table_model(args, reader)) {
    std::string table_name = args.table;
    if (table_name.empty()) {
      auto schemas = reader.get_all_table_schemas();
      if (schemas.empty() || !schemas[0]) {
        err << "Error: no table found in file\n";
        return kExitRuntime;
      }
      table_name = schemas[0]->get_table_name();
    }
    std::vector<std::string> cols = args.measurements;
    if (cols.empty()) {
      auto ts = reader.get_table_schema(table_name);
      if (ts) cols = ts->get_measurement_names();
    }
    qret = reader.query(table_name, cols, start, end, rs);
  } else {
    std::vector<std::string> devices;
    if (!args.device.empty()) {
      devices.push_back(args.device);
    } else {
      for (auto& d : reader.get_all_device_ids()) {
        if (d) devices.push_back(d->get_device_name());
      }
    }
    std::vector<std::string> paths;
    for (const std::string& dev : devices) {
      std::vector<std::string> ms = args.measurements;
      if (ms.empty()) {
        auto did = std::make_shared<storage::StringArrayDeviceID>(dev);
        std::vector<storage::MeasurementSchema> sch;
        if (reader.get_timeseries_schema(did, sch) == 0) {
          for (auto& m : sch) ms.push_back(m.measurement_name_);
        }
      }
      for (const std::string& m : ms) paths.push_back(dev + "." + m);
    }
    if (paths.empty()) {
      err << "Error: no time series found\n";
      return kExitRuntime;
    }
    qret = reader.query(paths, start, end, rs);
  }

  if (qret != 0 || rs == nullptr) {
    err << "Error: query failed (code " << qret << ")\n";
    if (rs != nullptr) reader.destroy_query_data_set(rs);
    return kExitRuntime;
  }

  int wret = write_result_set(rs, fmt, args.no_header, out, offset, limit);
  reader.destroy_query_data_set(rs);
  return wret == 0 ? kExitOk : kExitRuntime;
}

}  // namespace tsfile_cli
```

- [ ] **Step 4: Create `cpp/tools/commands/cmd_head.cc`**

```cpp
#include "commands/commands.h"

namespace tsfile_cli {
int cmd_head(const ParsedArgs& args, storage::TsFileReader& reader,
             OutputFormat fmt, std::ostream& out, std::ostream& err) {
  long long limit = args.limit < 0 ? 10 : args.limit;
  return run_row_query(args, reader, fmt, out, err, args.offset, limit);
}
}  // namespace tsfile_cli
```

- [ ] **Step 5: Create `cpp/tools/commands/cmd_cat.cc`**

```cpp
#include "commands/commands.h"

namespace tsfile_cli {
int cmd_cat(const ParsedArgs& args, storage::TsFileReader& reader,
            OutputFormat fmt, std::ostream& out, std::ostream& err) {
  return run_row_query(args, reader, fmt, out, err, args.offset, args.limit);
}
}  // namespace tsfile_cli
```

- [ ] **Step 6: Create `cpp/tools/commands/cmd_select.cc`**

```cpp
#include "commands/commands.h"

namespace tsfile_cli {
int cmd_select(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err) {
  return run_row_query(args, reader, fmt, out, err, args.offset, args.limit);
}
}  // namespace tsfile_cli
```

- [ ] **Step 7: Wire dispatch** — in `cpp/tools/cli/run_cli.cc`, add three branches before the final `else`:

```cpp
  } else if (p.command == "head") {
    code = cmd_head(p, reader, fmt, out, err);
  } else if (p.command == "cat") {
    code = cmd_cat(p, reader, fmt, out, err);
  } else if (p.command == "select") {
    code = cmd_select(p, reader, fmt, out, err);
```

- [ ] **Step 8: Add e2e tests** — append to `cpp/test/tools/command_e2e_test.cc`

```cpp
namespace {
size_t count_lines(const std::string& s) {
  size_t n = 0;
  for (char c : s) if (c == '\n') ++n;
  return n;
}
}  // namespace

TEST(CliE2E, HeadProjectsAndLimits) {
  Fixture f;
  std::ostringstream out, err;
  int code =
      tsfile_cli::run_cli({"head", "-m", "s1", "-n", "2", "-f", "tsv", f.path},
                          out, err);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(out.str(), "time\ts1\n0\t0\n1\t10\n");
}

TEST(CliE2E, CatReturnsAllRows) {
  Fixture f;
  std::ostringstream out, err;
  int code = tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "tsv", f.path}, out,
                                 err);
  EXPECT_EQ(code, 0);
  // header + 5 data rows
  EXPECT_EQ(count_lines(out.str()), 6u);
  EXPECT_NE(out.str().find("time\ts1\n"), std::string::npos);
}

TEST(CliE2E, SelectWithTimeRange) {
  Fixture f;
  std::ostringstream out, err;
  int code =
      tsfile_cli::run_cli({"select", "-m", "s1", "--start", "2", "--end", "3",
                           "-f", "tsv", f.path},
                          out, err);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(out.str(), "time\ts1\n2\t20\n3\t30\n");
}

TEST(CliE2E, SelectJsonIsNdjson) {
  Fixture f;
  std::ostringstream out, err;
  int code =
      tsfile_cli::run_cli({"select", "-m", "s1", "--start", "0", "--end", "0",
                           "-f", "json", f.path},
                          out, err);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(out.str(), "{\"time\":0,\"s1\":0}\n");
}
```

- [ ] **Step 9: Build and run tests**

Run: `cd cpp && bash build.sh -t=Debug 2>&1 | tail -8 && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.*`
Expected: all CliE2E tests PASS.

> **If a row-order or column-order assertion fails**, print the actual output
> (`./build/Debug/bin/tsfile head -m s1 -n 2 -f tsv examples/test_cpp.tsfile`)
> and align the expected string. The values (ts 0..4, s1 = ts*10) are fixed by
> the fixture; only column/row ordering could differ.

- [ ] **Step 10: Commit**

```bash
git add cpp/tools/format/result_set_format.h cpp/tools/format/result_set_format.cc cpp/tools/commands/commands.h cpp/tools/commands/row_query.cc cpp/tools/commands/cmd_head.cc cpp/tools/commands/cmd_cat.cc cpp/tools/commands/cmd_select.cc cpp/tools/cli/run_cli.cc cpp/test/tools/command_e2e_test.cc
git commit -m "feat(cpp-tools): implement 'head', 'cat', and 'select' row commands"
```

---

### Task 9: stderr fix, `install()`, full-suite run, manual verification

**Files:**
- Modify: `cpp/src/file/read_file.cc` (route open-error prints to stderr)
- Modify: `cpp/tools/CMakeLists.txt` (add `install`)

- [ ] **Step 1: Route open errors to stderr** — in `cpp/src/file/read_file.cc`, change the two `std::cout` lines inside the `if (fd_ < 0)` block (around lines 52-55) to `std::cerr`:

```cpp
    fd_ = ::open(file_path_.c_str(), O_RDONLY);
    if (fd_ < 0) {
        std::cerr << "open file " << file_path << "  error :" << fd_
                  << std::endl;
        std::cerr << "open error" << errno << "  " << strerror(errno)
                  << std::endl;
        return E_FILE_OPEN_ERR;
    }
```

Rationale: a CLI that emits diagnostics on stdout would corrupt `tsfile cat f | jq`. Errors belong on stderr.

- [ ] **Step 2: Run the FULL test suite** to confirm the library change causes no regression

Run: `cd cpp && bash build.sh -t=Debug 2>&1 | tail -5 && ./build/Debug/lib/TsFile_Test 2>&1 | tail -15`
Expected: all suites PASS (existing reader/file tests + the new `RunCliTest`, `ParseArgsTest`, `RowWriterTest`, `*NameTest`, `*EscapeTest`, `CliE2E`).

- [ ] **Step 3: Add `install()`** to the end of `cpp/tools/CMakeLists.txt`

```cmake
install(TARGETS tsfile_cli RUNTIME DESTINATION bin)
```

- [ ] **Step 4: Manual verification against the bundled example** (table-model file)

Run each and confirm behavior:

```bash
cd cpp
BIN=./build/Debug/bin/tsfile
F=examples/test_cpp.tsfile
$BIN ls -f tsv $F                 # -> name / table1
$BIN schema -f tsv $F             # -> header + rows incl. s1 INT64
$BIN stats -f tsv $F              # -> count/start/end per series
$BIN head -n 3 -f tsv $F          # -> header + 3 rows
$BIN cat -f csv $F | head -n 3    # -> CSV, pipe-clean (no log noise on stdout)
$BIN select -m s1 -f json $F      # -> NDJSON: one {"time":..,"s1":..} per line
$BIN cat $F                       # -> aligned table form (stdout is a TTY)
echo "exit on missing:"; $BIN ls nope.tsfile; echo "rc=$?"   # rc=2, error on stderr
```

Expected: data on stdout, diagnostics on stderr, exit codes per the table; the TTY run shows aligned columns while the piped run shows TSV/CSV.

- [ ] **Step 5: Tree-model manual check (only if a tree-model `.tsfile` is available)**

The automated e2e fixture is table-model. If you have a tree-model file (e.g. produced by `TsFileWriter::write_tree`), verify the tree branch:

```bash
$BIN ls -f tsv <tree.tsfile>          # -> device names, one per line
$BIN schema -f tsv <tree.tsfile>      # -> datatype + encoding + compression filled
$BIN cat -d <device> -m <m1> -f tsv <tree.tsfile>
```

If unavailable, note it in the PR description as untested-by-CI and rely on the shared `run_row_query`/formatter coverage from the table-model tests.

- [ ] **Step 6: Format check**

Run: `cd /Users/zhanghongyin/iotdb/tsfile && ./mvnw spotless:apply -P with-cpp 2>&1 | tail -5 && ./mvnw spotless:check -P with-cpp 2>&1 | tail -5`
Expected: clang-format applies cleanly; check passes. (Or run `clang-format -i` over `cpp/tools/**` and `cpp/test/tools/**` if invoking Maven is impractical locally.)

- [ ] **Step 7: Commit**

```bash
git add cpp/src/file/read_file.cc cpp/tools/CMakeLists.txt
git commit -m "feat(cpp-tools): install tsfile binary; route open errors to stderr"
```

---

## Plan self-review (spec coverage)

| Spec requirement | Covered by |
|---|---|
| Single multi-call `tsfile` binary, git-style dispatch | Task 1 (CMake `OUTPUT_NAME tsfile`, run_cli dispatch) |
| `ls` / `schema` / `stats` / `head` / `cat` / `select` | Tasks 5 / 6 / 7 / 8 |
| Hand-rolled arg parsing, no new deps | Task 2 |
| Data→stdout, diagnostics→stderr | Injected `out`/`err` everywhere; Task 9 lib fix |
| Exit codes 0/1/2/3 | `exit_codes.h` (Task 1); mapped in run_cli (Tasks 1, 5, 8) |
| TTY-adaptive default; `--format csv/tsv/json/table` | `resolve_format` (Task 3); run_cli isatty (Task 5) |
| CSV RFC-4180 quoting; NDJSON; null handling | `csv_escape`/`RowWriter`/`json_escape` (Task 3) |
| tree/table auto-detect + `--model` override | `is_table_model` (Task 5) |
| schema blanks encoding/compression for table model | `cmd_schema` enrichment branch (Task 6) |
| Timestamps as raw epoch | `cell_to_string` INT64 path (Task 4) |
| `BUILD_TOOLS` option; `install()` | Task 1 (option), Task 9 (install) |
| Tests: cli_args, formatters, model detect, e2e | Tasks 2, 3, 5-8 |
| License headers on new files | Conventions section + every Create step |

**Placeholder scan:** no `TBD`/`TODO`/"implement later" remain; the "filled in by later tasks" branch in run_cli is replaced concretely in Tasks 6-8. **Type consistency:** `ParsedArgs`, `OutputFormat`, `RowWriter` ctor (`out, fmt, header, types, no_header`), `write_result_set(rs, fmt, no_header, out, offset, limit)`, and the `cmd_*`/`run_row_query` signatures are used identically across all tasks.

**Known residual risks (validated during execution, not blockers):**
1. `get_timeseries_metadata()` yielding rows for table-model files — Task 6/7 notes give a fallback.
2. Exact column/row ordering in row-command output — Tasks 7/8 notes give the adjust-the-string fallback; values are fixture-guaranteed.
3. Fixture compile relying on transitive includes — Task 5 note lists the explicit headers to add.
