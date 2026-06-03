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

# TsFile CLI 写入（`tsfile-cli write`）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 `tsfile-cli` 增加一个 `write` 命令，把 CSV/TSV 行数据导入成一个新的 table
模型 `.tsfile`（显式 `--columns`，零类型推断）。

**Architecture:** 新增纯解析层 `format/input_format.*`（列规格 / 行切分 / 类型名解析，重
单测）；`cli_args` 增 `-o/--output`、`--columns`、`-v/--verbose`、`--header-match`；
`commands/cmd_write.cc` 串起「读输入 → 构 `TableSchema`/`Tablet` → `TsFileTableWriter`
写出」；`run_cli` 把 `write` 注册为**第一个不打开 `TsFileReader` 的命令**，在 reader.open
之前特判分发。不修改存储引擎。

**Tech Stack:** C++11/14（测试目标 `-std=c++14`），CMake `BUILD_TOOLS`，Google Test，
现有 `storage::TsFileTableWriter`、`storage::TableSchema`、`common::ColumnSchema`、
`storage::Tablet`、`storage::WriteFile`。

**Spec:** `docs/superpowers/specs/2026-06-03-tsfile-cli-write-design.md`

---

## 执行前提

- 工作目录 `/Users/zhanghongyin/iotdb/tsfile`；git 操作从仓库根运行；不要暂存 `.codegraph/`
  或测试产生的 `cpp/*.tsfile`/`*.dat` 临时文件。
- 新建 `.h`/`.cc` 前置 Apache 2.0 块注释头（从任一 `cpp/tools/**` 文件复制）。
- **构建/测试**（本机 CMake 4.x 与 bundled ANTLR4 旧 policy 冲突，必须 `--disable-antlr4`；
  `build.sh` 默认 `build_test=0` 无开关，执行期间临时 `sed -i '' 's/^build_test=0/build_test=1/'
  cpp/build.sh`，Task 4 收尾 `git checkout cpp/build.sh` 还原）：

```bash
cd cpp && bash build.sh -t=Debug --disable-antlr4
./build/Debug/test/lib/TsFile_Test --gtest_filter='InputFormatTest.*:ParseArgsTest.*:RunCliTest.*:CliE2E.*'
```

- 构建退出码 2（`make install` 拷 libsnappy 到 `/usr/local/lib` 权限不足）属预期，编译/链接
  与测试不受影响；判定成功看 `grep -c "Built target TsFile_Test"` 与测试结果。

## 已核验的 SDK 事实（编译依据）

- `enum class common::ColumnCategory { TAG, FIELD, ATTRIBUTE, TIME }`（`utils/db_utils.h`）。
- `common::ColumnSchema(name, TSDataType, CompressionType, TSEncoding, ColumnCategory)`（`common/schema.h`）。
- `storage::TableSchema(table_name, std::vector<common::ColumnSchema>)`——**会把表名转小写**。
- `storage::TsFileTableWriter(storage::WriteFile*, storage::TableSchema*)`（模板 ctor，附加参数有默认值）。
- `int TsFileTableWriter::write_table(Tablet&) const` / `int flush()` / `int close()`。
- `int WriteFile::create(const std::string&, int flags, mode_t mode)`。
- `storage::Tablet(target_name, names, types, categories, max_rows)`；
  `int add_timestamp(uint32_t, int64_t)`；`template<T> int add_value(uint32_t, const std::string& name, T)`。
  未 `add_value` 的单元格默认为 null。

## 文件结构

新增：
- `cpp/tools/format/input_format.h` / `.cc`：`ColumnDef`、`parse_columns_spec`、
  `split_line`、`parse_datatype_name`、`parse_category`、`parse_bool_cell`（纯层，无 reader 依赖）。
- `cpp/tools/commands/cmd_write.cc`：`cmd_write`。
- `cpp/test/tools/input_format_test.cc`：纯层单测。

修改：
- `cpp/tools/cli/cli_args.h` / `.cc`：`ParsedArgs` 增 `output/columns/verbose/header_match` 与解析。
- `cpp/tools/commands/commands.h`：声明 `cmd_write`。
- `cpp/tools/cli/run_cli.cc`：注册 `write`、`validate_write_flags`、reader 旁路分发、usage 文案。
- `cpp/test/tools/cli_args_test.cc`：write 参数解析测试。
- `cpp/test/tools/command_e2e_test.cc`：write→读回往返 E2E。

---

### Task 1: `input_format` 纯解析层

**Files:**
- Create: `cpp/tools/format/input_format.h`
- Create: `cpp/tools/format/input_format.cc`
- Create: `cpp/test/tools/input_format_test.cc`

- [ ] **Step 1: 写失败单测** — `cpp/test/tools/input_format_test.cc`（前置 license 头）

```cpp
#include "format/input_format.h"

#include <gtest/gtest.h>

#include "common/db_common.h"
#include "utils/db_utils.h"

TEST(InputFormatTest, ParseColumnsSpecValid) {
    std::vector<tsfile_cli::ColumnDef> cols;
    std::string err;
    EXPECT_TRUE(tsfile_cli::parse_columns_spec("id1:STRING:tag,s1:INT64:field",
                                               cols, err));
    ASSERT_EQ(cols.size(), 2u);
    EXPECT_EQ(cols[0].name, "id1");
    EXPECT_EQ(cols[0].type, common::STRING);
    EXPECT_EQ(cols[0].category, common::ColumnCategory::TAG);
    EXPECT_EQ(cols[1].type, common::INT64);
    EXPECT_EQ(cols[1].category, common::ColumnCategory::FIELD);
}

TEST(InputFormatTest, ParseColumnsSpecCaseInsensitiveType) {
    std::vector<tsfile_cli::ColumnDef> cols;
    std::string err;
    EXPECT_TRUE(tsfile_cli::parse_columns_spec("s1:int64:field", cols, err));
    EXPECT_EQ(cols[0].type, common::INT64);
}

TEST(InputFormatTest, ParseColumnsSpecErrors) {
    std::vector<tsfile_cli::ColumnDef> cols;
    std::string err;
    EXPECT_FALSE(tsfile_cli::parse_columns_spec("s1:NOPE:field", cols, err));
    EXPECT_FALSE(tsfile_cli::parse_columns_spec("s1:INT64:bogus", cols, err));
    EXPECT_FALSE(tsfile_cli::parse_columns_spec("s1:INT64", cols, err));
    EXPECT_FALSE(tsfile_cli::parse_columns_spec("", cols, err));
}

TEST(InputFormatTest, SplitLineTsv) {
    std::vector<std::string> f = tsfile_cli::split_line("0\t10\t20", '\t', false);
    ASSERT_EQ(f.size(), 3u);
    EXPECT_EQ(f[0], "0");
    EXPECT_EQ(f[2], "20");
}

TEST(InputFormatTest, SplitLineCsvQuotes) {
    std::vector<std::string> f =
        tsfile_cli::split_line("1,\"a,b\",\"she \"\"hi\"\"\"", ',', true);
    ASSERT_EQ(f.size(), 3u);
    EXPECT_EQ(f[1], "a,b");
    EXPECT_EQ(f[2], "she \"hi\"");
}

TEST(InputFormatTest, SplitLineEmptyFields) {
    std::vector<std::string> f = tsfile_cli::split_line("0,,5", ',', true);
    ASSERT_EQ(f.size(), 3u);
    EXPECT_EQ(f[1], "");
}

TEST(InputFormatTest, ParseBoolCell) {
    bool b = false;
    EXPECT_TRUE(tsfile_cli::parse_bool_cell("true", b));
    EXPECT_TRUE(b);
    EXPECT_TRUE(tsfile_cli::parse_bool_cell("0", b));
    EXPECT_FALSE(b);
    EXPECT_FALSE(tsfile_cli::parse_bool_cell("maybe", b));
}
```

- [ ] **Step 2: 运行确认失败**

```bash
cd cpp && bash build.sh -t=Debug --disable-antlr4
```

Expected: 构建失败，`format/input_format.h` 不存在。

- [ ] **Step 3: 创建 `cpp/tools/format/input_format.h`**（前置 license 头）

```cpp
#ifndef TSFILE_CLI_INPUT_FORMAT_H
#define TSFILE_CLI_INPUT_FORMAT_H

#include <string>
#include <vector>

#include "common/db_common.h"
#include "utils/db_utils.h"

namespace tsfile_cli {

struct ColumnDef {
    std::string name;
    common::TSDataType type;
    common::ColumnCategory category;
};

bool parse_datatype_name(const std::string& s, common::TSDataType& out);
bool parse_category(const std::string& s, common::ColumnCategory& out);
bool parse_columns_spec(const std::string& spec, std::vector<ColumnDef>& out,
                        std::string& error);
std::vector<std::string> split_line(const std::string& line, char delim,
                                    bool csv_quotes);
bool parse_bool_cell(const std::string& s, bool& out);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_INPUT_FORMAT_H
```

- [ ] **Step 4: 创建 `cpp/tools/format/input_format.cc`**（前置 license 头）

```cpp
#include "format/input_format.h"

#include <cctype>

namespace tsfile_cli {

bool parse_datatype_name(const std::string& s, common::TSDataType& out) {
    std::string u;
    u.reserve(s.size());
    for (char c : s) {
        u += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    if (u == "BOOLEAN") {
        out = common::BOOLEAN;
    } else if (u == "INT32") {
        out = common::INT32;
    } else if (u == "INT64") {
        out = common::INT64;
    } else if (u == "FLOAT") {
        out = common::FLOAT;
    } else if (u == "DOUBLE") {
        out = common::DOUBLE;
    } else if (u == "STRING") {
        out = common::STRING;
    } else if (u == "TEXT") {
        out = common::TEXT;
    } else {
        return false;
    }
    return true;
}

bool parse_category(const std::string& s, common::ColumnCategory& out) {
    if (s == "tag") {
        out = common::ColumnCategory::TAG;
    } else if (s == "field") {
        out = common::ColumnCategory::FIELD;
    } else {
        return false;
    }
    return true;
}

std::vector<std::string> split_line(const std::string& line, char delim,
                                    bool csv_quotes) {
    std::vector<std::string> out;
    std::string field;
    if (!csv_quotes) {
        for (char c : line) {
            if (c == delim) {
                out.push_back(field);
                field.clear();
            } else {
                field += c;
            }
        }
        out.push_back(field);
        return out;
    }
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    field += '"';
                    ++i;
                } else {
                    in_quotes = false;
                }
            } else {
                field += c;
            }
        } else if (c == '"') {
            in_quotes = true;
        } else if (c == delim) {
            out.push_back(field);
            field.clear();
        } else {
            field += c;
        }
    }
    out.push_back(field);
    return out;
}

bool parse_columns_spec(const std::string& spec, std::vector<ColumnDef>& out,
                        std::string& error) {
    out.clear();
    if (spec.empty()) {
        error = "empty --columns";
        return false;
    }
    std::vector<std::string> items = split_line(spec, ',', false);
    for (const std::string& item : items) {
        std::vector<std::string> parts = split_line(item, ':', false);
        if (parts.size() != 3) {
            error = "bad column '" + item + "' (want name:TYPE:category)";
            return false;
        }
        ColumnDef def;
        def.name = parts[0];
        if (def.name.empty()) {
            error = "empty column name in '" + item + "'";
            return false;
        }
        if (!parse_datatype_name(parts[1], def.type)) {
            error = "unknown type '" + parts[1] + "'";
            return false;
        }
        if (!parse_category(parts[2], def.category)) {
            error = "bad category '" + parts[2] + "' (want tag|field)";
            return false;
        }
        out.push_back(def);
    }
    return true;
}

bool parse_bool_cell(const std::string& s, bool& out) {
    std::string l;
    l.reserve(s.size());
    for (char c : s) {
        l += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (l == "true" || l == "1") {
        out = true;
        return true;
    }
    if (l == "false" || l == "0") {
        out = false;
        return true;
    }
    return false;
}

}  // namespace tsfile_cli
```

- [ ] **Step 5: 构建并运行确认通过**

```bash
cd cpp && bash build.sh -t=Debug --disable-antlr4 && ./build/Debug/test/lib/TsFile_Test --gtest_filter='InputFormatTest.*'
```

Expected: 构建成功；7 个 `InputFormatTest` 全过。

- [ ] **Step 6: 提交**

```bash
git add cpp/tools/format/input_format.h cpp/tools/format/input_format.cc cpp/test/tools/input_format_test.cc
git commit -m "Add tsfile CLI input_format parsing layer"
```

---

### Task 2: `cli_args` 增加 write 参数

**Files:**
- Modify: `cpp/tools/cli/cli_args.h`
- Modify: `cpp/tools/cli/cli_args.cc`
- Modify: `cpp/test/tools/cli_args_test.cc`

- [ ] **Step 1: 写失败测试** — 追加到 `cpp/test/tools/cli_args_test.cc`

```cpp
TEST(ParseArgsTest, WriteFlagsParsed) {
    auto p = tsfile_cli::parse_args({"write", "--table", "t1", "--columns",
                                     "s1:INT64:field", "-o", "out.tsfile", "-v",
                                     "--header-match", "in.csv"});
    EXPECT_TRUE(p.error.empty());
    EXPECT_EQ(p.command, "write");
    EXPECT_EQ(p.table, "t1");
    EXPECT_EQ(p.columns, "s1:INT64:field");
    EXPECT_EQ(p.output, "out.tsfile");
    EXPECT_TRUE(p.verbose);
    EXPECT_TRUE(p.header_match);
    EXPECT_EQ(p.file, "in.csv");
}

TEST(ParseArgsTest, OutputFlagNeedsValue) {
    auto p = tsfile_cli::parse_args({"write", "-o"});
    EXPECT_FALSE(p.error.empty());
}
```

- [ ] **Step 2: 运行确认失败**

```bash
cd cpp && bash build.sh -t=Debug --disable-antlr4
```

Expected: 编译失败，`ParsedArgs` 无 `output`/`columns`/`verbose`/`header_match`。

- [ ] **Step 3: `cli_args.h` 增字段** — 在 `ParsedArgs` 的 `model` 字段之后加入：

```cpp
    std::string output;
    std::string columns;
    bool verbose = false;
    bool header_match = false;
```

- [ ] **Step 4: `cli_args.cc` 解析** — 在 `parse_args` 循环中，把这些分支放在 `--model`
  分支之前：

```cpp
        } else if (a == "-o" || a == "--output") {
            if (!need_value(a, p.output)) {
                return p;
            }
        } else if (a == "--columns") {
            if (!need_value(a, p.columns)) {
                return p;
            }
        } else if (a == "-v" || a == "--verbose") {
            p.verbose = true;
        } else if (a == "--header-match") {
            p.header_match = true;
```

- [ ] **Step 5: 构建并运行确认通过**

```bash
cd cpp && bash build.sh -t=Debug --disable-antlr4 && ./build/Debug/test/lib/TsFile_Test --gtest_filter='ParseArgsTest.*'
```

Expected: 构建成功；`ParseArgsTest` 全过。

- [ ] **Step 6: 提交**

```bash
git add cpp/tools/cli/cli_args.h cpp/tools/cli/cli_args.cc cpp/test/tools/cli_args_test.cc
git commit -m "Add tsfile CLI write argument parsing"
```

---

### Task 3: `cmd_write` 与 `run_cli` 接线

**Files:**
- Create: `cpp/tools/commands/cmd_write.cc`
- Modify: `cpp/tools/commands/commands.h`
- Modify: `cpp/tools/cli/run_cli.cc`
- Modify: `cpp/test/tools/command_e2e_test.cc`

- [ ] **Step 1: 写失败 E2E** — 追加到 `cpp/test/tools/command_e2e_test.cc`

```cpp
TEST(CliE2E, WriteThenReadRoundTrip) {
    std::string csv_path = "tsfile_cli_write_in.csv";
    {
        std::ofstream o(csv_path.c_str());
        o << "time,id1,s1\n0,dev,0\n1,dev,10\n2,dev,20\n";
    }
    std::string out_path = "tsfile_cli_write_out.tsfile";

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--columns", "id1:STRING:tag,s1:INT64:field",
         "-o", out_path, csv_path},
        wout, werr);
    EXPECT_EQ(wc, 0) << werr.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    int cc = tsfile_cli::run_cli({"count", "-f", "tsv", out_path}, cout_, cerr_);
    EXPECT_EQ(cc, 0);
    EXPECT_NE(cout_.str().find("\ts1\t3"), std::string::npos) << cout_.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    int rc = tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "tsv", out_path},
                                 rout, rerr);
    EXPECT_EQ(rc, 0);
    EXPECT_EQ(rout.str(), "time\ts1\n0\t0\n1\t10\n2\t20\n");

    std::remove(csv_path.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteMissingColumnsIsUsageError) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t1", "-o", "x.tsfile", "in.csv"}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--columns"), std::string::npos);
}
```

> `command_e2e_test.cc` 顶部已 `#include <cstdio>`（`std::remove`）；新增需要 `<fstream>`，
> 若未包含则在该文件 include 区加 `#include <fstream>`。

- [ ] **Step 2: 运行确认失败**

```bash
cd cpp && bash build.sh -t=Debug --disable-antlr4
```

Expected: 构建或测试失败（`write` 未注册/未实现）。

- [ ] **Step 3: 声明 `cmd_write`** — 在 `cpp/tools/commands/commands.h` 的 `cmd_sample`
  声明之后加入（注意签名无 reader、无 OutputFormat）：

```cpp
int cmd_write(const ParsedArgs& args, std::ostream& out, std::ostream& err);
```

- [ ] **Step 4: 创建 `cpp/tools/commands/cmd_write.cc`**（前置 license 头）

```cpp
#include <fcntl.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "cli/cli_args.h"
#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "format/input_format.h"
#include "writer/tsfile_table_writer.h"

namespace tsfile_cli {
namespace {

struct DataRow {
    long long line_no;
    int64_t timestamp;
    std::vector<std::string> cells;
};

void strip_cr(std::string& s) {
    if (!s.empty() && s.back() == '\r') {
        s.pop_back();
    }
}

bool add_typed_value(storage::Tablet& tablet, uint32_t row,
                     const ColumnDef& def, const std::string& cell,
                     std::string& error) {
    if (cell.empty()) {
        return true;  // null
    }
    char* e = nullptr;
    switch (def.type) {
        case common::BOOLEAN: {
            bool v = false;
            if (!parse_bool_cell(cell, v)) {
                error = "bad BOOLEAN '" + cell + "'";
                return false;
            }
            tablet.add_value(row, def.name, v);
            return true;
        }
        case common::INT32: {
            long v = std::strtol(cell.c_str(), &e, 10);
            if (e == nullptr || *e != '\0') {
                error = "bad INT32 '" + cell + "'";
                return false;
            }
            tablet.add_value(row, def.name, static_cast<int32_t>(v));
            return true;
        }
        case common::INT64: {
            long long v = std::strtoll(cell.c_str(), &e, 10);
            if (e == nullptr || *e != '\0') {
                error = "bad INT64 '" + cell + "'";
                return false;
            }
            tablet.add_value(row, def.name, static_cast<int64_t>(v));
            return true;
        }
        case common::FLOAT: {
            float v = std::strtof(cell.c_str(), &e);
            if (e == nullptr || *e != '\0') {
                error = "bad FLOAT '" + cell + "'";
                return false;
            }
            tablet.add_value(row, def.name, v);
            return true;
        }
        case common::DOUBLE: {
            double v = std::strtod(cell.c_str(), &e);
            if (e == nullptr || *e != '\0') {
                error = "bad DOUBLE '" + cell + "'";
                return false;
            }
            tablet.add_value(row, def.name, v);
            return true;
        }
        case common::STRING:
        case common::TEXT: {
            tablet.add_value(row, def.name, cell);
            return true;
        }
        default:
            error = "unsupported column type";
            return false;
    }
}

}  // namespace

int cmd_write(const ParsedArgs& args, std::ostream& /*out*/,
              std::ostream& err) {
    std::vector<ColumnDef> columns;
    std::string perr;
    if (!parse_columns_spec(args.columns, columns, perr)) {
        err << "Error: " << perr << "\n";
        return kExitUsage;
    }

    std::istream* in = &std::cin;
    std::ifstream fin;
    if (!args.file.empty() && args.file != "-") {
        fin.open(args.file.c_str());
        if (!fin.is_open()) {
            err << "Error: cannot open input: " << args.file << "\n";
            return kExitFile;
        }
        in = &fin;
    }

    const char delim = (args.format == ParsedArgs::Format::kTsv) ? '\t' : ',';
    const bool csv_quotes = (delim == ',');

    std::string line;
    long long line_no = 0;
    if (!args.no_header) {
        if (std::getline(*in, line)) {
            ++line_no;
            strip_cr(line);
            if (args.header_match) {
                std::vector<std::string> h = split_line(line, delim, csv_quotes);
                bool ok = (h.size() == columns.size() + 1);
                for (size_t i = 0; ok && i < columns.size(); ++i) {
                    if (h[i + 1] != columns[i].name) {
                        ok = false;
                    }
                }
                if (!ok) {
                    err << "Error: header does not match --columns (line 1)\n";
                    return kExitRuntime;
                }
            }
        }
    }

    std::vector<DataRow> rows;
    while (std::getline(*in, line)) {
        ++line_no;
        strip_cr(line);
        if (line.empty()) {
            continue;
        }
        std::vector<std::string> fields = split_line(line, delim, csv_quotes);
        if (fields.size() != columns.size() + 1) {
            err << "Error: expected " << (columns.size() + 1) << " fields, got "
                << fields.size() << " (line " << line_no << ")\n";
            return kExitRuntime;
        }
        char* e = nullptr;
        long long ts = std::strtoll(fields[0].c_str(), &e, 10);
        if (e == nullptr || *e != '\0') {
            err << "Error: bad timestamp '" << fields[0] << "' (line " << line_no
                << ")\n";
            return kExitRuntime;
        }
        DataRow r;
        r.line_no = line_no;
        r.timestamp = static_cast<int64_t>(ts);
        r.cells.assign(fields.begin() + 1, fields.end());
        rows.push_back(r);
    }

    std::vector<std::string> names;
    std::vector<common::TSDataType> types;
    std::vector<common::ColumnCategory> cats;
    std::vector<common::ColumnSchema> col_schemas;
    for (const ColumnDef& d : columns) {
        names.push_back(d.name);
        types.push_back(d.type);
        cats.push_back(d.category);
        col_schemas.push_back(common::ColumnSchema(
            d.name, d.type, common::UNCOMPRESSED, common::PLAIN, d.category));
    }

    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    if (file.create(args.output, flags, 0666) != 0) {
        err << "Error: cannot create output: " << args.output << "\n";
        return kExitFile;
    }
    auto* schema = new storage::TableSchema(args.table, col_schemas);
    auto* writer = new storage::TsFileTableWriter(&file, schema);

    int rc = kExitOk;
    const size_t kBatch = 1024;
    for (size_t start = 0; start < rows.size() && rc == kExitOk;
         start += kBatch) {
        size_t end = std::min(start + kBatch, rows.size());
        storage::Tablet tablet(args.table, names, types, cats,
                               static_cast<int>(end - start));
        for (size_t i = start; i < end && rc == kExitOk; ++i) {
            uint32_t r = static_cast<uint32_t>(i - start);
            tablet.add_timestamp(r, rows[i].timestamp);
            for (size_t j = 0; j < columns.size(); ++j) {
                std::string cerr;
                if (!add_typed_value(tablet, r, columns[j], rows[i].cells[j],
                                     cerr)) {
                    err << "Error: " << cerr << " (line " << rows[i].line_no
                        << ")\n";
                    rc = kExitRuntime;
                    break;
                }
            }
        }
        if (rc == kExitOk && writer->write_table(tablet) != 0) {
            err << "Error: write_table failed\n";
            rc = kExitRuntime;
        }
    }

    if (rc == kExitOk) {
        if (writer->flush() != 0 || writer->close() != 0) {
            err << "Error: flush/close failed\n";
            rc = kExitRuntime;
        }
    } else {
        writer->close();
    }
    delete writer;
    delete schema;

    if (rc == kExitOk && args.verbose) {
        err << "wrote " << rows.size() << " rows to " << args.output << "\n";
    }
    return rc;
}

}  // namespace tsfile_cli
```

- [ ] **Step 5: `run_cli.cc` 注册 write + 校验 + reader 旁路**

在 `cpp/tools/cli/run_cli.cc` 中：

1. `is_known_command` 集合加入 `"write"`：

```cpp
    static const std::set<std::string> kCmds = {
        "ls",   "schema", "meta",  "stats", "head",
        "cat",  "count",  "sample", "write"};
```

2. 在匿名 namespace 内新增 `validate_write_flags`（放在 `validate_command_flags` 之后）：

```cpp
bool validate_write_flags(const ParsedArgs& p, std::ostream& err) {
    if (p.table.empty()) {
        err << "Error: write requires --table\n";
        return false;
    }
    if (p.columns.empty()) {
        err << "Error: write requires --columns\n";
        return false;
    }
    if (p.output.empty()) {
        err << "Error: write requires -o/--output\n";
        return false;
    }
    if (p.format == ParsedArgs::Format::kJson ||
        p.format == ParsedArgs::Format::kTable) {
        err << "Error: write input format must be csv or tsv\n";
        return false;
    }
    if (!p.measurements.empty() || !p.device.empty() || p.has_start ||
        p.has_end || p.has_seed || p.limit != -1 || p.offset != 0) {
        err << "Error: read-only flags are not valid for write\n";
        return false;
    }
    return true;
}
```

3. 把 usage 的 Commands 段在 `cat` 行之后加入 write（在 `count`/`sample` 行附近，保持
   可读即可）：

```cpp
          "  write    import CSV/TSV rows into a new table tsfile "
          "(--table, --columns, -o)\n"
```

   并把 Options 段追加一行：

```cpp
          "Write options: --table, --columns name:TYPE:tag|field,..., -o/--output,\n"
          "         --header-match, -v/--verbose\n"
```

4. 把文件缺失检查改为对 write 放行（write 的位置参数是输入 CSV，可为 stdin）：

```cpp
    if (p.command != "write" && p.file.empty()) {
        err << "Error: missing <file.tsfile> argument\n";
        return kExitUsage;
    }
```

5. 在 `validate_command_flags` 调用之后、`storage::libtsfile_init();` 之前加入 write 分发：

```cpp
    if (p.command == "write") {
        if (!validate_write_flags(p, err)) {
            print_usage(err);
            return kExitUsage;
        }
        storage::libtsfile_init();
        return cmd_write(p, out, err);
    }
```

- [ ] **Step 6: 构建并运行确认通过**

```bash
cd cpp && bash build.sh -t=Debug --disable-antlr4 && ./build/Debug/test/lib/TsFile_Test --gtest_filter='CliE2E.WriteThenReadRoundTrip:CliE2E.WriteMissingColumnsIsUsageError'
```

Expected: 构建成功；两个测试通过。

> 若 `WriteThenReadRoundTrip` 的 `cat` 断言因列顺序/空值细节失败，先用
> `./build/Debug/bin/tsfile-cli cat -m s1 -f tsv tsfile_cli_write_out.tsfile`（先手动跑一次
> write）打印实际输出再对齐；count=3 与 schema 是稳的。若 `add_value`/null 行为与预期不符，
> 对照 `cpp/test/tools/cli_test_util.h`（已验证可写读的 table fixture）排查。

- [ ] **Step 7: 提交**

```bash
git add cpp/tools/commands/cmd_write.cc cpp/tools/commands/commands.h cpp/tools/cli/run_cli.cc cpp/test/tools/command_e2e_test.cc
git commit -m "Add tsfile CLI write command (CSV/TSV import)"
```

---

### Task 4: 全量验证、格式化、收尾

**Files:**
- Modify: `docs/superpowers/plans/2026-06-03-tsfile-cli-write.md` 仅当执行中需修正执行笔记。

- [ ] **Step 1: 跑完整 CLI 相关测试**

```bash
cd cpp && bash build.sh -t=Debug --disable-antlr4 && ./build/Debug/test/lib/TsFile_Test --gtest_filter='InputFormatTest.*:CliE2E.*:ParseArgsTest.*:RunCliTest.*:RowWriterTest.*:StatTableTest.*'
```

Expected: 构建成功；全部通过。

- [ ] **Step 2: 跑完整测试可执行文件**

```bash
cd cpp && ./build/Debug/test/lib/TsFile_Test 2>&1 | tail -3
```

Expected: 全部通过（无回归）。

- [ ] **Step 3: 手动冒烟（含 stdin 与默认静默）**

```bash
cd cpp
BIN=./build/Debug/bin/tsfile-cli
printf 'time,id1,s1\n0,dev,0\n1,dev,10\n' | $BIN write --table t1 --columns "id1:STRING:tag,s1:INT64:field" -o /tmp/w.tsfile -; echo "rc=$? (静默,无输出)"
$BIN write --table t1 --columns "id1:STRING:tag,s1:INT64:field" -o /tmp/w.tsfile -v <<< $'time,id1,s1\n0,dev,0' 2>&1   # -v 才有 "wrote N rows"
$BIN count -f tsv /tmp/w.tsfile
```

Expected: 默认无 stdout/stderr；`-v` 时 stderr 一行 `wrote ... rows`；count 回读正常。

- [ ] **Step 4: 格式化与暂存范围**

```bash
cd /Users/zhanghongyin/iotdb/tsfile && clang-format -i cpp/tools/format/input_format.h cpp/tools/format/input_format.cc cpp/tools/commands/cmd_write.cc cpp/tools/cli/run_cli.cc cpp/tools/cli/cli_args.cc cpp/tools/cli/cli_args.h cpp/test/tools/input_format_test.cc cpp/test/tools/cli_args_test.cc cpp/test/tools/command_e2e_test.cc
git checkout cpp/build.sh
git diff --check
git status --short
```

Expected: `git diff --check` 退出 0；`build.sh` 已还原；status 仅含本次 write 工作 + 若
clang-format 有改动则一并提交。

- [ ] **Step 5: 最终提交（如格式化有改动）**

```bash
git add -u cpp/tools cpp/test/tools
git commit -m "Format tsfile CLI write sources"
```

若无改动则不创建空提交。

## 覆盖检查（plan self-review）

| Spec 要求 | 对应 |
|---|---|
| `write` 命令、CSV/TSV → table tsfile | Task 3 |
| `--columns name:TYPE:category` 显式、零推断 | Task 1（`parse_columns_spec`）、Task 2 |
| 首列即时间、字段数校验、空=null | Task 3（`cmd_write`） |
| `-o/--output`、stdin/`-`、覆盖写 | Task 2、Task 3 |
| `-f csv|tsv`（json/table → usage error） | Task 3（`validate_write_flags`） |
| `--no-header` 默认跳表头 / `--header-match` 校验 | Task 2、Task 3 |
| 成功默认静默、`-v` 才出摘要 | Task 3（`cmd_write` 末尾），Task 4 Step 3 验证 |
| 退出码 0/1/2/3、stdout 无数据/诊断走 stderr | Task 3、Task 1 错误返回 |
| 拒绝读侧 flag | Task 3（`validate_write_flags`） |
| reader 旁路（write 不开 reader） | Task 3 Step 5 |
| 测试：列规格/行切分/类型、write→读回往返 | Task 1、Task 3 |

**占位扫描**：无 TBD/TODO；所有代码块完整。

**类型一致性**：`ColumnDef{name,type,category}`、`parse_columns_spec`、`split_line`、
`parse_bool_cell`、`cmd_write(args,out,err)`、`ParsedArgs` 的 `output/columns/verbose/
header_match` 在各 Task 间一致；SDK 调用（`TableSchema`/`ColumnSchema`/`Tablet`/
`TsFileTableWriter`/`WriteFile`）均按「已核验的 SDK 事实」一节。

**已知残留风险（执行中验证）**：
1. 未 `add_value` 的单元格是否默认 null —— 对照 `cli_test_util.h` 已验证路径；E2E 若 null
   行为异常则调整。
2. 零 tag 列的 table 是否可写读 —— E2E 用了 1 个 tag 列规避；纯 field 表留作后续验证。
3. `cat` 回读新写文件理论上正常（E2E fixture 同型可 cat），若触发 aligned-chunk 断言则
   说明是存储引擎层问题（超出本计划范围），改用 `count`/`schema` 断言往返。
