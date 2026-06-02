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

# TsFile CLI（`tsfile`）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 从当前「半迁移」工作树出发，把 C++ `tsfile` CLI 收尾为完整的只读 8 动词工具
（`ls / schema / meta / stats / head / cat / count / sample`），并清掉残留的 `select`
死代码，使整套实现可构建、测试通过、可提交。

**Architecture:** 保留现有 `cpp/tools/` 分层：`cli/` 负责参数解析与分发，`commands/`
负责读 metadata 或 row query，`format/` 负责 `RowWriter` 与 `ResultSet` 输出。新增
`stat_table.*` 复用 `Statistic` 格式化逻辑给 `stats`/`count`/`meta` 共用；新增
sampled result-set writer 复用现有 cell extraction。不修改存储引擎。

**Tech Stack:** C++11/C++14 兼容代码（测试目标 `-std=c++14`），CMake `BUILD_TOOLS`，
Google Test 1.12.1，现有 `storage::TsFileReader`、`storage::Statistic`、`RowWriter`、
`write_result_set`。

**Spec:** `docs/superpowers/specs/2026-06-02-tsfile-cli-design.md`

---

## 执行前提

- 工作目录：`/Users/zhanghongyin/iotdb/tsfile`。
- 执行前 `git status --short`，确认未把 `.codegraph/` 或无关改动纳入暂存。
- 每个新建 `.h`/`.cc` 文件都以 Apache 2.0 块注释头（`/* ... */`）开头——从任一现有
  `cpp/tools/**` 文件原样复制。下文代码块为简洁省略了该头，**新建文件时务必前置**。
- 所有 CLI 代码在 `namespace tsfile_cli` 内。
- **构建环境注意（本机 CMake 4.3.2）**：bundled `third_party/antlr4-cpp-runtime-4`
  把已被移除的旧 CMake policy 设为 OLD，CMake 4.x 直接报错；必须 `--disable-antlr4`
  绕开（reader/CLI 不依赖 ANTLR4，已验证可编译可测试）。另外 `build.sh` 默认
  `build_test=0` 且无命令行开关，执行期间已临时改为 `build_test=1`（Task 6 收尾时
  `git checkout cpp/build.sh` 还原）。测试可执行文件落在 `build/Debug/test/lib/`。
- C++ 验证命令从 `cpp/` 目录运行：

```bash
bash build.sh -t=Debug --disable-antlr4
./build/Debug/test/lib/TsFile_Test --gtest_filter=CliE2E.*:ParseArgsTest.*:RunCliTest.*:RowWriterTest.*:ResolveFormatTest.*:CsvEscapeTest.*:JsonEscapeTest.*:TypeNameTest.*:EncodingNameTest.*:CompressionNameTest.*:StatTableTest.*
```

> 计划各 Task 内 `Run:` 行仍写的是旧的 `bash build.sh -t=Debug` 和
> `./build/Debug/lib/TsFile_Test`，请按上面这条「环境注意」统一替换为
> `--disable-antlr4` 构建命令与 `build/Debug/test/lib/TsFile_Test` 测试路径。

## 起点：当前工作树状态（2026-06-02）

- **已提交**（commit `a392a56f`，仅四个文件）：`cpp/tools/cli/cli_args.h`、
  `cli_args.cc`、`run_cli.cc`、`cpp/test/tools/cli_args_test.cc`。命令面已是 8 动词，
  含 `--seed` 解析、`validate_command_flags`；`select` 不在白名单；`meta`/`count`/
  `sample` 被 `is_unimplemented_command` 拦截返回 “command not implemented yet”。
- **未提交（untracked）**：`cpp/tools/CMakeLists.txt`、`tools_main.cc`、
  `cli/exit_codes.h`、`cli/run_cli.h`、`commands/`（`commands.h`、`row_query.cc`、
  `cmd_ls/cmd_schema/cmd_stats/cmd_head/cmd_cat/cmd_select.cc`）、`format/`
  （`output_format.*`、`result_set_format.*`）、`cpp/test/tools/cli_test_util.h`、
  `command_e2e_test.cc`、`output_format_test.cc`。
- **已修改（tracked）**：`cpp/CMakeLists.txt`（`BUILD_TOOLS`）、
  `cpp/src/file/read_file.cc`（open 错误改 stderr）、`cpp/test/CMakeLists.txt`
  （glob tools 测试、链接 `tsfile_cli_obj`）。
- **遗留不一致（Task 1 修复）**：`cmd_select.cc` 与其声明是死代码；
  `command_e2e_test.cc` 仍以 `select` 命令测试，与已移除 `select` 的命令面冲突。

## 文件结构

保留职责：`cli/cli_args.*`、`cli/run_cli.cc`、`commands/commands.h`、
`commands/row_query.cc`、`format/output_format.*`、`format/result_set_format.*`。

新增文件：

- `cpp/tools/commands/stat_table.h` / `.cc`：`SeriesStatRow`、`FileSummary`、
  `StatisticCells`，以及 `collect_series_stats`、`collect_file_summary`、
  `statistic_value_cells`，供 `stats`/`count`/`meta` 共用。
- `cpp/tools/commands/cmd_meta.cc`、`cmd_count.cc`、`cmd_sample.cc`。
- `cpp/test/tools/stat_table_test.cc`：统计值格式化 helper 单元测试。

删除文件：

- `cpp/tools/commands/cmd_select.cc`：`select` 能力已并入 `cat/head/sample` 共享参数。

---

### Task 1: 调和基线 —— 移除 `select` 死代码、构建变绿、提交既有实现

**Files:**
- Delete: `cpp/tools/commands/cmd_select.cc`
- Modify: `cpp/tools/commands/commands.h`
- Modify: `cpp/test/tools/command_e2e_test.cc`
- Modify: `cpp/test/tools/cli_args_test.cc`

本任务不引入新功能，只让 untracked 实现与已提交的 8 动词命令面一致，并把整套实现提交为
工作基线。

- [ ] **Step 1: 删除 `cmd_select.cc`**

```bash
rm cpp/tools/commands/cmd_select.cc
```

- [ ] **Step 2: 从 `commands.h` 删除 `cmd_select` 声明**

删除 `cpp/tools/commands/commands.h` 中这段：

```cpp
int cmd_select(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err);
```

- [ ] **Step 3: 把 `select` E2E 改写为 `cat`**

在 `cpp/test/tools/command_e2e_test.cc` 中，将 `SelectWithTimeRange` 改为：

```cpp
TEST(CliE2E, CatWithTimeRange) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "2", "--end",
                                    "3", "-f", "tsv", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "time\ts1\n2\t20\n3\t30\n");
}
```

将 `SelectJsonIsNdjson` 改为：

```cpp
TEST(CliE2E, CatJsonIsNdjson) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "0", "--end",
                                    "0", "-f", "json", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "{\"time\":0,\"s1\":0}\n");
}
```

- [ ] **Step 4: 修正过时的解析测试命令名**

在 `cpp/test/tools/cli_args_test.cc` 的 `MeasurementsSplitOnComma` 中，把命令从
`select` 改为 `cat`（仅 cosmetic，`parse_args` 不校验命令名）：

```cpp
TEST(ParseArgsTest, MeasurementsSplitOnComma) {
    auto p = tsfile_cli::parse_args({"cat", "-m", "s1,s2,s3", "data.tsfile"});
    ASSERT_EQ(p.measurements.size(), 3u);
    EXPECT_EQ(p.measurements[1], "s2");
}
```

- [ ] **Step 5: 构建并运行 CLI 测试，确认基线全绿**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.*:ParseArgsTest.*:RunCliTest.*:RowWriterTest.*:ResolveFormatTest.*:CsvEscapeTest.*:JsonEscapeTest.*:TypeNameTest.*:EncodingNameTest.*:CompressionNameTest.*
```

Expected: 构建成功；选定测试全部通过。其中 `RunCliTest.SelectIsNoLongerKnownCommand`、
`RunCliTest.NewCommandsAreExplicitlyUnimplementedBeforeReaderOpen` 仍通过（`meta`/
`count`/`sample` 此时仍是 stub）。

> 若 `CliE2E.SchemaTableMeasurementFilterOnlyShowsRequestedColumn` 等已有断言因字符串
> 细节失败，先用 `./build/Debug/bin/tsfile <cmd> -f tsv <fixture>` 打印实际输出再对齐，
> fixture 的数值（ts 0..4，s1=ts*10）是固定的。

- [ ] **Step 6: 手动确认 `select` 已不可用、help 不含 select**

Run:

```bash
cd cpp && ./build/Debug/bin/tsfile --help | grep -i select; echo "rc=$?"
```

Expected: 无输出，`rc=1`（grep 未命中）；help 列出 `ls schema meta stats head cat
count sample`。

- [ ] **Step 7: 提交工作基线**

```bash
git add cpp/CMakeLists.txt cpp/test/CMakeLists.txt cpp/src/file/read_file.cc \
        cpp/tools/CMakeLists.txt cpp/tools/tools_main.cc \
        cpp/tools/cli/exit_codes.h cpp/tools/cli/run_cli.h \
        cpp/tools/commands cpp/tools/format \
        cpp/test/tools/cli_test_util.h cpp/test/tools/command_e2e_test.cc \
        cpp/test/tools/output_format_test.cc cpp/test/tools/cli_args_test.cc
git commit -m "Add tsfile CLI ls/schema/stats/head/cat implementation and tests"
```

> 注意：`git add cpp/tools/commands` 会把已被 `rm` 的 `cmd_select.cc` 记为删除。提交前
> `git status --short` 确认未纳入 `.codegraph/`。

---

### Task 2: 统计 helper 与 `stats` 扩展到 min/max/first/last/sum

**Files:**
- Create: `cpp/tools/commands/stat_table.h`
- Create: `cpp/tools/commands/stat_table.cc`
- Modify: `cpp/tools/commands/cmd_stats.cc`
- Create: `cpp/test/tools/stat_table_test.cc`
- Modify: `cpp/test/tools/command_e2e_test.cc`

- [ ] **Step 1: 写失败测试，直接覆盖统计值格式化** — `cpp/test/tools/stat_table_test.cc`

```cpp
#include "commands/stat_table.h"

#include <gtest/gtest.h>

#include "common/statistic.h"

TEST(StatTableTest, Int64StatisticCellsContainValueSummaries) {
    storage::Int64Statistic st;
    st.update(1, static_cast<int64_t>(10));
    st.update(3, static_cast<int64_t>(30));
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_EQ(cells.values[0], "10");
    EXPECT_EQ(cells.values[1], "30");
    EXPECT_EQ(cells.values[2], "10");
    EXPECT_EQ(cells.values[3], "30");
    EXPECT_EQ(cells.values[4], "40");
    EXPECT_EQ(cells.is_null,
              std::vector<bool>({false, false, false, false, false}));
}

TEST(StatTableTest, BooleanStatisticLeavesMinMaxNull) {
    storage::BooleanStatistic st;
    st.update(1, true);
    st.update(2, false);
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_TRUE(cells.is_null[0]);
    EXPECT_TRUE(cells.is_null[1]);
    EXPECT_EQ(cells.values[2], "true");
    EXPECT_EQ(cells.values[3], "false");
    EXPECT_EQ(cells.values[4], "1");
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cd cpp && bash build.sh -t=Debug
```

Expected: 构建失败，因为 `commands/stat_table.h` 不存在。

- [ ] **Step 3: 创建 `cpp/tools/commands/stat_table.h`**（前置 license 头）

```cpp
#ifndef TSFILE_CLI_STAT_TABLE_H
#define TSFILE_CLI_STAT_TABLE_H

#include <string>
#include <vector>

#include "cli/cli_args.h"

namespace storage {
class Statistic;
class TsFileReader;
}  // namespace storage

namespace tsfile_cli {

struct StatisticCells {
    std::vector<std::string> values;
    std::vector<bool> is_null;
};

struct SeriesStatRow {
    std::string target;
    std::string measurement;
    long long count = 0;
    long long start_time = 0;
    long long end_time = 0;
    StatisticCells value_cells;
};

struct FileSummary {
    std::string file;
    std::string model;
    long long device_count = 0;
    long long table_count = 0;
    long long series_count = 0;
    long long start_time = 0;
    long long end_time = 0;
    bool has_time_range = false;
    long long file_size_bytes = 0;
};

StatisticCells statistic_value_cells(storage::Statistic* st);
std::vector<SeriesStatRow> collect_series_stats(const ParsedArgs& args,
                                                storage::TsFileReader& reader);
FileSummary collect_file_summary(const ParsedArgs& args,
                                 storage::TsFileReader& reader);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_STAT_TABLE_H
```

- [ ] **Step 4: 创建 `cpp/tools/commands/stat_table.cc`**（前置 license 头）

```cpp
#include "commands/stat_table.h"

#include <algorithm>
#include <fstream>
#include <limits>
#include <sstream>

#include "commands/commands.h"
#include "common/statistic.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {
namespace {

template <typename T>
std::string value_to_string(T value) {
    std::ostringstream ss;
    ss << value;
    return ss.str();
}

std::string bool_to_string(bool value) { return value ? "true" : "false"; }

std::string string_to_std(const common::String& value) {
    return value.to_std_string();
}

long long file_size(const std::string& path) {
    std::ifstream in(path.c_str(), std::ios::binary | std::ios::ate);
    if (!in.good()) {
        return 0;
    }
    return static_cast<long long>(in.tellg());
}

}  // namespace

StatisticCells statistic_value_cells(storage::Statistic* st) {
    StatisticCells cells;
    cells.values.assign(5, "");
    cells.is_null.assign(5, true);
    if (st == nullptr || st->get_count() == 0) {
        return cells;
    }

    switch (st->get_type()) {
        case common::BOOLEAN: {
            auto* s = static_cast<storage::BooleanStatistic*>(st);
            cells.values = {"", "", bool_to_string(s->first_value_),
                            bool_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {true, true, false, false, false};
            break;
        }
        case common::INT32:
        case common::DATE: {
            auto* s = static_cast<storage::Int32Statistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {false, false, false, false, false};
            break;
        }
        case common::INT64:
        case common::TIMESTAMP: {
            auto* s = static_cast<storage::Int64Statistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {false, false, false, false, false};
            break;
        }
        case common::FLOAT: {
            auto* s = static_cast<storage::FloatStatistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {false, false, false, false, false};
            break;
        }
        case common::DOUBLE: {
            auto* s = static_cast<storage::DoubleStatistic*>(st);
            cells.values = {value_to_string(s->min_value_),
                            value_to_string(s->max_value_),
                            value_to_string(s->first_value_),
                            value_to_string(s->last_value_),
                            value_to_string(s->sum_value_)};
            cells.is_null = {false, false, false, false, false};
            break;
        }
        case common::STRING: {
            auto* s = static_cast<storage::StringStatistic*>(st);
            cells.values = {string_to_std(s->min_value_),
                            string_to_std(s->max_value_),
                            string_to_std(s->first_value_),
                            string_to_std(s->last_value_), ""};
            cells.is_null = {false, false, false, false, true};
            break;
        }
        case common::TEXT: {
            auto* s = static_cast<storage::TextStatistic*>(st);
            cells.values = {"", "", string_to_std(s->first_value_),
                            string_to_std(s->last_value_), ""};
            cells.is_null = {true, true, false, false, true};
            break;
        }
        default:
            break;
    }
    return cells;
}

std::vector<SeriesStatRow> collect_series_stats(const ParsedArgs& args,
                                                storage::TsFileReader& reader) {
    std::vector<SeriesStatRow> rows;
    storage::DeviceTimeseriesMetadataMap meta =
        reader.get_timeseries_metadata();
    for (auto& kv : meta) {
        std::string target = kv.first ? kv.first->get_device_name() : "";
        if (!args.device.empty() && target != args.device) {
            continue;
        }
        if (!args.table.empty() && kv.first &&
            kv.first->get_table_name() != args.table) {
            continue;
        }
        for (auto& ts : kv.second) {
            if (!ts) {
                continue;
            }
            std::string measurement =
                ts->get_measurement_name().to_std_string();
            if (!args.measurements.empty() &&
                std::find(args.measurements.begin(), args.measurements.end(),
                          measurement) == args.measurements.end()) {
                continue;
            }
            storage::Statistic* st = ts->get_statistic();
            SeriesStatRow row;
            row.target = target;
            row.measurement = measurement;
            if (st != nullptr) {
                row.count = st->get_count();
                row.start_time = st->start_time_;
                row.end_time = st->end_time_;
                row.value_cells = statistic_value_cells(st);
            } else {
                row.value_cells.values.assign(5, "");
                row.value_cells.is_null.assign(5, true);
            }
            rows.push_back(row);
        }
    }
    return rows;
}

FileSummary collect_file_summary(const ParsedArgs& args,
                                 storage::TsFileReader& reader) {
    FileSummary s;
    s.file = args.file;
    s.model = is_table_model(args, reader) ? "table" : "tree";
    s.device_count =
        static_cast<long long>(reader.get_all_device_ids().size());
    s.table_count =
        static_cast<long long>(reader.get_all_table_schemas().size());
    s.file_size_bytes = file_size(args.file);

    ParsedArgs all = args;
    all.device.clear();
    all.table.clear();
    all.measurements.clear();
    std::vector<SeriesStatRow> rows = collect_series_stats(all, reader);
    s.series_count = static_cast<long long>(rows.size());
    long long min_start = std::numeric_limits<long long>::max();
    long long max_end = std::numeric_limits<long long>::min();
    for (const SeriesStatRow& row : rows) {
        if (row.count <= 0) {
            continue;
        }
        min_start = std::min(min_start, row.start_time);
        max_end = std::max(max_end, row.end_time);
        s.has_time_range = true;
    }
    if (s.has_time_range) {
        s.start_time = min_start;
        s.end_time = max_end;
    }
    return s;
}

}  // namespace tsfile_cli
```

> **编译风险提示**：上面对 `storage::Statistic` 子类字段（`min_value_`、`max_value_`、
> `first_value_`、`last_value_`、`sum_value_`、`start_time_`、`end_time_`）和访问器
> （`get_count()`、`get_type()`、`get_statistic()`）的引用，应在编译失败时对照
> `cpp/src/common/statistic.h` 校正名称，不要改测试期望值。

- [ ] **Step 5: 用 helper 改写 `cmd_stats.cc`，输出 10 列**

将 `cpp/tools/commands/cmd_stats.cc` 整个命令体替换为：

```cpp
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "commands/stat_table.h"

namespace tsfile_cli {

int cmd_stats(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(out, fmt,
                {"target", "measurement", "count", "start_time", "end_time",
                 "min", "max", "first", "last", "sum"},
                {common::STRING, common::STRING, common::INT64, common::INT64,
                 common::INT64, common::STRING, common::STRING, common::STRING,
                 common::STRING, common::STRING},
                args.no_header);

    std::vector<SeriesStatRow> rows = collect_series_stats(args, reader);
    for (const SeriesStatRow& row : rows) {
        std::vector<std::string> cells = {
            row.target, row.measurement, std::to_string(row.count),
            std::to_string(row.start_time), std::to_string(row.end_time)};
        cells.insert(cells.end(), row.value_cells.values.begin(),
                     row.value_cells.values.end());

        std::vector<bool> nulls = {false, false, false, row.count == 0,
                                   row.count == 0};
        nulls.insert(nulls.end(), row.value_cells.is_null.begin(),
                     row.value_cells.is_null.end());
        w.write(cells, nulls);
    }
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
```

- [ ] **Step 6: 更新 `stats` E2E 断言表头与值**

在 `cpp/test/tools/command_e2e_test.cc` 中，把 `StatsReportsCountAndTimeRange` 的两条
`EXPECT_NE` 替换为：

```cpp
    EXPECT_NE(out.str().find("target\tmeasurement\tcount\tstart_time\tend_"
                             "time\tmin\tmax\tfirst\tlast\tsum"),
              std::string::npos);
    EXPECT_NE(out.str().find("s1\t5\t0\t4\t0\t40\t0\t40\t100"),
              std::string::npos);
```

- [ ] **Step 7: 构建并运行测试确认通过**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=StatTableTest.*:CliE2E.StatsReportsCountAndTimeRange
```

Expected: 构建成功；选定测试通过。

- [ ] **Step 8: 提交**

```bash
git add cpp/tools/commands/stat_table.h cpp/tools/commands/stat_table.cc \
        cpp/tools/commands/cmd_stats.cc cpp/test/tools/stat_table_test.cc \
        cpp/test/tools/command_e2e_test.cc
git commit -m "Extend tsfile stats with value summaries and shared stat helpers"
```

---

### Task 3: 实现 `meta`

**Files:**
- Create: `cpp/tools/commands/cmd_meta.cc`
- Modify: `cpp/tools/commands/commands.h`
- Modify: `cpp/tools/cli/run_cli.cc`
- Modify: `cpp/test/tools/command_e2e_test.cc`
- Modify: `cpp/test/tools/cli_args_test.cc`

- [ ] **Step 1: 写失败 E2E 测试**

在 `cpp/test/tools/command_e2e_test.cc` 末尾追加：

```cpp
TEST(CliE2E, MetaReportsFileSummary) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"meta", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("file\tmodel\tversion\tdevice_count\ttable_"
                             "count\tseries_count\tstart_time\tend_time\tbloom_"
                             "filter\tfile_size_bytes"),
              std::string::npos);
    EXPECT_NE(out.str().find("\ttable\t"), std::string::npos);
}
```

- [ ] **Step 2: 把 `meta` 从「未实现」集合移除**

在 `cpp/test/tools/cli_args_test.cc` 的
`NewCommandsAreExplicitlyUnimplementedBeforeReaderOpen` 中，把循环范围从
`{"meta", "count", "sample"}` 改为：

```cpp
    for (const char* command : {"count", "sample"}) {
```

- [ ] **Step 3: 运行测试确认失败**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.MetaReportsFileSummary
```

Expected: 测试失败——`meta` 仍被 `is_unimplemented_command` 拦截，返回退出码 1。

- [ ] **Step 4: 声明 `cmd_meta`**

在 `cpp/tools/commands/commands.h` 的 `cmd_schema` 声明之后加入：

```cpp
int cmd_meta(const ParsedArgs& args, storage::TsFileReader& reader,
             OutputFormat fmt, std::ostream& out, std::ostream& err);
```

- [ ] **Step 5: 创建 `cpp/tools/commands/cmd_meta.cc`**（前置 license 头）

```cpp
#include "commands/commands.h"

#include "cli/exit_codes.h"
#include "commands/stat_table.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int cmd_meta(const ParsedArgs& args, storage::TsFileReader& reader,
             OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(out, fmt,
                {"file", "model", "version", "device_count", "table_count",
                 "series_count", "start_time", "end_time", "bloom_filter",
                 "file_size_bytes"},
                {common::STRING, common::STRING, common::STRING, common::INT64,
                 common::INT64, common::INT64, common::INT64, common::INT64,
                 common::STRING, common::INT64},
                args.no_header);

    FileSummary s = collect_file_summary(args, reader);
    w.write({s.file, s.model, "", std::to_string(s.device_count),
             std::to_string(s.table_count), std::to_string(s.series_count),
             s.has_time_range ? std::to_string(s.start_time) : "",
             s.has_time_range ? std::to_string(s.end_time) : "", "",
             std::to_string(s.file_size_bytes)},
            {false, false, true, false, false, false, !s.has_time_range,
             !s.has_time_range, true, false});
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
```

- [ ] **Step 6: 在 `run_cli.cc` 中放开 `meta` 并加入分发**

在 `cpp/tools/cli/run_cli.cc` 中：

1. 把 `is_unimplemented_command` 的集合改为：

```cpp
    static const std::set<std::string> kCmds = {"count", "sample"};
```

2. 在分发链的 `cmd_schema` 分支之后、`cmd_stats` 分支之前插入：

```cpp
    } else if (p.command == "meta") {
        code = cmd_meta(p, reader, fmt, out, err);
```

- [ ] **Step 7: 构建并运行测试确认通过**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.MetaReportsFileSummary:RunCliTest.NewCommandsAreExplicitlyUnimplementedBeforeReaderOpen
```

Expected: 构建成功；两个测试通过。

- [ ] **Step 8: 提交**

```bash
git add cpp/tools/commands/cmd_meta.cc cpp/tools/commands/commands.h \
        cpp/tools/cli/run_cli.cc cpp/test/tools/command_e2e_test.cc \
        cpp/test/tools/cli_args_test.cc
git commit -m "Add tsfile meta command"
```

---

### Task 4: 实现 `count`

**Files:**
- Create: `cpp/tools/commands/cmd_count.cc`
- Modify: `cpp/tools/commands/commands.h`
- Modify: `cpp/tools/cli/run_cli.cc`
- Modify: `cpp/test/tools/command_e2e_test.cc`
- Modify: `cpp/test/tools/cli_args_test.cc`

- [ ] **Step 1: 写失败 E2E 测试**

在 `cpp/test/tools/command_e2e_test.cc` 末尾追加：

```cpp
TEST(CliE2E, CountReportsSeriesCountsAndTotal) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"count", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("target\tmeasurement\tcount"), std::string::npos);
    EXPECT_NE(out.str().find("\ts1\t5"), std::string::npos);
    EXPECT_NE(out.str().find("total\t\t"), std::string::npos);
}
```

- [ ] **Step 2: 把 `count` 从「未实现」集合移除**

在 `cpp/test/tools/cli_args_test.cc` 的
`NewCommandsAreExplicitlyUnimplementedBeforeReaderOpen` 中，把循环范围改为：

```cpp
    for (const char* command : {"sample"}) {
```

- [ ] **Step 3: 运行测试确认失败**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.CountReportsSeriesCountsAndTotal
```

Expected: 测试失败——`count` 仍被拦截。

- [ ] **Step 4: 声明 `cmd_count`**

在 `cpp/tools/commands/commands.h` 的 `cmd_meta` 声明之后加入：

```cpp
int cmd_count(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& err);
```

- [ ] **Step 5: 创建 `cpp/tools/commands/cmd_count.cc`**（前置 license 头）

```cpp
#include "commands/commands.h"

#include "cli/exit_codes.h"
#include "commands/stat_table.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int cmd_count(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(out, fmt, {"target", "measurement", "count"},
                {common::STRING, common::STRING, common::INT64},
                args.no_header);

    long long total = 0;
    std::vector<SeriesStatRow> rows = collect_series_stats(args, reader);
    for (const SeriesStatRow& row : rows) {
        total += row.count;
        w.write({row.target, row.measurement, std::to_string(row.count)},
                {false, false, false});
    }
    w.write({"total", "", std::to_string(total)}, {false, true, false});
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
```

- [ ] **Step 6: 在 `run_cli.cc` 中放开 `count` 并加入分发**

在 `cpp/tools/cli/run_cli.cc` 中：

1. 把 `is_unimplemented_command` 的集合改为：

```cpp
    static const std::set<std::string> kCmds = {"sample"};
```

2. 在分发链的 `cmd_cat` 分支之后插入：

```cpp
    } else if (p.command == "count") {
        code = cmd_count(p, reader, fmt, out, err);
```

- [ ] **Step 7: 构建并运行测试确认通过**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.CountReportsSeriesCountsAndTotal:RunCliTest.NewCommandsAreExplicitlyUnimplementedBeforeReaderOpen
```

Expected: 构建成功；两个测试通过。

- [ ] **Step 8: 提交**

```bash
git add cpp/tools/commands/cmd_count.cc cpp/tools/commands/commands.h \
        cpp/tools/cli/run_cli.cc cpp/test/tools/command_e2e_test.cc \
        cpp/test/tools/cli_args_test.cc
git commit -m "Add tsfile count command"
```

---

### Task 5: 实现确定性 `sample`，并彻底移除「未实现」拦截

**Files:**
- Modify: `cpp/tools/format/result_set_format.h`
- Modify: `cpp/tools/format/result_set_format.cc`
- Create: `cpp/tools/commands/cmd_sample.cc`
- Modify: `cpp/tools/commands/commands.h`
- Modify: `cpp/tools/cli/run_cli.cc`
- Modify: `cpp/test/tools/command_e2e_test.cc`
- Modify: `cpp/test/tools/cli_args_test.cc`

- [ ] **Step 1: 写失败 E2E 测试**

在 `cpp/test/tools/command_e2e_test.cc` 末尾追加：

```cpp
TEST(CliE2E, SampleIsReproducibleWithSeed) {
    Fixture f;
    std::ostringstream out1;
    std::ostringstream err1;
    std::ostringstream out2;
    std::ostringstream err2;

    int code1 = tsfile_cli::run_cli(
        {"sample", "-m", "s1", "-n", "3", "--seed", "7", "-f", "tsv", f.path},
        out1, err1);
    int code2 = tsfile_cli::run_cli(
        {"sample", "-m", "s1", "-n", "3", "--seed", "7", "-f", "tsv", f.path},
        out2, err2);

    EXPECT_EQ(code1, 0);
    EXPECT_EQ(code2, 0);
    EXPECT_TRUE(err1.str().empty());
    EXPECT_TRUE(err2.str().empty());
    EXPECT_EQ(out1.str(), out2.str());
    EXPECT_EQ(count_lines(out1.str()), 4u);
    EXPECT_NE(out1.str().find("time\ts1\n"), std::string::npos);
}
```

- [ ] **Step 2: 删除 `cli_args_test.cc` 中的「未实现」测试**

`meta`/`count`/`sample` 都将实现，删除 `cpp/test/tools/cli_args_test.cc` 中整个
`TEST(RunCliTest, NewCommandsAreExplicitlyUnimplementedBeforeReaderOpen) { ... }`。

- [ ] **Step 3: 运行测试确认失败**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.SampleIsReproducibleWithSeed
```

Expected: 测试失败——`sample` 仍被拦截。

- [ ] **Step 4: 声明 sampled writer**

在 `cpp/tools/format/result_set_format.h` 中，`write_result_set` 声明之后追加：

```cpp
int write_result_set_sampled(storage::ResultSet* rs, OutputFormat fmt,
                             bool no_header, std::ostream& out, long long limit,
                             unsigned long long seed);
```

- [ ] **Step 5: 实现 sampled writer**

在 `cpp/tools/format/result_set_format.cc` 顶部 include 区加入：

```cpp
#include <random>
```

在 `write_result_set` 定义之后追加：

```cpp
namespace {

struct BufferedRow {
    std::vector<std::string> cells;
    std::vector<bool> nulls;
};

BufferedRow read_current_row(storage::ResultSet* rs,
                             const std::vector<common::TSDataType>& types) {
    BufferedRow row;
    const uint32_t ncol = static_cast<uint32_t>(types.size());
    row.cells.assign(ncol, "");
    row.nulls.assign(ncol, false);
    for (uint32_t i = 1; i <= ncol; ++i) {
        if (rs->is_null(i)) {
            row.nulls[i - 1] = true;
        } else {
            row.cells[i - 1] = cell_to_string(rs, i, types[i - 1]);
        }
    }
    return row;
}

}  // namespace

int write_result_set_sampled(storage::ResultSet* rs, OutputFormat fmt,
                             bool no_header, std::ostream& out, long long limit,
                             unsigned long long seed) {
    if (limit < 0) {
        limit = 10;
    }
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

    std::vector<BufferedRow> reservoir;
    reservoir.reserve(static_cast<size_t>(limit));
    std::mt19937_64 rng(seed);
    bool has_next = false;
    int code = common::E_OK;
    long long seen = 0;
    while ((code = rs->next(has_next)) == common::E_OK && has_next) {
        BufferedRow row = read_current_row(rs, types);
        if (limit == 0) {
            ++seen;
            continue;
        }
        if (static_cast<long long>(reservoir.size()) < limit) {
            reservoir.push_back(row);
        } else {
            std::uniform_int_distribution<long long> dist(0, seen);
            long long idx = dist(rng);
            if (idx < limit) {
                reservoir[static_cast<size_t>(idx)] = row;
            }
        }
        ++seen;
    }

    RowWriter writer(out, fmt, header, types, no_header);
    for (const BufferedRow& row : reservoir) {
        writer.write(row.cells, row.nulls);
    }
    writer.finish();
    return code;
}
```

- [ ] **Step 6: 声明 `cmd_sample`**

在 `cpp/tools/commands/commands.h` 的 `cmd_count` 声明之后加入：

```cpp
int cmd_sample(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err);
```

- [ ] **Step 7: 创建 `cpp/tools/commands/cmd_sample.cc`**（前置 license 头）

```cpp
#include "commands/commands.h"

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "common/device_id.h"
#include "common/schema.h"
#include "format/result_set_format.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int cmd_sample(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err) {
    const int64_t start = args.has_start ? static_cast<int64_t>(args.start)
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
            if (ts) {
                cols = ts->get_measurement_names();
            }
        }
        qret = reader.query(table_name, cols, start, end, rs);
    } else {
        std::vector<std::string> devices;
        if (!args.device.empty()) {
            devices.push_back(args.device);
        } else {
            for (auto& d : reader.get_all_device_ids()) {
                if (d) {
                    devices.push_back(d->get_device_name());
                }
            }
        }
        std::vector<std::string> paths;
        for (const std::string& dev : devices) {
            std::vector<std::string> ms = args.measurements;
            if (ms.empty()) {
                auto did = std::make_shared<storage::StringArrayDeviceID>(dev);
                std::vector<storage::MeasurementSchema> sch;
                if (reader.get_timeseries_schema(did, sch) == 0) {
                    for (auto& m : sch) {
                        ms.push_back(m.measurement_name_);
                    }
                }
            }
            for (const std::string& m : ms) {
                paths.push_back(dev + "." + m);
            }
        }
        if (paths.empty()) {
            err << "Error: no time series found\n";
            return kExitRuntime;
        }
        qret = reader.query(paths, start, end, rs);
    }

    if (qret != 0 || rs == nullptr) {
        err << "Error: query failed (code " << qret << ")\n";
        if (rs != nullptr) {
            reader.destroy_query_data_set(rs);
        }
        return kExitRuntime;
    }

    const long long limit = args.limit < 0 ? 10 : args.limit;
    const unsigned long long seed =
        args.has_seed ? static_cast<unsigned long long>(args.seed) : 0ULL;
    int wret =
        write_result_set_sampled(rs, fmt, args.no_header, out, limit, seed);
    reader.destroy_query_data_set(rs);
    return wret == 0 ? kExitOk : kExitRuntime;
}

}  // namespace tsfile_cli
```

> `cmd_sample` 的 query 构造与 `commands/row_query.cc::run_row_query` 几乎相同；二者唯一
> 差异是 `sample` 走 `write_result_set_sampled`、不接受 `--offset`。先保持各自独立，
> 待第二个真实共享点出现再抽取——不要为消除这一处重复提前抽象（YAGNI）。

- [ ] **Step 8: 移除「未实现」拦截，加入 `sample` 分发**

在 `cpp/tools/cli/run_cli.cc` 中：

1. 删除整个 `is_unimplemented_command` 函数定义。

2. 删除 `run_cli` 中调用它的守卫块：

```cpp
    if (is_unimplemented_command(p.command)) {
        err << "Error: command not implemented yet: " << p.command << "\n";
        print_usage(err);
        return kExitUsage;
    }
```

3. 在分发链的 `cmd_count` 分支之后插入：

```cpp
    } else if (p.command == "sample") {
        code = cmd_sample(p, reader, fmt, out, err);
```

- [ ] **Step 9: 构建并运行测试确认通过**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.SampleIsReproducibleWithSeed:CliE2E.*:RunCliTest.*
```

Expected: 构建成功；`sample` 可复现测试与全部 CLI 测试通过；不再有 `RunCliTest`
引用 `command not implemented yet`。

- [ ] **Step 10: 提交**

```bash
git add cpp/tools/format/result_set_format.h cpp/tools/format/result_set_format.cc \
        cpp/tools/commands/cmd_sample.cc cpp/tools/commands/commands.h \
        cpp/tools/cli/run_cli.cc cpp/test/tools/command_e2e_test.cc \
        cpp/test/tools/cli_args_test.cc
git commit -m "Add deterministic tsfile sample command"
```

---

### Task 6: 全量验证、help 快照与最终检查

**Files:**
- Modify: `docs/superpowers/plans/2026-06-02-tsfile-cli.md`（仅当执行中需修正执行笔记）。

- [ ] **Step 1: 跑完整 CLI 相关测试**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.*:ParseArgsTest.*:RunCliTest.*:RowWriterTest.*:ResolveFormatTest.*:CsvEscapeTest.*:JsonEscapeTest.*:TypeNameTest.*:EncodingNameTest.*:CompressionNameTest.*:StatTableTest.*
```

Expected: 构建成功；选定测试全部通过。

- [ ] **Step 2: 跑完整 C++ 测试可执行文件**

Run:

```bash
cd cpp && ./build/Debug/lib/TsFile_Test
```

Expected: 全部通过。若有与本计划无关的既有测试失败，记录确切失败名与输出，再决定是否
缩小验证范围。

- [ ] **Step 3: 手动检查 help 与命令面**

Run:

```bash
cd cpp && ./build/Debug/bin/tsfile --help
```

Expected: stdout 含 `ls schema meta stats head cat count sample`；不含 `select`、
不含 “not implemented”。

- [ ] **Step 4: 针对自带样例手动冒烟**

Run（样例为 table 模型）：

```bash
cd cpp
BIN=./build/Debug/bin/tsfile
F=examples/test_cpp.tsfile
$BIN ls -f tsv $F
$BIN meta -f tsv $F
$BIN stats -f tsv $F
$BIN count -f tsv $F
$BIN head -n 3 -f tsv $F
$BIN sample -m s1 -n 3 --seed 7 -f tsv $F
echo "missing file:"; $BIN ls nope.tsfile; echo "rc=$?"
```

Expected: 数据在 stdout、诊断在 stderr；`ls nope.tsfile` 退出码 2 且错误在 stderr。

- [ ] **Step 5: 格式化与暂存范围检查**

Run:

```bash
cd /Users/zhanghongyin/iotdb/tsfile && ./mvnw spotless:apply -P with-cpp 2>&1 | tail -5 && ./mvnw spotless:check -P with-cpp 2>&1 | tail -5
git diff --check
git status --short
```

Expected: clang-format 干净通过；`git diff --check` 退出 0；`git status --short` 仅含本
CLI 工作，`.codegraph/` 等无关项未被暂存。

- [ ] **Step 6: 最终提交（如有格式化/笔记改动）**

```bash
git add -u cpp/tools cpp/test/tools
git commit -m "Format tsfile CLI sources"
```

若本任务未产生文件改动，不创建空提交。

## 覆盖检查（plan self-review）

| Spec 要求 | 对应 |
|---|---|
| 单 `tsfile` 二进制、git 式子命令分发 | 已实现（基线，Task 1 提交） |
| `ls`/`schema`/`head`/`cat` | 已实现（基线，Task 1 提交） |
| `select` 删除（动词 + 死代码） | 命令面已删（`a392a56f`）；死代码 + 测试 Task 1 |
| `stats` 扩展 min/max/first/last/sum | Task 2 |
| `meta` | Task 3 |
| `count` | Task 4 |
| `sample` 与 `--seed` 可复现 | `--seed` 解析已提交；writer + 命令 Task 5 |
| 共享参数：投影/时间范围/limit/offset | 基线 `row_query.cc` 已实现；Task 1 `cat` E2E 覆盖时间范围 |
| 输出格式 csv/tsv/json/table、stdout/stderr 分离 | 基线 formatter + `read_file.cc` 改动，Task 1 提交、Task 6 验证 |
| tree/table 自动检测 + `--model` | 基线 `is_table_model`；`stats`/`count`/`meta` 经 `collect_*` 支持作用域 |
| 退出码 0/1/2/3 | `exit_codes.h`（基线）；各命令返回值 |
| `BUILD_TOOLS` + `install()` | 基线 `cpp/tools/CMakeLists.txt`，Task 1 提交 |

**占位扫描**：无 `TBD`/`TODO`/“implement later”。`run_cli.cc` 的
`is_unimplemented_command` 拦截在 Task 3/4/5 逐步收窄并于 Task 5 完全删除。

**类型一致性**：`ParsedArgs`、`OutputFormat`、`RowWriter(out, fmt, header, types,
no_header)`、`write_result_set(rs, fmt, no_header, out, offset, limit)`、
`write_result_set_sampled(rs, fmt, no_header, out, limit, seed)`、
`collect_series_stats`/`collect_file_summary`/`statistic_value_cells`、各
`cmd_*(args, reader, fmt, out, err)` 签名在各任务间一致。

**已知残留风险（执行中验证，非阻塞）**：
1. `storage::Statistic` 子类字段/访问器名称——Task 2 Step 4 注明编译失败时对照
   `cpp/src/common/statistic.h` 校正，不改测试期望。
2. 行/列顺序导致的 E2E 字符串断言——用 `tsfile <cmd> -f tsv <fixture>` 打印实际输出对齐；
   fixture 数值（ts 0..4，s1=ts*10）固定。
3. table 模型下 `get_timeseries_metadata()` 是否为每序列返回统计量——若 `meta`/`count`/
   `stats` 行数为空，对照基线 `cmd_schema.cc` 已验证的 metadata 读取路径排查。
