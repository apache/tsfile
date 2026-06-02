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

# TsFile CLI Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将当前 C++ `tsfile` v1 CLI 从 `ls/schema/stats/head/cat/select` 调整为 `ls/schema/meta/stats/head/cat/count/sample`，并让投影、时间范围、limit/offset 作为行输出命令的共享参数工作。

**Architecture:** 保留现有 `cpp/tools/` 分层：`cli/` 负责参数解析与分发，`commands/` 负责读 metadata 或 row query，`format/` 负责 `RowWriter` 和 `ResultSet` 输出。新增 metadata/stat helper 复用 `Statistic` 格式化逻辑，新增 sampled result-set writer 复用现有 cell extraction，避免在命令层复制行输出代码。

**Tech Stack:** C++11/C++14 兼容代码，CMake `BUILD_TOOLS`，Google Test，现有 `storage::TsFileReader`、`storage::Statistic`、`RowWriter`、`write_result_set`。

---

## 执行前提

- 工作目录：`/Users/zhanghongyin/iotdb/tsfile`
- 执行实现前先确认 `git status --short`，不要 stage `.codegraph/` 或与本计划无关的改动。
- C++ 验证命令从 `cpp/` 目录运行：

```bash
bash build.sh -t=Debug
./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.*:ParseArgsTest.*:RunCliTest.*:StatTableTest.*:ResultSetSampleTest.*
```

## 文件结构

现有文件继续保留职责：

- `cpp/tools/cli/cli_args.h` / `cpp/tools/cli/cli_args.cc`：解析命令、flag、数值参数。
- `cpp/tools/cli/run_cli.cc`：顶层 usage、命令白名单、命令/flag 组合校验、reader open、分发。
- `cpp/tools/commands/commands.h`：命令函数和共享 helper 声明。
- `cpp/tools/commands/row_query.cc`：`head`、`cat`、`sample` 共用的 query 构造。
- `cpp/tools/format/output_format.*`：`RowWriter` 和标量格式转换。
- `cpp/tools/format/result_set_format.*`：从 `ResultSet` 抽取行并写出。
- `cpp/test/tools/*_test.cc`：CLI 单元测试和 in-process E2E 测试。

新增文件：

- `cpp/tools/commands/stat_table.h`：定义 `SeriesStatRow`、`FileSummary`，声明 metadata/stat 收集与统计值格式化 helper。
- `cpp/tools/commands/stat_table.cc`：实现 `collect_series_stats`、`collect_file_summary`、`statistic_value_cells`，供 `stats`、`count`、`meta` 共用。
- `cpp/tools/commands/cmd_meta.cc`：实现 `tsfile meta`。
- `cpp/tools/commands/cmd_count.cc`：实现 `tsfile count`。
- `cpp/tools/commands/cmd_sample.cc`：实现 `tsfile sample`。
- `cpp/test/tools/stat_table_test.cc`：直接测试 `Statistic` 值格式化和汇总 helper 的稳定行为。
- `cpp/test/tools/result_set_sample_test.cc`：测试抽样 writer 的确定性行为。

删除文件：

- `cpp/tools/commands/cmd_select.cc`：`select` 能力并入 `cat/head/sample` 的共享参数。

---

### Task 1: 命令面、参数解析和 flag 组合校验

**Files:**
- Modify: `cpp/tools/cli/cli_args.h`
- Modify: `cpp/tools/cli/cli_args.cc`
- Modify: `cpp/tools/cli/run_cli.cc`
- Modify: `cpp/test/tools/cli_args_test.cc`

- [ ] **Step 1: 写失败测试，覆盖 `--seed`、新命令和删除 `select`**

在 `cpp/test/tools/cli_args_test.cc` 末尾追加：

```cpp
TEST(ParseArgsTest, SeedFlagParsed) {
    auto p = tsfile_cli::parse_args(
        {"sample", "-m", "s1", "-n", "3", "--seed", "42", "data.tsfile"});
    EXPECT_TRUE(p.error.empty());
    EXPECT_EQ(p.command, "sample");
    EXPECT_EQ(p.limit, 3);
    EXPECT_TRUE(p.has_seed);
    EXPECT_EQ(p.seed, 42);
}

TEST(ParseArgsTest, BadSeedValueIsError) {
    auto p = tsfile_cli::parse_args(
        {"sample", "--seed", "not_a_number", "data.tsfile"});
    EXPECT_FALSE(p.error.empty());
    EXPECT_NE(p.error.find("Invalid --seed"), std::string::npos);
}

TEST(RunCliTest, SelectIsNoLongerKnownCommand) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"select", "x.tsfile"}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("Unknown command"), std::string::npos);
}

TEST(RunCliTest, SeedOnCatIsUsageError) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"cat", "--seed", "7", "x.tsfile"}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--seed is only valid for sample"),
              std::string::npos);
}

TEST(RunCliTest, OffsetOnSampleIsUsageError) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"sample", "--offset", "2", "x.tsfile"}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--offset is not valid for sample"),
              std::string::npos);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cd cpp && ./build/Debug/lib/TsFile_Test --gtest_filter=ParseArgsTest.SeedFlagParsed:ParseArgsTest.BadSeedValueIsError:RunCliTest.SelectIsNoLongerKnownCommand:RunCliTest.SeedOnCatIsUsageError:RunCliTest.OffsetOnSampleIsUsageError
```

Expected: 编译或测试失败，至少包含 `ParsedArgs` 没有 `seed` / `has_seed`，或 `select` 仍是已知命令。

- [ ] **Step 3: 在 `ParsedArgs` 中加入 seed 字段**

在 `cpp/tools/cli/cli_args.h` 的 `ParsedArgs` 内、`has_end` 后加入：

```cpp
    long long seed = 0;
    bool has_seed = false;
```

- [ ] **Step 4: 解析 `--seed`**

在 `cpp/tools/cli/cli_args.cc` 的 `parse_args` 循环中，把下面分支放在 `--end` 分支之后、`--model` 分支之前：

```cpp
        } else if (a == "--seed") {
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_ll(val, p.seed)) {
                p.error = "Invalid --seed: " + val;
                return p;
            }
            p.has_seed = true;
```

- [ ] **Step 5: 更新 `run_cli.cc` 的 usage、白名单和 flag 组合校验**

在 `cpp/tools/cli/run_cli.cc` 中：

1. 将 usage 的 Commands 段替换为：

```cpp
          "  ls       list devices (tree) or tables (table)\n"
          "  schema   per-measurement data type/encoding/compression\n"
          "  meta     file-level summary without data-page scans\n"
          "  stats    per-series statistics\n"
          "  head     first N rows (default 10, use -n)\n"
          "  cat      matching rows of a device/table\n"
          "  count    per-series row counts from statistics\n"
          "  sample   sampled rows (default 10, use -n and --seed)\n"
```

2. 将 Options 段替换为：

```cpp
          "Options: -f/--format csv|tsv|json|table, -d/--device, -t/--table,\n"
          "         -m/--measurements a,b, -n/--limit, --offset, --start,\n"
          "         --end, --seed, --no-header, --model tree|table,\n"
          "         -h/--help, --version\n";
```

3. 将 `is_known_command` 的集合替换为：

```cpp
    static const std::set<std::string> kCmds = {
        "ls",    "schema", "meta",  "stats",
        "head",  "cat",    "count", "sample"};
```

4. 在匿名 namespace 中新增：

```cpp
bool validate_command_flags(const ParsedArgs& p, std::ostream& err) {
    if (p.has_seed && p.command != "sample") {
        err << "Error: --seed is only valid for sample\n";
        return false;
    }
    if (p.command == "sample" && p.offset != 0) {
        err << "Error: --offset is not valid for sample\n";
        return false;
    }
    if (!p.device.empty() && !p.table.empty()) {
        err << "Error: use either --device or --table, not both\n";
        return false;
    }
    if (p.limit < -1) {
        err << "Error: --limit must be >= 0\n";
        return false;
    }
    if (p.offset < 0) {
        err << "Error: --offset must be >= 0\n";
        return false;
    }
    if (p.has_start && p.has_end && p.start > p.end) {
        err << "Error: --start must be <= --end\n";
        return false;
    }
    return true;
}
```

5. 在 `if (p.file.empty())` 检查之后、`storage::libtsfile_init();` 之前加入：

```cpp
    if (!validate_command_flags(p, err)) {
        print_usage(err);
        return kExitUsage;
    }
```

- [ ] **Step 6: 运行测试确认通过**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=ParseArgsTest.*:RunCliTest.*
```

Expected: build succeeds; selected tests pass.

- [ ] **Step 7: 提交**

```bash
git add cpp/tools/cli/cli_args.h cpp/tools/cli/cli_args.cc cpp/tools/cli/run_cli.cc cpp/test/tools/cli_args_test.cc
git commit -m "Update tsfile CLI command surface"
```

---

### Task 2: 统计 helper 与 `stats` 扩展字段

**Files:**
- Create: `cpp/tools/commands/stat_table.h`
- Create: `cpp/tools/commands/stat_table.cc`
- Modify: `cpp/tools/commands/cmd_stats.cc`
- Create: `cpp/test/tools/stat_table_test.cc`
- Modify: `cpp/test/tools/command_e2e_test.cc`

- [ ] **Step 1: 写失败测试，直接覆盖统计值格式化**

新增 `cpp/test/tools/stat_table_test.cc`：

```cpp
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
    EXPECT_EQ(cells.is_null, std::vector<bool>({false, false, false, false, false}));
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
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=StatTableTest.*
```

Expected: build fails because `commands/stat_table.h` does not exist.

- [ ] **Step 3: 创建 `stat_table.h`**

新增 `cpp/tools/commands/stat_table.h`：

```cpp
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
std::vector<SeriesStatRow> collect_series_stats(
    const ParsedArgs& args, storage::TsFileReader& reader);
FileSummary collect_file_summary(const ParsedArgs& args,
                                 storage::TsFileReader& reader);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_STAT_TABLE_H
```

- [ ] **Step 4: 创建 `stat_table.cc`**

新增 `cpp/tools/commands/stat_table.cc`，核心实现如下：

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

std::vector<SeriesStatRow> collect_series_stats(
    const ParsedArgs& args, storage::TsFileReader& reader) {
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

- [ ] **Step 5: 用 helper 改写 `cmd_stats.cc`**

将 `cpp/tools/commands/cmd_stats.cc` 的命令体改为输出 10 列：

```cpp
#include "commands/stat_table.h"

int cmd_stats(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(
        out, fmt,
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

        std::vector<bool> nulls = {false, false, false,
                                   row.count == 0, row.count == 0};
        nulls.insert(nulls.end(), row.value_cells.is_null.begin(),
                     row.value_cells.is_null.end());
        w.write(cells, nulls);
    }
    w.finish();
    return kExitOk;
}
```

- [ ] **Step 6: 更新 E2E 断言新 stats 表头和值**

在 `cpp/test/tools/command_e2e_test.cc` 中，将 `StatsReportsCountAndTimeRange` 的表头断言替换为：

```cpp
    EXPECT_NE(out.str().find(
                  "target\tmeasurement\tcount\tstart_time\tend_time\tmin\tmax\tfirst\tlast\tsum"),
              std::string::npos);
    EXPECT_NE(out.str().find("s1\t5\t0\t4\t0\t40\t0\t40\t100"),
              std::string::npos);
```

- [ ] **Step 7: 运行测试确认通过**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=StatTableTest.*:CliE2E.StatsReportsCountAndTimeRange
```

Expected: build succeeds; selected tests pass.

- [ ] **Step 8: 提交**

```bash
git add cpp/tools/commands/stat_table.h cpp/tools/commands/stat_table.cc cpp/tools/commands/cmd_stats.cc cpp/test/tools/stat_table_test.cc cpp/test/tools/command_e2e_test.cc
git commit -m "Add tsfile CLI statistic helpers"
```

---

### Task 3: 实现 `meta` 和 `count`

**Files:**
- Create: `cpp/tools/commands/cmd_meta.cc`
- Create: `cpp/tools/commands/cmd_count.cc`
- Modify: `cpp/tools/commands/commands.h`
- Modify: `cpp/tools/cli/run_cli.cc`
- Modify: `cpp/test/tools/command_e2e_test.cc`

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
    EXPECT_NE(out.str().find(
                  "file\tmodel\tversion\tdevice_count\ttable_count\tseries_count\tstart_time\tend_time\tbloom_filter\tfile_size_bytes"),
              std::string::npos);
    EXPECT_NE(out.str().find("\ttable\t"), std::string::npos);
    EXPECT_NE(out.str().find("\t1\t"), std::string::npos);
}

TEST(CliE2E, CountReportsSeriesCountsAndTotal) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"count", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("target\tmeasurement\tcount"), std::string::npos);
    EXPECT_NE(out.str().find("table1\ts1\t5"), std::string::npos);
    EXPECT_NE(out.str().find("total\t\t"), std::string::npos);
}
```

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.MetaReportsFileSummary:CliE2E.CountReportsSeriesCountsAndTotal
```

Expected: build or tests fail because `meta` and `count` are not dispatched.

- [ ] **Step 3: 更新命令声明**

在 `cpp/tools/commands/commands.h` 中，删除 `cmd_select` 声明，并在 `cmd_schema` 与 `cmd_stats` 附近加入：

```cpp
int cmd_meta(const ParsedArgs& args, storage::TsFileReader& reader,
             OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_count(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& err);
```

- [ ] **Step 4: 新增 `cmd_meta.cc`**

创建 `cpp/tools/commands/cmd_meta.cc`：

```cpp
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "commands/commands.h"

#include "cli/exit_codes.h"
#include "commands/stat_table.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {

int cmd_meta(const ParsedArgs& args, storage::TsFileReader& reader,
             OutputFormat fmt, std::ostream& out, std::ostream& /*err*/) {
    RowWriter w(
        out, fmt,
        {"file", "model", "version", "device_count", "table_count",
         "series_count", "start_time", "end_time", "bloom_filter",
         "file_size_bytes"},
        {common::STRING, common::STRING, common::STRING, common::INT64,
         common::INT64, common::INT64, common::INT64, common::INT64,
         common::STRING, common::INT64},
        args.no_header);

    FileSummary s = collect_file_summary(args, reader);
    w.write({s.file,
             s.model,
             "",
             std::to_string(s.device_count),
             std::to_string(s.table_count),
             std::to_string(s.series_count),
             s.has_time_range ? std::to_string(s.start_time) : "",
             s.has_time_range ? std::to_string(s.end_time) : "",
             "",
             std::to_string(s.file_size_bytes)},
            {false, false, true, false, false, false,
             !s.has_time_range, !s.has_time_range, true, false});
    w.finish();
    return kExitOk;
}

}  // namespace tsfile_cli
```

- [ ] **Step 5: 新增 `cmd_count.cc`**

创建 `cpp/tools/commands/cmd_count.cc`：

```cpp
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

- [ ] **Step 6: 更新分发**

在 `cpp/tools/cli/run_cli.cc` 的命令分发链中：

```cpp
    } else if (p.command == "schema") {
        code = cmd_schema(p, reader, fmt, out, err);
    } else if (p.command == "meta") {
        code = cmd_meta(p, reader, fmt, out, err);
    } else if (p.command == "stats") {
        code = cmd_stats(p, reader, fmt, out, err);
```

并在 `cat` 后加入：

```cpp
    } else if (p.command == "count") {
        code = cmd_count(p, reader, fmt, out, err);
```

- [ ] **Step 7: 运行测试确认通过**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.MetaReportsFileSummary:CliE2E.CountReportsSeriesCountsAndTotal
```

Expected: build succeeds; selected tests pass.

- [ ] **Step 8: 提交**

```bash
git add cpp/tools/commands/cmd_meta.cc cpp/tools/commands/cmd_count.cc cpp/tools/commands/commands.h cpp/tools/cli/run_cli.cc cpp/test/tools/command_e2e_test.cc
git commit -m "Add tsfile meta and count commands"
```

---

### Task 4: 实现 deterministic `sample`

**Files:**
- Modify: `cpp/tools/format/result_set_format.h`
- Modify: `cpp/tools/format/result_set_format.cc`
- Create: `cpp/tools/commands/cmd_sample.cc`
- Modify: `cpp/tools/commands/commands.h`
- Modify: `cpp/tools/cli/run_cli.cc`
- Create: `cpp/test/tools/result_set_sample_test.cc`
- Modify: `cpp/test/tools/command_e2e_test.cc`

- [ ] **Step 1: 写失败 E2E 测试**

在 `cpp/test/tools/command_e2e_test.cc` 末尾追加：

```cpp
TEST(CliE2E, SampleIsReproducibleWithSeed) {
    Fixture f;
    std::ostringstream out1;
    std::ostringstream err1;
    std::ostringstream out2;
    std::ostringstream err2;

    int code1 = tsfile_cli::run_cli({"sample", "-m", "s1", "-n", "3",
                                     "--seed", "7", "-f", "tsv", f.path},
                                    out1, err1);
    int code2 = tsfile_cli::run_cli({"sample", "-m", "s1", "-n", "3",
                                     "--seed", "7", "-f", "tsv", f.path},
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

- [ ] **Step 2: 运行测试确认失败**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.SampleIsReproducibleWithSeed
```

Expected: build or test fails because `sample` is not dispatched.

- [ ] **Step 3: 声明 sampled writer**

在 `cpp/tools/format/result_set_format.h` 中追加：

```cpp
int write_result_set_sampled(storage::ResultSet* rs, OutputFormat fmt,
                             bool no_header, std::ostream& out,
                             long long limit, unsigned long long seed);
```

- [ ] **Step 4: 实现 sampled writer**

在 `cpp/tools/format/result_set_format.cc` 中新增 include：

```cpp
#include <random>
```

在 `write_result_set` 之后新增：

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
                             bool no_header, std::ostream& out,
                             long long limit, unsigned long long seed) {
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

- [ ] **Step 5: 新增 `cmd_sample.cc`**

创建 `cpp/tools/commands/cmd_sample.cc`：

```cpp
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
    int wret = write_result_set_sampled(rs, fmt, args.no_header, out, limit,
                                        seed);
    reader.destroy_query_data_set(rs);
    return wret == 0 ? kExitOk : kExitRuntime;
}

}  // namespace tsfile_cli
```

- [ ] **Step 6: 更新声明和分发**

在 `cpp/tools/commands/commands.h` 加入：

```cpp
int cmd_sample(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err);
```

在 `cpp/tools/cli/run_cli.cc` 的分发链中 `count` 后加入：

```cpp
    } else if (p.command == "sample") {
        code = cmd_sample(p, reader, fmt, out, err);
```

- [ ] **Step 7: 运行测试确认通过**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.SampleIsReproducibleWithSeed
```

Expected: build succeeds; selected test passes.

- [ ] **Step 8: 提交**

```bash
git add cpp/tools/format/result_set_format.h cpp/tools/format/result_set_format.cc cpp/tools/commands/cmd_sample.cc cpp/tools/commands/commands.h cpp/tools/cli/run_cli.cc cpp/test/tools/command_e2e_test.cc
git commit -m "Add deterministic tsfile sample command"
```

---

### Task 5: 移除 `select` 并把时间范围测试迁到 `cat`

**Files:**
- Delete: `cpp/tools/commands/cmd_select.cc`
- Modify: `cpp/test/tools/cli_args_test.cc`
- Modify: `cpp/test/tools/command_e2e_test.cc`

- [ ] **Step 1: 更新解析测试里的旧命令名**

在 `cpp/test/tools/cli_args_test.cc` 中，将 `MeasurementsSplitOnComma` 的输入从：

```cpp
auto p =
    tsfile_cli::parse_args({"select", "-m", "s1,s2,s3", "data.tsfile"});
```

改为：

```cpp
auto p =
    tsfile_cli::parse_args({"cat", "-m", "s1,s2,s3", "data.tsfile"});
```

- [ ] **Step 2: 将 `select` E2E 改为 `cat`**

在 `cpp/test/tools/command_e2e_test.cc` 中，把 `SelectWithTimeRange` 改名为 `CatWithTimeRange`，命令从 `select` 改为 `cat`：

```cpp
TEST(CliE2E, CatWithTimeRange) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "2",
                                    "--end", "3", "-f", "tsv", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "time\ts1\n2\t20\n3\t30\n");
}
```

把 `SelectJsonIsNdjson` 改名为 `CatJsonIsNdjson`，命令从 `select` 改为 `cat`：

```cpp
TEST(CliE2E, CatJsonIsNdjson) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "0",
                                    "--end", "0", "-f", "json", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "{\"time\":0,\"s1\":0}\n");
}
```

- [ ] **Step 3: 删除 `cmd_select.cc`**

Run:

```bash
rm cpp/tools/commands/cmd_select.cc
```

- [ ] **Step 4: 运行测试确认通过**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=ParseArgsTest.MeasurementsSplitOnComma:RunCliTest.SelectIsNoLongerKnownCommand:CliE2E.CatWithTimeRange:CliE2E.CatJsonIsNdjson
```

Expected: build succeeds; selected tests pass.

- [ ] **Step 5: 提交**

```bash
git add cpp/test/tools/cli_args_test.cc cpp/test/tools/command_e2e_test.cc cpp/tools/commands/cmd_select.cc
git commit -m "Remove tsfile select command"
```

---

### Task 6: 全量验证、help 文案快照和最终提交检查

**Files:**
- Modify: `docs/superpowers/plans/2026-06-02-tsfile-cli-redesign.md` only if execution notes need correction during implementation.

- [ ] **Step 1: 跑完整 CLI 相关测试**

Run:

```bash
cd cpp && bash build.sh -t=Debug && ./build/Debug/lib/TsFile_Test --gtest_filter=CliE2E.*:ParseArgsTest.*:RunCliTest.*:RowWriterTest.*:ResolveFormatTest.*:CsvEscapeTest.*:JsonEscapeTest.*:TypeNameTest.*:EncodingNameTest.*:CompressionNameTest.*:StatTableTest.*
```

Expected: build succeeds; selected tests pass.

- [ ] **Step 2: 跑完整 C++ 测试可执行文件**

Run:

```bash
cd cpp && ./build/Debug/lib/TsFile_Test
```

Expected: all tests pass. If unrelated existing tests fail, capture the exact failing test names and output before deciding whether to narrow verification.

- [ ] **Step 3: 手动检查 CLI help 不再出现 `select`**

Run:

```bash
cd cpp && ./build/Debug/bin/tsfile --help
```

Expected: stdout contains `meta`, `count`, `sample`; stdout does not contain `select`.

- [ ] **Step 4: 检查 whitespace 和暂存范围**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` exits 0. `git status --short` shows only this CLI redesign work and any pre-existing unrelated files remain unstaged.

- [ ] **Step 5: 最终提交**

如果 Task 6 只产生测试/文档微调，提交它们：

```bash
git add docs/superpowers/plans/2026-06-02-tsfile-cli-redesign.md
git commit -m "Document tsfile CLI redesign execution notes"
```

如果 Task 6 没有产生文件改动，不创建空提交。

## 覆盖检查

- `select` 删除：Task 1、Task 5。
- `meta`：Task 3。
- `count`：Task 3。
- `sample` 与 `--seed`：Task 1、Task 4。
- `stats` 扩展到 min/max/first/last/sum：Task 2。
- 共享参数投影、时间范围、limit/offset：现有 `row_query.cc` 保留，Task 5 用 `cat` E2E 覆盖时间范围。
- 输出格式与 stdout/stderr：现有 formatter 测试保留，Task 6 跑完整相关测试。
- 构建、安装和 CMake glob：现有 `cpp/tools/CMakeLists.txt` 使用 `GLOB_RECURSE`，新增 `.cc` 自动纳入，Task 6 通过 build 验证。
