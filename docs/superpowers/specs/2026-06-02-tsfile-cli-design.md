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

# Design: TsFile C++ CLI（`tsfile`）

- **日期**：2026-06-02
- **模块**：`cpp/`（新增 `cpp/tools/`、`cpp/test/tools/`）
- **状态**：设计已批准；部分实现（见 §10「实现现状」），剩余工作见
  `docs/superpowers/plans/2026-06-02-tsfile-cli.md`
- **目标参照**：Parquet 的 `parquet-cli` / `pqrs` —— 让 `.tsfile` 像 `.parquet`
  一样可以在命令行里被浏览、检视、预览、导出。
- **调研依据**：
  - `/Users/zhanghongyin/reasearchNotes/research/tsfile/Report.md`（主报告 §5.3）
  - `/Users/zhanghongyin/reasearchNotes/research/tsfile/调研报告/各文件格式CLI工具调研.md`

本文是「实现 tsfile-cli」的单一权威设计文档，取代此前拆分的
`2026-06-01-tsfile-unix-cli-design.md`。

## 1. 目标

为 TsFile 提供一个单二进制、可组合、适合管道使用的 C++ 命令行工具：

```sh
tsfile-cli <command> [options] <file.tsfile>
tsfile-cli --help | --version
tsfile-cli help <command>
```

让用户能像查看其他自描述数据文件一样查看 `.tsfile`：发现命名空间、查看 schema 和
元数据、预览行、流式导出行、统计行数、抽样行，而不需要自己写 reader 代码。

命令面贴近 Parquet 及相近数据格式的工具谱系：动词为
`ls / schema / meta / stats / head / cat / count / sample`，投影、时间范围、limit、
offset 作为行输出命令的共享参数。

## 2. 调研结论对设计的约束

TsFile 同时有两个身份：

1. **像 Parquet 的文件形态**：封存、不可变、自描述、列式，带 footer 元数据、偏移和
   统计量。因此 Parquet CLI 是最重要的命令设计参照。
2. **像 HDF5/netCDF 的命名空间**：TsFile 不总是单表文件；tree 模型下有多 device，
   table 模型下有多 table。因此它需要一个 `ls` 式命名空间命令。

CLI 调研把不可变数据文件的只读工具谱系统一为：

```text
schema | meta(/footer/stats) | head(/cat) | count | sample
```

Parquet 是最完整模板：Apache `parquet-cli` 提供 `schema`、`meta`、`footer`、`head`、
`cat` 以及索引/统计命令；Rust `pqrs` 补齐了特别有用的 `rowcount` 和 `sample`。ORC
与 Avro 也印证同一模式（`meta`/`data`/`count`、`getschema`/`getmeta`/`cat`/`count`）。
HDF5 和 netCDF 提供命名空间与 header 经验：`h5ls`、`h5dump -H`、`ncdump -h` 的价值在于
不用打开应用就能查看文件内部结构。

调研的一句话结论是：TsFile **缺的不是「有没有 CLI」，而是「动词齐不齐 + 是否统一分发 +
能否被通用查看器看见」**。本设计解决前两者——统一成 `tsfile <subcommand>` 分发器并补齐
只读动词；通用查看器接入（DuckDB/ClickHouse/VisiData reader）属后续工作（§9）。

## 3. 范围

包含：

- 一个名为 `tsfile-cli` 的多命令二进制。
- 只读命令：`ls`、`schema`、`meta`、`stats`、`head`、`cat`、`count`、`sample`。
- 输出格式、模型选择、列投影、行数限制、offset、时间范围、抽样种子等共享参数。
- 基于现有 `storage::TsFileReader` 读路径实现，不修改存储引擎。
- 遵守 Unix 风格：数据输出到 stdout，诊断和错误输出到 stderr，便于接入 `awk`、`jq`、
  `sort`、导入工具和 shell 管道。

不包含：

- 写入、转换、合并、重写命令。
- 与 Java `TsFileSketchTool` 完全等价的字节结构 dump。
- FUSE 挂载、DuckDB/ClickHouse/VisiData connector 或 SQL replacement scan。
- ISO 时间格式化，以及超出时间范围和 measurement 投影的复杂谓词。
- 拆分为多个 `tsfile-*` 二进制；不引入第三方参数解析库。

## 4. 命令谱系

| 动词 | 谱系来源 | 目的 | 主要 reader 支撑 |
|---|---|---|---|
| `ls` | `h5ls`、`ncdump -h` | tree 模型列 device，table 模型列 table，一行一个名字 | `get_all_device_ids()`、`get_all_table_schemas()` |
| `schema` | `parquet-cli schema`、Avro `getschema`、SQL `DESCRIBE` | 输出序列或列的类型信息 | `get_timeseries_metadata()`、`get_timeseries_schema()` |
| `meta` | `parquet-cli meta/footer`、Avro `getmeta` | 输出文件级摘要：模型、版本、命名空间规模、全局时间范围、Bloom filter、文件大小 | reader 元数据 + 文件系统元数据 |
| `stats` | `parquet-cli column-index/check-stats`、ORC statistics、SQL `SUMMARIZE` | 输出每条序列的 count、时间范围、min、max、first、last、sum | `get_timeseries_metadata()` 统计量 |
| `head` | `parquet-cli head`、`pqrs head`、SQL `LIMIT` | 输出前 N 行 | 共享 row query 路径 |
| `cat` | `parquet-cli cat/scan`、Avro `cat`/`tojson`、ORC `data` | 流式输出匹配行 | 共享 row query 路径 |
| `count` | `pqrs rowcount`、ORC `count`、Avro `count`、SQL `count(*)` | 不扫描数据页，从统计量输出行数 | `get_timeseries_metadata()` 统计量 |
| `sample` | `pqrs sample`、SQL sampling | 输出可复现样本行 | 共享 row query 路径 + 确定性抽样 |

`select` **不是**独立动词。它实际承载的是投影、时间过滤、limit 和 offset；这些能力作为
`head`、`cat`、`sample` 的共享参数存在，与 Parquet 工具把列选择挂到行输出命令上的习惯
一致。

## 5. 命令语义

### `ls`

输出顶层逻辑命名空间：tree 模型每行一个 device ID，table 模型每行一个 table name。默认
输出刻意保持简单稳定，便于管道处理；measurement / column 级细节由 `schema` 负责。

### `schema`

输出统一的逻辑 schema 表：

```text
target, measurement, datatype, encoding, compression
```

tree 模型下 `target` 是 device、`measurement` 是测点；table 模型下 `target` 是 table、
`measurement` 是列名。若当前公开 API 能拿到 datatype 但拿不到 encoding/compression
（如 table 模型），CSV/TSV 输出空字段，JSON 输出 `null`。`-m` 可投影到指定列。

### `meta`

输出无需解码数据页即可回答的文件级信息：

```text
file, model, version, device_count, table_count, series_count,
start_time, end_time, bloom_filter, file_size_bytes
```

对应 Parquet `meta`/`footer`：先快速了解文件，再决定是否继续查看 schema、stats 或
行数据。若某字段当前公开 reader API 无法直接暴露（如 `version`、`bloom_filter`），输出
空值而不是扫描数据页。

### `stats`

输出每条序列的统计量：

```text
target, measurement, count, start_time, end_time, min, max, first, last, sum
```

直接暴露 TsFile 的格式优势：Chunk/Page 级统计量包含 count 和数值摘要，很多查看问题不
需要读取或解码数据页。`min`/`max`/`first`/`last`/`sum` 按类型可空（如布尔无 min/max，
文本无 sum）。

### `head` 与 `cat`

行输出命令：

- `head` 默认输出前 10 行，并接受 `-n, --limit` 覆盖行数。
- `cat` 默认流式输出全部匹配行，除非显式指定 limit。
- 两者都通过共享 row query 路径接受投影（`-m`）、时间范围（`--start`/`--end`）、offset。

`head` 本质上等价于带默认 limit 的 `cat`。

### `count`

从统计量读取行数，不通过 row iterator 扫描数据。这是 TsFile 优于 `parquet-cli` 表面的
地方（后者没有独立 row-count 子命令）。作用域规则：

- 不指定作用域：输出所有序列的 count，并给出总数行；
- `--device`：限定某个 tree-model device；
- `--table`：限定某个 table-model table。

### `sample`

通过共享 row query 和确定性抽样输出 N 条样本行，默认 N=10，接受 `--seed` 保证可复现。
实现使用 reservoir sampling。设计要求：同一文件、作用域、投影、时间范围、limit 和 seed
下输出稳定。

## 6. 共享参数

| 参数 | 含义 | 适用命令 |
|---|---|---|
| `-f, --format csv\|tsv\|json\|table` | 输出格式；默认随 stdout 是否为 TTY 自适应 | 全部 |
| `-d, --device <id>` | 限定 tree-model device | 行输出命令、`schema`、`stats`、`count` |
| `-t, --table <name>` | 限定 table-model table | 行输出命令、`schema`、`stats`、`count` |
| `-m, --measurements a,b,c` | measurement / column 投影 | `schema`、`head`、`cat`、`sample` |
| `-n, --limit N` | 最大输出行数；`head` 用它作为行数 | `head`、`cat`、`sample` |
| `--offset N` | 跳过开头 N 行 | `head`、`cat` |
| `--start <ts>` / `--end <ts>` | epoch milliseconds 时间范围，闭区间 | `head`、`cat`、`sample` |
| `--seed N` | 可复现抽样种子 | `sample` |
| `--no-header` | 不输出表头 | 表格类输出 |
| `--model tree\|table` | 强制模型，覆盖自动检测 | 全部 |
| `-h, --help` / `--version` | 帮助和版本 | 顶层和单命令 |

参数与命令不匹配时按 usage error 处理（退出码 `1`，错误到 stderr）。已实现的组合校验
（`run_cli.cc::validate_command_flags`）：

- `--seed` 仅对 `sample` 有效；
- `--offset` 对 `sample` 无效；
- `--device` 与 `--table` 不能同时使用；
- `--limit >= -1`、`--offset >= 0`、`--start <= --end`。

## 7. Tree 与 table 模型

模型检测规则自动化：

```text
get_all_table_schemas() non-empty => table model
otherwise                         => tree model
```

`--model tree|table` 可覆盖自动检测。统一命令面下的行为：

- `ls` 在 tree 文件中列 device，在 table 文件中列 table。
- `schema`、`stats`、`count` 可用 `--device` 或 `--table` 收窄作用域。
- 行输出始终把时间列视为第一列；tree 模型用 device + measurements，table 模型用
  table + columns。

## 8. 输出格式与退出码

formatter（`format/output_format.*`、`format/result_set_format.*`）：

- `table`：面向人的对齐表格；stdout 是终端时默认使用。
- `tsv`：tab 分隔；stdout 被 pipe 或 redirect 时默认使用。
- `csv`：按 RFC 4180 引号规则输出（字段含分隔符/引号/换行时加引号，内部引号双写）。
- `json`：NDJSON，一行一个 JSON object；数值/布尔裸输出，其余加引号，null 输出 `null`。

null 在 CSV/TSV 中输出为空字段。时间戳输出存储中的 epoch milliseconds 整数（ISO 格式化
是后续工作）。数据→stdout，诊断/usage/错误→stderr。

退出码：

| 退出码 | 条件 |
|---|---|
| `0` | 成功 |
| `1` | usage 或参数错误 |
| `2` | 文件打不开或文件损坏 |
| `3` | 查询或运行时错误 |

`ReadFile::open`（`cpp/src/file/read_file.cc`）原先向 stdout 打印打开错误，会污染
`tsfile cat f | jq`，已改为向 stderr 输出。

## 9. 架构

```text
cpp/tools/
├── CMakeLists.txt              # OBJECT 库 tsfile_cli_obj + 可执行文件 tsfile-cli
├── tools_main.cc               # main(): 转发 argv 给 run_cli
├── cli/
│   ├── exit_codes.h            # kExitOk/kExitUsage/kExitFile/kExitRuntime
│   ├── cli_args.h / .cc        # ParsedArgs + parse_args()
│   └── run_cli.h / .cc         # 顶层 usage、白名单、flag 组合校验、reader open、分发
├── format/
│   ├── output_format.h / .cc   # 纯层：resolve_format、转义、类型名、RowWriter
│   └── result_set_format.h/.cc # ResultSet 泵：cell_to_string、write_result_set[_sampled]
└── commands/
    ├── commands.h              # is_table_model + run_row_query + cmd_* 声明
    ├── row_query.cc            # head/cat/sample 共用的 query 构造
    ├── stat_table.h / .cc      # collect_series_stats / collect_file_summary / 统计值格式化
    ├── cmd_ls.cc  cmd_schema.cc  cmd_meta.cc  cmd_stats.cc
    └── cmd_head.cc cmd_cat.cc   cmd_count.cc  cmd_sample.cc

cpp/test/tools/
├── cli_test_util.h             # 写一个 table-model fixture .tsfile 到临时路径
├── cli_args_test.cc            # parse_args + run_cli 参数/分发单元测试
├── output_format_test.cc       # 纯 formatter 单元测试
├── stat_table_test.cc          # 统计值格式化与汇总 helper 单元测试
└── command_e2e_test.cc         # 通过 run_cli in-process 跑每个命令的 E2E（含确定性抽样）
```

设计要点：

- CLI 逻辑编译为 OBJECT 库 `tsfile_cli_obj`，既链入可执行文件 `tsfile`，也链入
  `TsFile_Test`，使命令可在进程内对注入的 `std::ostream&` 测试。
- formatter 分纯层（无 reader 依赖、重单元测试）和 `ResultSet` 泵层（E2E 测试）。
- 手写参数 parser，零新依赖。
- 不修改存储引擎：所有命令使用现有 reader 元数据或现有 row query API。

构建：`cpp/CMakeLists.txt` 提供 `option(BUILD_TOOLS ... ON)`，开启时
`add_subdirectory(tools)`，链接 `libtsfile` 产出 `tsfile` 可执行文件，并 `install()` 到
`bin`。`cpp/tools/CMakeLists.txt` 用 `GLOB_RECURSE` 收集源文件，新增 `.cc` 自动纳入。

## 10. 实现现状（2026-06-02）

工作树处于「半迁移」状态，剩余工作详见
`docs/superpowers/plans/2026-06-02-tsfile-cli.md`：

- **已提交**（commit `a392a56f`，仅 `cli/` 层 + `cli_args_test.cc`）：
  - 8 动词命令面、usage/help、白名单、`--seed` 解析、`validate_command_flags`；
  - `select` 已从白名单移除（`select` → `Unknown command`，退出码 1）；
  - `meta`/`count`/`sample` 在白名单内，但被 `is_unimplemented_command` 拦截，返回
    “command not implemented yet”。
- **已实现但未提交**（untracked）：`ls`、`schema`、`stats`（仅 5 列旧版）、`head`、`cat`
  及其依赖（`commands/`、`format/`、`tools_main.cc`、`CMakeLists.txt` 等）和 E2E 测试。
- **遗留不一致**：
  - `cmd_select.cc` 与 `commands.h` 中 `cmd_select` 声明仍在，但不被分发——死代码。
  - `command_e2e_test.cc` 仍以 `select` 命令测试 `SelectWithTimeRange` /
    `SelectJsonIsNdjson`，与已移除 `select` 的命令面冲突——若构建会失败。
- **尚未实现**：`stats` 扩展到 min/max/first/last/sum；`meta`；`count`；`sample`。

## 11. 测试

测试放在 `cpp/test/tools/`，使用 Google Test，只验证 CLI 行为和真实 reader 路径，不新增
存储引擎行为。

单元测试覆盖：`cli_args`（命令与参数解析、`--seed`、未知命令/参数、命令/参数不匹配）；
formatter（csv/tsv/json/table，含 null、分隔符、引号、换行）；模型检测（含 `--model`
覆盖）；统计值格式化（`statistic_value_cells` 各类型）。

E2E 测试：生成 table-model fixture，通过进程内 `run_cli` 跑每个命令，断言退出码、stdout
形状、stderr 行为；确定性抽样由固定 `--seed` 跑两次断言输出一致覆盖；TTY 自适应格式由
单元测试覆盖，E2E 显式指定 `--format`。

## 12. 被拒绝的方案

- **保留 `select` 动词**：拒绝。它与 `cat`/`head` 重叠，真正提供的是投影和过滤，应落到
  共享参数上（Parquet 风格）。
- **把 `count` 折叠进 `stats` 或 `meta`**：拒绝。`count` 足够常用，且 TsFile 可从统计量
  低成本回答，显式保留能让这个格式优势更易被发现。
- **为完全模仿 Parquet 删除 `ls`**：拒绝。TsFile 不总是单逻辑表，多 device/多 table
  命名空间使 `ls` 成为用户经常需要的第一个命令。
- **现在实现写入或转换命令**：拒绝。本阶段只读命令风险更低，正对应调研结论。

## 13. 后续工作

- 与 Java `TsFileSketchTool` 对齐的结构 dump 命令。
- ISO 时间格式化；超出时间范围和 measurement 投影的复杂谓词。
- 写入、转换、合并、重写命令。
- DuckDB / ClickHouse / VisiData reader，让 TsFile 进入多格式查询/查看工具
  （对应主报告 §6.3.3「缺连接器宿主的适配层」）。
- 只读 FUSE 命名空间或 TableFS 视图（若项目选择通过文件系统路径暴露 TsFile）。
