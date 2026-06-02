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

# Design: TsFile C++ CLI 重设计

- **日期**：2026-06-02
- **模块**：`cpp/`
- **状态**：设计已批准，待编写实现计划
- **调研依据**：
  `/Users/zhanghongyin/reasearchNotes/research/tsfile/Report.md` 第 5.3 节，
  以及
  `/Users/zhanghongyin/reasearchNotes/research/tsfile/调研报告/各文件格式CLI工具调研.md`

## 目标

为 TsFile 提供一个单二进制、可组合、适合管道使用的 C++ 命令行工具：

```sh
tsfile <command> [options] <file.tsfile>
tsfile --help | --version
tsfile help <command>
```

这个 CLI 要让用户能像查看其他自描述数据文件一样查看 `.tsfile`：发现命名空间、查看
schema 和元数据、预览行、流式导出行、统计行数、抽样行，而不需要自己写 reader 代码。

本次重设计保留 v1 的整体方向，但让命令面更贴近 Parquet 及相近数据格式的工具谱系。
可见变化是：删除 `select` 动词，新增 `meta`、`count`、`sample`，并把投影、时间范围、
limit、offset 下沉为行输出命令的共享参数。

## 调研结论对设计的约束

TsFile 同时有两个身份：

1. **像 Parquet 的文件形态**：封存、不可变、自描述、列式，带 footer 元数据、偏移和统计量。
   因此 Parquet CLI 是最重要的命令设计参照。
2. **像 HDF5/netCDF 的命名空间**：TsFile 不总是单表文件；tree 模型下有多 device，
   table 模型下有多 table。因此它需要一个 `ls` 式命名空间命令。

CLI 调研把不可变数据文件的只读工具谱系统一为：

```text
schema | meta(/footer/stats) | head(/cat) | count | sample
```

Parquet 是最完整模板：Apache `parquet-cli` 提供 `schema`、`meta`、`footer`、`head`、
`cat` 以及索引/统计命令；Rust `pqrs` 补齐了特别有用的 `rowcount` 和 `sample`。
ORC 与 Avro 也印证同一模式：官方工具提供 `meta`/`data`/`count`、`getschema`/
`getmeta`/`cat`/`count`。HDF5 和 netCDF 则提供命名空间与 header 经验：`h5ls`、
`h5dump -H`、`ncdump -h` 的价值在于不用打开应用就能查看文件内部结构。

映射到 TsFile 后，除了五动词谱系，还需要额外保留 `ls`，因为 TsFile 文件内部存在
device/table 命名空间。

## 范围

本次重设计包含：

- 一个名为 `tsfile` 的多命令二进制。
- 只读命令：`ls`、`schema`、`meta`、`stats`、`head`、`cat`、`count`、`sample`。
- 输出格式、模型选择、列投影、行数限制、offset、时间范围等共享参数。
- 基于现有 `storage::TsFileReader` 读路径实现。
- 遵守 Unix 风格：数据输出到 stdout，诊断和错误输出到 stderr，便于接入 `awk`、`jq`、
  `sort`、导入工具和 shell 管道。

本次重设计不包含：

- 写入、转换、合并、重写命令。
- 与 Java `TsFileSketchTool` 完全等价的字节结构 dump。
- FUSE 挂载、DuckDB/ClickHouse/VisiData connector 或 SQL replacement scan。
- ISO 时间格式化，以及超出时间范围和 measurement 投影的复杂谓词。
- 拆分为多个 `tsfile-*` 二进制。

## 命令谱系

命令集合对齐 Parquet/ORC/Avro 的只读谱系，并吸收 HDF5/netCDF 的命名空间查看能力。

| 动词 | 谱系来源 | 目的 | 主要 reader 支撑 |
|---|---|---|---|
| `ls` | `h5ls`、`ncdump -h`；Parquet 通常不需要 | tree 模型列 device，table 模型列 table，一行一个名字 | `get_all_device_ids()`、`get_all_table_schemas()` |
| `schema` | `parquet-cli schema`、Avro `getschema`、SQL `DESCRIBE` | 输出序列或列的类型信息 | `get_timeseries_schema()`、`get_table_schema()` |
| `meta` | `parquet-cli meta/footer`、Avro `getmeta`、DuckDB metadata 函数 | 输出文件级摘要：模型、版本、命名空间规模、全局时间范围、Bloom filter、文件大小 | reader 元数据和文件系统元数据 |
| `stats` | `parquet-cli column-index/check-stats`、ORC statistics、SQL `SUMMARIZE` | 输出每条序列的 count、时间范围、min、max、first、last、sum | `get_timeseries_metadata()` 统计量 |
| `head` | `parquet-cli head`、`pqrs head`、SQL `LIMIT` | 输出前 N 行 | 共享 row query 路径 |
| `cat` | `parquet-cli cat/scan`、Avro `cat`/`tojson`、ORC `data` | 流式输出匹配行 | 共享 row query 路径 |
| `count` | `pqrs rowcount`、ORC `count`、Avro `count`、SQL `count(*)` | 不扫描数据页，输出序列或作用域内行数 | `get_timeseries_metadata()` 统计量 |
| `sample` | `pqrs sample`、SQL sampling | 输出可复现样本行 | 共享 row query 路径加确定性抽样 |

`select` 不再作为独立动词。它实际承载的是投影、时间过滤、limit 和 offset；这些能力应作为
`head`、`cat`、`sample` 的共享参数存在。这也更接近 Parquet 工具把列选择挂到
行输出命令上的习惯。

## 命令语义

### `ls`

`ls` 输出顶层逻辑命名空间：

- tree 模型：每行一个 device ID；
- table 模型：每行一个 table name。

默认输出刻意保持简单稳定，便于管道处理。measurement 或 column 级细节由 `schema` 负责。

### `schema`

`schema` 输出统一的逻辑 schema 表：

```text
target, measurement, datatype, encoding, compression
```

tree 模型下，`target` 是 device，`measurement` 是测点。table 模型下，`target` 是 table，
`measurement` 是列名。若当前公开 API 能拿到 datatype 但拿不到 encoding/compression，
CSV/TSV 输出空字段，JSON 输出 `null`。

### `meta`

`meta` 输出无需解码数据页即可回答的文件级信息。目标字段为：

```text
file, model, version, device_count, table_count, series_count,
start_time, end_time, bloom_filter, file_size_bytes
```

它是 TsFile 对 Parquet `meta`/`footer` 的对应命令：先快速了解文件，再决定是否继续查看
schema、stats 或行数据。若某个文件级字段当前公开 reader API 无法直接暴露，实现时应输出
空值而不是扫描数据页。

### `stats`

`stats` 输出每条序列的统计量：

```text
target, measurement, count, start_time, end_time,
min, max, first, last, sum
```

这直接暴露 TsFile 的格式优势：Chunk/Page 级统计量包含 count 和数值摘要，很多查看问题
不需要读取或解码数据页。

### `head` 与 `cat`

`head` 和 `cat` 是行输出命令：

- `head` 默认输出前 10 行，并接受 `-n, --limit` 覆盖行数。
- `cat` 默认流式输出全部匹配行，除非显式指定 limit。
- 两者都通过共享 row query 路径接受投影（`--measurements`）和时间范围（`--start`、
  `--end`）。

`head` 是面向用户的便捷命令，本质上等价于带默认 limit 的 `cat`。

### `count`

`count` 从统计量中读取行数，不通过 row iterator 扫描数据。这是 TsFile 可以优于常见
Parquet CLI 表面的地方：`parquet-cli` 没有独立 row-count 命令，而 TsFile 的统计量能
低成本回答 count。

作用域规则：

- 不指定作用域：输出所有序列的 count，并在适合的格式中给出总数；
- `--device`：输出某个 tree-model device 下的 count；
- `--table`：输出某个 table-model table 下的 count。

### `sample`

`sample` 通过共享 row query 和 formatter 输出 N 条样本行，默认 N 为 10，并接受
`--seed` 保证可复现。

实现可以使用 reservoir sampling 或确定性 skip 策略。设计要求是：同一文件、作用域、
投影、时间范围、limit 和 seed 下，输出稳定。

## 共享参数

| 参数 | 含义 | 适用命令 |
|---|---|---|
| `-f, --format csv\|tsv\|json\|table` | 输出格式；默认随 stdout 是否为 TTY 自适应 | 全部 |
| `-d, --device <id>` | 限定 tree-model device | 行输出命令、`schema`、`stats`、`count` |
| `-t, --table <name>` | 限定 table-model table | 行输出命令、`schema`、`stats`、`count` |
| `-m, --measurements a,b,c` | measurement 或 column 投影 | `head`、`cat`、`sample` |
| `-n, --limit N` | 最大输出行数；`head` 用它作为行数 | `head`、`cat`、`sample` |
| `--offset N` | 跳过开头 N 行 | `head`、`cat` |
| `--start <ts>` / `--end <ts>` | epoch milliseconds 时间范围，闭区间 | `head`、`cat`、`sample` |
| `--seed N` | 可复现抽样种子 | `sample` |
| `--no-header` | 不输出表头 | 表格类输出 |
| `--model tree\|table` | 强制模型，覆盖自动检测 | 全部 |
| `-h, --help` / `--version` | 帮助和版本 | 顶层和单命令 |

参数与命令不匹配时按 usage error 处理。例如在非 `sample` 命令使用 `--seed`，或在
`sample` 命令使用 `--offset`，应返回退出码 `1`，并向 stderr 输出明确错误信息。

## Tree 与 table 模型

模型检测规则保持自动化：

```text
get_all_table_schemas() non-empty => table model
otherwise                         => tree model
```

`--model tree|table` 可覆盖自动检测。

统一命令面下的行为：

- `ls` 在 tree 文件中列 device，在 table 文件中列 table。
- `schema`、`stats`、`count` 可用 `--device` 或 `--table` 收窄作用域。
- 行输出始终把时间列视为第一列。
- tree 模型行输出使用 device + measurements；table 模型行输出使用 table + columns。

## 输出格式

保留 v1 formatter 设计：

- `table`：面向人的对齐表格；stdout 是终端时默认使用。
- `tsv`：tab 分隔；stdout 被 pipe 或 redirect 时默认使用。
- `csv`：按 RFC 4180 引号规则输出。字段包含分隔符、引号或换行时加引号，内部引号双写。
- `json`：NDJSON，一行一个 JSON object。

null 在 CSV/TSV 中输出为空字段，在 JSON 中输出为 `null`。时间戳输出存储中的 epoch
milliseconds 整数。ISO 时间格式是后续工作。

数据输出到 stdout；诊断、usage、错误输出到 stderr。

## 退出码

| 退出码 | 条件 |
|---|---|
| `0` | 成功 |
| `1` | usage 或参数错误 |
| `2` | 文件打不开或文件损坏 |
| `3` | 查询或运行时错误 |

`ReadFile::open` 中当前会向 stdout 打印打开错误（`cpp/src/file/read_file.cc`）。CLI
路径必须避免污染 stdout，应改为向 stderr 输出诊断。

## 架构与 v1 迁移

当前未提交的 v1 实现已经形成合理边界：

```text
cpp/tools/
├── CMakeLists.txt
├── tools_main.cc
├── cli/
│   ├── cli_args.h
│   ├── cli_args.cc
│   ├── run_cli.h
│   ├── run_cli.cc
│   └── exit_codes.h
├── format/
│   ├── output_format.h
│   ├── output_format.cc
│   ├── result_set_format.h
│   └── result_set_format.cc
└── commands/
    ├── commands.h
    ├── row_query.cc
    ├── cmd_ls.cc
    ├── cmd_schema.cc
    ├── cmd_stats.cc
    ├── cmd_head.cc
    ├── cmd_cat.cc
    └── cmd_select.cc
```

重设计后的目标结构是在上述基础上调整 commands：

```text
cpp/tools/commands/
├── commands.h
├── row_query.cc
├── cmd_ls.cc
├── cmd_schema.cc
├── cmd_meta.cc
├── cmd_stats.cc
├── cmd_head.cc
├── cmd_cat.cc
├── cmd_count.cc
└── cmd_sample.cc
```

迁移项：

- 删除 `cmd_select.cc`。
- 新增 `cmd_meta.cc`、`cmd_count.cc`、`cmd_sample.cc`。
- 在 `ParsedArgs` 中新增 `seed`，并在 `cli_args.cc` 解析 `--seed`。
- 更新 `run_cli.cc` 的命令注册、help 文案和命令校验。
- 更新 `commands.h` 声明。
- 保留 `row_query.cc` 作为 `head`、`cat`、`sample` 的共享行读取路径。
- 保留 formatter 模块；仅在新命令的结果形状需要时复用通用 row/table 输出能力。
- 不改 storage engine。新增命令全部使用现有 reader 元数据或现有 row query API。

不引入第三方参数解析库。当前手写 parser 足以覆盖这个命令面，也保持 C++ 模块的低依赖。

## 构建与发布

`cpp/CMakeLists.txt` 在工具开启时包含 `cpp/tools/`，构建链接 `libtsfile` 的 `tsfile`
可执行文件。

该二进制随 C++ 产物安装。`cpp/examples/` 继续保留示例定位；CLI 放在 `cpp/tools/`，
因为它是面向用户的工具，不是示例代码。

## 测试

测试放在 `cpp/test/tools/`，使用 Google Test。

单元测试覆盖：

- `cli_args`：命令与参数解析，包括 `--seed`、未知命令、错误参数值、缺失文件参数、
  命令与参数不匹配。
- formatter：`csv`、`tsv`、`json`、`table`，覆盖 null、包含分隔符的字符串、引号、
  换行。
- 模型检测：存在 table schema 即 table，否则 tree；`--model` 覆盖两者。
- `meta`：聚合文件级字段，不触发数据页扫描。
- `count`：基于 `Statistic.count`，不通过 row iterator。
- `sample`：固定 seed 下输出可复现。

端到端测试覆盖：

- 生成或复用一个小 `.tsfile` fixture。
- 通过构建出的 `tsfile` 二进制对子进程运行每个命令。
- 断言退出码、stdout 形状和 stderr 行为。
- TTY 自适应格式通过单元测试覆盖；子进程测试显式覆盖 `--format`。

测试只验证 CLI 行为和真实 reader 路径，不新增 storage engine 行为。

## 被拒绝的方案

### 保留 `select` 动词

拒绝。`select` 让 CLI 更像 SQL，但和 `cat`、`head` 重叠。它真正提供的是投影和过滤，
因此应落到共享参数上。Parquet 风格工具把列选择放在行输出命令上，TsFile 也应如此。

### 把 `count` 折叠进 `stats` 或 `meta`

拒绝。`count` 足够常用，且 TsFile 可以从统计量低成本回答。显式保留 `count` 能让这个
格式优势更容易被用户发现。

### 为了完全模仿 Parquet 删除 `ls`

拒绝。TsFile 不总是单逻辑表。多 device 和多 table 命名空间使 `ls` 成为用户经常需要的
第一个命令，就像 HDF5 中 `h5ls` 很自然一样。

### 现在实现写入或转换命令

拒绝。本阶段只读命令风险更低，也正好对应调研结论：TsFile 不是完全没有 CLI，而是动词
不齐、没有统一分发器、还不能被通用查看器直接看见。

## 后续工作

- 与 Java `TsFileSketchTool` 对齐的结构 dump 命令。
- ISO 时间格式化。
- 超出时间范围和 measurement 投影的复杂谓词。
- 写入、转换、合并、重写命令。
- DuckDB、ClickHouse、VisiData reader，让 TsFile 进入多格式查询/查看工具。
- 如果项目选择通过文件系统路径暴露 TsFile，设计只读 FUSE 命名空间或 TableFS 视图。
