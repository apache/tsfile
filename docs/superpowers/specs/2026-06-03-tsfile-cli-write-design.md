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

# Design: TsFile CLI 写入（`tsfile-cli write`）

- **日期**：2026-06-03
- **模块**：`cpp/`（扩展 `cpp/tools/`、`cpp/test/tools/`）
- **状态**：设计已批准，待编写实现计划
- **关系**：在只读 CLI（`docs/superpowers/specs/2026-06-02-tsfile-cli-design.md`）之上新增
  第一个写入命令；该读侧设计把写入列为「后续工作」，本文把其中「文本导入」这一块具体化。
- **调研依据**：`/Users/zhanghongyin/reasearchNotes/research/tsfile/调研报告/各文件格式CLI工具调研.md`
  第 2、3、5 章的写路径动词（Parquet `convert-csv`、ORC `convert`、Avro `fromjson`）。

## 1. 目标

为 `tsfile-cli` 增加一个 `write` 命令，把 **CSV/TSV 行数据导入成一个新的 table 模型
`.tsfile`**。与读侧 `cat -f csv|tsv` 的输出对称，使读出的数据能经管道重新写回：

```sh
tsfile-cli cat -m s1 -f csv in.tsfile | tsfile-cli write --table t1 \
    --columns "s1:INT64:field" -o out.tsfile
```

设计原则与读侧一致：单二进制、可组合、stdout/stderr 分离、零新第三方依赖、不修改存储
引擎（仅调用现有 `storage::TsFileTableWriter` 写路径）。

## 2. 范围

包含：

- 一个 `write` 命令：CSV/TSV → 单个 table 模型 `.tsfile`。
- 显式 schema（`--columns` + `--table`），**零类型推断**。
- 输入来自文件或 stdin；输出到 `-o` 指定的 `.tsfile`（覆盖写）。

不包含（YAGNI，列入后续工作）：

- tree 模型导入。
- JSON / NDJSON 输入。
- 类型推断。
- 编码 / 压缩选择（v1 固定 `PLAIN` / `UNCOMPRESSED`）。
- append / 合并 / `tsfile → tsfile` 转换 / 重写。
- 引号字段内的换行（v1 假设每条记录占一行）。

## 3. 命令形态

```
tsfile-cli write --table <name> --columns <spec> -o <out.tsfile> \
                 [-f csv|tsv] [--no-header] [--header-match] [-v] [<input> | -]
```

| 参数 | 含义 | 必填 |
|---|---|---|
| `<input>` 位置参数 | 输入文件路径；省略或 `-` 表示从 **stdin** 读 | 否（默认 stdin） |
| `-o, --output <path>` | 输出 `.tsfile` 路径；已存在则覆盖（`O_TRUNC`） | 是 |
| `--table <name>` | 输出表名 | 是 |
| `--columns <spec>` | 数据列规格（见 §5），按序描述**除时间列外**的列 | 是 |
| `-f, --format csv\|tsv` | 输入分隔符，默认 `csv`；`json`/`table` 视为 usage error | 否 |
| `--no-header` | 输入无表头行（默认认为首行是表头并跳过） | 否 |
| `--header-match` | 校验首行表头列名与 `--columns`（及首列 `time`）一致，不符即报错 | 否 |
| `-v, --verbose` | 成功后向 stderr 打印一行摘要；默认静默 | 否 |

`write` 只使用上述参数；读侧的 `-d/--device`、`-m/--measurements`、`-n/--limit`、
`--offset`、`--start/--end`、`--seed` 对 `write` 无意义，**出现即按 usage error 处理**
（退出码 `1`），以免静默误用。

## 4. 输入格式与行约定

- 一行一条记录，字段用分隔符分隔（`csv` = `,`，`tsv` = `\t`）。
- **第一列固定是时间戳**：epoch 毫秒整数（`INT64`）。它不出现在 `--columns` 里。
- 其余字段按 `--columns` 的顺序一一对应；每条数据行的字段数必须等于
  `1 + len(--columns)`，否则报错（§7）。
- 默认首行为表头并跳过；表头内容**默认不校验**（列身份完全由 `--columns` 决定）。
  `--no-header` 时不跳过首行。加 `--header-match` 时校验首行：首列名任意（约定为 `time`），
  其余列名须与 `--columns` 顺序逐一相等，不符即报错（§7）。
- **空单元格 = null**：该行该列不写入（`Tablet` 不 `add_value`，留 null）。
- CSV 解析遵循 RFC 4180 引号规则（字段含分隔符/引号时用 `"` 包裹，内部 `"` 双写）；
  TSV 按 `\t` 切分、不做引号处理。引号字段内不支持换行（v1）。

## 5. Schema 规格（`--columns`）

逗号分隔的列项，每项 `name:TYPE:category`：

- `name`：列名，不含 `:` 和 `,`。
- `TYPE`：TSDataType 名，**大小写不敏感**；v1 支持
  `BOOLEAN | INT32 | INT64 | FLOAT | DOUBLE | STRING | TEXT`。
- `category`：`tag` 或 `field`（小写）。

示例：`--columns "id1:STRING:tag,id2:STRING:tag,s1:INT64:field"`。

解析为有序的 `ColumnDef{name, type, category}` 列表，任何一项缺字段、类型名未知、
category 非法都按 usage error 处理（退出码 `1`，stderr 给出错误项）。

## 6. 写入路径

1. `TableSchema(table, [ColumnSchema(def.name, def.type, common::UNCOMPRESSED,
   common::PLAIN, def.category) for def in columns])`。
2. `storage::WriteFile`：`create(output, O_WRONLY|O_CREAT|O_TRUNC[, O_BINARY], 0666)`。
3. `storage::TsFileTableWriter(&file, schema)`。
4. 构造一个批量 `Tablet`（列 = `--columns` 的列名/类型/类别，容量如 `1024` 行）：逐行
   `add_timestamp(i, ts)`；非空单元格按列类型 `add_value(i, name, typedValue)`。
5. 批满即 `write_table(tablet)` 后复用/重置 tablet；EOF 后写出残余批。
6. `flush()` → `close()`。

类型转换：单元格字符串 → 列类型。`INT32/INT64` 用 `strtoll`，`FLOAT/DOUBLE` 用
`strtod`，`BOOLEAN` 接受 `true/false`（大小写不敏感）与 `1/0`，`STRING/TEXT` 原样。
不可解析 → 运行时错误（§7）。

## 7. 退出码与输出

| 退出码 | 条件 |
|---|---|
| `0` | 成功 |
| `1` | usage / 参数错误（缺 `--table`/`--columns`/`-o`，`--columns` 语法错，`-f json|table`，混入读侧 flag） |
| `2` | 输入打不开 / 输出创建失败 |
| `3` | 行级错误：字段数不符、`--header-match` 下表头不符、单元格类型解析失败、写库返回错误（stderr 标出行号） |

`write` 不向 stdout 输出数据；进度/诊断/错误一律走 stderr。**成功时默认全静默**（无 stdout、
无 stderr 输出，遵循 Unix「silence is golden」）；仅当加 `-v/--verbose` 时向 stderr 打印一行
摘要：`wrote <N> rows to <out.tsfile>`。

## 8. 架构

新增/改动文件：

```text
cpp/tools/
├── cli/
│   ├── cli_args.h / .cc        # 新增 output(-o)、columns(--columns)、verbose(-v)、header_match(--header-match)
│   └── run_cli.cc              # 注册 write；在 reader.open 之前特判 write 并分发
├── commands/
│   ├── commands.h              # 声明 cmd_write
│   └── cmd_write.cc            # 读输入/构 schema+tablet/写出
└── format/
    ├── input_format.h / .cc    # parse_columns_spec、split_delimited(csv/tsv)、parse_cell
cpp/test/tools/
├── input_format_test.cc        # 列规格解析、行切分、单元格类型解析（含 null/错误）
└── command_e2e_test.cc         # 追加 write→读回 的往返 E2E
```

关键设计点 —— **`write` 是第一个不打开 `TsFileReader` 的命令**。当前 `run_cli` 对所有命令
都 `reader.open(p.file)`，而 `write` 的位置参数是**输入 CSV**（或 stdin），不是要打开的
`.tsfile`。因此在 `run_cli` 中：

- 把 `write` 加入 `is_known_command`。
- 新增 `validate_write_flags`（缺 `--table`/`--columns`/`-o`、`-f` 非 csv/tsv、混入读侧
  flag → usage error）。
- 在 `storage::libtsfile_init()` 之后、构造 `TsFileReader` 之前插入：
  `if (p.command == "write") return cmd_write(p, out, err);` —— 完全跳过 reader 路径。

`cmd_write` 签名不同于读侧命令（无 reader、无 OutputFormat）：

```cpp
int cmd_write(const ParsedArgs& args, std::ostream& out, std::ostream& err);
```

`input_format` 为纯层（不依赖 reader）：列规格解析、按分隔符切行（引号感知）、单元格→
类型转换，便于单测。`cmd_write` 负责打开输入流（文件或 `std::cin`）、串起 schema/tablet/
writer。复用现有 `cli/exit_codes.h`。

## 9. 测试

- **单元**（`input_format_test.cc`）：`parse_columns_spec` 正例与各类错误；`split_delimited`
  的 csv 引号/转义、tsv 切分；`parse_cell` 各类型正例、空=null、解析失败。
- **E2E**（追加到 `command_e2e_test.cc`）：把一段 CSV 写到临时文件，`run_cli({"write",
  "--table","t1","--columns","s1:INT64:field","-o",out,csv})`，断言退出 0；随后在进程内
  用读路径 `run_cli({"schema"/"count"/"cat", out})` 回读，断言表名、列、行数、行值与输入
  一致（往返）。另覆盖：缺 `--columns` → 1；行字段数不符 → 3；`--header-match` 下表头不符
  → 3；输出到不可写路径 → 2；成功默认静默、仅 `-v` 才有摘要。

只验证 CLI/写库行为，不新增存储引擎行为。

## 10. 被拒绝的方案

- **类型推断**：拒绝。CSV 类型推断（`1` vs `1.0` vs `"01"`）易误判；显式 `--columns`
  零歧义、实现最简，符合「先稳后省事」。推断可作后续便利项。
- **首列以外某命名列作时间**：v1 拒绝（约定首列即时间，最简单且与读侧输出对齐）；
  `--time-column` 可后续再加。
- **第二个位置参数作输出**：拒绝。现有 parser 只有一个位置参数；用 `-o/--output` 更显式，
  也避免改动位置参数语义。
- **同时支持 tree 模型 / JSON**：本阶段拒绝（YAGNI），列入后续。

## 11. 后续工作

- tree 模型导入（device + measurements，aligned/非 aligned）。
- JSON/NDJSON 输入（与读侧 `-f json` 对称）。
- 类型推断、`--time-column`、编码/压缩 flag。
- `tsfile → tsfile` 的 convert/rewrite/merge。
