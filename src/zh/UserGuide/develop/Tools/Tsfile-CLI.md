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

`tsfile-cli` 是一个单一、对管道友好的 C++ 命令行工具，用于在 shell 中检视 **并** 导入 Apache
TsFile（`.tsfile`）文件。读取类命令将数据打印到
**stdout**、诊断信息打印到 **stderr**，因此可与 `awk`、`jq`、`sort` 等组合使用；`write` 命令
将 CSV/TSV 导入为新的 `.tsfile`。它构建于公开的 `TsFileReader` 与 `TsFileTableWriter` 接口之上。

## 从源码构建

CLI 是 C++ 模块的一部分。用 Maven 包装器构建即可——它会下载固定版本的 CMake，为你编译整个
C++ 模块（`libtsfile` 共享库 + `tsfile-cli` 可执行文件）。

**前置条件**：用于运行 Maven 的 JDK（8+），以及 C++11 编译器（GCC / Clang）。第三方 C++ 依赖
（Snappy、LZ4、LZOKAY、Zlib 等）已捆绑在 `cpp/third_party/` 下并自动构建。

在仓库根目录执行：

```bash
./mvnw clean package -P with-cpp
```

会在 `cpp/target/build/` 下生成：

| 产物 | 路径 |
|---|---|
| CLI 可执行文件 | `cpp/target/build/bin/tsfile-cli` |
| 共享库 | `cpp/target/build/lib/libtsfile.so`（Linux）——macOS 为 `libtsfile.dylib` |

`tsfile-cli` 动态链接 `libtsfile`。用完整路径 **就地** 运行即可自动找到该库：

```bash
cpp/target/build/bin/tsfile-cli --version    # -> tsfile-cli (Apache TsFile C++) <version>
cpp/target/build/bin/tsfile-cli --help
```

若要在 **别处** 运行该二进制（例如把它从构建目录拷出来后），动态加载器必须能找到 `libtsfile.so`。
要么让加载器指向构建目录下的 `lib/`，要么把该库拷到标准位置：

```bash
# 让加载器指向构建目录的 lib（Linux；macOS 用 DYLD_LIBRARY_PATH）
export LD_LIBRARY_PATH=/path/to/cpp/target/build/lib:$LD_LIBRARY_PATH

# —— 或者 —— 把库拷到系统库路径
sudo cp cpp/target/build/lib/libtsfile.so /usr/local/lib/ && sudo ldconfig
```

## 使用方式

```text
tsfile-cli <command> [options] <file.tsfile>
tsfile-cli --help | --version | help
```

退出码：`0` 成功，`1` 用法/参数错误，`2` 文件打开/损坏，`3` 查询/运行时错误。

### 读取

| 命令 | 说明 |
|---|---|
| `ls` | 列出设备（树模型）或表（表模型），每行一个名称 |
| `schema` | 每序列的 `target, measurement, datatype, encoding, compression` |
| `meta` | 文件概要：模型、设备/表/序列数、时间范围、文件大小 |
| `stats` | 每序列的 `count, start_time, end_time, min, max, first, last, sum` |
| `count` | 每序列行数及一行 `total`（来自统计信息，不扫描 page） |
| `head` | 前 N 行（默认 10；用 `-n`） |
| `cat` | 所有匹配行，流式输出（`table` 格式会缓冲以对齐列） |
| `sample` | 可复现的蓄水池抽样（默认 10；`-n`、`--seed`） |

元数据类命令（`ls` / `schema` / `meta` / `stats` / `count`）无需解码数据即可回答大多数问题。

通用选项：

| 选项 | 含义 |
|---|---|
| `-f, --format csv\|tsv\|json\|table` | 输出格式；TTY 下默认 `table`，管道下默认 `tsv` |
| `-d, --device <id>` / `-t, --table <name>` | 限定到一个设备 / 表（互斥） |
| `-m, --measurements a,b,c` | 列投影（`schema`、`stats`、`count`、`head`、`cat`、`sample`） |
| `-n, --limit N` / `--offset N` | 最大行数 / 跳过行数（`head`、`cat`；`--offset` 不适用于 `sample`） |
| `--start <ms>` / `--end <ms>` | 闭区间的毫秒时间范围（`head`、`cat`、`sample`） |
| `--seed N` | 可复现抽样种子（仅 `sample`） |
| `--tag-filter C OP V` / `--tag-between C L U` / `--tag-not-between C L U` | `head`、`cat`、`sample` 的表标签谓词；`OP` 为 `eq`、`neq`、`lt`、`lteq`、`gt`、`gteq`、`regexp`、`not-regexp` |
| `--no-header` | 不输出表头行 |
| `--model tree\|table` | 强制指定模型（否则自动检测） |

`json` 输出为 NDJSON（每行一个对象；数字/布尔裸输出，其他值加引号，空值为 `null`；非有限浮点数
——NaN/Inf——变为 `null`）。CSV 输出遵循 RFC 4180。时间戳为原始毫秒时间戳。`table` 格式会在内存中
缓冲所有行以对齐列，因此导出大文件时优先用 `csv`/`tsv`/`json`。

```bash
BIN=cpp/build/Debug/bin/tsfile-cli
$BIN ls -f tsv data.tsfile                          # 列出表 / 设备
$BIN meta data.tsfile                               # 快速文件概览
$BIN count -t table1 -f tsv data.tsfile             # 行数，不扫描 page
$BIN cat -t table1 --tag-filter device eq dev_1 -m temp -f tsv data.tsfile
$BIN cat -m temp,humidity --start 1700000000000 -f csv data.tsfile | head
$BIN sample -m temp -n 20 --seed 42 -f json data.tsfile | jq .
```

> 对于表模型文件，行命令（`head` / `cat` / `sample`）在不指定 `-t <table>` 时只查询 **第一个** 表；
> `count` 覆盖所有表。

### 写入（导入）

`tsfile-cli write` 将 CSV/TSV 行导入为一个 **新的表模型** `.tsfile`（输出会被覆盖）。输入的第一列是
时间戳（毫秒）；其余列通过 `--columns` 显式声明——不做类型推断。

时间戳必须 **按设备严格递增**，设备由其 `tag` 列取值标识（共享相同标签的行构成同一设备的时间线）。
不同标签组合的行可自由交错并复用时间戳。乱序输入会被拒绝并报出错行号，导入失败不会留下输出文件。
`--output` 必须与输入文件不同。

```text
tsfile-cli write --table <name> --columns <spec> -o <out.tsfile> \
                 [-f csv|tsv] [--no-header] [--header-match] [-v] [<input> | -]
```

`--columns` 是逗号分隔的 `name:TYPE:category` 列表，其中 `category`（不区分大小写）为 `tag` 或
`field`，`TYPE`（不区分大小写）为 `BOOLEAN, INT32, INT64, FLOAT, DOUBLE, STRING, TEXT, TIMESTAMP,
DATE, BLOB` 之一——例如 `--columns "id1:STRING:tag,s1:INT64:field"`。`DATE` 单元格写作
`YYYY-MM-DD`，`TIMESTAMP` 单元格为毫秒。每列按其类型的引擎默认编码与压缩存储。

| 选项 | 含义 |
|---|---|
| `--table <name>` | 输出表名（会转小写） |
| `--columns <spec>` | 有序数据列（不含开头的时间戳列） |
| `-o, --output <path>` | 输出 `.tsfile`（必填；会被覆盖） |
| `<input>` / `-` | 输入文件，或 `-` / 省略表示 stdin |
| `-f csv\|tsv` | 输入分隔符（默认 csv；`json` / `table` 被拒绝） |
| `--no-header` | 输入无表头行（默认首行为表头并跳过） |
| `--header-match` | 校验表头名是否与 `--columns` 一致 |
| `-v, --verbose` | 向 stderr 打印 `wrote N rows to <out>`（否则成功时静默） |

空单元格写为 null。成功时静默（Unix 风格），用 `-v` 打印一行摘要。

```bash
# 通过管道往返
printf 'time,id1,s1\n0,dev,0\n1,dev,10\n' \
  | tsfile-cli write --table t1 --columns "id1:STRING:tag,s1:INT64:field" -o out.tsfile -
tsfile-cli count -f tsv out.tsfile          # -> t1.dev  s1  2
```

## 与 AI 助手配合使用 skill

`cpp/tools/skills/tsfile-cli/SKILL.md` 是一份机器可读的参考，描述了如何
正确驱动 `tsfile-cli`。支持 skill 的 AI 编码助手可以加载它，从而辅助检视与导入 `.tsfile`
文件。
