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

# SQLite + TsFile 扩展自测报告

日期：2026-11-03

分支：`experiment-sqlite`（基于 `develop` 的 `931a5d3`，SQLite 扩展为独立功能分支）

构建与手工测试步骤见 [TESTING.md](TESTING.md)。

## 1. 输入和环境

### 1.1 自测目标

验证 `tsfile_hybrid` 虚拟表、`tsfile_seal`、`tsfile_compact`、
`tsfile_export`、`tsfile_table_info`、`tsfile_device_info`、
`tsfile_schema`、`tsfile_verify` 的正常路径、错误路径、事务语义和
文件生命周期，并记录写入吞吐、seal 耗时和进程内存。

### 1.2 测试环境

#### 本地功能与性能环境

| 项目 | 输入 |
| --- | --- |
| 机器 | Apple M5 Pro，32GB 内存 |
| 操作系统 | macOS 26.6.2 |
| 编译器 | Apple Clang（Command Line Tools） |
| SQLite | Homebrew SQLite 3.53.3，支持 load extension |
| 构建 | CMake Release；`BUILD_SQLITE_EXTENSION=ON`、`TSFILE_BUILD_SHARED=ON`、`BUILD_TEST=ON` |
| 扩展产物 | `build/develop/lib/tsfile_sqlite.dylib` |

#### 远端规模环境

| 项目 | 输入 |
| --- | --- |
| 机器 | datadev，32 核，62GB 内存 |
| 操作系统 | Ubuntu 24.04，Linux 6.8 |
| 编译器 | gcc 13.3 |
| SQLite | 3.45.1（`libsqlite3-dev`） |
| 构建 | CMake Release + Ninja；关闭 ANTLR4/SIMD/SNAPPY/LZ4/LZOKAY/ZLIB/ZSTD |
| 数据目录 | `/data/colin/tsfile-sqlite-bench` |

树莓派 4B（8GB，aarch64）已完成依赖安装和源码上传，本轮未完成构建，
不计入通过结果。

### 1.3 构建输入

```bash
cmake -S cpp -B build/develop -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON
cmake --build build/develop --target tsfile_sqlite TsFile_Sqlite_Test -j 8
```

macOS 需要把 `SQLite3_INCLUDE_DIR` / `SQLite3_LIBRARY` 指向 Homebrew SQLite；
Linux 使用系统 `libsqlite3-dev`。完整的安装和构建步骤见
[TESTING.md](TESTING.md)。

### 1.4 数据输入

#### 自动化与手工功能测试

- 表结构：`time TIMESTAMP TIME`，2 个 `STRING TAG`，5 个 `FIELD`
  （DOUBLE、INT64、FLOAT、INT32、BOOLEAN，另有 TEXT/BLOB/DATE 类型用例）。
- 数据规模：每张测试表 1 到 10 行；大小切分用例约 2.4MB。
- 时间精度：`ms`。
- 目录：每个用例使用独立的临时目录。

#### 本地性能测试

- 1000 个设备，每设备 1 万行，共 1000 万行。
- 每 100 万行 seal 一次，共 10 次。
- 每设备 2 个 TAG、5 个 FIELD。

#### datadev 规模测试

- 1000 个设备；每设备最终 100 万行，总计 10 亿行。
- 每轮每设备写 100 行，累计 10 万行 seal 一次；规划 10000 轮。
- 单次 seal 1000 万行的流式路径另行验证。

### 1.5 测试输入清单

| 输入 | 内容 |
| --- | --- |
| 自动化用例 | 41 个 `TsFileSqliteTest` gtest 用例，覆盖建表、CRUD、查询下推、seal、compact、export、事务、重连、诊断函数、错误路径和模块生命周期 |
| 手工冒烟 | 35 条 SQL 检查，按真实用户操作顺序执行 |
| 错误输入 | 错误参数类型、缺表、只读表写入、重复键、旧时间、非法大小、路径重叠、父目录不存在、损坏/替换文件 |
| 性能输入 | 本地 1000 万行；datadev 每 10 万行 seal 的持续写入；datadev 单次 1000 万行 seal |

## 2. 输出和预期

### 2.1 自动化测试输出

| 项目 | 预期 | 实际 |
| --- | --- | --- |
| 用例数量 | 41 | 41 |
| 通过数 | 41 | **41** |
| 失败数 | 0 | **0** |
| 总耗时 | 秒级 | 约 0.14 秒 |

执行命令：

```bash
DYLD_LIBRARY_PATH=build/develop/lib \
  build/develop/test/lib/TsFile_Sqlite_Test
```

覆盖范围：

| 类别 | 输出预期 | 实际 |
| --- | --- | --- |
| 建表与声明 | 三种建表模式、引号/附加库、非法声明被拒绝 | PASS |
| 热区 CRUD | INSERT/UPDATE/DELETE、类型范围、NULL/BLOB、rowid 命名 | PASS |
| 查询下推 | 热区和冷区的时间、TAG、FIELD 条件结果一致 | PASS |
| seal | 正常、空热区、大小切分、单行超限、事务/回滚、无 TAG | PASS |
| compact | 合并、无可合并、阈值/目标校验、超大行、文件变化、回滚 | PASS |
| export | 可写/只读、空表、多文件、热区不导出、路径/文件变化/重名 | PASS |
| 管理函数边界 | 参数类型、缺表、只读、非法大小、路径重叠、standalone 限制 | PASS |
| 事务与重连 | savepoint、回滚、重连后的设备边界 | PASS |
| 诊断函数 | table_info/device_info/schema 的列和边界值 | PASS |
| verify | OK、MISSING、UNREGISTERED、CORRUPT、MISMATCH | PASS |
| 模块生命周期 | ALTER RENAME 拒绝、DROP 清理影子表、改 TAG、OR IGNORE/FAIL | PASS |

### 2.2 手工 SQL 冒烟输出

| 输入操作 | 预期输出 | 实际 |
| --- | --- | --- |
| 写入 4 行 | `count(*)=4` | PASS |
| 更新 1 行、删除 1 行 | 剩余 3 行，更新值生效 | PASS |
| 热区 `v1>20` | 2 行 | PASS |
| `tsfile_seal('main.sensor',64)` | 返回 3，热区清零 | PASS |
| seal 后查询与 FIELD 过滤 | 行数和过滤结果不变 | PASS |
| 时间+TAG+FIELD 组合 | 2 行 | PASS |
| `tsfile_table_info` | hot_rows=0、device_count=2、file_count=1、watermark=2001 | PASS |
| `tsfile_device_info` | `g0,d1` watermark=2001，`g1,d2` watermark=3001 | PASS |
| `tsfile_schema` | 8 列，TIMESTAMP:TIME、STRING:TAG、DOUBLE:FIELD，顺序正确 | PASS |
| `tsfile_export` | 返回 1，输出目录有 TsFile | PASS |
| 只读文件表查询/写入 | 查询 3 行；写入被拒绝 | PASS |
| 目录源只读表 | 查询 3 行，mode=readonly | PASS |
| 无可合并文件时 compact | 返回 0 | PASS |
| verify 自有表/只读表 | 状态 OK | PASS |
| 重复键、旧时间、只读 DELETE、缺表 | 均被拒绝 | PASS |

总计：**35/35 PASS**。

### 2.3 性能与内存输出

#### 本地 macOS（M5 Pro）

预期：写入稳定完成，seal 文件不超过大小上限，内存不随已封存文件数增长。

| 指标 | 实际输出 |
| --- | --- |
| 写入吞吐 | 约 16.3 万行/秒 |
| 单次 seal（100 万行） | 约 0.92 秒 |
| 10 次 seal 总耗时 | 约 11.1 秒 |
| 峰值 RSS | 约 1.1GB |
| SQLite DB | 约 87MB |
| TsFile 输出 | 约 61MB / 10 个文件 |
| 最终状态 | hot_rows=0、file_count=10、device_count=1000、watermark=10001 |

#### datadev 每 10 万行 seal

预期：写入持续进行，每次 seal 10 万行，峰值内存稳定，不随轮数增长。

| 指标 | 实际输出 |
| --- | --- |
| 写入吞吐 | 约 8.6 万行/秒 |
| 单次 seal（10 万行） | 约 0.33 秒 |
| 峰值 RSS | 约 146MB |
| 已运行轮数 | 2804 轮（约 2.804 亿行），手动停止 |
| 已生成文件 | 2804 个，约 4.1GB |
| SQLite DB | 约 9.4MB |

#### datadev 单次 1000 万行 seal

预期：seal 成功，输出文件不超过上限，内存有界。

| 指标 | 流式 seal（当前） | 原物化 seal（基线） |
| --- | --- | --- |
| seal 耗时 | 34.3 秒 | 46.1 秒 |
| 峰值 RSS | 4.7GB | 11.0GB |
| 输出文件 | 3 个，约 114MB | 1 个，约 118MB |

### 2.4 发现的问题与修复输出

| 输入场景 | 预期 | 修复前实际 | 修复后 |
| --- | --- | --- | --- |
| 单次 seal 大热区 | 内存有界 | 1000 万行峰值 11GB，1 亿行会 OOM | 4M 行分块流式，峰值 4.7GB |
| TEXT FIELD 等值过滤 | 不当作 TAG | 被误判为 TAG 条件 | 只对 TAG 列构造 TAG 过滤器 |
| 失败写入后 DETACH | 可以 DETACH | database is locked | 回滚路径不再执行读语句 |
| 查看原始 TsFile schema | 能看到类型/类别 | `DESC` 只显示 SQLite 类型 | 新增 `tsfile_schema` |
| DROP TABLE | 影子表全部删除 | `_devices` 残留 | `xDestroy` 补删 `_devices` |

### 2.5 未覆盖范围

函数入口、参数校验、只读/缺表、路径、状态码和事务回滚已经覆盖。仍无法在
常规单元测试中稳定复现的是操作系统级故障注入：磁盘写满、fsync 失败、
rename 与提交之间进程崩溃、权限变化导致 staging 清理失败。这些路径未计入
本轮通过结果。

### 2.6 结论

- **输入侧**：41 个自动化用例、35 条手工 SQL、1000 设备规模数据和
  1000 万行单次 seal 场景均已执行。
- **输出侧**：自动化 41/41 通过，手工 35/35 通过；写入、seal 和内存数据
  符合预期。
- 流式 seal 已消除大热区 seal 的主要 OOM 风险；剩余风险集中在查询结果物化、
  compact 内存和低内存边缘设备的块大小上。
- 当前版本可以进入评审/联调。
