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

构建和手工测试步骤见 [TESTING.md](TESTING.md)。

## 1. 输入和环境

### 1.1 自测目标

验证以下 SQL 入口的正常路径、边界条件、错误路径、事务语义和文件生命周期：

- 虚拟表模块 `tsfile_hybrid`
- `tsfile_seal`
- `tsfile_compact`
- `tsfile_export`
- `tsfile_table_info`
- `tsfile_device_info`
- `tsfile_schema`
- `tsfile_verify`

同时记录写入吞吐、seal 耗时、进程内存、生成文件数量和 SQLite 数据库大小。

### 1.2 测试环境

#### 本地功能和性能环境

| 项目 | 输入 |
| --- | --- |
| 机器 | Apple M5 Pro，32GB 内存 |
| 操作系统 | macOS 26.6.2 |
| 编译器 | Apple Clang（Command Line Tools） |
| SQLite | Homebrew SQLite 3.53.3，支持 load extension |
| 构建类型 | CMake Release |
| 构建选项 | `BUILD_SQLITE_EXTENSION=ON`、`TSFILE_BUILD_SHARED=ON`、`BUILD_TEST=ON` |
| 扩展产物 | `build/develop/lib/tsfile_sqlite.dylib` |
| 测试程序 | `build/develop/test/lib/TsFile_Sqlite_Test` |

#### 远端规模环境

| 项目 | 输入 |
| --- | --- |
| 机器 | datadev，32 核，62GB 内存 |
| 操作系统 | Ubuntu 24.04，Linux 6.8 |
| 编译器 | gcc 13.3 |
| SQLite | 3.45.1（`libsqlite3-dev`） |
| 构建类型 | CMake Release + Ninja |
| 依赖开关 | 关闭 ANTLR4、SIMD、SNAPPY、LZ4、LZOKAY、ZLIB、ZSTD |
| 数据目录 | `/data/colin/tsfile-sqlite-bench` |

树莓派 4B（8GB，aarch64）已完成依赖安装和源码上传，本轮未完成构建，
不计入通过结果。

### 1.3 构建输入

本地构建：

```bash
cmake -S cpp -B build/develop -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON
cmake --build build/develop --target tsfile_sqlite TsFile_Sqlite_Test -j 8
```

macOS 需要把 `SQLite3_INCLUDE_DIR` 和 `SQLite3_LIBRARY` 指向 Homebrew
SQLite；Linux 使用系统 `libsqlite3-dev`。完整依赖安装、离线构建和排障见
[TESTING.md](TESTING.md)。

### 1.4 数据输入

#### 表结构和类型

- 时间列：`TIMESTAMP TIME`，精度 `ms`。
- 设备标识：1 到 2 个 `STRING TAG`。
- 字段：`BOOLEAN`、`INT32`、`INT64`、`FLOAT`、`DOUBLE`、`TEXT`、`STRING`、
  `BLOB`、`DATE`、`TIMESTAMP` 类型的 FIELD。
- NULL：TIME 不允许 NULL；TAG 和 FIELD 允许 NULL。

#### 边界数据

| 输入项 | 取值 |
| --- | --- |
| INT32/DATE | `-2147483648`、`2147483647`、NULL |
| INT64/TIMESTAMP | `-9223372036854775808`、`9223372036854775807`、NULL |
| FLOAT | `1.5`、`-1.5`、NULL |
| DOUBLE | `2.25`、`-2.25`、NULL |
| TEXT/STRING | 普通文本、空字符串、NULL |
| BLOB | `X'00FF'`、空 BLOB、NULL |
| TIME | `INT64_MIN`、`-1`、`0`、`1`、`INT64_MAX` |
| TAG | NULL、空字符串、普通值、双 TAG 的 NULL/空组合 |

#### 查询输入

- 比较运算：`=`、`<>`、`>`、`>=`、`<`、`<=`。
- 组合条件：`AND`、`OR`、`NOT`、`BETWEEN`、`IN`。
- 空值条件：`IS NULL`、`IS NOT NULL`。
- 文本条件：`LIKE`、`GLOB`（由 SQLite 残余执行）。
- 排序分页：`ORDER BY`、`LIMIT`、`OFFSET`。
- 聚合：`GROUP BY`、`HAVING`、`count`。
- 投影：部分列、`count(*)`、表达式列。

#### seal、compact 和 export 输入

| 操作 | 输入 |
| --- | --- |
| seal | 空热区、1 行、3 行、10 万行、1000 万行；`max_file_size_mb` 为 0、1、2、64 |
| compact | 2 到 3 个小文件、无可合并文件、目标小于阈值、单行超限、文件被修改 |
| export | 空表、单文件、多文件、只读文件表、只读目录、热区未 seal、输出已存在、父目录缺失、路径重叠、源文件被修改、输出重名 |
| 事务 | 显式事务、savepoint、提交、回滚、跨建表回滚 |

#### 性能输入

| 场景 | 输入 |
| --- | --- |
| 本地规模 | 1000 设备 × 每设备 1 万行 = 1000 万行；每 100 万行 seal |
| datadev 持续写入 | 1000 设备 × 每轮每设备 100 行；累计 10 万行 seal；规划 10000 轮 |
| datadev 单次大 seal | 单次 seal 1000 万行 |

### 1.5 测试用例输入清单

自动化用例共 47 个，按输入类别分组：

| 类别 | 用例 |
| --- | --- |
| 建表与声明 | `SqlDeclarationsAndNullableLogicalKeys`、`NoTagsAndInvalidDeclarations`、`QuotedSchemaAndRejectedEmptyPrecision`、`AttachedDatabaseUsesItsOwnShadowTables` |
| 热区 CRUD 与类型 | `HotCrudAndSealRollback`、`Int32RangeAndColdRowsAreImmutable`、`NullAndBlobSurviveSeal`、`RowidNamedColumnsAndBooleanValues`、`BusinessColumnsDoNotCarryManagementCommands`、`AllFieldTypesRoundTripAtExtremes` |
| 查询与下推 | `FieldPredicatesFilterHotAndColdRows`、`FieldPredicatesSupportTypedFields`、`QueryOperatorAndResidualMatrix`、`TimeBoundaryQueriesAndWatermarks`、`MultiTagNullEmptyCombinations` |
| seal | `SealSplitsOutputBySizeLimit`、`NoTagSealingAndWholeStatementColdFailures`、`SealPreservesPreexistingTemporarySymlink`、`CommitNeverOverwritesExistingDirectoryEntries` |
| compact | `CompactMergesSmallSealedFiles`、`CompactRollbackKeepsOriginalFiles`、`CompactThresholdAndChangedFile`、`CompactCanSplitIntoMultipleOutputFiles` |
| export | `ExportCopiesSealedFilesAndRoundTrips`、`ExportFailureRetainsCommittedSealAndManagementGuards`、`DirectorySourceAndReadonlyExport`、`RawMultiTablePrecisionAndExportIsolation`、`ExportRejectsChangedSourceAndDuplicateNames`、`ExportCountsEverySealedFile` |
| 管理函数边界 | `ManagementFunctionGuardsAndValidation` |
| 事务与回滚 | `ManagementSealRespectsTransactionsAndSavepoints`、`RollbackPastTableCreationPreservesOuterTransaction`、`DiagnosticsAndInt64Exhaustion` |
| 重连与文件校验 | `PersistentReconnectAndConcurrentWatermark`、`ReconnectedFileTableRestoresDeviceBoundaries`、`SourceSchemaPersistsWhenFileBecomesUnavailable`、`SourceValidationAndPreserveUnknownFiles`、`VerifyReportsUnregisteredCorruptAndMismatch` |
| 目录归属 | `DirectoryOwnershipAndCreationRollback`、`CopiedDatabaseCannotShareWritableDirectory` |
| 诊断函数 | `DeviceInfoReportsPerDeviceWatermarks`、`DiagnosticsExposeSourceAndEmptyNoTagDevices`、`SchemaFunctionListsTsfileTypes`、`SchemaFunctionWorksForInferredFileTables` |
| 模块生命周期 | `HybridModuleLifecycleAndTagUpdates` |
| 临时表和标识符 | `TempTableAndQuotedReservedIdentifiers` |
| 外部文件 | `ExternalFileInferredSchemaAndAppendBoundary` |

手工冒烟输入为 35 条 SQL 检查，按建表、写入、更新删除、过滤、seal、诊断、
export、只读、目录源、compact、verify 和错误路径顺序执行。

## 2. 输出和预期

### 2.1 自动化测试总体输出

执行命令：

```bash
DYLD_LIBRARY_PATH=build/develop/lib \
  build/develop/test/lib/TsFile_Sqlite_Test
```

| 项目 | 预期 | 实际输出 |
| --- | --- | --- |
| 用例数量 | 47 | 47 |
| 通过 | 47 | **47** |
| 失败 | 0 | **0** |
| 总耗时 | 秒级 | 约 0.15 秒 |

### 2.2 分类输出

| 类别 | 预期输出 | 实际输出 |
| --- | --- | --- |
| 建表与声明 | 三种建表模式正常；非法声明、重复声明、缺精度被拒绝 | 通过 |
| 热区 CRUD | 插入、更新、删除、类型范围、NULL、BLOB、rowid 命名正确 | 通过 |
| 全部字段类型 | 10 种 FIELD 类型的极值和 NULL 在 seal 前后保持一致 | 通过 |
| 时间边界 | `INT64_MIN`、`INT64_MAX`、负时间、开闭区间查询正确 | 通过 |
| TAG 组合 | 双 TAG 的 NULL/空字符串/普通值可区分，设备数正确 | 通过 |
| 查询下推 | 时间/TAG/FIELD 条件下推，OR/NOT/LIKE 等残余条件结果正确 | 通过 |
| seal | 空热区返回 0；大小切分不超限；单行超限报错；事务可回滚 | 通过 |
| compact | 正常合并、多输出、阈值跳过、文件变化报错、回滚保留原文件 | 通过 |
| export | 复制已封存文件、文件数正确、热区不导出、异常路径报错 | 通过 |
| 管理函数边界 | 参数、缺表、只读、非法大小、路径重叠、standalone 限制均符合预期 | 通过 |
| 事务与重连 | savepoint、回滚、重连后的设备边界和 schema 正确 | 通过 |
| 诊断函数 | 表/设备/schema/verify 的列和状态值正确 | 通过 |
| 模块生命周期 | RENAME 拒绝；DROP 清理全部影子表并保留文件 | 通过 |
| 临时表和引号 | `temp` 表、带空格表名、保留字列名可正常使用 | 通过 |

### 2.3 关键边界用例输出

| 输入 | 预期输出 | 实际输出 |
| --- | --- | --- |
| 写入 `INT64_MIN` 和 `INT64_MAX` 时间 | 可写入、可查询；最大时间设备 watermark 为 NULL | 通过 |
| `time>0 AND time<1` | 0 行 | 通过 |
| `time BETWEEN INT64_MIN AND 0` | 3 行 | 通过 |
| 全类型极值写入 3 行（含全 NULL 行） | 10 种 FIELD 类型值均正确，schema 12 列 | 通过 |
| 双 TAG 的 5 种 NULL/空组合 | 5 个设备可区分，重复组合在相同时间被拒绝 | 通过 |
| 冷数据 `device='d0' OR temperature>5` | 4 行，OR 的两个分支都完整返回 | 通过 |
| `NOT (temperature>3)` | 2 行，NULL 不满足 | 通过 |
| `label LIKE 'a%'` / `LIKE '%ta'` / `GLOB 'a*'` | 1 / 2 / 1 行 | 通过 |
| `ORDER BY time DESC LIMIT 2 OFFSET 2` | `'4,3'` | 通过 |
| seal 12 行、1MB 上限 | 返回 12，生成多个文件且每个不超过 1MB | 通过 |
| seal 单行 1.2MB、1MB 上限 | 返回错误，热行保留，文件数不变 | 通过 |
| compact 两个 600KB 文件、目标 1MB | 返回 2，输出两个文件，数据 2 行 | 通过 |
| compact 阈值 1MB、存在 1.2MB 文件 | 只选中小文件，返回 0 | 通过 |
| compact 前修改已封存文件 | 返回错误，原文件保留 | 通过 |
| export 时源文件被修改 | 返回错误，不发布部分输出 | 通过 |
| export 输出目录存在同名文件 | 返回错误，不覆盖 | 通过 |
| DROP TABLE | 影子表全部删除，TsFile 和 `.tsfile-owner` 保留 | 通过 |
| `ALTER TABLE ... RENAME` | 被拒绝，原表仍可用 | 通过 |

### 2.4 手工 SQL 冒烟输出

| 输入操作 | 预期输出 | 实际输出 |
| --- | --- | --- |
| 写入 4 行 | `count(*)=4` | 通过 |
| 更新 1 行、删除 1 行 | 剩余 3 行，更新值生效 | 通过 |
| 热区 `v1>20` | 2 行 | 通过 |
| `tsfile_seal('main.sensor',64)` | 返回 3，热区清零 | 通过 |
| seal 后查询和 FIELD 过滤 | 行数和过滤结果不变 | 通过 |
| 时间+TAG+FIELD 组合 | 2 行 | 通过 |
| `tsfile_table_info` | hot_rows=0、device_count=2、file_count=1、watermark=2001 | 通过 |
| `tsfile_device_info` | `g0,d1` watermark=2001，`g1,d2` watermark=3001 | 通过 |
| `tsfile_schema` | 8 列，TIMESTAMP:TIME、STRING:TAG、DOUBLE:FIELD，顺序正确 | 通过 |
| `tsfile_export` | 返回 1，输出目录有 TsFile | 通过 |
| 只读文件表查询/写入 | 查询 3 行；写入被拒绝 | 通过 |
| 目录源只读表 | 查询 3 行，mode=readonly | 通过 |
| 无可合并文件时 compact | 返回 0 | 通过 |
| verify 自有表/只读表 | 状态 OK | 通过 |
| 重复键、旧时间、只读 DELETE、缺表 | 均被拒绝 | 通过 |

总计：**35/35 通过**。

### 2.5 性能和内存输出

#### 本地 macOS（M5 Pro）

预期：写入稳定完成，seal 输出文件不超过大小上限，内存不随已封存文件数增长。

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

预期：持续写入，每次 seal 10 万行，峰值内存稳定，不随轮数增长。

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

### 2.6 发现的问题与修复输出

| 输入场景 | 预期 | 修复前实际 | 修复后 |
| --- | --- | --- | --- |
| 单次 seal 大热区 | 内存有界 | 1000 万行峰值 11GB，1 亿行会 OOM | 400 万行分块流式，峰值 4.7GB |
| TEXT FIELD 等值过滤 | 不当作 TAG | 被误判为 TAG 条件 | 只对 TAG 列构造 TAG 过滤器 |
| 失败写入后 DETACH | 可以 DETACH | database is locked | 回滚路径不再执行读语句 |
| 查看原始 TsFile schema | 能看到类型/类别 | `DESC` 只显示 SQLite 类型 | 新增 `tsfile_schema` |
| DROP TABLE | 影子表全部删除 | `_devices` 残留 | `xDestroy` 补删 `_devices` |
| 冷数据 OR 查询 | 两个分支都完整返回 | 下推后 MULTI-INDEX OR 按过滤后位置生成行号，行号碰撞导致丢行 | 冷行行号改为由段、时间和 TAG 值生成稳定值 |

### 2.7 未覆盖范围

函数入口、参数校验、只读/缺表、路径、状态码和事务回滚已经覆盖。仍无法在
常规单元测试中稳定复现的是操作系统级故障注入：磁盘写满、fsync 失败、
rename 与提交之间进程崩溃、权限变化导致 staging 清理失败。这些路径未计入
本轮通过结果。

### 2.8 结论

- **输入侧**：47 个自动化用例、35 条手工 SQL、1000 设备规模数据和
  1000 万行单次 seal 场景均已执行。
- **输出侧**：自动化 47/47 通过，手工 35/35 通过；写入、seal 和内存数据
  符合预期。
- 流式 seal 已消除大热区 seal 的主要 OOM 风险；剩余风险集中在查询结果物化、
  compact 内存和低内存边缘设备的块大小上。
- 当前版本可以进入评审/联调。
