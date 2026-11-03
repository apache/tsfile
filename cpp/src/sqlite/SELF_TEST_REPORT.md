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

分支：`experiment-sqlite`

提交：`4f7aa3ba1` 之后的自测工作树（包含 develop 基座、FIELD 下推、流式 seal、
`tsfile_schema` 以及本轮补齐的 7 组边界测试；测试与 DROP 修复尚未提交）

## 1. 自测环境

### 本地功能与性能验证

| 项目 | 配置 |
| --- | --- |
| 机器 | Apple M5 Pro，32GB 内存 |
| 系统 | macOS 26.6.2 |
| 编译器 | Apple Clang（Command Line Tools） |
| SQLite | Homebrew SQLite 3.53.3（支持 load extension） |
| 构建 | CMake Release，`BUILD_SQLITE_EXTENSION=ON`、`TSFILE_BUILD_SHARED=ON`、`BUILD_TEST=ON` |
| 扩展 | `build/develop/lib/tsfile_sqlite.dylib` |

### 远端规模验证

| 项目 | 配置 |
| --- | --- |
| 机器 | datadev，32 核，62GB 内存，Linux 6.8 |
| 系统 | Ubuntu 24.04，gcc 13.3，SQLite 3.45.1 |
| 构建 | CMake Release、Ninja；关闭 ANTLR4/SIMD/SNAPPY/LZ4/LZOKAY/ZLIB/ZSTD |
| 数据目录 | `/data/colin/tsfile-sqlite-bench` |

树莓派 4B（8GB，aarch64）已完成编译依赖安装和源码上传，但本轮未完成
配置与编译，未列入通过结果。

## 2. 自测范围与方法

1. **自动化集成测试**：`TsFile_Sqlite_Test`，覆盖建表、冷热读写、事务、
   文件管理、诊断函数和错误路径。
2. **手工 SQL 冒烟测试**：通过 `sqlite3` CLI 加载扩展，按真实用户操作顺序
   执行 35 项检查，覆盖自动化用例之外的组合场景。
3. **性能与内存测试**：
   - 本地 1000 设备、每设备 1 万行（共 1000 万行），每 100 万行 seal 一次；
   - datadev 1000 设备、2 TAG、5 FIELD，每累计 10 万行 seal 一次；
   - datadev 单次 seal 1000 万行的流式路径验证。

## 3. 自动化测试结果

执行方式：

```bash
DYLD_LIBRARY_PATH=build/develop/lib \
  build/develop/test/lib/TsFile_Sqlite_Test
```

结果：**41 passed / 0 failed**，总耗时约 0.14 秒。

覆盖的用例：

| 类别 | 用例 |
| --- | --- |
| 建表与声明 | `SqlDeclarationsAndNullableLogicalKeys`、`NoTagsAndInvalidDeclarations`、`QuotedSchemaAndRejectedEmptyPrecision`、`AttachedDatabaseUsesItsOwnShadowTables` |
| 热区 CRUD 与类型 | `HotCrudAndSealRollback`、`Int32RangeAndColdRowsAreImmutable`、`NullAndBlobSurviveSeal`、`RowidNamedColumnsAndBooleanValues`、`BusinessColumnsDoNotCarryManagementCommands` |
| 查询与下推 | `FieldPredicatesFilterHotAndColdRows`、`FieldPredicatesSupportTypedFields` |
| seal | `SealSplitsOutputBySizeLimit`、`NoTagSealingAndWholeStatementColdFailures`、`SealPreservesPreexistingTemporarySymlink`、`CommitNeverOverwritesExistingDirectoryEntries` |
| compact | `CompactMergesSmallSealedFiles`、`CompactRollbackKeepsOriginalFiles`、`CompactRejectsOversizedRowAndChangedFile` |
| export | `ExportCopiesSealedFilesAndRoundTrips`、`ExportFailureRetainsCommittedSealAndManagementGuards`、`DirectorySourceAndReadonlyExport`、`RawMultiTablePrecisionAndExportIsolation`、`ExportRejectsChangedSourceAndDuplicateNames`、`ExportCountsEverySealedFile` |
| 管理函数边界 | `ManagementFunctionGuardsAndValidation`（参数类型、缺表、只读、非法大小、路径重叠、standalone 限制） |
| 事务与回滚 | `ManagementSealRespectsTransactionsAndSavepoints`、`RollbackPastTableCreationPreservesOuterTransaction`、`DiagnosticsAndInt64Exhaustion` |
| 重连与文件校验 | `PersistentReconnectAndConcurrentWatermark`、`ReconnectedFileTableRestoresDeviceBoundaries`、`SourceSchemaPersistsWhenFileBecomesUnavailable`、`SourceValidationAndPreserveUnknownFiles`、`VerifyReportsUnregisteredCorruptAndMismatch` |
| 目录归属 | `DirectoryOwnershipAndCreationRollback`、`CopiedDatabaseCannotShareWritableDirectory` |
| 诊断函数 | `DeviceInfoReportsPerDeviceWatermarks`、`DiagnosticsExposeSourceAndEmptyNoTagDevices`、`SchemaFunctionListsTsfileTypes`、`SchemaFunctionWorksForInferredFileTables` |
| 模块生命周期 | `HybridModuleLifecycleAndTagUpdates`（ALTER RENAME 拒绝、DROP 清理、修改 TAG、OR IGNORE/FAIL） |
| 外部文件 | `ExternalFileInferredSchemaAndAppendBoundary` |

### 剩余未覆盖范围

函数入口、参数校验、只读/缺表、路径、状态码和事务回滚已经覆盖。仍无法在常规
单元测试中稳定复现的是操作系统级故障注入，包括：磁盘写满、fsync 失败、进程在
rename 与提交之间崩溃、权限变化导致 staging 清理失败。这些路径需要专门的故障
注入环境，未计入本轮通过结果。

## 4. 手工 SQL 冒烟结果

执行方式：Homebrew `sqlite3` 加载 `tsfile_sqlite.dylib`，使用持久 SQLite
数据库连续执行多轮 SQL。

结果：**35 passed / 0 failed**。

| 场景 | 检查内容 | 结果 |
| --- | --- | --- |
| 建表与写入 | 2 TAG、5 FIELD 表写入 4 行 | PASS |
| 热区 UPDATE/DELETE | 更新 1 行、删除 1 行后计数和值正确 | PASS |
| 热区 FIELD 过滤 | `v1 > 20` 返回 2 行 | PASS |
| seal | 返回封存行数 3，热区清零 | PASS |
| 冷热合并查询 | seal 后总行数和 FIELD 过滤结果不变 | PASS |
| 时间+TAG+FIELD 组合 | 下推组合条件返回 2 行 | PASS |
| `tsfile_table_info` | hot_rows=0、device_count=2、file_count=1、watermark=2001 | PASS |
| `tsfile_device_info` | 2 个设备；g0,d1=2001、g1,d2=3001 | PASS |
| `tsfile_schema` | 8 列；TIMESTAMP:TIME、STRING:TAG、DOUBLE:FIELD；声明顺序正确 | PASS |
| export | 返回 1，输出目录存在 TsFile | PASS |
| 只读文件表 | 查询 3 行、FIELD 过滤 2 行；写入被拒绝 | PASS |
| 目录源 | 只读目录表查询 3 行，mode=readonly | PASS |
| compact 无可合并文件 | 返回 0 | PASS |
| verify | 自有表和只读表均为 OK | PASS |
| 错误路径 | 重复键、旧时间写入、只读 DELETE、缺失表均被拒绝 | PASS |

## 5. 性能与内存结果

### 本地 macOS（M5 Pro）

配置：1000 设备，每设备 1 万行，共 1000 万行；每 100 万行 seal 一次。

| 指标 | 结果 |
| --- | --- |
| 写入吞吐 | 约 **16.3 万行/秒** |
| 单次 seal（100 万行） | 约 **0.92 秒** |
| 10 次 seal 总耗时 | 约 11.1 秒 |
| 峰值 RSS | 约 **1.1GB** |
| SQLite DB | 约 87MB |
| TsFile 输出 | 约 61MB / 10 个文件 |
| 最终状态 | hot_rows=0、file_count=10、device_count=1000、watermark=10001 |

### datadev（按“累计 10 万行 seal”）

配置：1000 设备，每轮每设备 100 行，累计 10 万行 seal 一次。

| 指标 | 结果 |
| --- | --- |
| 写入吞吐 | 约 **8.6 万行/秒** |
| 单次 seal（10 万行） | 约 **0.33 秒** |
| 峰值 RSS | 约 **146MB**，不随轮数增长 |
| 已运行轮数 | 2804 轮（约 2.804 亿行），手动停止 |
| 已生成文件 | 2804 个，约 4.1GB |
| SQLite DB | 约 9.4MB（seal 后热区清空） |

### datadev 单次 1000 万行 seal

| 指标 | 流式 seal（当前） | 原物化 seal（基线） |
| --- | --- | --- |
| seal 耗时 | 34.3 秒 | 46.1 秒 |
| 峰值 RSS | **4.7GB** | **11.0GB** |
| 输出文件 | 3 个，约 114MB | 1 个，约 118MB |

## 6. 发现并修复的问题

1. **seal 一次性物化全部热行**：1000 万行峰值约 11GB，按 1000 设备 ×
   10 万行（1 亿行/次）外推约 110GB，会 OOM。已改为每 400 万行分块流式读取
   和写文件（提交 `f7de24ac0`），1000 万行 seal 峰值降到 4.7GB，耗时从
   46.1 秒降到 34.3 秒。
2. **TEXT FIELD 等值条件被误判为 TAG 条件**：FIELD 下推后，
   `apply_constraint_bounds` 只对真正的 TAG 列构造 TAG 过滤器。
3. **回滚路径中的读事务阻止 DETACH**：xRollback 不再在 SQLite 回滚过程中执行
   `load_config`，失败写入后可以正常 `DETACH`。
4. **`DESC` 丢失 TsFile 语义**：新增 `tsfile_schema('schema.table')`，返回
   原始类型、TIME/TAG/FIELD 类别、SQLite 映射类型和 not_null。
5. **DROP TABLE 遗留 `_devices` 影子表**：补齐模块生命周期测试时发现
   `xDestroy` 只清理 hot/segments/config，已补上 `_devices`，并验证 DROP 后
   影子表全部消失、已封存文件和目录标记保留。

## 7. 已知限制与风险

1. 查询结果仍在游标中整体物化，大结果集内存随行数增长。
2. seal 的流式块大小固定为 400 万行，峰值约 4.7GB；8GB 树莓派等边缘设备需要
   更小的块或按可用内存自适应。
3. compact 仍会把参与合并的行整体读入内存，大文件合并存在内存风险。
4. 每 10 万行 seal 会产生大量小文件（2804 轮已产生 2804 个）；后续查询会打开
   较多文件，建议周期性 compact。
5. FIELD 下推不覆盖 FLOAT、超出精确 double 范围的整数字面量以及 BOOLEAN
   非等值比较，这些谓词仍由 SQLite 做残余判断。
6. 没有后台自动 seal、保留期和 schema 迁移；这些操作由调用方显式执行。
7. 树莓派 aarch64 的编译与运行验证尚未完成。

## 8. 结论

- 功能自测通过：41 项自动化用例和 35 项手工 SQL 检查全部通过。
- 函数入口和错误/边界分支已补齐；剩余未覆盖项仅为需要故障注入的操作系统级
  故障，不影响本轮功能结论。
- 性能与内存符合当前设计预期；流式 seal 已消除大热区 seal 的主要 OOM 风险。
- 当前版本可以进入评审/联调；边缘设备低内存场景建议先降低 seal 块大小，
  并补充 compact 的流式处理。
