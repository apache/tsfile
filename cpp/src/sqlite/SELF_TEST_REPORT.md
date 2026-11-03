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

分支：`experiment-sqlite`（基于 `develop` 的 `931a5d3`）

构建和手工测试步骤见 [TESTING.md](TESTING.md)。

## 1. 测试环境

### 1.1 本地环境

- 机器：Apple M5 Pro，32GB 内存。
- 系统：macOS 26.6.2。
- 编译器：Apple Clang（Command Line Tools）。
- SQLite：Homebrew SQLite 3.53.3，支持 load extension。
- 构建：CMake Release，`BUILD_SQLITE_EXTENSION=ON`、`TSFILE_BUILD_SHARED=ON`、`BUILD_TEST=ON`。
- 产物：`build/develop/lib/tsfile_sqlite.dylib` 和 `build/develop/test/lib/TsFile_Sqlite_Test`。

### 1.2 远端环境

- 机器：datadev，32 核，62GB 内存。
- 系统：Ubuntu 24.04，Linux 6.8，gcc 13.3。
- SQLite：3.45.1（`libsqlite3-dev`）。
- 构建：CMake Release + Ninja，关闭 ANTLR4/SIMD/SNAPPY/LZ4/LZOKAY/ZLIB/ZSTD。
- 数据目录：`/data/colin/tsfile-sqlite-bench`。

树莓派 4B（8GB，aarch64）已完成依赖安装和源码上传，本轮未完成构建。

### 1.3 构建与执行命令

```bash
cmake -S cpp -B build/develop -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON
cmake --build build/develop --target tsfile_sqlite TsFile_Sqlite_Test -j 8
DYLD_LIBRARY_PATH=build/develop/lib \
  build/develop/test/lib/TsFile_Sqlite_Test
```

完整依赖安装和平台差异见 [TESTING.md](TESTING.md)。

## 2. 自动化自测

以下每个小节对应一个 `TsFileSqliteTest` 用例。每节固定记录四项：

- **输入**：测试使用的数据或操作。
- **构造方式**：如何通过 SQL、文件或事务构造该场景。
- **预期输出**：断言要求的结果。
- **验证**：实际执行结果。

### 2.1 建表与声明

#### 2.1.1 `SqlDeclarationsAndNullableLogicalKeys`

- **输入**：同一时间下三种 device：NULL、空字符串、字符串 `null`，以及重复的 NULL 键。
- **构造方式**：创建带 `device STRING TAG` 的表，插入 `(1,NULL)`、`(1,'')`、`(1,'null')`，再尝试插入第二条 `(1,NULL)`。
- **预期输出**：三种键可共存并分别查询；重复 NULL 键报 `SQLITE_CONSTRAINT`；更新 NULL 行成功。
- **验证**：查询 `count(*)=3`，分别按 `IS NULL`、`=''`、`='null'` 命中；重复插入返回约束错误。实际：通过。

#### 2.1.2 `NoTagsAndInvalidDeclarations`

- **输入**：无 TAG 表；非法列类别、重复 `timestamp_precision`。
- **构造方式**：创建只有 TIME 和 FIELD 的表；尝试创建缺少 FIELD 类别、以及重复精度的表。
- **预期输出**：无 TAG 表同一时间只能有一行；非法声明建表失败。
- **验证**：第二次同一时间插入返回约束错误；两个非法 `CREATE VIRTUAL TABLE` 返回非 OK。实际：通过。

#### 2.1.3 `QuotedSchemaAndRejectedEmptyPrecision`

- **输入**：表名 `odd.table`，列名 `event time` 和 `sensor value`；空精度参数。
- **构造方式**：使用双引号创建表，写入一行，seal 后 export，再以导出文件创建只读表。
- **预期输出**：引号标识符可正常建表、封存、导出和重新读取；空精度被拒绝。
- **验证**：导出文件可读回 1 行；`timestamp_precision=''` 的建表返回错误。实际：通过。

#### 2.1.4 `AttachedDatabaseUsesItsOwnShadowTables`

- **输入**：通过 `ATTACH ':memory:' AS aux` 打开的独立数据库。
- **构造方式**：在 `aux` 中创建虚拟表并插入一行。
- **预期输出**：热表影子表建在 `aux`，不在 `main`。
- **验证**：`aux."attached_tsfile$hot"` 有 1 行；`main.sqlite_master` 中不存在同名表。实际：通过。

### 2.2 热区 CRUD 与类型

#### 2.2.1 `HotCrudAndSealRollback`

- **输入**：2 行同设备数据；一次事务内 seal 后回滚。
- **构造方式**：插入两行，`BEGIN` 后 seal，再 `ROLLBACK`；随后重新 seal，并插入旧时间。
- **预期输出**：回滚后两行仍是热数据且段数为 0；提交 seal 后段数为 1；旧时间写入失败。
- **验证**：回滚后 `count(*)=2`、段表 0 行；seal 后查询两行、段表 1 行；插入时间 1 返回约束错误。实际：通过。

#### 2.2.2 `Int32RangeAndColdRowsAreImmutable`

- **输入**：超出 INT32 范围的值 2147483648；已封存行。
- **构造方式**：插入越界值，再插入合法值并 seal，随后尝试 UPDATE/DELETE 冷行。
- **预期输出**：越界值报类型错误；冷行更新/删除返回 `SQLITE_READONLY`。
- **验证**：越界插入返回 `SQLITE_MISMATCH`；UPDATE/DELETE 返回 `SQLITE_READONLY`；查询仍得到 42。实际：通过。

#### 2.2.3 `NullAndBlobSurviveSeal`

- **输入**：BLOB `X'000102'`、空字符串、全 NULL 行。
- **构造方式**：插入三行并 seal，查询 BLOB 十六进制、NULL 字段和空文本长度。
- **预期输出**：BLOB、NULL、空字符串在 seal 前后保持一致。
- **验证**：`hex(payload)='000102'`、NULL 行字段均为 NULL、`length(note)=0` 各命中 1 行。实际：通过。

#### 2.2.4 `RowidNamedColumnsAndBooleanValues`

- **输入**：业务列名为 `rowid`、`oid`、`_rowid_`，以及 BOOLEAN 值。
- **构造方式**：创建含这些列的表，插入两行，更新业务 `rowid`，删除一行后 seal。
- **预期输出**：业务列不干扰内部 rowid；BOOLEAN 值正确；seal 后数据保留。
- **验证**：更新后 `rowid=999/oid=88/_rowid_=77` 可查；删除后剩 1 行；seal 后仍为 1 行。实际：通过。

#### 2.2.5 `BusinessColumnsDoNotCarryManagementCommands`

- **输入**：业务列 `_tsfile_command`，值为字符串 `seal`。
- **构造方式**：插入该字符串，查询并 seal。
- **预期输出**：字符串只作为业务数据，不触发管理操作；表列数仍为 2。
- **验证**：查询命中 `_tsfile_command='seal'`；`pragma_table_xinfo` 列数为 2；seal 成功。实际：通过。

#### 2.2.6 `AllFieldTypesRoundTripAtExtremes`

- **输入**：BOOLEAN、INT32、INT64、FLOAT、DOUBLE、TEXT、STRING、BLOB、DATE、TIMESTAMP 十种 FIELD 的最小值、最大值、空值和全 NULL 行。
- **构造方式**：创建 12 列（TIME + TAG + 10 FIELD）表，插入 3 行，seal 后逐类型查询。
- **预期输出**：所有极值、空字符串、空 BLOB 和 NULL 均保持；schema 为 12 列。
- **验证**：INT32/INT64 极值、FLOAT/DOUBLE、TEXT/STRING、BLOB 长度、DATE/TIMESTAMP 极值、全 NULL 行均命中预期计数；`tsfile_schema` 返回 12 行。实际：通过。

### 2.3 查询与下推

#### 2.3.1 `FieldPredicatesFilterHotAndColdRows`

- **输入**：5 行传感器数据，其中一行温度为 NULL。
- **构造方式**：先查询热区，再 seal，再用相同的 FIELD、时间、TAG 条件查询冷区。
- **预期输出**：`temperature>2` 热区和冷区均为 3 行；`=2.5` 为 1 行；`>=2 AND <5` 为 2 行；`AND device='d1'` 为 1 行；NULL 条件为 1 行。
- **验证**：seal 前后计数一致；`temperature IS NULL` 命中 1 行。实际：通过。

#### 2.3.2 `FieldPredicatesSupportTypedFields`

- **输入**：TEXT、INT32、BOOLEAN 三种 FIELD，包含大小写和 NULL。
- **构造方式**：插入 `alpha/beta/NULL`、10/20/30、true/false，seal 后查询。
- **预期输出**：文本等值区分大小写；INT32 范围正确；BOOLEAN 等值正确；组合条件正确。
- **验证**：`label='alpha'` 命中 1，`'ALPHA'` 命中 0；`qty>=20` 命中 2；`flag=1` 命中 2；组合条件命中 1。实际：通过。

#### 2.3.3 `TimeBoundaryQueriesAndWatermarks`

- **输入**：时间 `INT64_MIN`、`INT64_MAX`、`-1`、`0`、`1`。
- **构造方式**：五个设备各写一个边界时间，seal 后查询区间和 watermark。
- **预期输出**：边界时间可写可查；`>0 AND <1` 为 0 行；`BETWEEN INT64_MIN AND 0` 为 3 行；最大时间设备 watermark 为 NULL；最小时间设备 watermark 为 `INT64_MIN+1`。
- **验证**：各计数符合；最大设备写入被拒绝；最小设备写入原最小值被拒绝、`INT64_MIN+1` 成功。实际：通过。

#### 2.3.4 `QueryOperatorAndResidualMatrix`

- **输入**：6 行数据，2 个设备，温度含 NULL，文本标签。
- **构造方式**：seal 后执行 `=、<>、>、BETWEEN、IN、IS NULL、NOT、OR、LIKE、GLOB`、排序分页和 `GROUP BY`。
- **预期输出**：`device='d0' OR temperature>5` 返回 4 行（两个分支都完整返回）；`<>2.5` 为 4 行；`NOT(>3)` 为 2 行；`LIKE '%ta'` 为 2 行；倒序分页为 `6,5` 和 `4,3`。
- **验证**：逐项断言通过，特别是 OR 查询暴露并验证了稳定冷行号修复。实际：通过。

#### 2.3.5 `MultiTagNullEmptyCombinations`

- **输入**：双 TAG 的 5 种组合：`(NULL,NULL)`、`('',NULL)`、`(NULL,'')`、`('','')`、`('a','b')`。
- **构造方式**：同一时间写入 5 行，seal 后逐组合查询，并尝试重复 NULL 组合。
- **预期输出**：5 个设备可区分；重复组合在相同时间被拒绝；不同时间可写入。
- **验证**：5 个组合各命中 1 行；`device_info` 5 行；重复插入约束失败；时间 2 插入成功且该设备 `hot_rows=1`。实际：通过。

### 2.4 seal

#### 2.4.1 `SealSplitsOutputBySizeLimit`

- **输入**：12 行，每行 200KB BLOB，`max_file_size_mb=1`。
- **构造方式**：递归 CTE 生成 12 行并 seal。
- **预期输出**：返回 12，至少生成 2 个文件，每个文件不超过 1MB；查询仍为 12 行。
- **验证**：文件数为 4，每个约 603KB；`file_count` 与目录文件数一致。实际：通过。

#### 2.4.2 `NoTagSealingAndWholeStatementColdFailures`

- **输入**：无 TAG 表；多条写入中包含旧时间；空热区 seal。
- **构造方式**：先 seal 2 行，再尝试 UPDATE/DELETE；插入 `(3,..),(2,..)`，再分别 seal 空热区、写入后 seal。
- **预期输出**：冷行不可改；整条语句因一行旧时间失败且不留下部分热行；空 seal 返回 0。
- **验证**：UPDATE/DELETE 返回 `SQLITE_READONLY`；多行插入失败后仍为 2 行；空 seal 0，后续 seal 返回 1，watermark=4。实际：通过。

#### 2.4.3 `SealPreservesPreexistingTemporarySymlink`

- **输入**：目录中预先存在名为 `tsfilesensor-<pid>-0.tmp` 的悬空符号链接。
- **构造方式**：创建该链接后插入一行并 seal。
- **预期输出**：seal 跳过被占用的临时名，不覆盖符号链接，数据正常封存。
- **验证**：链接仍存在且仍是符号链接；查询返回 1 行。实际：通过。

#### 2.4.4 `CommitNeverOverwritesExistingDirectoryEntries`

- **输入**：seal 提交前，在临时文件对应的最终路径放置符号链接。
- **构造方式**：事务内 seal，找到 `.tmp` 对应最终名并创建符号链接，然后 COMMIT。
- **预期输出**：COMMIT 失败；符号链接不被覆盖；热行保留。
- **验证**：COMMIT 返回错误；`lstat` 仍为符号链接；查询和 `hot_rows=1` 正常。实际：通过。

### 2.5 compact

#### 2.5.1 `CompactMergesSmallSealedFiles`

- **输入**：3 次单行 seal，生成 3 个小文件。
- **构造方式**：每次插入一行后 seal，然后 `tsfile_compact(table,10,20)`。
- **预期输出**：返回 1，文件数和段数降为 1，数据 3 行，watermark 不变；再次 compact 返回 0；目标小于阈值报错。
- **验证**：文件数 1、段数 1、行数 3、watermark=4；verify 为 OK；第二次 0；非法参数返回约束错误。实际：通过。

#### 2.5.2 `CompactRollbackKeepsOriginalFiles`

- **输入**：3 个小文件，事务内 compact 后回滚。
- **构造方式**：`BEGIN`，compact 返回 1，`ROLLBACK`。
- **预期输出**：回滚后 3 个文件、3 个段、3 行都保留；再次 compact 成功。
- **验证**：回滚后目录文件数 3、段数 3、行数 3；再次 compact 返回 1，文件数 1。实际：通过。

#### 2.5.3 `CompactThresholdAndChangedFile`

- **输入**：一个小文件（约 1KB）和一个大文件（约 1.2MB），`min=1MB,target=2MB`；随后修改大文件。
- **构造方式**：两次 seal 生成两个文件，先按阈值 compact，再覆盖其中一个文件后 compact。
- **预期输出**：阈值过滤后只有小文件参与，因不足 2 个返回 0；文件被修改时 compact 报错，原文件保留。
- **验证**：第一次返回 0 且文件数仍为 2；修改文件后 compact 返回非 OK，`file_count=2`。实际：通过。

#### 2.5.4 `CompactCanSplitIntoMultipleOutputFiles`

- **输入**：两个约 600KB 的文件，`min=1MB,target=1MB`。
- **构造方式**：分两次 seal，然后 compact。
- **预期输出**：两个输入合并后超过目标，拆成 2 个输出文件；数据 2 行；verify 两个文件均 OK。
- **验证**：compact 返回 2，目录 2 个文件，`file_count=2`，查询 2 行，verify OK 2 行。实际：通过。

### 2.6 export

#### 2.6.1 `ExportCopiesSealedFilesAndRoundTrips`

- **输入**：2 行热数据；导出前未 seal。
- **构造方式**：先 export 一次（热区未封存），再 seal 后 export，创建只读表读回。
- **预期输出**：首次 export 返回 0 且目录为空；seal 后 export 返回 1；只读表 2 行；热区数据不进入导出；已有输出路径报错；显式事务不可 export。
- **验证**：首次 0、空目录；seal 后 1；只读表 2 行且 NULL 行正确；重复路径失败；事务内失败。实际：通过。

#### 2.6.2 `ExportCountsEverySealedFile`

- **输入**：两次 seal，生成两个自有文件。
- **构造方式**：每次插入一行并 seal，然后 export。
- **预期输出**：返回 2，输出目录包含 2 个 TsFile。
- **验证**：返回 2，`count_tsfiles(output)=2`。实际：通过。

#### 2.6.3 `ExportRejectsChangedSourceAndDuplicateNames`

- **输入**：只读源文件被修改；两个不同目录下同名 `same.tsfile` 的手工段登记。
- **构造方式**：覆盖源文件后 export；再构造同名文件并插入段表后 export。
- **预期输出**：两种情况都报错，输出目录不产生；不覆盖目标。
- **验证**：两次 export 均非 OK，两个目标目录都不存在。实际：通过。

#### 2.6.4 `ExportFailureRetainsCommittedSealAndManagementGuards`

- **输入**：在行查询中调用 seal；超长输出路径；合法路径重试。
- **构造方式**：`SELECT tsfile_seal(...) FROM sensor`；export 到 240 字符路径；再 seal 后 export 到新目录。
- **预期输出**：行查询中的管理调用被拒绝；超长路径失败且不自动 seal；重试成功。
- **验证**：前两步返回错误，热行仍为 1；seal 后 export 返回 1。实际：通过。

#### 2.6.5 `DirectorySourceAndReadonlyExport`

- **输入**：一个包含两个 TsFile 的目录。
- **构造方式**：两次 seal 后，以 `file=<目录>`、`source_table='sensor'` 创建只读表，查询并 export。
- **预期输出**：只读表 2 行、mode=readonly、file_count=2；export 返回 2 并复制两个文件。
- **验证**：计数 2、2、2；`count_tsfiles(output)=2`。实际：通过。

#### 2.6.6 `RawMultiTablePrecisionAndExportIsolation`

- **输入**：一个包含 `selected`、`unrelated`、`empty` 三张表的原始 TsFile。
- **构造方式**：推断 `selected` 为表；创建可写 renamed 表并 seal；导出后检查文件内表；空表导出。
- **预期输出**：只导出所选表；不包含 unrelated；空表返回 0；可写表的外部源不进入自有导出。
- **验证**：导出文件只有 `renamed` 表；空表输出 0；时间精度 unknown 可查询。实际：通过。

### 2.7 管理函数边界

#### 2.7.1 `ManagementFunctionGuardsAndValidation`

- **输入**：错误参数类型、缺表、0/超大大小、父目录缺失、输出重叠、行查询调用、只读表。
- **构造方式**：逐项调用 `tsfile_seal`、`tsfile_compact`、`tsfile_export`，并创建只读表。
- **预期输出**：参数错误返回 `SQLITE_MISUSE`；缺表返回错误；非法大小返回 `SQLITE_CONSTRAINT`；只读 seal/compact 返回 `SQLITE_READONLY`；只读 export 成功。
- **验证**：所有断言返回码符合；只读 export 返回 1。实际：通过。

### 2.8 事务与回滚

#### 2.8.1 `ManagementSealRespectsTransactionsAndSavepoints`

- **输入**：3 行热数据；外层事务内两次 savepoint 和写入。
- **构造方式**：BEGIN、seal、savepoint、插入、savepoint、插入、逐层回滚到 savepoint，最后整体回滚并重新 seal。
- **预期输出**：回滚到内层后热行数 1，回滚到外层后 0，整体回滚后恢复 3；重新 seal 后 0。
- **验证**：各阶段 `hot_rows` 为 1/0/3/0；最终 verify 1 个 OK。实际：通过。

#### 2.8.2 `RollbackPastTableCreationPreservesOuterTransaction`

- **输入**：外层事务已有 `keep` 表；savepoint 内创建并 seal 临时虚拟表。
- **构造方式**：`BEGIN`、写入 keep、savepoint、CREATE VIRTUAL TABLE、INSERT、seal、ROLLBACK TO savepoint、RELEASE、COMMIT。
- **预期输出**：keep 数据保留；临时表消失；临时目录被回收。
- **验证**：commit 后 `keep=42`；`sqlite_master` 无临时表；目录不存在。实际：通过。

#### 2.8.3 `DiagnosticsAndInt64Exhaustion`

- **输入**：时间 `INT64_MAX` 的热行；已封存文件被删除。
- **构造方式**：插入最大时间、seal、export，再向同设备插入最大时间；最后删除源文件并 verify。
- **预期输出**：设备 watermark 为 NULL；同设备写入失败；verify 为 MISSING；查询失败。
- **验证**：device/table watermark 均为 NULL；重复写入约束错误；verify MISSING 1 行；`SELECT *` 失败。实际：通过。

### 2.9 重连与文件校验

#### 2.9.1 `PersistentReconnectAndConcurrentWatermark`

- **输入**：文件数据库中的无 TAG 表；两个 SQLite 连接。
- **构造方式**：连接 A 插入两行并 seal，连接 B 读取、插入旧时间/新时间，随后 DETACH/ATTACH。
- **预期输出**：连接 B 能读到已提交数据；旧时间失败、新时间成功；重连后热行 1、watermark=3。
- **验证**：两连接行为符合；重连后计数 3、hot_rows 1、watermark 3。实际：通过。

#### 2.9.2 `ReconnectedFileTableRestoresDeviceBoundaries`

- **输入**：外部 TsFile 源 + 可写目录的文件模式表。
- **构造方式**：写入合法时间，DETACH/ATTACH 后写入旧设备时间和新设备时间。
- **预期输出**：重连后设备边界仍生效：同设备旧时间失败，新设备旧时间成功。
- **验证**：旧设备时间返回约束错误；新设备插入成功，总数 3。实际：通过。

#### 2.9.3 `SourceSchemaPersistsWhenFileBecomesUnavailable`

- **输入**：只读源文件 + 可写目录；连接期间源文件被移走。
- **构造方式**：建表、插入热行、DETACH、重命名源文件、再 ATTACH。
- **预期输出**：schema 仍为 3 列；verify 报 MISSING；查询失败；恢复文件后可查询。
- **验证**：pragma 3 列、verify MISSING、查询失败、恢复后 2 行。实际：通过。

#### 2.9.4 `SourceValidationAndPreserveUnknownFiles`

- **输入**：错误的 source_table、错误精度、未知 `.tsfile`。
- **构造方式**：用不存在的表名/冲突精度建表；在自有目录放入未知文件；再 seal。
- **预期输出**：非法建表失败；未知文件不被删除。
- **验证**：三次非法建表失败；未知文件仍存在。实际：通过。

#### 2.9.5 `VerifyReportsUnregisteredCorruptAndMismatch`

- **输入**：未登记文件、被替换为另一张表的有效文件、被写成垃圾的登记文件、缺失表。
- **构造方式**：分别制造上述文件状态并查询 `tsfile_verify`。
- **预期输出**：状态依次为 `UNREGISTERED`、`MISMATCH`、`CORRUPT`；缺表报错。
- **验证**：三种状态各命中 1 行；缺失表调用返回错误。实际：通过。

### 2.10 目录、诊断与模块生命周期

#### 2.10.1 `DirectoryOwnershipAndCreationRollback`

- **输入**：与其他表重合/嵌套的目录；事务内创建后回滚；影子表名冲突。
- **构造方式**：尝试非法目录建表；事务内建表后 ROLLBACK；预先创建 `collision_tsfile$config`。
- **预期输出**：非法目录失败；回滚删除新目录；影子表冲突时建表失败且原表保留。
- **验证**：非法建表失败；回滚目录不存在；冲突表仍在且属性保留。实际：通过。

#### 2.10.2 `CopiedDatabaseCannotShareWritableDirectory`

- **输入**：复制数据库文件；同一表名。
- **构造方式**：在原数据库建表写入，复制数据库，打开副本写同一表。
- **预期输出**：副本写入被拒绝；原数据库继续可写。
- **验证**：副本写入失败；原数据库插入后 2 行。实际：通过。

#### 2.10.3 `DeviceInfoReportsPerDeviceWatermarks`

- **输入**：两个设备，时间 1 和 5；seal 前和 seal 后。
- **构造方式**：插入两行，查询 device_info，再 seal 后查询。
- **预期输出**：seal 前 hot_rows=1、max_time 正确；seal 后 watermark 为 2 和 6；表级 watermark 取最小值 2。
- **验证**：各字段值符合；device_count=2。实际：通过。

#### 2.10.4 `DiagnosticsExposeSourceAndEmptyNoTagDevices`

- **输入**：可写表、只读文件表、无 TAG 空表、缺表。
- **构造方式**：seal 后查询 table_info/device_info；创建只读源表；创建空无 TAG 表并插入一行。
- **预期输出**：自有表 directory 非空、source 字段为 NULL；只读表 source_file/source_table/source_max_time 正确；空无 TAG 表有 1 个占位设备，watermark 为 INT64_MIN；缺表报错。
- **验证**：各字段断言通过；插入后 max_time=5、hot_rows=1。实际：通过。

#### 2.10.5 `SchemaFunctionListsTsfileTypes`

- **输入**：10 列类型表（TIME + 双 TAG + 7 FIELD）。
- **构造方式**：建表后查询 `tsfile_schema`。
- **预期输出**：10 行，按声明顺序返回原始类型、类别、SQLite 类型、not_null；缺表报错。
- **验证**：TIMESTAMP:TIME、STRING:TAG、DOUBLE/INT32/BLOB 对应类型、TIME not_null=1、顺序正确；缺表失败。实际：通过。

#### 2.10.6 `SchemaFunctionWorksForInferredFileTables`

- **输入**：seal 后的文件，只读推断表。
- **构造方式**：以 `file=`、`source_table='sensor'` 创建只读表，查询 schema。
- **预期输出**：3 列；time 为 TIMESTAMP TIME，device 为 STRING TAG，temperature 为 DOUBLE FIELD。
- **验证**：各字段和 not_null 符合。实际：通过。

#### 2.10.7 `HybridModuleLifecycleAndTagUpdates`

- **输入**：`ALTER TABLE RENAME`；修改 TAG；`OR IGNORE/FAIL`；DROP TABLE。
- **构造方式**：尝试 RENAME；插入两行后把一行 device 改为 d1；删除；冲突选项；seal 后 DROP。
- **预期输出**：RENAME 被拒绝；TAG 修改后查询和 device_info 正确；IGNORE 成功、FAIL 失败；DROP 删除全部影子表并保留文件和目录标记。
- **验证**：各查询和文件状态符合；`sqlite_master` 无影子表。实际：通过。

#### 2.10.8 `TempTableAndQuotedReservedIdentifiers`

- **输入**：`temp` 库中的虚拟表；带空格表名；列名 `select`、`group`、`order`。
- **构造方式**：创建 temp 表并 seal/drop；创建带空格和保留字列的表并 seal。
- **预期输出**：temp 表可管理、可 seal；保留字列可查询，schema cid 正确。
- **验证**：temp 表 mode=writable 且查询 1 行；保留字表查询 1 行；schema 返回 `select:0:INTEGER:TIME`。实际：通过。

### 2.11 外部文件

#### 2.11.1 `ExternalFileInferredSchemaAndAppendBoundary`

- **输入**：已封存的 sensor 文件（d0 时间 1、d1 时间 2）；只读表和可写续表。
- **构造方式**：先用 `file=` 创建只读 history，再用 `file=` + `directory=` 创建 continuation；向 continuation 写入 d1 旧时间、新设备时间，更新热行后 seal。
- **预期输出**：history 推断出 3 列，只读写入被拒绝；continuation 对同一设备旧时间拒绝、新设备旧时间允许；seal 后查询 3 行。
- **验证**：history 2 行；只读 INSERT/UPDATE/DELETE 返回错误；continuation 时间 2 的 d1 失败、时间 3 的 NULL 设备成功；更新后查询和 seal 结果正确。实际：通过。

## 3. 手工 SQL 冒烟自测

手工测试通过 Homebrew `sqlite3` 加载扩展，使用持久数据库按用户操作顺序执行。以下按场景分节。

### 3.1 建表与写入

- **输入**：2 TAG、5 FIELD 的表，4 行数据。
- **构造方式**：`CREATE VIRTUAL TABLE` 后执行多行 `INSERT`。
- **预期输出**：`count(*)=4`。
- **验证**：查询返回 4。实际：通过。

### 3.2 热区更新和删除

- **输入**：4 行中的 1 行更新、1 行删除。
- **构造方式**：`UPDATE ... WHERE time=2000`，`DELETE ... WHERE time=4000`。
- **预期输出**：更新值 22.5 可查，剩余 3 行。
- **验证**：`v1=22.5` 命中 1，`count(*)=3`。实际：通过。

### 3.3 热区 FIELD 过滤

- **输入**：热区数据，条件 `v1>20`。
- **构造方式**：直接查询。
- **预期输出**：2 行。
- **验证**：计数 2。实际：通过。

### 3.4 seal 与冷热合并查询

- **输入**：3 行热数据，`tsfile_seal('main.sensor',64)`。
- **构造方式**：seal 后重新查询总数和 `v1>20`。
- **预期输出**：seal 返回 3；总数 3；过滤 2 行。
- **验证**：返回值与计数符合。实际：通过。

### 3.5 时间+TAG+FIELD 组合下推

- **输入**：`time>=1000 AND time<3000 AND group_id='g0' AND v1>20`。
- **构造方式**：冷区查询。
- **预期输出**：2 行。
- **验证**：计数 2。实际：通过。

### 3.6 `tsfile_table_info`

- **输入**：seal 后的表。
- **构造方式**：查询 hot_rows、device_count、file_count、watermark。
- **预期输出**：0、2、1、2001。
- **验证**：四项值符合。实际：通过。

### 3.7 `tsfile_device_info`

- **输入**：两个设备 g0,d1 和 g1,d2。
- **构造方式**：seal 后查询 device_key/watermark/max_time。
- **预期输出**：`g0,d1` watermark=2001；`g1,d2` watermark=3001。
- **验证**：两行值符合。实际：通过。

### 3.8 `tsfile_schema`

- **输入**：TIME + 双 TAG + 5 FIELD。
- **构造方式**：查询 cid/name/type/category/sqlite_type/not_null。
- **预期输出**：8 列，类型和类别正确，顺序与声明一致。
- **验证**：schema 计数 8，时间/标签/字段类型正确。实际：通过。

### 3.9 export 与只读回读

- **输入**：seal 后的文件。
- **构造方式**：`tsfile_export` 到新目录，再用导出的文件创建只读表。
- **预期输出**：export 返回 1；只读表 3 行；FIELD 过滤 2 行；写入被拒绝。
- **验证**：返回值、行数、只读错误符合。实际：通过。

### 3.10 目录源与 compact

- **输入**：包含已封存文件的目录。
- **构造方式**：以目录创建只读表；对无可合并文件的表执行 compact。
- **预期输出**：只读表 3 行、mode=readonly；compact 返回 0。
- **验证**：计数和返回值符合。实际：通过。

### 3.11 verify 与错误路径

- **输入**：自有表、只读表、重复键、旧时间、只读 DELETE、缺表。
- **构造方式**：分别调用 verify 和错误写入。
- **预期输出**：verify 为 OK；所有错误写入被拒绝。
- **验证**：verify 计数 1；错误调用均返回非 OK。实际：通过。

手工冒烟共 35 项检查，全部通过。

## 4. 性能与内存自测

### 4.1 本地 macOS

- **输入**：1000 设备，每设备 1 万行，共 1000 万行；每 100 万行 seal。
- **构造方式**：C++ 驱动使用 prepared INSERT 逐行写入，每 100 万行提交并 seal；采样进程 RSS。
- **预期输出**：写入稳定，seal 成功，内存不随已封存文件数增长。
- **验证**：写入约 16.3 万行/秒；单次 seal 约 0.92 秒；峰值 RSS 约 1.1GB；输出约 61MB/10 个文件。

### 4.2 datadev 每 10 万行 seal

- **输入**：1000 设备，每轮每设备 100 行，累计 10 万行 seal。
- **构造方式**：逐设备轮转写入 100 轮时间点，每轮提交并 seal；持续采样。
- **预期输出**：写入持续，seal 10 万行成功，内存稳定。
- **验证**：约 8.6 万行/秒；单次 seal 约 0.33 秒；峰值 RSS 约 146MB；运行 2804 轮（约 2.804 亿行）后手动停止，文件约 4.1GB。

### 4.3 datadev 单次 1000 万行 seal

- **输入**：1000 万行热数据。
- **构造方式**：写入后单次 seal，分别测物化版本和流式版本。
- **预期输出**：流式版本内存有界且耗时相当或更短。
- **验证**：流式 34.3 秒/4.7GB，物化 46.1 秒/11.0GB；流式输出 3 个文件约 114MB。

## 5. 问题回归

### 5.1 seal 内存放大

- **输入**：1000 万行热数据。
- **构造方式**：原实现一次性读取全部热行，修复后按 400 万行分块读取。
- **预期输出**：峰值内存从线性增长降为有界。
- **验证**：峰值从 11.0GB 降到 4.7GB，耗时从 46.1 秒降到 34.3 秒。

### 5.2 TEXT FIELD 条件误判

- **输入**：`label LIKE ...` 前的 TEXT 等值条件 `label='alpha'`。
- **构造方式**：字段下推后执行文本等值查询。
- **预期输出**：只作为 FIELD 条件处理，不进入 TAG 过滤器。
- **验证**：大小写敏感查询命中 1，不命中 0。实际：通过。

### 5.3 回滚后 DETACH 锁定

- **输入**：失败写入后执行 `DETACH`。
- **构造方式**：触发约束错误，再 DETACH。
- **预期输出**：DETACH 成功。
- **验证**：修复后 `DETACH` 返回 OK。实际：通过。

### 5.4 DROP 遗留设备影子表

- **输入**：已封存的表执行 `DROP TABLE`。
- **构造方式**：DROP 后检查 `sqlite_master` 和目录。
- **预期输出**：hot/segments/devices/config 全部删除，文件和 `.tsfile-owner` 保留。
- **验证**：影子表 0 行；文件和标记存在。实际：通过。

### 5.5 冷数据 OR 查询丢行

- **输入**：`device='d0' OR temperature>5`。
- **构造方式**：seal 后执行 MULTI-INDEX OR，两次扫描按过滤结果生成行号。
- **预期输出**：两个分支的行都返回，共 4 行。
- **验证**：修复前 3 行；改为基于段、时间和 TAG 的稳定冷行号后 4 行。实际：通过。

## 6. 未覆盖范围

函数入口、参数校验、只读/缺表、路径、状态码和事务回滚已经覆盖。仍无法在
常规单元测试中稳定复现的是操作系统级故障注入：磁盘写满、fsync 失败、
rename 与提交之间进程崩溃、权限变化导致 staging 清理失败。这些路径未计入
本轮通过结果。

## 7. 结论

- 47 个自动化用例全部通过，35 项手工 SQL 检查全部通过。
- 类型极值、时间边界、谓词矩阵、多 TAG 空值、compact 多输出、temp 表、
  引号/保留字标识符和模块生命周期均已覆盖。
- 补测发现并修复了冷数据 OR 查询行号不稳定导致丢行的问题。
- 流式 seal 已消除大热区 seal 的主要 OOM 风险；剩余风险集中在查询结果物化、
  compact 内存和低内存边缘设备的块大小上。
- 当前版本可以进入评审/联调。
