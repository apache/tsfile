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

# SQLite + TsFile 技术报告

本文说明当前实验实现；日常操作与排障见 [中文用户手册](USER_GUIDE.md)，
英文版本见 [Technical report](TECHNICAL_GUIDE_EN.md)。

## 1. 实现结构

`tsfile_sqlite.cc` 实现统一虚拟表 `tsfile_hybrid`，负责参数、schema、冷热读取、
热区写入、封存和事务回调。`tsfile_sqlite_management.inc` 位于同一匿名命名空间，
提供 seal/export 管理函数和状态、校验虚拟表。集成测试位于
`../../test/sqlite/tsfile_sqlite_test.cc`，使用真实 SQLite 连接和 TsFile 读写器。

连接维护按 SQLite schema 与逻辑表名索引的表对象。每张表保存列、模式、源文件映射、
时间精度、watermark、待提交文件和 savepoint 标记。游标当前在内存中收集查询结果。

显式定义加目录、外部文件加目录、外部文件不加目录三种建表方式，最终都形成同一套
列结构，再通过 `sqlite3_declare_vtab` 声明给 SQLite。前两种可写，第三种只读。
文件推断只处理明确指定的 source_table，并扫描其最大时间；空源表通过完整 schema
元数据识别。普通源文件的隐含时间列名为 time，扩展生成文件使用 Property
`tsfile_sqlite.time_column` 保存自定义时间列名。

重连从 SQLite 恢复已登记的结构，不因外部文件变化重新推断。源文件缺失时仍能查看
结构和诊断，实际读取通过文件特征检查后才继续。

## 2. SQLite 内部状态

| 对象 | 内容 |
| --- | --- |
| `<table>_tsfile$hot` | 可写表的业务列及独立内部行号 |
| `<table>_tsfile$segments` | 文件路径、cutoff、行数、文件内部表名及内容指纹 |
| `<table>_tsfile$config` | 结构、模式、精度、目录、watermark、源文件及最大时间 |
| `<table>_tsfile$key` | 热数据的逻辑唯一键索引 |

只读表不创建 hot。内部对象与逻辑表处于相同 SQLite schema，名称全部转义；冲突时
拒绝创建。schema 以带长度的列名、TsFile 类型和类别序列保存，不因 SQLite 类型映射
丢失原类型信息。直接修改内部对象不属于受支持接口。

每个 TAG 在唯一索引中使用 `(tag IS NULL)` 和 `coalesce(tag,'')` 两个分量，最后
追加 TIME。这使 NULL 键相互冲突，同时保留 NULL 与空字符串的区别。无 TAG 时只有
TIME 键。源文件原有重复行保持原样，新数据必须晚于源表最大时间，不能覆盖历史。

热区内部行号选择不与业务列冲突的 `tsfile$rowid` 名称，业务 rowid/oid/_rowid_ 不影响
内部定位。冷行使用负的合成行号，仅用于操作内定位，不是稳定业务主键。

## 3. 查询和写入

xBestIndex 提供整数时间范围、BINARY TAG 等值和列裁剪信息。xFilter 读取热区及登记
冷文件，并使用每段保存的 source_table 映射；事务内新封存的段从本次临时路径读取。
支持的时间和 TAG 条件下推给 TsFile，剩余谓词、关联、聚合、排序和分页由 SQLite
执行，结果顺序需要显式 ORDER BY。

xUpdate 检查类型、TIME 非空、唯一键和 watermark，只写入热区。实际命中冷行的
UPDATE/DELETE，以及只读表实际写入，返回非约束类错误 SQLITE_READONLY，保证
IGNORE/FAIL 也不能留下同一语句的部分热区修改。SQLite 不调用零行 DML 的 xUpdate，
所以只读表上的无目标行操作可以作为空操作成功。

watermark 是可接受新时间的闭下界。普通空表从 INT64_MIN 开始；有源数据时为
source_max + 1。可写表的 NULL watermark 表示时间空间耗尽，与只读表通过 mode 区分。
xBegin 重新加载 SQLite 中的边界，避免另一连接封存后仍使用旧缓存允许迟到写入。

## 4. 文件归属

可写目录在建表时必须不存在或为空，不能与其他自有目录重合、嵌套或包含源文件。
扩展解析已有祖先的真实路径，以独占创建并 fsync 的 `.tsfile-owner` 记录归属。
标记绑定 SQLite 数据库规范路径及表名；内存数据库使用连接身份。改变 ATTACH 别名
不改变归属，但复制或移动数据库不能直接复用原可写目录。重连和写事务检查归属。

失败或回滚建表仅回收本次创建且拥有的标记和空目录。DROP 保留已提交文件及标记，
避免事务回滚后失去目录归属；后续清理由调用方负责。

内容特征使用文件大小和遍历全部字节的 FNV-1a 指纹，用于检测变化，不提供密码学
完整性证明。外部源文件必须由调用方保持路径和内容不变。扩展不因文件后缀就删除
未知 `.tmp` 或 `.tsfile`，也不把源文件父目录当作自己拥有的目录。

## 5. 显式 seal 与事务

管理函数只接受独立顶层 SELECT。seal 创建内部 savepoint，并通过虚拟表的零行
UPDATE 使 SQLite 注册该表的写事务回调；随后直接执行封存。因此封存参与同一连接
的外层事务及 savepoint。连接上的管理重入保护阻止嵌套管理操作。

非空封存流程如下：

1. 读取符合半开边界的热行，按可空 TAG 和时间排序。
2. 写入临时 TsFile、完成 footer 并 fsync，保存 schema 和已知精度。
3. 在 SQLite 事务中登记最终路径及指纹、删除对应热行、推进 watermark。
4. xSync 重新打开临时文件检查元数据，以不覆盖已有目录项的原子 rename 发布文件，
   再 fsync 自有目录。
5. SQLite 提交登记状态，xCommit 清除本次待提交文件记录。

空封存只推进边界，不生成文件。显式 cutoff 为 int64 半开上界；export 自动封存
可以额外覆盖 INT64_MAX 并将可写时间标记为耗尽。

xRollback 只删除本次操作拥有的临时或已发布文件。xSavepoint 按 SQLite savepoint ID
记录待提交文件数，xRollbackTo 删除其后的文件并恢复缓存配置，xRelease 移除释放的
标记。回滚跨越新建表时，允许 SQLite 已删除 config，并释放本次新目录资源。

macOS 使用 `renamex_np` 的 RENAME_EXCL，Linux 使用 `renameat2` 的 RENAME_NOREPLACE。
已有目录项，包括悬空符号链接，均不会被替换。不支持该操作的文件系统或内核会报错。

文件持久化先于 SQLite 提交文件引用。进程在 SQLite 提交前中断可能留下未登记文件，
但 SQLite 仍保留旧热行，该文件不参与查询。verify 可以报告未登记的 TsFile，重连
不会猜测归属并自动删除。持久性依赖 SQLite journal/synchronous 设置和文件系统
正确执行 fsync，这不是跨文件系统的分布式事务。

## 6. 自动封存导出

export 禁止放入用户显式事务，先检查目标路径，再在内部事务中验证并捕获冷数据，
对可写表注册写事务后读取热数据。SQLite 的快照和写锁使并发冲突明确失败，不会让
一次导出静默混入边界检查之外的写入。

本次所有热行自动封存并提交后，将捕获的完整数据排序、重写到独立 staging 目录。
非空输出为一个 `part-000001.tsfile`，空表输出空目录。输出只含所选逻辑表，文件内部
表名使用逻辑表名，保留已知时间精度。源文件的其他表不会被带出。

输出完成后 fsync staging，以不覆盖方式原子发布目标目录，再 fsync 父目录。
封存提交与输出发布为两个阶段；第二阶段失败不会把冷数据变回热数据，也不会回退
watermark。错误说明封存是否已提交；父目录 fsync 失败时还会说明完整输出已发布。
普通失败清理本次已知 staging 文件，进程中断可以留下隔离目录，不发布部分结果。
封存提交后的新写入不进入本次输出，仍保留在热区。

## 7. 诊断和验证范围

`tsfile_table_info` 从当前事务视图读取配置、热行数和文件数。`tsfile_verify` 检查
登记文件是否存在、Reader 是否可解析元数据、指定表 schema 是否存在，以及全文
指纹是否一致；不逐页解码所有数据。它只扫描自有目录当前层的普通 `.tsfile`，不跟随
扫描项的符号链接，也不递归或扫描外部源文件父目录。状态为 OK、MISSING、CORRUPT、
MISMATCH、UNREGISTERED，仅报告，不注册、修复或删除文件。

集成测试覆盖三种建表、NULL 逻辑键、无 TAG、源表映射、空及多表文件、精度、INT64
耗尽、业务 rowid 列、冷热写约束、事务/savepoint、重连、并发边界、源文件变化与缺失、
目录归属、自动导出和标准 Reader 回读。回归还覆盖封存已提交后的导出失败、回滚跨越
建表、复制数据库抢占原目录，以及发布时已有目录项。

额外子进程检查在 DELETE/WAL 两种 journal 模式下，封存提交前后直接退出并重新打开
数据库，验证数据与冷热状态。这些检查不代表每个断电或文件系统故障点都已验证。
构建及测试命令见 [用户手册](USER_GUIDE.md)。

## 8. 当前限制

查询、封存及导出会在内存中收集数据，导出排序并重写完整快照，文件指纹也需要完整
读取文件；当前尚不是限制内存用量的流式实现。早期原型的 column=、隐藏管理列及
内部表布局已替换，不自动迁移原型数据库。首版不提供后台封存、compaction、自动
保留期、历史修正、批量文件注册、原地 schema 演进或完整数据库备份恢复。
Linux 特有文件发布路径需要 Linux CI 验证；本地验证环境是 macOS 和 Homebrew SQLite。
