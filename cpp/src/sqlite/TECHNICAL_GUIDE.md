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
提供 seal/export/compact 管理函数和表级/设备级状态、校验虚拟表。集成测试位于
`../../test/sqlite/tsfile_sqlite_test.cc`，使用真实 SQLite 连接和 TsFile 读写器。

连接维护按 SQLite schema 与逻辑表名索引的表对象。每张表保存列、schema、源文件或
源目录、设备列、待提交文件、待删除文件和 savepoint 标记。游标当前在内存中收集
查询结果。

显式定义加目录、外部文件加目录、外部文件不加目录三种建表方式，最终都形成同一套
列结构，再通过 `sqlite3_declare_vtab` 声明给 SQLite。前两种可写，第三种只读。
文件推断接受单个 TsFile 或目录；目录只扫描第一层的普通 `.tsfile`，并要求各文件的
目标表 schema 一致。推断只处理明确指定的 source_table，按完整 TAG 组合记录各设备
最大时间；空源表通过完整 schema 元数据识别。普通源文件的隐含时间列名为 time，
扩展生成文件使用 Property `tsfile_sqlite.time_column` 保存自定义时间列名。

重连从 SQLite 恢复已登记的结构，不因外部文件变化重新推断。源文件缺失时仍能查看
结构和诊断，实际读取通过文件特征检查后才继续。

## 2. SQLite 内部状态

| 对象 | 内容 |
| --- | --- |
| `<table>_tsfile$hot` | 可写表的业务列、内部设备键列及独立内部行号 |
| `<table>_tsfile$segments` | 文件路径、行数、文件内部表名、内容指纹及 size/mtime/inode |
| `<table>_tsfile$devices` | 完整 TAG 组合的编码键、watermark 和最大时间 |
| `<table>_tsfile$config` | 结构、模式、精度、目录、源文件及源表最大时间 |
| `<table>_tsfile$key` | 热数据的逻辑唯一键索引 |

只读表不创建 hot。内部对象与逻辑表处于相同 SQLite schema，名称全部转义；冲突时
拒绝创建。schema 以带长度的列名、TsFile 类型和类别序列保存，不因 SQLite 类型映射
丢失原类型信息。直接修改内部对象不属于受支持接口。

设备键按 TAG 顺序编码为 `N;`（NULL）或 `S<长度>:<字节>;`，避免 NULL、空字符串和
分隔符混淆；`tsfile_device_info` 返回解码后的可读值。

每个 TAG 在唯一索引中使用 `(tag IS NULL)` 和 `coalesce(tag,'')` 两个分量，最后
追加 TIME。这使 NULL 键相互冲突，同时保留 NULL 与空字符串的区别。无 TAG 时只有
TIME 键。源文件原有重复行保持原样；新数据必须不早于该设备的 watermark，不能回填
该设备的历史空隙，新设备不受其他设备边界限制。

热区内部行号选择不与业务列冲突的 `tsfile$rowid` 名称，业务 rowid/oid/_rowid_ 不影响
内部定位。冷行使用负的合成行号，仅用于操作内定位，不是稳定业务主键。

## 3. 查询和写入

xBestIndex 提供整数时间范围、BINARY TAG 等值和 BINARY FIELD 比较（=、>、>=、<、
<=）及列裁剪信息。xFilter 读取热区及登记冷文件，并使用每段保存的 source_table
映射；事务内新封存的段从本次临时路径读取。冷文件通过 `TableQueryExecutor` 把时间、
TAG 和字段比较下推给 TsFile 读取层；字段条件下推到 `AlignedFieldFilter`，可参与
页级裁剪。无法下推的表达式、关联、聚合、排序和分页由 SQLite 执行，结果顺序需要
显式 ORDER BY。为避免与 SQLite 比较语义不一致，FLOAT 列、超出精确 double 范围的
整数字面量以及 BOOLEAN 的非等值比较不进入字段下推，仍由 SQLite 保留残余判断。

xUpdate 检查类型、TIME 非空、唯一键和设备 watermark，只写入热区。实际命中冷行的
UPDATE/DELETE，以及只读表实际写入，返回非约束类错误 SQLITE_READONLY，保证
IGNORE/FAIL 也不能留下同一语句的部分热区修改。SQLite 不调用零行 DML 的 xUpdate，
所以只读表上的无目标行操作可以作为空操作成功。

watermark 是每个设备可接受新时间的闭下界。无源数据的设备从 INT64_MIN 开始；
有源数据时为该设备 source max + 1；新设备没有记录，不受其他设备约束。可写表的
NULL watermark 表示该设备时间空间耗尽，与只读表通过 mode 区分。xBegin 重新加载
SQLite 中的边界，避免另一连接封存后仍使用旧缓存允许迟到写入。

## 4. 文件归属

可写目录在建表时必须不存在或为空，不能与其他自有目录重合、嵌套或包含源文件。
扩展解析已有祖先的真实路径，以独占创建并 fsync 的 `.tsfile-owner` 记录归属。
标记绑定 SQLite 数据库规范路径及表名；内存数据库使用连接身份。改变 ATTACH 别名
不改变归属，但复制或移动数据库不能直接复用原可写目录。重连和写事务检查归属。

失败或回滚建表仅回收本次创建且拥有的标记和空目录。DROP 保留已提交文件及标记，
避免事务回滚后失去目录归属；后续清理由调用方负责。

内容指纹使用文件大小和遍历全部字节的 FNV-1a，在登记时计算一次并保存，用于
`tsfile_verify` 和导出前的内容一致性判断；普通查询只比较文件大小、mtime 和 inode。
指纹不提供密码学完整性证明。外部源文件必须由调用方保持路径和内容不变。扩展不因
文件后缀就删除未知 `.tmp` 或 `.tsfile`，也不把源文件父目录当作自己拥有的目录。

## 5. 显式 seal、compact 与事务

管理函数只接受独立顶层 SELECT。seal/compact 创建内部 savepoint，并通过虚拟表的
零行 UPDATE 使 SQLite 注册该表的写事务回调；随后直接执行操作。因此它们参与同一
连接的外层事务及 savepoint。连接上的管理重入保护阻止嵌套管理操作。

seal 接收 `max_file_size_mb`，不接收时间边界，流程如下：

1. 读取全部热行，按可空 TAG 和时间排序，并汇总各设备本次最大时间。
2. 按大小上限写一个或多个临时 TsFile；递归二分直到每个文件不超限；单行本身超限
   时返回错误并保留热行。每个文件完成 footer 并 fsync，保存 schema 和已知精度。
3. 在 SQLite 事务中登记最终路径、指纹及轻量元数据，删除全部热行。
4. 将本次有热行的设备 watermark 推进到该设备最大时间 + 1；INT64_MAX 写为 NULL。
   没有热行的设备保持不变。
5. xSync 重新打开临时文件检查元数据，以不覆盖已有目录项的原子 rename 发布文件，
   再 fsync 自有目录。
6. SQLite 提交登记状态，xCommit 清除本次待提交文件记录。

热区为空时返回 0，不生成文件，也不改变设备边界。

compact 接收小文件阈值和目标大小，只选择自有目录中低于阈值且已完整提交的登记
文件（跳过本次事务中尚未发布的文件）。它先做轻量元数据检查，再按文件指纹和登记
行数校验内容，通过 `TableQueryExecutor` 读回全部行并按目标大小重写。新文件先
登记，旧登记在事务中删除；未提交时仅记录待删除路径，xCommit 发布新文件后才删除
旧文件。失败或回滚保留原文件，不发布部分结果。热区和 watermark 不受影响。

xRollback 只删除本次操作拥有的临时或已发布文件，并放弃待删除路径。xSavepoint
按 SQLite savepoint ID 记录待提交文件和待删除文件的数量，xRollbackTo 删除其后的
待发布文件并截断待删除列表，xRelease 移除释放的标记。回滚跨越新建表时，允许
SQLite 已删除 config，并释放本次新目录资源。回滚路径不在 SQLite 回滚过程中执行
读语句，避免留下阻止 DETACH 的读事务。

macOS 使用 `renamex_np` 的 RENAME_EXCL，Linux 使用 `renameat2` 的 RENAME_NOREPLACE。
已有目录项，包括悬空符号链接，均不会被替换。不支持该操作的文件系统或内核会报错。

文件持久化先于 SQLite 提交文件引用。进程在 SQLite 提交前中断可能留下未登记文件，
但 SQLite 仍保留旧热行，该文件不参与查询。verify 可以报告未登记的 TsFile，重连
不会猜测归属并自动删除。持久性依赖 SQLite journal/synchronous 设置和文件系统
正确执行 fsync，这不是跨文件系统的分布式事务。

## 6. 导出

export 禁止放入用户显式事务。它不封存热行，只复制该表自有目录中的已登记文件；
只读表复制其登记的源文件。可写表的外部源文件不在自有目录中，不进入导出。输出必须
是新的绝对目录，父目录存在，且不与自有目录或源文件重叠。已有路径不会被覆盖。

流程是先检查路径，再创建名称含 `.tsfile-export-` 的 staging 目录。逐文件复制并
同时计算内容指纹，与登记指纹不一致时立即失败。文件名沿用原文件；重名或空名使
导出失败。全部复制完成并 fsync staging 后，以不覆盖方式原子发布目标目录，再
fsync 父目录。失败清理本次已知 staging 文件，进程中断可以留下隔离目录，不发布
部分结果。非空表返回复制文件数，空表创建空目录并返回 0。输出保留封存时的文件
内部表名和时间精度，不包含源文件的其他表。

## 7. 诊断和验证范围

`tsfile_table_info` 从当前事务视图读取模式、精度、设备数、设备 watermark 最小值、
源表最大时间、热行数、登记文件数及路径信息；空表 watermark 为 INT64_MIN，只读表
为 NULL。`tsfile_device_info` 按完整 TAG 组合返回解码后的键、watermark、当前最大
时间（含热行）和该设备热行数。

`tsfile_verify` 检查登记文件是否存在、Reader 是否可解析元数据、指定表 schema 是否
存在，以及全文指纹是否一致；它完整读取文件计算指纹，但不会逐页解码业务数据。
它只扫描自有目录当前层的普通 `.tsfile`，不跟随扫描项的符号链接，也不递归或扫描
外部源文件父目录。状态为 OK、MISSING、CORRUPT、MISMATCH、UNREGISTERED，仅报告，
不注册、修复或删除文件。

集成测试覆盖三种建表、目录源、NULL 逻辑键、无 TAG、源表映射、空及多表文件、精度、
INT64 耗尽、业务 rowid 列、冷热写约束、设备级边界、事务/savepoint、重连、并发边界、
字段与时间/TAG 组合过滤、大小上限切分、compact 与回滚、只读导出、源文件变化与缺失、
目录归属和标准 Reader 回读。回归还覆盖导出失败、回滚跨越建表、复制数据库抢占原
目录，以及发布时已有目录项。

额外子进程检查在 DELETE/WAL 两种 journal 模式下，封存提交前后直接退出并重新打开
数据库，验证数据与冷热状态。这些检查不代表每个断电或文件系统故障点都已验证。
构建及测试命令见 [用户手册](USER_GUIDE.md)。

## 8. 当前限制

查询和 compact 会在内存中收集数据；seal 按固定行数分块流式读取热区，内存上限
不再随热行总量线性增长。导出直接复制文件，耗时与文件大小相关；文件指纹校验也
需要完整读取文件。查询结果仍会整体物化，尚未实现流式返回。字段下推只覆盖 BINARY
的整数、字符串及可精确表示的 double 比较，FLOAT 和 BOOLEAN 非等值比较留给 SQLite
残余判断。早期原型的 column=、隐藏管理列及内部表布局已替换，不自动迁移原型数据库。
首版不提供后台封存、保留期、历史修正、批量文件注册、原地 schema 演进或完整数据库
备份恢复。Linux 特有文件发布路径需要 Linux CI 验证；本地验证环境是 macOS 和
Homebrew SQLite。
