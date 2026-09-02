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

# tsfile_sqlite 技术手册

本文说明 SQLite + TsFile Hybrid MVP 的设计边界、内部数据结构、SQLite
virtual table 回调、冷热读写路径以及事务和崩溃一致性。

实现入口是 `tsfile_sqlite.cc`，对 SQLite 注册的模块名为 `tsfile_hybrid`。

## 1. 目标和非目标

### 1.1 目标

- 让应用通过一张 SQLite 逻辑表查询热数据和 TsFile 历史数据；
- 让近期可变数据保留完整 SQLite 事务和 CRUD 语义；
- 通过显式时间水位把历史数据导出为标准、不可变 TsFile；
- 使文件生成、SQLite manifest 更新和热数据删除具备事务一致性；
- 复用 libtsfile 的 table model、Tablet writer 和 C++ ResultSet reader。

### 1.2 非目标

- 不修改 SQLite pager、B-tree、WAL、VFS 或 SQL parser；
- 不让 TsFile 成为普通 SQLite database file；
- 不提供历史 correction/tombstone；
- 不实现自动 seal、compaction、分布式 manifest 或在线备份协议；
- 不在 MVP 中承诺跨数据源的物理输出顺序。

因此当前实现是 SQLite loadable extension，而不是 SQLite 内核 fork。若应用不
希望执行 `.load`，未来可以把相同模块静态链接到定制 SQLite，并通过
`sqlite3_auto_extension()` 注册；这不会改变本文的数据模型。

## 2. 总体架构

```text
                         SQLite connection
                                |
                    SQL / transaction / planner
                                |
                     tsfile_hybrid virtual table
                      /                       \
          SQLite shadow hot table        TsFile segment files
          CRUD, WAL, constraints         immutable table model
                      \                       /
                       merged virtual cursor
```

主要组件职责：

| 组件 | 职责 |
| --- | --- |
| SQLite | SQL 执行、事务、WAL/journal、锁、残余谓词、排序和聚合 |
| `tsfile_sqlite` | virtual table 适配、schema 校验、冷热路由、seal、manifest |
| `libtsfile` | TsFile table model 编码、文件 footer、查询和 ResultSet |

扩展使用 `sqlite3ext.h` 和 `SQLITE_EXTENSION_INIT1/2` 获取宿主 SQLite API，
导出标准入口 `sqlite3_extension_init`。入口首次执行时初始化 libtsfile，然后
通过 `sqlite3_create_module_v2()` 注册 `tsfile_hybrid`。

## 3. 构建集成

根 CMake 增加默认关闭的选项：

```text
BUILD_SQLITE_EXTENSION=OFF
```

启用时：

- 拒绝 Windows；
- 要求 `TSFILE_BUILD_SHARED=ON`；
- 通过 `find_package(SQLite3 3.31 REQUIRED)` 校验 SQLite；
- 构建 `MODULE` library `tsfile_sqlite`；
- 链接同一构建中的 `tsfile` target；
- Linux 使用 `$ORIGIN`，macOS 使用 `@loader_path` 查找同目录 `libtsfile`；
- 不给模块添加 Unix 默认的 `lib` 前缀。

测试 target `TsFile_Sqlite_Test` 动态加载刚构建的扩展，避免测试到系统中其他
版本的插件。

## 4. Virtual table schema

扩展解析重复的 `column=name:TYPE:CATEGORY` 参数，并向 SQLite 声明：

```sql
CREATE TABLE x(
  <public columns>,
  _tsfile_command TEXT HIDDEN,
  _tsfile_cutoff INTEGER HIDDEN
);
```

两个隐藏列只用于把控制命令送入 `xUpdate`，普通 `SELECT *` 不会返回它们。

初始化阶段验证：

- 目录是绝对路径；
- 时间精度属于 `ms/us/ns`；
- 第一列且唯一 TIME 列是 `TIMESTAMP:TIME`；
- 至少一个 `STRING:TAG`；
- 列名非空、大小写不重复且不占用隐藏列名称。

模块调用：

```c
sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);
sqlite3_vtab_config(db, SQLITE_VTAB_CONSTRAINT_SUPPORT, 1);
```

`DIRECTONLY` 限制从 view、trigger 等间接 schema 对象调用虚拟表，减少恶意
数据库文件在连接打开时触发外部文件访问的风险。约束支持标志允许 `xUpdate`
向 SQLite 返回精确的约束错误。

## 5. Shadow tables

假设逻辑表名为 `sensor`，`xCreate` 在逻辑表所属 schema 中创建：

### 5.1 `sensor_data`

```sql
CREATE TABLE sensor_data(
  time INTEGER NOT NULL,
  device TEXT NOT NULL,
  temperature REAL,
  UNIQUE(device, time)
);
```

真实列会根据用户 schema 展开。所有 TAG 加 TIME 形成复合唯一键。它是唯一的
可变数据存储，rowid 由 SQLite 分配。

### 5.2 `sensor_segments`

```sql
CREATE TABLE sensor_segments(
  path TEXT PRIMARY KEY,
  cutoff INTEGER NOT NULL,
  row_count INTEGER NOT NULL
);
```

这是冷段 manifest。`path` 是绝对文件路径，`cutoff` 是生成该段时的新水位，
`row_count` 用于诊断和后续优化。

### 5.3 `sensor_config`

```sql
CREATE TABLE sensor_config(
  id INTEGER PRIMARY KEY CHECK(id = 1),
  watermark INTEGER NOT NULL,
  precision TEXT NOT NULL,
  directory TEXT NOT NULL,
  schema TEXT NOT NULL
);
```

初始 watermark 是 `INT64_MIN`。schema 以列名、TsFile 类型枚举和类别枚举组成
签名。`xConnect` 重新打开逻辑表时验证 precision、directory 和 schema 签名，
不一致返回 `SQLITE_CORRUPT`。

`xShadowName` 将 `_data`、`_segments` 和 `_config` 后缀报告给 SQLite。名称通过
schema 限定和 identifier quoting 生成，因此 attached database 的 shadow
table 不会错误创建到 `main`。

`xDestroy` 删除三个 shadow table，但刻意不删除 TsFile 文件，避免 `DROP TABLE`
隐式执行不可恢复的外部文件删除。

## 6. 类型转换

扩展内部用 `Value` 保存精确类型、NULL 标志、数值或带长度的字节串。

写入方向：

```text
sqlite3_value -> Value -> SQLite hot binding / Tablet value
```

读取方向：

```text
SQLite column / C++ ResultSet -> Value -> sqlite3_result_*
```

TEXT、STRING 和 BLOB 都使用显式指针加长度复制，不把内容当作零结尾 C 字符串，
因此空字符串、嵌入 `\0` 的 BLOB 和 NULL 可以区分。冷读直接使用 C++
`ResultSet`，不依赖当前 C wrapper 的字符串接口。

SQLite 是动态类型系统，扩展在 `xUpdate` 中做严格运行时检查。INT32/DATE 会
额外检查 int32 上下界。BOOLEAN 接受 INTEGER，并归一化为 0 或 1。

## 7. 热区 DML 路径

所有 `INSERT/UPDATE/DELETE` 通过 `xUpdate` 路由：

### INSERT

1. 将 SQLite values 转成 schema 指定的 `Value`；
2. 验证 TIME 和 TAG 非 NULL；
3. 验证 `time >= watermark`；
4. 参数化插入 `_data`；
5. 由 shadow table 的 UNIQUE 约束检查 `(TAG..., TIME)`。

### UPDATE

1. 冷行的合成 rowid 为负，直接返回 `SQLITE_CONSTRAINT`；
2. 不允许改变 SQLite rowid；
3. 校验完整的新行值和新时间；
4. 按正 rowid 更新 `_data`。

### DELETE

- 负 rowid 返回 `SQLITE_CONSTRAINT`；
- 正 rowid 从 `_data` 删除。

因为 DML 最终是同一连接上的 SQLite shadow table DML，所以 journal/WAL、事务
隔离、写锁和回滚仍由 SQLite 提供。

## 8. 查询规划和执行

### 8.1 `xBestIndex`

当前接受以下可用约束：

- TIME 的 `>`、`>=`、`<`、`<=`；
- 使用 BINARY collation 的 TAG `=`。

对于接受的约束，`argvIndex` 被编码进 `idxStr`。`omit` 保持为 0，要求 SQLite
对返回行再次执行原 SQL 条件。这样即使 libtsfile 与 SQLite 在边界、类型或
collation 上存在差异，也不会产生错误结果。

`sqlite3_index_info.colUsed` 用于计算投影列。TIME 和所有下推约束涉及的列会被
强制加入内部投影，以便执行边界检查和 SQLite 残余检查。

MVP 不设置 `orderByConsumed`，也不下推跨冷热来源的 `LIMIT/OFFSET`。

### 8.2 `xFilter`

`xFilter` 解码约束和投影，然后依次调用：

```text
read_hot() -> read_cold() -> HybridCursor.rows
```

当前 cursor 会物化本次查询的所有候选行。`xNext/xEof/xColumn/xRowid` 在该
内存数组上实现 SQLite cursor 接口。这使 MVP 的合并逻辑简单，但不适合无界
大结果集；后续应改造成 hot/cold 流式 cursor 和 k-way merge。

### 8.3 热读取

`read_hot` 动态生成参数化 SQL，只选择投影需要的列，并把接受的 TIME/TAG
约束写入 `_data` 查询。结果按 hot rowid 读取，然后执行一次内部边界和 TAG
字节比较；返回 SQLite 后仍有 SQLite 的最终残余检查。

### 8.4 冷读取

`read_cold` 从 manifest 按 path 遍历每个 TsFile：

1. 为当前文件创建 `TsFileReader`；
2. 将 TAG 等值条件组合成 `TagFilterBuilder` AND filter；
3. 将开闭时间范围转换为 TsFileReader 的闭区间；
4. 只请求投影所需的 value columns；
5. 从 C++ ResultSet 恢复精确类型、NULL 和字节长度；
6. 关闭 ResultSet 和 reader。

当前 manifest 的 cutoff 尚未用于跳过不相交的段；reader 会对每个段应用时间
范围。FIELD 谓词不进入 TsFile reader，由 SQLite 在结果返回后复核。

### 8.5 Rowid

热行直接暴露 SQLite `_data.rowid`，通常为正值。冷行按
`(segment ordinal, row ordinal)` 编码为负 int64。负号同时充当不可变标志，
使 `xUpdate` 可以拒绝冷行修改。

冷 rowid 只对一次遍历有意义，不属于持久存储协议。

## 9. Seal 写路径

控制语句：

```sql
INSERT INTO sensor(_tsfile_command, _tsfile_cutoff)
VALUES ('seal', ?);
```

在 `xUpdate` 中识别命令，cutoff 必须是 SQLite INTEGER 且不能小于当前
watermark。

### 9.1 段生成

`write_segment()` 完成：

1. 确保绝对目录存在；
2. 使用表名、进程 ID 和单调计数器生成不冲突的 `.tmp`/`.tsfile` 路径；
3. 以 `O_CREAT | O_EXCL` 创建临时文件；
4. 从 `_data` 读取 `[watermark, cutoff)`；
5. 按所有 TAG、TIME 排序；
6. 每 1024 行填充一个 table-model `Tablet`；
7. 通过 `TsFileTableWriter` 写入；
8. 写入时间精度 property、flush、footer 并 fsync 文件。

若区间没有行，writer 仍会被正确关闭，随后删除空临时文件。调用方仍更新
watermark，但不插入 manifest 记录。

### 9.2 SQLite 状态变更

非空段写完临时文件后，`seal()` 在当前 SQLite 事务中：

1. 向 `_segments` 插入最终文件路径、cutoff 和 row count；
2. 从 `_data` 删除 `[watermark, cutoff)`；
3. 更新 `_config.watermark`；
4. 把文件记录到当前事务的 `pending` 数组。

这些 shadow table 修改与调用 seal 的外层 SQL 事务相同，不单独提交。

## 10. 事务回调和文件原子性

SQLite 数据库事务不能直接回滚外部文件，因此扩展通过 virtual table transaction
callbacks 协调两者。

```text
xBegin
  |
xUpdate(seal): write .tmp + update transactional shadow state
  |
xSync: validate .tmp -> atomic rename -> fsync directory
  |
SQLite commits database/WAL
  |
xCommit: forget pending state
```

### 10.1 `xSync`

SQLite 准备提交时，扩展对每个 pending 文件执行：

1. 使用独立 `TsFileReader` 重新打开临时文件，验证 footer 可读；
2. 确认最终路径不存在；
3. 在同一目录执行原子 `rename()`；
4. `fsync()` 目录，持久化目录项。

只有全部成功，SQLite 才继续提交数据库事务。

### 10.2 `xRollback`

回滚时删除本事务仍存在的 `.tmp`，也删除已经 rename 但数据库事务未提交的
`.tsfile`，然后从 `_config` 重新加载 watermark。

### 10.3 Savepoint

`xSavepoint` 记录 pending 数组长度。`xRollbackTo` 删除 savepoint 之后创建的
文件并重新加载事务可见的配置，`xRelease` 移除相应标记。

### 10.4 崩溃窗口

| 崩溃位置 | SQLite 恢复结果 | 文件结果与恢复方式 |
| --- | --- | --- |
| 写临时文件过程中 | 热数据和旧 manifest 保留 | 遗留 `.tmp`，下次 seal 清理 |
| 临时文件完成、rename 前 | 热数据和旧 manifest 保留 | 遗留 `.tmp`，下次 seal 清理 |
| rename 后、SQLite COMMIT 前 | 热数据和旧 manifest 保留 | 遗留未引用 `.tsfile`，下次 seal 清理 |
| SQLite COMMIT 后 | 新 manifest、水位和热表删除持久化 | manifest 引用的 `.tsfile` 已完成 rename 和目录 fsync |

`cleanup_orphans()` 在 seal 已取得 SQLite 写事务的上下文中运行。它读取 manifest，
删除独占目录内未被引用且不属于当前 pending 集合的 `.tmp` 和 `.tsfile`。这也是
为什么目录独占不是建议，而是正确性前提。

## 11. 并发模型

- 热 DML、manifest 和配置依赖 SQLite 的连接事务与写锁；
- seal 是同步写操作，不会在后台线程与 SQLite 事务脱离运行；
- 读事务看到由 SQLite 快照决定的 shadow state；
- manifest 只在 SQLite 事务提交后对其他连接可见；
- TsFile 发布使用同目录原子 rename，读取者不会观察到半写文件。

当前设计不提供跨进程独立修改 TsFile 目录的协调机制。目录必须只由拥有 SQLite
manifest 的扩展实例管理。

## 12. 安全和运维边界

- `DIRECTONLY` 限制间接调用，但加载 native extension 本身仍等价于加载本地
  代码，应用应只加载可信二进制；
- 配置要求绝对路径，避免工作目录变化导致数据库重新打开到不同数据集；
- SQL identifier 和字符串均经过 quoting，运行时值使用 prepared statement；
- TsFile 文件权限当前为 `0644`，目录创建权限当前为 `0755`，最终仍受进程
  `umask` 影响；
- `DROP TABLE` 不清理外部文件；
- SQLite backup API 不会自动包含 TsFile，备份协议必须覆盖数据库和目录。

## 13. 测试覆盖

`cpp/test/sqlite/tsfile_sqlite_test.cc` 当前覆盖：

- 热数据 CRUD；
- seal 的 commit 和 rollback；
- watermark 对旧时间写入的约束；
- INT32 越界；
- 冷行 UPDATE/DELETE 拒绝；
- NULL、空 TEXT 和包含零字节的 BLOB 往返；
- attached database 的 shadow table 隔离。

实现开发过程中还应持续补充：

- 多连接 WAL 并发；
- 多次 seal 和多 TAG 查询；
- 独立 TsFileReader 验证 property/schema/row count；
- write/close/fsync/rename/commit 各故障点注入；
- 重启和孤儿文件清理；
- SQLite 3.45.3 主版本与 3.31 最低版本矩阵；
- Linux/macOS CI。

## 14. 当前限制与后续方向

当前 MVP 的主要技术债务：

1. 把查询候选行全部物化到内存，应改为流式 merge cursor；
2. manifest 仅保存 cutoff，尚未记录每段 min/max time、TAG 统计和校验信息；
3. 没有跨段 compaction，段数增加后每次查询都要打开更多文件；
4. 没有 correction/tombstone，无法修改冷历史；
5. 没有后台 seal 调度和限流；
6. 没有冷段删除与 retention policy；
7. schema 演进需要新建逻辑表；
8. 没有一体化在线备份、迁移和目录重定位工具；
9. 尚未实现 Windows 文件同步和原子发布语义。

若继续演进，建议优先处理流式查询、segment pruning、故障注入测试和一致性备份
协议，再考虑自动 seal、compaction 和历史修正。
