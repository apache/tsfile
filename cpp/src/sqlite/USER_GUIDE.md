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

# tsfile_sqlite 用户手册

`tsfile_sqlite` 是一个实验性的 SQLite loadable extension。它提供
`tsfile_hybrid` 虚拟表，让一张逻辑表同时使用两种物理存储：

- 尚未封存的近期数据保存在 SQLite shadow table 中，支持事务和 CRUD；
- 已封存的历史数据保存在不可变的 TsFile 段文件中；
- 应用继续对同一张虚拟表执行 SQL，扩展自动合并冷热数据。

它适合以追加为主、近期数据偶尔需要修正、历史数据可以冻结的时序场景。
它不是 SQLite 通用表的替代存储引擎，也不会自动把已有 SQLite 表转换为
TsFile。

## 1. 环境要求

- Linux 或 macOS；
- SQLite 3.31 或更高版本；
- 支持加载扩展的 SQLite 构建；
- CMake 构建时启用共享版 `libtsfile`；
- TsFile 目录必须使用绝对路径，并由一张逻辑表独占。

当前 MVP 不支持 Windows。

## 2. 构建

在仓库根目录执行：

```bash
cmake -S cpp -B cpp/build/sqlite \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON

cmake --build cpp/build/sqlite --target tsfile_sqlite -j
```

产物位于构建目录的 `lib` 子目录：

- Linux：`cpp/build/sqlite/lib/tsfile_sqlite.so`
- macOS：`cpp/build/sqlite/lib/tsfile_sqlite.dylib`

扩展依赖同一次构建产生的 `libtsfile`。默认 RPATH 会从扩展所在目录寻找
`libtsfile`，部署时建议把二者放在同一目录。

运行扩展测试：

```bash
cmake --build cpp/build/sqlite --target TsFile_Sqlite_Test -j
ctest --test-dir cpp/build/sqlite/test -R TsFileSqliteTest \
  --output-on-failure
```

## 3. 加载扩展

### 3.1 SQLite CLI

```sql
.load /absolute/path/to/tsfile_sqlite
```

SQLite CLI 通常会根据平台自动补全 `.so` 或 `.dylib` 后缀。也可以传入完整
文件名。

### 3.2 C/C++ 应用

```c
sqlite3_enable_load_extension(db, 1);

char *error = NULL;
int rc = sqlite3_load_extension(
    db, "/absolute/path/to/tsfile_sqlite", NULL, &error);

sqlite3_enable_load_extension(db, 0);
```

应用应在打开数据库连接后、访问 hybrid 表之前加载扩展。生产环境建议加载
完成后立即关闭动态扩展加载能力。

## 4. 创建逻辑表

<!-- tag-comment-scope-C-start mode="block" hash="sha256:f3cf7479aed193524e15a16fc86dd8f64529f4d67415b3632a4d1e91dd3d6465" -->
```sql
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  directory='/var/lib/example/sensor',
  timestamp_precision='ms',
  column='time:TIMESTAMP:TIME',
  column='device:STRING:TAG',
  column='region:STRING:TAG',
  column='temperature:DOUBLE:FIELD',
  column='status:STRING:FIELD',
  column='payload:BLOB:FIELD'
);
```
<!-- tag-comment-scope-C-end mode="block" -->
<!-- tag-comment-thread-C
cmd4 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:26:11.739Z","body":"这个语法可以再简洁一些吗？还是说都得这样，逐个column= 来指定？\n"}
cmd10 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:35:29.187Z","body":"回复 cmd4：可以简化，逐个 `column=` 是当前扩展 `parse_args()` 自己定义的格式，并非 SQLite 的强制要求。SQLite 会把 USING 后的各个参数文本交给模块解析，见 [CREATE VIRTUAL TABLE 官方说明](https://www.sqlite.org/lang_createvtab.html)。\n\n建议下一版支持更接近普通建表的写法，例如 `USING tsfile_hybrid(time TIMESTAMP TIME, device STRING TAG, temperature DOUBLE FIELD, directory='...', timestamp_precision='ms')`，并可让 FIELD 成为省略类别时的默认值。这里是语法提案，当前分支还不能执行；现有示例准确反映当前实现，所以本次保留正文，先回复这个设计问题。"}
cmd19 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:39:59.171Z","body":"按照这种来修改吧，修改正文。 这个userguide 就当我们的功能设计手册。 "}
-->

模块参数如下：

| 参数 | 要求 |
| --- | --- |
| `directory` | 必填、绝对路径、由当前逻辑表独占 |
| `timestamp_precision` | 必填，只能是 `ms`、`us` 或 `ns` |
| `column` | 可重复，格式为 `名称:类型:类别` |

<!-- tag-comment-scope-B-start mode="block" hash="sha256:46aac11794b9cf17d2e4e7e584bfbb5a3f42ee2a10015dbe7f719d50c91f3f49" -->
列定义必须满足：

- 恰好一个 `TIME` 列；
- `TIME` 必须是第一列，类型必须为 `TIMESTAMP`；
- 至少一个 `TAG` 列；
- `TAG` 类型必须为 `STRING`，值不能为 `NULL`；
- `FIELD` 可以为 `NULL`；
- 列名大小写不敏感地唯一；
- `_tsfile_command` 和 `_tsfile_cutoff` 是保留名称。
<!-- tag-comment-scope-B-end mode="block" -->
<!-- tag-comment-thread-B
cmd2 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:25:21.571Z","body":"可以没有tag 列，列名是否唯一的问题， 在tsfile 的层次上是大小不敏感的， 但是在sqlite 里面我不清楚 \n\n"}
cmd3 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:25:44.770Z","body":"这里的 _tsfile_command 与 _tsfile_cutoff 是什么呢？"}
cmd12 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:35:55.995Z","body":"回复 cmd2：这里应区分 TsFile 数据模型的能力与当前 SQLite 扩展的限制。支持零 TAG 的目标合理：这种表可以视为只有一条设备序列，唯一键退化为 TIME。但当前 `parse_args()` 明确拒绝空 TAG 列表，所以现在还不能把手册改成“已支持无 TAG”。实现上也不能只删这条检查：`create_shadow_tables()` 在追加 TIME 前固定输出逗号，无 TAG 时会生成非法的 `UNIQUE(,time)`；需要一起调整，并验证无 TAG 的写入、封存、读取和时间唯一性。\n\n列名方面，我用本机 SQLite 3.51.0 实测：`CREATE TABLE t(Foo INT, foo INT)` 报 `duplicate column name: foo`，给两个名字加双引号也一样。因此至少 ASCII 大小写不同不能用来区分列名，当前文档这条约束应保留；这不意味着已验证任意 Unicode 字符的大小写折叠规则。本次先记录实现差距，不把尚未实现的能力写成现状。"}
cmd13 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:36:09.840Z","body":"回复 cmd3：这两个是扩展声明的 HIDDEN 控制列，不是用户的时间序列字段，也不会写入 TsFile 的业务 schema。\n\n- `_tsfile_command`：操作名，目前只识别 `seal`。\n- `_tsfile_cutoff`：该操作的整数时间上界，单位与 `timestamp_precision` 相同；封存范围是 `[旧 watermark, cutoff)`，等于 cutoff 的行仍可修改。\n\n例如 `INSERT INTO sensor(_tsfile_command, _tsfile_cutoff) VALUES ('seal', 1700086400000)` 会进入 `xUpdate` 的命令分支，不会插入一条普通数据行。`SELECT *` 不显示这两个列；可用 `PRAGMA table_xinfo(sensor)` 查看其 hidden 标记。参见 [SQLite table_xinfo](https://www.sqlite.org/pragma.html#pragma_table_xinfo)。"}
cmd23 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:58:58.023Z","body":"我们更新一下文档吧。 就支持不带tag。以及封口的机制看后面的描述。 "}
-->

所有 TAG 列与 TIME 列共同组成唯一键。例如上表的唯一键是：

```text
(device, region, time)
```

### 4.1 数据类型映射

| TsFile 类型 | SQLite 表现 | 写入要求 |
| --- | --- | --- |
| `BOOLEAN` | INTEGER | 必须传 SQLite INTEGER；0 为假，非 0 为真 |
| `INT32` | INTEGER | 必须在 int32 范围内 |
| `INT64` | INTEGER | SQLite int64 |
| `FLOAT` | REAL | INTEGER 或 REAL |
| `DOUBLE` | REAL | INTEGER 或 REAL |
| `TEXT` | TEXT | SQLite TEXT |
| `STRING` | TEXT | SQLite TEXT |
| `BLOB` | BLOB | SQLite BLOB，保留长度和二进制零字节 |
| `DATE` | INTEGER | 必须在 int32 范围内 |
| `TIMESTAMP` | INTEGER | SQLite int64 |

扩展不会换算时间戳。`timestamp_precision` 仅声明整数时间戳的单位，并写入
非空 TsFile 段的 `tsfile_sqlite.timestamp_precision` property。

### 4.2 创建后的固定配置

schema、目录和时间精度会记录在配置 shadow table 中。数据库重新打开时，
扩展会验证这些信息是否与 `CREATE VIRTUAL TABLE` 中保存的参数一致。

当前版本不支持修改 schema、目录或时间精度。需要变更时，应创建一张新的
逻辑表并迁移数据。

## 5. 写入和修改热数据

<!-- tag-comment-scope-E-start mode="block" hash="sha256:32362269b36a7c24879ad5debdd05cca2d3415f63029faff299d431e0af79f4f" -->
普通 DML 的用法与 SQLite 表一致：

```sql
INSERT INTO sensor(time, device, region, temperature, status)
VALUES (1700000000000, 'device-1', 'cn-east', 21.5, 'ok');

UPDATE sensor
SET temperature = 22.0
WHERE device = 'device-1'
  AND region = 'cn-east'
  AND time = 1700000000000;

DELETE FROM sensor
WHERE device = 'device-1'
  AND region = 'cn-east'
  AND time = 1700000000000;
```

热数据实际写入 `<虚拟表名>_data` shadow table，因此自动使用 SQLite 的
rollback journal/WAL、锁、唯一约束、事务和 savepoint。

```sql
BEGIN;

INSERT INTO sensor(time, device, region, temperature)
VALUES (1700000001000, 'device-1', 'cn-east', 22.1);

UPDATE sensor
SET status = 'checked'
WHERE device = 'device-1' AND time = 1700000001000;

COMMIT;
```
<!-- tag-comment-scope-E-end mode="block" -->
<!-- tag-comment-thread-E
cmd6 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:28:30.003Z","body":"这部分应该是针对shadow 表的，但是其余的表是不是就不行了？怎么区分呢？或者用户该怎么处理知道自己的操作的作用范围呢？"}
cmd15 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:36:40.742Z","body":"回复 cmd6：示例的操作入口是用户创建的虚拟表 `sensor`，用户不应直接对 shadow 表发 DML。可以按三类理解作用范围：\n\n1. 普通 SQLite 表：继续使用 SQLite 原有行为，加载扩展不会接管这些表。\n2. `USING tsfile_hybrid` 创建的表：SELECT 查询该逻辑表的冷热数据；INSERT 只能写 `time >= watermark`；UPDATE/DELETE 只能修改命中的热行，命中冷行会返回约束错误，不会自动忽略冷行。\n3. `sensor_data`、`sensor_segments`、`sensor_config`：扩展维护的内部表，用户只做诊断读取。\n\n用户应始终通过逻辑表操作，并用业务 WHERE 条件限定对象。若只想修改热数据，可在业务条件之外增加 `time >= (SELECT watermark FROM sensor_config WHERE id=1)`。每张 hybrid 表都有自己的 watermark 和目录，其他 hybrid 表不会因此一起封存或修改。针对整个 schema 的区分，SQLite 3.37+ 的 `PRAGMA main.table_list` 提供 table/virtual/shadow 类型；兼容最低版本时可查看 `main.sqlite_master` 中的建表 SQL。参见 [SQLite table_list](https://www.sqlite.org/pragma.html#pragma_table_list)。"}
cmd24 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:59:22.105Z","body":"我的意思是， 用户如果操作一些数据的话， 那他就必须得知道他可操作的数据范围？如果数据被写下去了， 封口成tsfile ，就不能再处理了。 所以每次更新都得做一下检查之类的？ 当然我们持续追加写倒是没啥问题。 "}
-->

## 6. 查询冷热数据

无论数据位于 SQLite 还是 TsFile，都查询同一张虚拟表：

```sql
SELECT time, device, temperature, status
FROM sensor
WHERE device = 'device-1'
  AND time >= 1700000000000
  AND time < 1700086400000
ORDER BY time;
```

常规 SQLite SQL 仍然可用，包括 FIELD 条件、表达式、聚合、排序和分页：

```sql
SELECT device, avg(temperature)
FROM sensor
WHERE time >= 1700000000000
  AND temperature IS NOT NULL
GROUP BY device;
```

为了得到更好的 TsFile 扫描效率，查询应尽量包含：

- 整数时间范围；
- 使用 `BINARY` collation 的 TAG 等值条件；
- 只选择需要的列。

扩展会把这些条件和投影下推到冷热读取路径。FIELD 条件、排序、聚合、
`LIMIT/OFFSET` 由 SQLite 在合并结果上处理。扩展不会声称原始输出已经满足
`ORDER BY`，因此需要稳定顺序时必须显式写出 `ORDER BY`。

所有已下推的约束仍由 SQLite 二次检查，以保证 SQL 结果正确。

## 7. 封存历史数据

使用两个隐藏列发送 `seal` 命令：

```sql
INSERT INTO sensor(_tsfile_command, _tsfile_cutoff)
VALUES ('seal', 1700086400000);
```

`cutoff` 是不包含的上界。上述操作封存：

```text
旧 watermark <= time < 1700086400000
```

其中 `time == 1700086400000` 的行仍在热区，可以继续修改。

一次非空 seal 会：

1. 按全部 TAG、TIME 排序读取待封存热数据；
2. 使用 Tablet 批量写入临时 TsFile；
3. 校验并将临时文件原子改名为 `.tsfile`；
4. 在 manifest 中登记文件；
5. 从热表删除已封存行；
6. 将 watermark 推进到 cutoff。

如果区间内没有数据，不生成 TsFile，但仍会推进 watermark。cutoff 不能小于
当前 watermark。

seal 是同步写操作，在完成期间会占用 SQLite 写事务。可以显式把它放入事务：

<!-- tag-comment-scope-F-start mode="block" hash="sha256:540dace662a28daed4b54340806373729d21db411caf166c62a92f3b3d55b425" -->
```sql
BEGIN IMMEDIATE;

INSERT INTO sensor(_tsfile_command, _tsfile_cutoff)
VALUES ('seal', 1700086400000);

COMMIT;
```
<!-- tag-comment-scope-F-end mode="block" -->
<!-- tag-comment-thread-F
cmd7 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:29:53.389Z","body":"有没有更优雅的指令方法呢？"}
cmd16 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:36:55.665Z","body":"回复 cmd7：可以提供更直观的入口。我建议面向用户增加类似 `SELECT tsfile_seal('main.sensor', 1700086400000);` 的管理函数，让它在内部执行现有隐藏列 INSERT，继续走 virtual table 的事务回调；隐藏列方式可保留为底层兼容接口。这只是提案，当前扩展还没有注册这个函数，不能直接执行。\n\n管理函数需要限定为直接调用、校验目标确实是 tsfile_hybrid 表，并验证自动提交、显式事务和 savepoint 下的行为。暂不建议引入 `SEAL TABLE ...` 新 SQL 语法，因为这会涉及 SQLite parser 的修改。现有示例中的 `BEGIN IMMEDIATE/COMMIT` 也是可选事务包装；单条 seal INSERT 本身已经可以在自动提交模式下执行。"}
cmd21 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:44:58.785Z","body":"我建议按照这个来， 使用一个udf 来显式的处理。 而不是向所谓的列里插入数据。 "}
-->

如果事务回滚，manifest、watermark 和热数据删除都会回滚，扩展也会删除本次
事务产生的临时文件或已经改名的段文件。

## 8. Watermark 和冷数据不可变性

watermark 把逻辑时间轴分成两部分：

```text
time < watermark     冷区：不可修改
time >= watermark    热区：允许 INSERT/UPDATE/DELETE
```

以下操作会返回约束错误：

- 插入 `time < watermark` 的行；
- 把热行的时间更新到 watermark 之前；
- 更新或删除已经位于 TsFile 的冷行；
- 写入重复的 `(所有 TAG, TIME)` 唯一键。

当前版本没有 correction 或 tombstone。如果业务必须修正历史数据，需要重建
逻辑表或在业务层保留单独的修正数据。

## 9. 内部状态与诊断

每张名为 `sensor` 的 hybrid 表拥有三个 SQLite shadow table：

| 表 | 内容 |
| --- | --- |
| `sensor_data` | 可变热数据 |
| `sensor_segments` | TsFile 路径、cutoff 和行数 manifest |
| `sensor_config` | watermark、精度、目录和 schema 签名 |

可以只读查看它们进行诊断：

```sql
SELECT watermark, precision, directory FROM sensor_config;

SELECT path, cutoff, row_count
FROM sensor_segments
ORDER BY cutoff;

SELECT count(*) AS hot_rows FROM sensor_data;
```

<!-- tag-comment-scope-G-start mode="block" hash="sha256:42fc02103e60aa6f6a98ce6001a2caa190ed56a90eac5d4c20e316aed12f197a" -->
不要直接修改 shadow table。绕过虚拟表写入会破坏 watermark、manifest 和文件
之间的一致性。
<!-- tag-comment-scope-G-end mode="block" -->
<!-- tag-comment-thread-G
cmd8 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:31:55.454Z","body":"这个用户可以直观地查看到这张表吗？ 感觉这个名字还是可能会和用户的名字撞车的。"}
cmd17 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:37:10.407Z","body":"回复 cmd8：可以看到。Shadow 表不是隐藏文件，也不是独立命名空间；它们是 SQLite schema 内的真实表。用户可查询 `SELECT name, sql FROM main.sqlite_master WHERE name IN ('sensor_data','sensor_segments','sensor_config');`，SQLite 3.37+ 还可用 `PRAGMA main.table_list` 查看 shadow 类型。\n\n撞名风险确实存在。如果同一 schema 已有用户表 `sensor_data`，当前创建 `sensor` 时执行普通 `CREATE TABLE sensor_data ...` 会失败，不会自动覆盖或复用它。`xShadowName` 负责声明内部表身份，并不能消除命名冲突。可改进为：创建前检查三个派生名，返回明确的冲突对象；采用更有辨识度的内部后缀；在用户工具中把 shadow 表单独显示。这些是待做的改进。\n\n另一个边界是：单靠 xShadowName 不保证禁止直接写入；SQLite 在连接启用 `SQLITE_DBCONFIG_DEFENSIVE` 时才限制普通 SQL 写 shadow 表。当前扩展没有替宿主启用该设置。参见 [SQLite shadow table 机制](https://www.sqlite.org/vtab.html#the_xshadowname_method)。"}
cmd25 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:59:52.187Z","body":"我觉得这个名字还得再内部一点，加一些符号，_xxx_xxx。"}
-->

热行使用 SQLite 的正 rowid；冷行使用扩展生成的负 rowid。冷 rowid 是内部
实现标识，不应作为跨查询或跨版本稳定的业务主键。

## 10. 数据目录和生命周期

每张逻辑表必须使用独占目录。扩展会在 seal 前清理目录中未被 manifest 引用的
`.tmp` 和 `.tsfile` 文件，用来恢复进程崩溃后遗留的孤儿文件。因此不要把手工
创建的 TsFile、其他表的段文件或任何同后缀文件放进该目录。

执行：

```sql
DROP TABLE sensor;
```

会删除虚拟表及其三个 shadow table，但不会删除已经导出的 TsFile。删除或归档
这些文件需要由运维流程显式完成。

<!-- tag-comment-scope-H-start mode="block" hash="sha256:ce1291072b0568bf2bce6f718ae12f960bce613e928d1de8dae206b73666c519" -->
## 11. 备份与恢复

完整数据由两部分共同组成：

1. SQLite 数据库文件及其 WAL/journal；
2. manifest 引用的 TsFile 目录。

只备份 SQLite 文件会丢失冷数据，只备份 TsFile 目录会丢失热数据和 manifest。
备份工具必须同时捕获两部分的一致视图。MVP 尚未提供在线快照命令，推荐在停止
写入后备份，或者在应用层协调 SQLite checkpoint/事务与目录快照。

恢复时必须保持配置中记录的绝对目录可用；如果恢复到不同路径，应创建新的逻辑
表并执行受控迁移，而不是手工修改 `sensor_config`。
<!-- tag-comment-scope-H-end mode="block" -->
<!-- tag-comment-thread-H
cmd9 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:33:44.758Z","body":"这里应该给出导出命令， 用户如果直接拷贝或者移动走了tsfile ， 那应该也得识别出来？"}
cmd18 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:37:35.440Z","body":"回复 cmd9：这里确实缺少面向用户的完整导出/恢复入口，应该补产品能力。当前分支只有 seal，没有统一的 export、restore 或 verify 命令；不能把尚不存在的命令写成可执行用法。导出也应区分“只导出历史 TsFile”与“完整备份逻辑表”：后者必须包含热数据、manifest、配置和引用的段文件。\n\n对直接文件操作，目前行为是：\n\n- 复制到管理目录之外且保留原文件：原库继续使用原路径，扩展不会感知多了一份副本，也无需自动改变 manifest；这份副本本身不是完整备份。\n- 移走或删除 manifest 引用的文件：`read_cold()` 尝试打开它时返回 `SQLITE_IOERR`，不会静默跳过；若整个目录消失，重新连接虚拟表时会报 `SQLITE_CANTOPEN`。当前没有持续监控、自动定位新路径或详细的缺失文件诊断。\n- 把文件复制进受管理目录：不会自动导入；未登记的 `.tsfile` 可能在下一次 seal 的孤儿清理中被删除。文件路径也不等于文件身份，当前没有完善的校验机制来识别同路径下被替换的另一个有效 TsFile。\n\n建议提供导出、恢复和校验三个明确入口：导出在一致快照下生成 SQLite 备份与段文件包，附带 schema、精度、文件清单和校验信息；恢复负责验证并重建路径映射；校验返回缺失、损坏、被替换或未登记文件的具体路径。移动/迁移应通过受控命令更新 manifest，而不是看到目录变化就自动接受文件。命令语法需要和前面的 seal 管理接口一起确定。本次先把能力缺口和现有检测行为回复清楚，没有修改实现或虚构已支持的导出命令。"}
cmd22 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:57:28.150Z","body":"我建议在这里给出这里的定义， 就是导出、导入的机制。 我们做一下设计。 "}
-->

## 12. 常见问题

### 加载时报 `not authorized` 或扩展加载被禁用

确认 SQLite 构建允许 loadable extension，并在应用连接上调用
`sqlite3_enable_load_extension()`。CLI 使用 `.load` 即可。

### 加载时报找不到 `libtsfile`

把当前构建对应的 `libtsfile` 与 `tsfile_sqlite` 放到同一目录，避免混用不同
版本的库。必要时检查平台动态链接器的搜索路径。

### 创建表时报目录错误

`directory` 必须是绝对路径，父目录必须可创建或可写。连接已有数据库时，该
目录必须仍然存在。

### seal 后无法更新某些行

这是 watermark 的预期行为。任何 `time < watermark` 的数据已经进入不可变
冷区。

### 查询没有固定顺序

虚拟表会合并多个来源，但不承诺自然顺序。需要顺序时使用显式 `ORDER BY`。

## 13. MVP 限制

- seal 只能显式、同步执行；
- 冷数据不可更新或删除；
- 不支持后台自动封存、compaction 和冷段删除；
- 不支持原地 schema 演进；
- 查询游标当前会在内存中汇集冷热结果；
- 不跨冷热来源下推排序和 `LIMIT/OFFSET`；
- Windows 尚未支持；
- 这仍是实验性扩展，接口和内部格式可能继续演进。
