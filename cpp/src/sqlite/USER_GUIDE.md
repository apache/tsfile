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

<!-- tag-comment-scope-C-start mode="block" hash="sha256:13df5ef0379347db120798fac52653ced0ff7057593c462093d83174d5012841" -->
以下按目标功能设计定义建表语法；示例表达待实现接口，不代表当前原型已支持。
列定义采用 `列名 类型 [类别]`，省略类别时默认为 `FIELD`；`TIME` 和 `TAG` 显式声明。

```sql
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  region STRING TAG,
  temperature DOUBLE,
  status STRING,
  payload BLOB,
  directory='/var/lib/example/sensor',
  timestamp_precision='ms'
);
```

列定义按书写顺序组成 schema，表级选项使用 `key=value`。未知选项、重复的表级
选项和不合法的列定义在建表时返回明确错误。标识符支持双引号转义，例如
`"sensor value" DOUBLE`；字符串选项使用单引号。
<!-- tag-comment-scope-C-end mode="block" -->
<!-- tag-comment-thread-C
cmd4 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:26:11.739Z","body":"这个语法可以再简洁一些吗？还是说都得这样，逐个column= 来指定？\n"}
cmd10 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:35:29.187Z","body":"回复 cmd4：可以简化，逐个 `column=` 是当前扩展 `parse_args()` 自己定义的格式，并非 SQLite 的强制要求。SQLite 会把 USING 后的各个参数文本交给模块解析，见 [CREATE VIRTUAL TABLE 官方说明](https://www.sqlite.org/lang_createvtab.html)。\n\n建议下一版支持更接近普通建表的写法，例如 `USING tsfile_hybrid(time TIMESTAMP TIME, device STRING TAG, temperature DOUBLE FIELD, directory='...', timestamp_precision='ms')`，并可让 FIELD 成为省略类别时的默认值。这里是语法提案，当前分支还不能执行；现有示例准确反映当前实现，所以本次保留正文，先回复这个设计问题。"}
cmd19 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:39:59.171Z","body":"按照这种来修改吧，修改正文。 这个userguide 就当我们的功能设计手册。 "}
cmd27 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:01:58.610Z","body":"回复 cmd19：已按你确定的方向修改这段正文，把它作为目标功能设计：采用 `列名 类型 [类别]`，FIELD 可以省略，TIME/TAG 显式声明，表级配置继续用 key=value。并补上重复选项、未知选项和标识符转义的规则。后续讨论以期望的产品行为为准，当前原型的能力只用于识别实现差距。本轮按评论逐段更新，不会再用“尚未实现”阻止设计进入正文。"}
cmd33 {"author":"colin","kind":"human","createdAt":"2026-09-09T04:08:01.365Z","body":"这里需要强制说明FIELD 列吧。 "}
-->

模块参数如下：

| 参数 | 要求 |
| --- | --- |
| `directory` | 必填、绝对路径、由当前逻辑表独占 |
| `timestamp_precision` | 必填，只能是 `ms`、`us` 或 `ns` |
| `column` | 可重复，格式为 `名称:类型:类别` |

<!-- tag-comment-scope-B-start mode="block" hash="sha256:fa3f346ce3e0c13306fc53f37df980f7f7ed55fcafcb46ffa72c746e17a264ac" -->
列定义的目标规则如下：

- 恰好一个 `TIME` 列，必须是第一列，类型为 `TIMESTAMP`，值不能为 `NULL`；
- `TAG` 列可以有零个或多个；存在时类型必须为 `STRING`，值不能为 `NULL`；
- 有 TAG 时，全部 TAG 与 TIME 共同组成唯一键；无 TAG 时，TIME 单独组成唯一键；
- 不声明类别的列默认为 `FIELD`，FIELD 值可以为 `NULL`；
- 列名不能仅靠 ASCII 大小写区分，例如 `Temperature` 和 `temperature` 视为重名；
- 封存通过第 7 节的管理 UDF 发起，业务 schema 不需要声明或操作
  `_tsfile_command`、`_tsfile_cutoff` 控制列。

无 TAG 表可按以下方式定义；每个时间戳最多对应一行：

```sql
CREATE VIRTUAL TABLE readings USING tsfile_hybrid(
  time TIMESTAMP TIME,
  value DOUBLE,
  directory='/var/lib/example/readings',
  timestamp_precision='ms'
);
```
<!-- tag-comment-scope-B-end mode="block" -->
<!-- tag-comment-thread-B
cmd2 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:25:21.571Z","body":"可以没有tag 列，列名是否唯一的问题， 在tsfile 的层次上是大小不敏感的， 但是在sqlite 里面我不清楚 \n\n"}
cmd3 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:25:44.770Z","body":"这里的 _tsfile_command 与 _tsfile_cutoff 是什么呢？"}
cmd12 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:35:55.995Z","body":"回复 cmd2：这里应区分 TsFile 数据模型的能力与当前 SQLite 扩展的限制。支持零 TAG 的目标合理：这种表可以视为只有一条设备序列，唯一键退化为 TIME。但当前 `parse_args()` 明确拒绝空 TAG 列表，所以现在还不能把手册改成“已支持无 TAG”。实现上也不能只删这条检查：`create_shadow_tables()` 在追加 TIME 前固定输出逗号，无 TAG 时会生成非法的 `UNIQUE(,time)`；需要一起调整，并验证无 TAG 的写入、封存、读取和时间唯一性。\n\n列名方面，我用本机 SQLite 3.51.0 实测：`CREATE TABLE t(Foo INT, foo INT)` 报 `duplicate column name: foo`，给两个名字加双引号也一样。因此至少 ASCII 大小写不同不能用来区分列名，当前文档这条约束应保留；这不意味着已验证任意 Unicode 字符的大小写折叠规则。本次先记录实现差距，不把尚未实现的能力写成现状。"}
cmd13 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:36:09.840Z","body":"回复 cmd3：这两个是扩展声明的 HIDDEN 控制列，不是用户的时间序列字段，也不会写入 TsFile 的业务 schema。\n\n- `_tsfile_command`：操作名，目前只识别 `seal`。\n- `_tsfile_cutoff`：该操作的整数时间上界，单位与 `timestamp_precision` 相同；封存范围是 `[旧 watermark, cutoff)`，等于 cutoff 的行仍可修改。\n\n例如 `INSERT INTO sensor(_tsfile_command, _tsfile_cutoff) VALUES ('seal', 1700086400000)` 会进入 `xUpdate` 的命令分支，不会插入一条普通数据行。`SELECT *` 不显示这两个列；可用 `PRAGMA table_xinfo(sensor)` 查看其 hidden 标记。参见 [SQLite table_xinfo](https://www.sqlite.org/pragma.html#pragma_table_xinfo)。"}
cmd23 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:58:58.023Z","body":"我们更新一下文档吧。 就支持不带tag。以及封口的机制看后面的描述。 "}
cmd28 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:02:23.023Z","body":"回复 cmd23：已更新正文，明确支持零 TAG：有 TAG 时唯一键为 `(TAG..., TIME)`，无 TAG 时唯一键为 TIME，同一时间戳不能写入多行。封存统一引用后面确定的管理 UDF，用户不需要再接触那两个控制列。列名继续禁止仅靠 ASCII 大小写区分；非 ASCII 名称的归一化规则需要实现时与 TsFile、SQLite 两端一起验证，不扩大成未经定义的 Unicode 折叠。"}
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
cmd29 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:02:47.981Z","body":"回复 cmd24：是的，封存会改变数据的可修改范围，但不应要求用户每次 UPDATE 前手动查一遍。建议把“检查范围、保证并发一致性、给出清楚错误”作为扩展的职责：\n\n1. 默认严格更新：UPDATE/DELETE 命中任意冷行，就让整条语句失败并撤销本语句已经做的修改，不能出现前面热行已改、后面遇到冷行才失败的部分成功。INSERT 或 UPDATE 后的新时间必须满足 `time >= watermark`。之前已完成的其他语句是否保留，遵循外层事务的回滚选择。\n2. 更新和封存使用同一 SQLite 写事务协调；取得写权限后重新读取有效 watermark，再检查和修改。用户提前查到的范围只能用于提示，不能作为后续写操作的授权凭据，避免“查完后另一连接封存”的竞态。\n3. 提供只读 `tsfile_table_info('main.sensor')` 表值函数，返回 watermark、precision 等状态。用户明确要“只修改仍可变的数据”时，可把该状态条件写进同一条 UPDATE 的 WHERE 中；默认 UPDATE 不自动过滤冷行。\n4. 报错应包含逻辑表名、watermark、时间单位以及冲突时间或键，并使用可识别的约束错误。正常追加不要求额外调用检查接口，但迟到数据只要落到 watermark 之前也要拒绝。\n\n另一种方案是静默跳过冷行，操作更省事却容易让用户误以为全部更新成功；再一种是给冷数据增加 correction 层，会明显扩大本期范围。我建议先采用上述“严格失败 + 可查状态 + 显式限定热区”的设计。业务上，调用 seal 就表示放弃 cutoff 之前的修改权，cutoff 应覆盖业务允许的迟到和修正窗口。这条先回复设计取舍，待你确认后再整理成 DML 正文规则。"}
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

<!-- tag-comment-scope-F-start mode="block" hash="sha256:1f446daecdb2f477ab745bbc3f467efc90ba0765d82b29270e73c9b3e933b494" -->
目标设计使用显式管理 UDF 封存数据：

```sql
SELECT tsfile_seal('main.sensor', 1700086400000);
```

`tsfile_seal(table_name, cutoff)` 的接口契约：

- `table_name` 指定一张 hybrid 逻辑表，示例中为 main schema 下的 sensor；
- `cutoff` 必须为整数，单位与该表的 timestamp_precision 相同；
- 封存 `[当前 watermark, cutoff)`，等于 cutoff 的行继续留在热区；
- cutoff 小于当前 watermark 时返回约束错误，等于 watermark 时返回 0；
- 成功返回本次封存的行数。空区间返回 0，但 cutoff 更大时仍推进 watermark；
- 取得写权限后重新读取 watermark。封存与普通写入使用同一事务协调；
- 自动提交模式下，独立语句完成时提交；显式事务内不擅自提交外层事务，
  返回行数仅表示本事务已执行封存，最终持久化取决于外层 COMMIT；
- 失败时撤销本次调用的元数据、热数据删除和待发布文件，不能部分封存。

也可以由调用方显式控制事务：

```sql
BEGIN IMMEDIATE;
SELECT tsfile_seal('main.sensor', 1700086400000);
COMMIT;
```

该 UDF 属于有副作用的管理操作，规定以独立顶层 `SELECT` 调用，不支持放入
逐行查询、视图、触发器或其他 schema 表达式。注册时不标记为 deterministic，
并限制间接调用。业务接口不再要求向隐藏控制列插入数据。
<!-- tag-comment-scope-F-end mode="block" -->
<!-- tag-comment-thread-F
cmd7 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:29:53.389Z","body":"有没有更优雅的指令方法呢？"}
cmd16 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:36:55.665Z","body":"回复 cmd7：可以提供更直观的入口。我建议面向用户增加类似 `SELECT tsfile_seal('main.sensor', 1700086400000);` 的管理函数，让它在内部执行现有隐藏列 INSERT，继续走 virtual table 的事务回调；隐藏列方式可保留为底层兼容接口。这只是提案，当前扩展还没有注册这个函数，不能直接执行。\n\n管理函数需要限定为直接调用、校验目标确实是 tsfile_hybrid 表，并验证自动提交、显式事务和 savepoint 下的行为。暂不建议引入 `SEAL TABLE ...` 新 SQL 语法，因为这会涉及 SQLite parser 的修改。现有示例中的 `BEGIN IMMEDIATE/COMMIT` 也是可选事务包装；单条 seal INSERT 本身已经可以在自动提交模式下执行。"}
cmd21 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:44:58.785Z","body":"我建议按照这个来， 使用一个udf 来显式的处理。 而不是向所谓的列里插入数据。 "}
cmd30 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:03:14.872Z","body":"回复 cmd21：已将这段正文改成 `SELECT tsfile_seal('main.sensor', cutoff)`，并定义参数、半开区间、返回行数、空区间、重复 cutoff 和错误行为。UDF 必须加入调用方事务，不能自行提前提交；它成功返回也不等于外层事务已经提交。用户接口不再暴露“向控制列 INSERT”的做法。内部如何接入虚拟表事务回调留在技术实现层，功能设计只约束原子性和可见行为。"}
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
cmd26 {"author":"colin","kind":"human","createdAt":"2026-09-09T04:00:17.054Z","body":"我觉得这个名字还得再内部一点，加一些符号，_xxx_xxx，或者带上 _xx_xx_tsfile_xx 关于tsfile 的相关标记。 "}
cmd31 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:03:34.191Z","body":"回复 cmd26：同意，名字应明确带有 TsFile 内部对象标记。建议采用 `\"\u003c逻辑表名>_tsfile$\u003c用途>\"`，例如 `\"sensor_tsfile$hot\"`、`\"sensor_tsfile$segments\"`、`\"sensor_tsfile$config\"`，统一通过双引号引用。这样普通列表里也能一眼看出是 sensor 所属的内部表。\n\n这里有一个命名约束：SQLite 原生 xShadowName 按“最后一个下划线之前是虚拟表名、之后是用途后缀”识别 shadow 表。直接改为 `_tsfile_sensor_data` 或 `sensor__tsfile_data`，会破坏它与 sensor 的默认关联。把内部标记和用途放在同一个后缀里，并用 `$` 分隔，可以同时保留关联和明显的内部标记。参见 [SQLite xShadowName](https://www.sqlite.org/vtab.html#the_xshadowname_method)。\n\n若希望名字更朴素，也可以用 `sensor_tsfiledata` 等无符号后缀；我倾向前一组。无论哪种命名，建表时都必须预检同名对象并明确拒绝，不能覆盖或误认用户表。用户日常查询状态应通过公开的只读状态接口，内部名称不作为业务 API。本条先回复具体命名方案和约束，正文中的实际名字待确定后统一替换。"}
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

<!-- tag-comment-scope-H-start mode="block" hash="sha256:19c5dab067d18d5f839c3673e8c7bbb9789ed4d80193f3186f48bf1ea2cc1d7e" -->
## 11. 导出、导入与文件校验（功能设计草案）

本节定义目标接口，供设计评审；导出以一张 hybrid 逻辑表为单位，包含该表完整的
冷热数据，不包含数据库中的其他业务表。导入第一版只创建新表，不合并或覆盖已有表。
本节的受控文件管理规则是目标行为，现有原型的目录清理方式需要随之调整。

### 11.1 导出接口和内容

```sql
SELECT tsfile_export('main.sensor', '/backup/sensor-snapshot-001');
```

`tsfile_export(table_name, output_directory)` 导出逻辑表快照，成功返回快照 ID。
目标目录必须为绝对路径且尚不存在；拒绝写入源表管理目录或其子目录。
导出不会推进 watermark，也不会为了生成快照而隐式封存热数据。

导出包采用目录格式：

```text
sensor-snapshot-001/
  manifest.json
  hot.sqlite
  segments/
    <segment-id>.tsfile
```

- `manifest.json`：包格式版本、快照 ID、来源表标识、逻辑 schema、精度、watermark、
  热数据行数，以及各文件的相对路径、大小和 SHA-256；段记录额外保存段 ID、行数、
  时间范围及文件内部的 TsFile 表名。
- `hot.sqlite`：同一快照中的可变数据，按逻辑列和唯一键保存，不暴露源库内部表名。
- `segments/`：该快照引用的不可变 TsFile 副本，保留原文件内容；包不依赖源目录绝对路径。

第一版采用阻止源库并发写入的保守导出方式：取得 SQLite 写保留锁，在同一事务视图内
读取配置、热数据和段清单，复制并校验所有文件后释放锁。导出时间较长时会阻塞该
SQLite 数据库的其他写入，不宣称提供无阻塞在线快照。

先写入目标旁的专用暂存目录，完成文件同步、清单校验后再原子发布整个目录，并同步
父目录。任一步失败不返回快照 ID，不留下看似完整的目标包，源数据保持不变。
导出只允许作为独立管理语句执行，调用前不能存在用户显式事务，避免把未提交数据
导出为可独立使用的包；成功返回时包已发布。

### 11.2 导入接口和冲突规则

```sql
SELECT tsfile_import(
  '/backup/sensor-snapshot-001',
  'main.sensor_restored',
  '/var/lib/example/sensor-restored'
);
```

`tsfile_import(package_directory, target_table, target_directory)` 成功返回新逻辑表
的名称。目标表及其内部对象必须不存在，目标目录必须尚不存在；不覆盖、合并或
自动删除现有对象。包目录与目标目录必须彼此独立，不允许互为父子目录。

导入应执行以下检查与发布流程：

1. 验证包版本、清单完整性、schema、时间精度和全部文件的大小与校验值；拒绝绝对
   包内路径、`..` 路径逃逸及符号链接文件，不执行包里提供的任意 SQL。
2. 验证热数据满足唯一键且时间不小于 watermark；冷段的 schema、精度、时间范围
   与清单一致，冷数据时间小于 watermark，同一逻辑键不能重复。通过实际数据扫描
   完成必要校验，不仅相信清单中的统计值。
3. 复制段文件到本次导入专用的暂存区，并生成新的目录所有权标识；保留源包不变。
4. 在目标 SQLite 写事务中创建新逻辑表、内部对象和热数据，恢复原 watermark，
   将 manifest 路径映射到新的目录。
5. 在提交前验证并发布目标文件，持久化文件及目录后提交 SQLite 状态。成功后新表
   才对其他连接完整可见。失败时回滚本次新建对象并清理本次导入拥有的文件。

允许更换逻辑表名和目录。文件内部 TsFile 表名与外部 SQLite 表名分别记录，Reader
通过 manifest 映射访问原文件内部表名，不能简单假设它等于新逻辑表名。

导入也要求独立管理调用，不加入用户已有的显式事务。包校验和文件准备可以先完成，
取得目标写锁后再次检查名称与目录冲突；进程中断后的恢复只能清理有本次操作所有权
记录的暂存内容，不触碰其他文件。第一版暂不提供向现有表合并数据的语义。

### 11.3 文件复制、移动与校验

```sql
SELECT * FROM tsfile_verify('main.sensor');
```

`tsfile_verify(table_name)` 是只读表值接口，返回
`segment_id, path, status, detail`。检查结果包括 `OK`、`MISSING`、`CORRUPT`、
`MISMATCH` 和 `UNREGISTERED`；检查已登记文件的存在性、内容校验值、schema 和精度，
并报告管理目录中的未登记文件。检查本身不导入、删除或修复文件。

- 复制到管理目录之外且保留源文件，不改变源库，也不自动登记副本。
- 已登记文件被移走或删除时，查询必须失败并报告具体逻辑表、段和原路径，不能
  静默跳过；verify 提供完整诊断，不承诺持续文件监控。
- 同路径文件被替换时，通过校验值与元数据识别。完整校验由 verify 执行；普通
  查询的轻量检查不能被描述为能识别所有任意字节修改。
- 放入目录的任意 `.tsfile` 不自动成为数据；后续 seal 也不能仅按后缀将其删除。
  孤儿回收只处理具有扩展所有权和失败操作记录的文件，未知文件只报告。
- 迁移使用 export/import 完成，不自动搜索移动后的路径。成功导入并校验后，由用户
  单独决定是否删除源逻辑表或源文件，导入不隐式搬走源数据。

复制单个 TsFile 只获得部分历史数据，不等于完整导出包。第一版 import 仅接受上述
完整包；导入任意独立 TsFile 需要另行定义热冷边界、冲突和归属规则，不混入此接口。

### 11.4 验收场景

- 有 TAG、无 TAG、全热、全冷、冷热混合以及空表都能导出并恢复；
- 恢复到不同表名和目录后，逻辑查询结果、精度与 watermark 与源快照一致；
- 并发写入期间导出仍获得一致快照，且不推进源表 watermark；
- 名称冲突、目录冲突、包损坏、文件缺失、校验失败和不支持的包版本明确失败；
- 在复制、同步、目录发布和 SQLite 提交边界注入故障，验证源数据不变、目标不出现
  可查询的半成品、重试不会覆盖用户文件；
- verify 能区分丢失、被替换及未登记文件，并保持文件系统和数据库不变。
<!-- tag-comment-scope-H-end mode="block" -->
<!-- tag-comment-thread-H
cmd9 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:33:44.758Z","body":"这里应该给出导出命令， 用户如果直接拷贝或者移动走了tsfile ， 那应该也得识别出来？"}
cmd18 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:37:35.440Z","body":"回复 cmd9：这里确实缺少面向用户的完整导出/恢复入口，应该补产品能力。当前分支只有 seal，没有统一的 export、restore 或 verify 命令；不能把尚不存在的命令写成可执行用法。导出也应区分“只导出历史 TsFile”与“完整备份逻辑表”：后者必须包含热数据、manifest、配置和引用的段文件。\n\n对直接文件操作，目前行为是：\n\n- 复制到管理目录之外且保留原文件：原库继续使用原路径，扩展不会感知多了一份副本，也无需自动改变 manifest；这份副本本身不是完整备份。\n- 移走或删除 manifest 引用的文件：`read_cold()` 尝试打开它时返回 `SQLITE_IOERR`，不会静默跳过；若整个目录消失，重新连接虚拟表时会报 `SQLITE_CANTOPEN`。当前没有持续监控、自动定位新路径或详细的缺失文件诊断。\n- 把文件复制进受管理目录：不会自动导入；未登记的 `.tsfile` 可能在下一次 seal 的孤儿清理中被删除。文件路径也不等于文件身份，当前没有完善的校验机制来识别同路径下被替换的另一个有效 TsFile。\n\n建议提供导出、恢复和校验三个明确入口：导出在一致快照下生成 SQLite 备份与段文件包，附带 schema、精度、文件清单和校验信息；恢复负责验证并重建路径映射；校验返回缺失、损坏、被替换或未登记文件的具体路径。移动/迁移应通过受控命令更新 manifest，而不是看到目录变化就自动接受文件。命令语法需要和前面的 seal 管理接口一起确定。本次先把能力缺口和现有检测行为回复清楚，没有修改实现或虚构已支持的导出命令。"}
cmd22 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:57:28.150Z","body":"我建议在这里给出这里的定义， 就是导出、导入的机制。 我们做一下设计。 "}
cmd32 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:05:05.658Z","body":"回复 cmd22：已在本节写入导出/导入设计草案，给出三个明确入口：`tsfile_export` 导出完整单表快照，`tsfile_import` 恢复为新表，`tsfile_verify` 只读检查文件。第一版采用保守的写锁一致快照，不自动封存、不覆盖已有表，也不把任意单个 TsFile 当完整备份导入。\n\n重点补了两个边界：导入改名时要保留文件内部表名映射，否则 Reader 用新逻辑名读旧 TsFile 会出错；未知文件不能仅因后缀匹配就被“孤儿清理”删除。草案同时规定包内容、校验、路径迁移、失败回滚和验收场景。需要你继续评审的主要取舍是：先接受导出阻塞写入、导入只建新表，后续再扩展无阻塞快照和合并导入。"}
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
