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

以下按目标功能设计定义建表语法；示例表达待实现接口，不代表当前原型已支持。
列定义采用 `列名 类型 [类别]`，省略类别时默认为 `FIELD`；`TIME` 和 `TAG` 显式声明。

<!-- tag-comment-scope-A-start mode="block" hash="sha256:0e9a78743b7900e3d2c4dcd95c70a64cbd9c7fd7881724a7432e026c455a6ecd" -->
```sql
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  region STRING TAG,
  temperature DOUBLE FIELD,
  status STRING FIELD,
  payload BLOB FIELD,
  directory='/var/lib/example/sensor',
  timestamp_precision='ms'
);
```
<!-- tag-comment-scope-A-end mode="block" -->
<!-- tag-comment-thread-A
cmd33 {"author":"colin","kind":"human","createdAt":"2026-09-09T04:08:28.882Z","body":"这里强制说明FIELD 列吧，就要求带。 "}
cmd39 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:28:49.464Z","body":"回复 cmd33：按这个要求确定语法：每列都必须写明 `列名 类型 类别`，类别只能是 TIME、TAG 或 FIELD，不再默认 FIELD。已给选中示例的 temperature、status、payload 补上 FIELD。省略类别应在建表时报错并指出列名。\n\n本次按该评论修改选中示例；相邻的旧默认说明以及另一条待评审规则中仍有“默认 FIELD”措辞，先保留其评论范围，全文定稿时需要一起统一为强制类别。"}
-->

列定义按书写顺序组成 schema，表级选项使用 `key=value`。未知选项、重复的表级
选项和不合法的列定义在建表时返回明确错误。标识符支持双引号转义，例如
`"sensor value" DOUBLE`；字符串选项使用单引号。

模块参数如下：

| 参数 | 要求 |
| --- | --- |
| `directory` | 必填、绝对路径、由当前逻辑表独占 |
| `timestamp_precision` | 必填，只能是 `ms`、`us` 或 `ns` |
| `column` | 可重复，格式为 `名称:类型:类别` |

列定义的目标规则如下：

<!-- tag-comment-scope-B-start mode="block" hash="sha256:693e3cfecfa5246c1c684e7865fffda20008ac5c6e0e82c242c350da16e68f69" -->
- 恰好一个 `TIME` 列，必须是第一列，类型为 `TIMESTAMP`，值不能为 `NULL`；
- `TAG` 列可以有零个或多个；存在时类型必须为 `STRING`，值允许为 SQL `NULL`；
- 有 TAG 时，全部 TAG 与 TIME 共同组成唯一键；无 TAG 时，TIME 单独组成唯一键；
- 不声明类别的列默认为 `FIELD`，FIELD 值可以为 `NULL`；
- 列名不能仅靠 ASCII 大小写区分，例如 `Temperature` 和 `temperature` 视为重名；
- 封存通过第 7 节的管理 UDF 发起，业务 schema 不需要声明或操作
  `_tsfile_command`、`_tsfile_cutoff` 控制列。

NULL TAG 的目标语义：

- SQL `NULL`、空字符串 `''` 和字符串 `'null'` 是三个不同值，封存和查询必须保留区别；
- 判定逻辑唯一键时，相同位置的两个 NULL TAG 视为同一个键分量。例如同一 TIME 下，
  两条 `(device=NULL, region='cn-east')` 记录冲突；
- NULL 查询使用 `IS NULL`，普通 WHERE 表达式继续遵循 SQLite 的 NULL 语义，
  不把 `= NULL` 改成相等比较；
- 热数据的唯一键检查必须显式处理 NULL，不能仅依赖 SQLite 默认 UNIQUE 对 NULL
  的处理，也不能用可能与实际 TAG 冲突的字符串替换 NULL。
<!-- tag-comment-scope-B-end mode="block" -->
<!-- tag-comment-thread-B
cmd34 {"author":"colin","kind":"human","createdAt":"2026-09-09T04:12:36.036Z","body":"TAG 允许设置为null，这个应该是底层的能力吧"}
cmd40 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:29:13.195Z","body":"回复 cmd34：是底层已有的数据表示能力。`Tablet::get_device_id()` 会为 NULL TAG 生成 nullptr 分量，`StringArrayDeviceID` 保留它，并在相等比较中把同位置的两个 nullptr 视为相等；空字符串另有表示。已把当前选中规则改成允许 NULL TAG。\n\n同时补了必要的键语义：NULL、空字符串、字面值 'null' 必须区分；在逻辑唯一键中，相同位置的 NULL 视为同一分量，防止同一设备同一时间出现重复行。SQLite 默认 UNIQUE 会允许多个含 NULL 的键，因此需要在扩展层用保留类型/NULL 标志的键编码或等效检查来保证一致，而不是只移除 NOT NULL。查询仍用 IS NULL，普通 SQL 三值逻辑不变。参见 [SQLite NULL 处理](https://www.sqlite.org/nulls.html)。这些是设计规则，底层 NULL TAG 的端到端封存读回仍应列入验收。"}
-->

无 TAG 表可按以下方式定义；每个时间戳最多对应一行：

```sql
CREATE VIRTUAL TABLE readings USING tsfile_hybrid(
  time TIMESTAMP TIME,
  value DOUBLE,
  directory='/var/lib/example/readings',
  timestamp_precision='ms'
);
```

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
cmd37 {"author":"colin","kind":"human","createdAt":"2026-09-09T04:25:38.739Z","body":"我有机会发现他的操作命中了冷读数据吗？ 如果发现更新在冷数据上，或者删除在冷数据上， 我就直接拒绝好了。 "}
cmd41 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:29:35.664Z","body":"回复 cmd37：可以，而且在真正修改之前就有明确的拦截点。SQLite 对虚拟表执行 UPDATE/DELETE 时，会把目标行的旧 rowid 交给 `xUpdate`；我们根据该 rowid 对应的来源判断它是热行还是 TsFile 冷行，冷行直接拒绝，无需让用户提前检查。当前原型已经给冷行分配负 rowid，并在 `update_hot()`、`delete_hot()` 开头拒绝负值。参见 [SQLite xUpdate](https://www.sqlite.org/vtab.html#the_xupdate_method)。\n\n这里应判断“真正被 UPDATE/DELETE 命中的目标行”，不能因为查询扫描到了冷数据就拒绝。WHERE 未匹配的冷行、子查询里只读的冷数据都不应触发写入错误；因此不要在通用 xFilter/read_cold 里直接拒绝读取。\n\n设计规则建议就定为：命中冷行即拒绝整条写语句，其他已修改热行也要回滚；只命中热行时正常执行。插入和修改热行时间另检查新时间不得小于 watermark。冷行来源标识必须在本语句期间稳定且不与热行混淆。\n\n还需处理冲突策略：当前开启 CONSTRAINT_SUPPORT 后，单纯返回 SQLITE_CONSTRAINT 可能受到 UPDATE OR IGNORE/OR FAIL 的影响。冷数据不可修改属于能力边界，不应被 IGNORE 静默跳过；建议统一用不可写错误（如 SQLITE_READONLY）拒绝，并用“先热后冷、多行语句、OR IGNORE/FAIL、外层事务”测试保证整条语句撤销。参见 [SQLite 虚拟表约束处理](https://www.sqlite.org/c3ref/c_vtab_constraint_support.html)。本条先回复拦截机制与原子性要求，不改动 DML 示例。"}
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

内部对象采用带符号的命名形式 `"<逻辑表名>_tsfile$<用途>"`。例如 sensor 对应：

| 内部表 | 用途 |
| --- | --- |
| `"sensor_tsfile$hot"` | 可变热数据 |
| `"sensor_tsfile$segments"` | TsFile 文件登记信息 |
| `"sensor_tsfile$config"` | schema、精度、watermark 和目录配置 |

名字必须统一做标识符转义。建表前检查全部派生名；任何同名对象都使创建失败并明确
报告冲突，不覆盖或复用用户对象。符号用于提高辨识度，不能被当作绝不撞名的保证。
最后一个下划线之前保留完整逻辑表名，以维持 SQLite 的 shadow 表识别关系。

这些名字属于内部实现，不作为业务 API。日常诊断通过公开状态接口完成；用户能够
在 schema 中查看内部对象，但不应直接修改它们。绕过虚拟表写入会破坏 watermark、
文件登记和实际文件之间的一致性。

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

<!-- tag-comment-scope-H-start mode="block" hash="sha256:1a861167f2fc4826a3f722848caedc1e0749da51b58aa2339a8dda2c1300f804" -->
## 11. TsFile 导出、导入与文件校验（功能设计草案）

本节定义面向标准 TsFile 的交换和查询接口。导出产物只有 `.tsfile` 文件，不导出
SQLite 数据库、热表、WAL、shadow 表或独立的 JSON 清单，也不承诺恢复源表的可写状态。
导入接口及只读语义在 11.2 节定义。

### 11.1 仅导出 TsFile

```sql
SELECT tsfile_export('main.sensor', '/export/sensor-001');
```

`tsfile_export(table_name, output_directory)` 导出指定逻辑表已经落到 TsFile 的
数据，成功返回产出的文件数。SQLite 中尚未封存的热数据不在导出范围内。
如需导出这些热数据，调用方先显式 seal 到选定 cutoff，提交后再执行 export。
export 本身不封存、不修改源数据、不推进 watermark。

目标目录必须为绝对路径且尚不存在，并且不得与源数据目录重合或互相嵌套。
输出示例：

```text
sensor-001/
  part-000001.tsfile
  part-000002.tsfile
```

- 表名、列类型和类别从 TsFile 自身元数据读取；可用的时间精度放在文件 Properties。
- 输出文件只包含所选逻辑表的数据。如果输入段同时含其他表，需要导出所选表为独立
  TsFile，不能直接复制而意外带出其他表。文件内部表名使用该逻辑表的导出名称。
- 文件可由标准 TsFile Reader 独立读取，不依赖本扩展的 JSON 清单或 SQLite 文件。
- 没有已封存数据时返回 0，目标目录为空；不会制造可恢复空表或热数据的假象。

第一版采用保守的一致性方式：取得源 SQLite 数据库的写保留锁，在同一事务视图内
固定该表的文件登记集合，复制或重写完成后释放锁。期间阻塞该数据库的其他写入。
对外部只读文件也要检查读取错误和文件变化；外部修改不属于 SQLite 锁的保护范围。

先在目标旁的专用暂存目录生成文件，校验 footer、schema 和已知精度，完成文件同步，
再原子发布整个目录并同步父目录。失败不发布完整目标，源文件不变。
仅允许独立管理调用，不接受用户已有的显式事务；成功返回时输出目录已发布。

### 11.2 扫描目录并建立只读查询表

```sql
SELECT tsfile_import('/data/archive');
```

`tsfile_import(directory [, target_schema])` 默认把发现的表注册到 `main`；可显式
指定一个已经存在的 SQLite schema。成功返回新注册的逻辑表数量。

```sql
SELECT tsfile_import('/data/archive', 'archive');
```

这里的导入指登记外部 TsFile 并建立查询入口：默认直接引用原文件，不复制、移动、
重写文件，也不把行装入 SQLite 热表。源目录必须是可持续访问的绝对路径，文件需
由调用方保持不可变。SQLite 中只保存查询所需的文件登记和 schema 元数据。

扫描与分组规则：

1. 第一版扫描指定目录当前层的普通 `.tsfile` 文件，不递归子目录，不跟随符号链接；
   非 TsFile 文件忽略，扩展名匹配但无法读取的文件使本次导入失败并报告路径。
2. 从每个文件的元数据枚举全部表名；一个文件包含多张表时分别登记，不按文件名猜
   表名。同一张表出现在多个文件中时，组成同一个只读逻辑表。
3. 同名表要求列定义、类型、类别和顺序兼容；第一版要求完全一致，不自动补列、
   类型提升或统一不同 schema。按 SQLite 标识符比较产生的大小写冲突应明确报错。
4. 接受普通 TsFile，不要求来自本扩展，也不要求携带私有快照清单。时间精度属性
   存在时读取；缺失时标记为 unknown 并按原始整数时间查询，不默认为 ms。
   同名表的已知精度必须一致；已知/未知混合也拒绝自动合并，避免混淆时间单位。
5. 同名表跨文件的结果按 `UNION ALL` 语义读取，重叠时间和重复逻辑键保留，不自动
   覆盖、去重或取最新值。只读导入不对外部数据强加可写 hybrid 表的唯一键约束。
6. 保存“SQLite 逻辑表 → 文件路径集合 → 各文件内部表名”的映射，所有路径和表名
   都按数据处理并正确转义。文件内部表名不会因 SQL 名称变化而被重写。

用户通过普通 SQL 查询，例如目录内包含 sensor 和 meter 两个表时：

```sql
SELECT time, device, temperature FROM sensor ORDER BY time;
SELECT count(*) FROM meter;
```

所有导入表默认且固定为只读：拒绝 INSERT、UPDATE、DELETE 和 seal，即使当前表为空
也不接受写入。只读表不创建可写热区，不恢复源库 watermark，也不因为时间较新就
自动允许修改。持续追加写入仍使用另行创建的可写 hybrid 表。

注册时先完整枚举、验证并建立映射，再在同一个 SQLite 事务中发布所有表。目标
schema 中任何同名用户表、虚拟表或所需内部对象冲突，都使整批失败；不覆盖、不
合并到已存在的表。对同一目录重复调用也遵循此规则，不会重复追加登记。

导入要求独立管理调用，不能嵌入已有用户显式事务。取得目标写锁后再次检查对象
冲突；失败回滚本次新建的所有登记，源文件保持不变。空目录或未发现表时返回 0。
本次扫描后新加入目录的文件不会自动进入已注册表，第一版不提供后台监控或刷新；
如需重新登记，可删除相关只读逻辑表后再导入，删除逻辑表不会删除外部文件。

### 11.3 文件所有权、移动与校验

```sql
SELECT * FROM tsfile_verify('main.sensor');
```

`tsfile_verify(table_name)` 是只读表值接口，返回
`segment_id, path, status, detail`。状态包括 `OK`、`MISSING`、`CORRUPT`、
`MISMATCH` 和 `UNREGISTERED`。导入时建立文件指纹，verify 对比文件内容、schema
和可用精度；普通文件不需要预先存储本扩展的校验属性。

- 自有 hybrid 段与外部只读引用必须有明确的所有权区别。导入不取得源文件的删除权，
  DROP TABLE、seal 清理和失败恢复都不得删除外部引用文件。
- 文件被移走或删除时，访问该文件的查询失败并报告表名、文件标识和原路径，不能
  静默跳过；verify 可集中报告问题，不承诺持续监控或自动搜索新路径。
- 同路径文件被替换或修改时，完整 verify 对比导入指纹并报告差异；普通查询的轻量
  检查不宣称能够发现所有字节修改。调用方必须保证登记后外部文件保持不可变。
- 外部目录新增文件不会自动导入；verify 报告发现的未登记 TsFile，且不删除它们。
  自有目录中的未知文件也不能仅凭 `.tsfile` 后缀被孤儿清理删除。
- 复制到其他位置且保留源文件不影响已有查询；移动目录后应在保留源数据的前提下
  重新建立登记，不直接改写内部表路径。

导出目录只包含 TsFile，导入后得到只读数据集；这一过程不是恢复完整 SQLite 数据库
或可写 hybrid 状态的备份协议。

### 11.4 验收场景

- 导出目录只含标准 `.tsfile`；热数据不被导出，watermark 不改变，无冷数据返回 0；
- 目录内多文件、多表能被完整发现，同表跨文件汇成一个只读表；
- 不带本扩展私有 Properties 的普通 TsFile 可以查询，缺失精度标为 unknown；
- 多文件重叠时间和重复键按 UNION ALL 保留；NULL TAG 与空字符串不混淆；
- schema 不兼容、精度冲突、表名冲突及损坏文件使批量导入失败且不留下部分注册；
- 对导入表执行 INSERT、UPDATE、DELETE、seal 均被拒绝，源文件字节保持不变；
- 文件移走、替换和新增分别被诊断为缺失、变化和未登记；verify 不修改任何文件；
- 删除只读逻辑表、导入失败及进程重启清理都不会删除外部文件；
- 对导出复制、文件同步、目录发布和导入登记提交注入故障，验证源数据不变及
  目标没有部分可见的发布结果。
<!-- tag-comment-scope-H-end mode="block" -->
<!-- tag-comment-thread-H
cmd9 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:33:44.758Z","body":"这里应该给出导出命令， 用户如果直接拷贝或者移动走了tsfile ， 那应该也得识别出来？"}
cmd18 {"author":"codex","kind":"codex","createdAt":"2026-09-09T03:37:35.440Z","body":"回复 cmd9：这里确实缺少面向用户的完整导出/恢复入口，应该补产品能力。当前分支只有 seal，没有统一的 export、restore 或 verify 命令；不能把尚不存在的命令写成可执行用法。导出也应区分“只导出历史 TsFile”与“完整备份逻辑表”：后者必须包含热数据、manifest、配置和引用的段文件。\n\n对直接文件操作，目前行为是：\n\n- 复制到管理目录之外且保留原文件：原库继续使用原路径，扩展不会感知多了一份副本，也无需自动改变 manifest；这份副本本身不是完整备份。\n- 移走或删除 manifest 引用的文件：`read_cold()` 尝试打开它时返回 `SQLITE_IOERR`，不会静默跳过；若整个目录消失，重新连接虚拟表时会报 `SQLITE_CANTOPEN`。当前没有持续监控、自动定位新路径或详细的缺失文件诊断。\n- 把文件复制进受管理目录：不会自动导入；未登记的 `.tsfile` 可能在下一次 seal 的孤儿清理中被删除。文件路径也不等于文件身份，当前没有完善的校验机制来识别同路径下被替换的另一个有效 TsFile。\n\n建议提供导出、恢复和校验三个明确入口：导出在一致快照下生成 SQLite 备份与段文件包，附带 schema、精度、文件清单和校验信息；恢复负责验证并重建路径映射；校验返回缺失、损坏、被替换或未登记文件的具体路径。移动/迁移应通过受控命令更新 manifest，而不是看到目录变化就自动接受文件。命令语法需要和前面的 seal 管理接口一起确定。本次先把能力缺口和现有检测行为回复清楚，没有修改实现或虚构已支持的导出命令。"}
cmd22 {"author":"colin","kind":"human","createdAt":"2026-09-09T03:57:28.150Z","body":"我建议在这里给出这里的定义， 就是导出、导入的机制。 我们做一下设计。 "}
cmd32 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:05:05.658Z","body":"回复 cmd22：已在本节写入导出/导入设计草案，给出三个明确入口：`tsfile_export` 导出完整单表快照，`tsfile_import` 恢复为新表，`tsfile_verify` 只读检查文件。第一版采用保守的写锁一致快照，不自动封存、不覆盖已有表，也不把任意单个 TsFile 当完整备份导入。\n\n重点补了两个边界：导入改名时要保留文件内部表名映射，否则 Reader 用新逻辑名读旧 TsFile 会出错；未知文件不能仅因后缀匹配就被“孤儿清理”删除。草案同时规定包内容、校验、路径迁移、失败回滚和验收场景。需要你继续评审的主要取舍是：先接受导出阻塞写入、导入只建新表，后续再扩展无阻塞快照和合并导入。"}
cmd35 {"author":"colin","kind":"human","createdAt":"2026-09-09T04:19:45.943Z","body":"导出只会导出tsfile，不会导出sqlite 的东西了。 "}
cmd36 {"author":"colin","kind":"human","createdAt":"2026-09-09T04:24:28.857Z","body":"导入的话， 给定一个文件夹， 然后遍历其中的表名称，都做导入吧。 导入的数据应该不做更新。 就是默认的tsfile文件，可供查询。 "}
cmd43 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:30:43.459Z","body":"回复 cmd35：已把导出部分改成仅生成标准 `.tsfile`，去掉 hot.sqlite、SQLite 状态和 JSON 包清单。这里按“只导出已经落入 TsFile 的数据”定义：热数据不自动转换，用户需要时先显式 seal，再 export；export 不改变原表 watermark，返回实际生成的文件数。没有冷数据就返回 0。\n\n还补了一个多表文件边界：导出指定表时，如果源文件含其他表，要重写出所选表，不能整文件复制而带出额外表。当前先完成这一条导出修改；下一条关于遍历目录和只读导入的评论单独处理。"}
cmd44 {"author":"codex","kind":"codex","createdAt":"2026-09-09T04:32:15.927Z","body":"回复 cmd36：已按“给目录、发现所有表、只读查询”重写导入设计。`tsfile_import('/data/archive')` 扫描文件元数据中的全部表名，同名表跨文件组成一张查询表；不同文件内的重复行按 UNION ALL 保留，不替用户做覆盖或去重。\n\n默认采用引用方式，文件仍留在原目录，SQLite 只保存登记。导入表固定只读，INSERT/UPDATE/DELETE/seal 都拒绝，也不会恢复热区和 watermark。DROP TABLE 或失败清理只删除登记，不删除外部文件。普通 TsFile 没有本扩展的精度 Property 也能读取，精度记为 unknown，不猜测单位。\n\n第一版还明确了边界：扫描目录当前层、不递归；已有同名表或 schema/精度冲突时整批失败；新增文件不自动刷新。上述默认让目录导入保持简单、可预测。第 11 节的验证和验收也已同步改成这套只读导入语义，前一条确定的“仅导出已封存 TsFile”保持不变。"}
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
