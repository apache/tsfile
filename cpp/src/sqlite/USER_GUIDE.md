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
