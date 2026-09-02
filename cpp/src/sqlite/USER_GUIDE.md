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

模块参数如下：

| 参数 | 要求 |
| --- | --- |
| `directory` | 必填、绝对路径、由当前逻辑表独占 |
| `timestamp_precision` | 必填，只能是 `ms`、`us` 或 `ns` |
| `column` | 可重复，格式为 `名称:类型:类别` |

列定义必须满足：

- 恰好一个 `TIME` 列；
- `TIME` 必须是第一列，类型必须为 `TIMESTAMP`；
- 至少一个 `TAG` 列；
- `TAG` 类型必须为 `STRING`，值不能为 `NULL`；
- `FIELD` 可以为 `NULL`；
- 列名大小写不敏感地唯一；
- `_tsfile_command` 和 `_tsfile_cutoff` 是保留名称。

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

```sql
BEGIN IMMEDIATE;

INSERT INTO sensor(_tsfile_command, _tsfile_cutoff)
VALUES ('seal', 1700086400000);

COMMIT;
```

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

不要直接修改 shadow table。绕过虚拟表写入会破坏 watermark、manifest 和文件
之间的一致性。

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

## 11. 备份与恢复

完整数据由两部分共同组成：

1. SQLite 数据库文件及其 WAL/journal；
2. manifest 引用的 TsFile 目录。

只备份 SQLite 文件会丢失冷数据，只备份 TsFile 目录会丢失热数据和 manifest。
备份工具必须同时捕获两部分的一致视图。MVP 尚未提供在线快照命令，推荐在停止
写入后备份，或者在应用层协调 SQLite checkpoint/事务与目录快照。

恢复时必须保持配置中记录的绝对目录可用；如果恢复到不同路径，应创建新的逻辑
表并执行受控迁移，而不是手工修改 `sensor_config`。

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
