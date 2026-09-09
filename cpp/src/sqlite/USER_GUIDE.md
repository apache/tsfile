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

# SQLite + TsFile 用户手册

使用 `tsfile_sqlite`，你可以通过 SQL 查询已有 TsFile，也可以持续写入新数据，
再将它们保存为 TsFile。新增数据先保存在 SQLite 中，可以更新和删除；封存后的
数据保存在 TsFile 中，仍然可以查询，但不能再修改。

本手册带你完成一次建表、读写、导出和重新读取，然后介绍日常使用与排障。
示例使用 SQLite 命令行；应用程序也可以执行相同的 SQL。
[English](USER_GUIDE_EN.md) · [技术报告](TECHNICAL_GUIDE.md)

## 1. 准备运行环境

需要 Linux 或 macOS、支持加载扩展的 SQLite 3.31 或更高版本，以及同一次构建生成的
`tsfile_sqlite` 和共享库 `libtsfile`。已有这两个库时，可以直接进入下一节。
当前支持 TsFile 的表模型文件。

从源码构建时，在仓库根目录执行：

```bash
cmake -S cpp -B cpp/build/sqlite \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON
cmake --build cpp/build/sqlite --target tsfile_sqlite -j
```

构建产物位于 `cpp/build/sqlite/lib`。Linux 扩展名为 `tsfile_sqlite.so`，macOS 为
`tsfile_sqlite.dylib`。部署时将扩展和 `libtsfile` 放在同一目录。

macOS 系统 SDK 的 SQLite 头文件禁用了扩展加载。使用 Homebrew SQLite 时，可以
改用下面的配置命令，然后执行上面的构建命令：

```bash
cmake -S cpp -B cpp/build/sqlite \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON \
  -DSQLite3_INCLUDE_DIR="$(brew --prefix sqlite)/include" \
  -DSQLite3_LIBRARY="$(brew --prefix sqlite)/lib/libsqlite3.dylib"
```

命令行也应使用支持扩展加载的 SQLite。Homebrew 安装的命令可通过
`"$(brew --prefix sqlite)/bin/sqlite3"` 启动。

## 2. 跑通第一个示例

本节的步骤可以按顺序执行，完成后会得到一个 SQLite 数据库和一个可独立读取的
TsFile。示例使用 `/tmp/tsfile-demo`；请选择一个尚不存在的目录。重复练习时换一个
目录名，并替换后续示例中的路径。正式数据应使用持久存储目录。

### 打开数据库并加载扩展

在终端执行：

```bash
mkdir /tmp/tsfile-demo
sqlite3 /tmp/tsfile-demo/demo.db
```

进入 SQLite 后，将下面的扩展路径替换为构建产物的绝对路径：

```sql
.load /absolute/path/to/tsfile_sqlite
.headers on
.mode column
```

每次重新打开连接都需要加载扩展。`.load` 可以使用完整的 `.so` 或 `.dylib` 文件名。

### 创建一张表并写入数据

```sql
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  temperature DOUBLE FIELD,
  directory='/tmp/tsfile-demo/sensor-segments',
  timestamp_precision='ms'
);

INSERT INTO sensor VALUES
  (1000, 'd1', 21.5),
  (2000, 'd1', 22.0),
  (3000, 'd2', 19.0);
```

这里的 time 保存毫秒时间戳，device 标识设备，temperature 保存测量值。
`directory` 是这张表封存数据时使用的独占目录，扩展会创建它；建表时它必须不存在
或为空。此时三行数据都在 SQLite 中，还没有封存。

### 查询和修改

```sql
SELECT time, device, temperature FROM sensor ORDER BY time;
```

结果为：

```text
time  device  temperature
1000  d1      21.5
2000  d1      22.0
3000  d2      19.0
```

修正第二条记录，再查看结果：

```sql
UPDATE sensor SET temperature=22.5 WHERE time=2000 AND device='d1';
SELECT temperature FROM sensor WHERE time=2000 AND device='d1';
```

查询返回 `22.5`。尚未封存的数据也可以通过普通 DELETE 删除。

### 导出为 TsFile

```sql
SELECT tsfile_export('main.sensor', '/tmp/tsfile-demo/export-001');
```

返回 `1`，表示生成了一个文件：

```text
/tmp/tsfile-demo/export-001/part-000001.tsfile
```

这次调用会自动封存当前三行数据，再导出完整表内容，不需要提前执行 seal。
导出文件是独立副本，可以交给标准 TsFile Reader 读取。原表仍然能查到三行数据，
但这些行已经封存，不能再更新或删除。

查看现在还有多少可修改的数据，以及下一次写入允许的起点：

```sql
SELECT hot_rows, watermark FROM tsfile_table_info('main.sensor');
```

结果为 `hot_rows=0`、`watermark=3001`：热数据已全部封存，后续新增时间必须不小于
3001。导出函数中的 `main.sensor` 指当前数据库的 sensor 表，管理操作需要带上这个
数据库前缀。

### 直接查询刚导出的文件

```sql
CREATE VIRTUAL TABLE temp.history USING tsfile_hybrid(
  file='/tmp/tsfile-demo/export-001/part-000001.tsfile',
  source_table='sensor'
);

SELECT time, device, temperature FROM history ORDER BY time;
```

结果与刚才导出的三行数据一致，包括修正后的 `22.5`。没有提供 directory，所以
history 只用于查询。`temp` 表在关闭连接后消失，文件仍然保留。

这里不需要声明列。`source_table='sensor'` 选择文件内部的 sensor 表，扩展会读取
它的列结构；SQLite 中的名称 history 可以与文件内部表名不同。

### 基于这份文件继续写入

```sql
CREATE VIRTUAL TABLE continued USING tsfile_hybrid(
  file='/tmp/tsfile-demo/export-001/part-000001.tsfile',
  source_table='sensor',
  directory='/tmp/tsfile-demo/continued-segments'
);

INSERT INTO continued VALUES (4000, 'd1', 23.0);
SELECT time, device, temperature FROM continued ORDER BY time;
```

这次查询返回四行。前三行仍从原 TsFile 读取，新增的 4000 行保存在 SQLite 热区，
可以继续修改；原文件和 history 的查询结果不变。

文件中最大时间是 3000，因此 continued 只能接受晚于 3000 的新数据，即使写入的是
另一个设备也一样。提供 directory 就表示为后续写入准备独立存储空间。

## 3. 使用你自己的数据

### 从空表开始采集

按照示例中的 sensor 建表方式，换成业务列名、独占目录和实际时间单位。
每列使用 `列名 类型 类别` 声明：第一列是 `TIMESTAMP TIME`，设备等标识列使用
`STRING TAG`，测量值使用 FIELD。

TAG 可以有多个，也可以没有。例如每个时间只记录一个值时：

```sql
CREATE VIRTUAL TABLE readings USING tsfile_hybrid(
  time TIMESTAMP TIME,
  value DOUBLE FIELD,
  directory='/tmp/tsfile-demo/readings-segments',
  timestamp_precision='ms'
);
```

readings 每个时间戳只能新增一条记录；sensor 则以 `(device, time)` 区分记录。
列名包含空格时使用双引号，例如 `"sensor value" DOUBLE FIELD`。列名不能仅靠
大小写区分，每列都要写明 TIME、TAG 或 FIELD。

### 打开已有 TsFile

先确认文件的绝对路径和文件内部表名，再按照 history 或 continued 的示例建表。
只查询时省略 directory；需要追加时提供一个新的独占目录。每次建表选择一个文件
中的一张表，不从文件名猜测表名，也不自动读取文件中的其他表。

文件建表不再写列定义。创建后可以查看 SQLite 识别到的结构：

```sql
PRAGMA table_info(continued);
```

示例的三列为 `time INTEGER`、`device TEXT` 和 `temperature REAL`。查询和写入使用
这里显示的列名。普通 TsFile 的时间列通常显示为 time；由本扩展生成的文件也会保留
自定义时间列名。

如果文件没有时间精度信息，只查询时可以保持 unknown。追加数据前必须确认原文件
时间单位，并在建表参数中补充 `timestamp_precision='ms'`、`'us'` 或 `'ns'`。
已带精度的文件会自动继承该精度；显式填写的值必须与它一致。

请保持源文件路径和内容不变。新增数据写入 SQLite，不会追加到或改写这个源文件。
源文件后续被替换时，原表不会自动刷新。

### 选择正确的值和时间单位

时间戳直接存储为整数，不自动在秒、毫秒、微秒之间换算。例如源数据是秒而表声明为
ms，应用需要先完成单位换算，再写入正确的毫秒值。

| 声明类型 | 写入值 |
| --- | --- |
| BOOLEAN | 整数；0 为假，非 0 为真，查询返回 0 或 1 |
| INT32、DATE | int32 范围内的整数 |
| INT64、TIMESTAMP | int64 范围内的整数 |
| FLOAT、DOUBLE | 整数或小数 |
| STRING、TEXT | 文本 |
| BLOB | 二进制值，保留长度和零字节 |

TIME 不能为 NULL。TAG 和 FIELD 可以为 NULL；TAG 必须是 STRING 类型。
同一时间、相同 TAG 组合的新记录会冲突。在这个唯一键中，相同位置的 NULL 也视为
相同分量，因此不能用 NULL 绕过重复检查。NULL、空字符串和文本 `'null'` 各不相同。
查找 NULL 使用 `IS NULL`。

已有源文件中的重复记录会按原样查询，不会在建表时自动去重。

## 4. 日常查询、修改与封存

### 用普通 SQL 查询

对逻辑表执行查询时，无需区分数据在 SQLite 还是 TsFile 中。可以使用条件、关联、
聚合和排序。例如在完成第二节后：

```sql
SELECT device, avg(temperature) AS avg_temperature
FROM continued
WHERE time >= 1000 AND time < 5000
GROUP BY device
ORDER BY device;
```

需要固定顺序时写出 ORDER BY。时间范围和设备等 TAG 条件有助于减少扫描量。
不要把隐含 rowid 当作持久业务主键；封存和重新查询可能改变它。

### 成批写入或修正近期数据

```sql
BEGIN;
INSERT INTO continued VALUES (5000, 'd2', 20.0);
UPDATE continued SET temperature=23.5 WHERE time=4000 AND device='d1';
COMMIT;
```

需要取消整批修改时，用 ROLLBACK 代替 COMMIT。热数据也支持 savepoint。
通常应通过时间和 TAG 精确定位要修改的记录。

若 UPDATE 或 DELETE 实际命中任意已封存行，整条语句都会失败，包括对其他热行的
修改；IGNORE、FAIL 等冲突选项也不能跳过这个限制。仅查询冷数据不受影响。

### 提前冻结一段历史

当某个时间之前的数据不再需要修正时，可以主动封存，不必等到导出：

```sql
SELECT tsfile_seal('main.continued', 4500);
```

如果已按本节顺序操作，返回 `1`：4000 的记录被封存，5000 的记录继续留在热区。
4500 是不包含的上界，时间恰好等于 4500 的记录也会留在热区。此后新数据必须不早于
4500。封存不改变查询结果。

即使边界之前没有热行，较大的 cutoff 仍会推进写入起点。因此，应按业务允许的迟到
和修正窗口选择 cutoff。重复使用当前边界返回 0，使用更早的边界会失败。

seal 同步执行，可以参与显式事务：

```sql
BEGIN IMMEDIATE;
SELECT tsfile_seal('main.continued', 5001);
ROLLBACK;
```

这里的回滚会撤销本次封存，5000 的记录仍是热数据。正式保留封存结果时改用 COMMIT。
在外层事务提交之前，seal 返回成功只表示当前事务内操作成功。

## 5. 导出和交付数据

每次需要一份独立的完整数据副本时，直接执行 export，并换用一个尚不存在的输出目录：

```sql
SELECT tsfile_export('main.continued', '/tmp/tsfile-demo/export-002');
```

先提交或回滚当前事务，再单独执行这条 SELECT。export 和 seal 都应独立调用，
不要放入逐行查询、视图、触发器或其他表达式中。

export 包含本次数据视图中的外部历史、已封存记录和全部当前热记录。它自动封存热数据，
所以输出包含刚写入的数据；自动封存提交之后才到达的新写入留给下一次导出。
只读文件表也可以 export，输出只包含选中的表。

当前非空表导出一个 `part-000001.tsfile`，返回 1；空表导出空目录，返回 0。
输出文件内部表名使用当前逻辑表名，例如这次是 continued。交付给其他使用者时，同时
告知这个表名，便于对方通过 source_table 选择它。已知时间精度随文件保留。

输出目录的父目录必须已经存在。不要把输出放在表的自有段目录内，也不要让它包含
或替代源文件；和外部文件放在同一个父目录下可以。已有输出路径不会被覆盖。
删除导出副本不会影响原表，但若另建了引用该副本的表，例如第二节的 history，则
仍需保留副本供它读取。

导出成功后，本次热数据已经冻结，新写入必须晚于这些数据的最大时间。若需要继续
修改近期记录，请在修改完成后再导出。

### 导出失败后如何处理

先看错误信息，再查询 `tsfile_table_info` 确认热行数和 watermark：

| 错误发生时的状态 | 下一步 |
| --- | --- |
| 路径检查失败或自动封存尚未提交 | 修正路径、权限或文件问题后重试；本次操作没有提交封存变化 |
| 已提交自动封存，生成输出失败 | 数据仍可查询，但已经封存；修复输出问题后使用新目录重试 |
| 完整输出已发布，父目录同步失败 | 先检查目标目录及文件，不要直接覆盖；需要重新导出时使用新目录 |

失败不会把已经提交的冷数据恢复成可修改的热数据。进程中断可能留下名称包含
`.tsfile-export-` 的临时目录，不要把它当作已完成的交付结果。

导出保存表数据，不保存原 SQLite 数据库的全部表、业务配置或冷热状态。空表导出也
不保存表定义。它不能替代完整的业务数据库备份。

## 6. 查看状态和处理常见问题

### 确认还能写入什么数据

```sql
SELECT mode, hot_rows, watermark, append_available, timestamp_precision
FROM tsfile_table_info('main.continued');
```

hot_rows 是尚可修改的热行数，watermark 是新增时间的最小允许值。mode 为 readonly
时只能查询。append_available 为 1 表示仍有可表示的新时间，但写入仍需满足唯一键
等约束。状态只反映查询当时的情况，最终以写操作结果为准。

若最大时间已达到 INT64_MAX，append_available 为 0，watermark 为 NULL，不能再
追加更晚数据，原有数据仍可查询和导出。显式 seal 的半开 int64 上界无法覆盖时间
为 INT64_MAX 的热行；export 可以自动封存它。

### INSERT、UPDATE 或 DELETE 失败

先用上面的状态查询检查模式和时间边界。引用文件但没有指定 directory 的表只读；
需要追加时，以新名称和新目录另建可写表。没有目标行的写语句可能作为空操作成功，
这不表示只读表变成了可写表。

可写表中，新时间必须不小于 watermark。引用外部文件时，这意味着严格晚于源表的
最大时间，不能回填历史空隙。再检查是否重复了 `(全部 TAG, time)`、TIME 是否为 NULL，
以及类型和数值范围是否正确。UPDATE/DELETE 失败时还应确认目标记录尚未封存。

### 源文件找不到、查询失败或怀疑文件变化

```sql
SELECT path, status, detail FROM tsfile_verify('main.continued');
```

按报告检查对应路径：

| 状态 | 如何处理 |
| --- | --- |
| OK | 本次文件检查通过 |
| MISSING | 确认文件是否被移动或删除，恢复登记路径下的原文件 |
| CORRUPT | 文件无法打开或解析，检查读取权限及文件是否完整 |
| MISMATCH | 文件与登记时不一致，核对是否被替换，恢复原始文件 |
| UNREGISTERED | 自有目录中有未登记 TsFile，先核对来源，不要直接将其当作表数据或删除 |

verify 只检查和报告，不修复、注册或删除文件。它也不会扫描外部源文件旁边的其他
文件。检查包括文件特征和元数据，但不逐页解码全部数据。

### 加载扩展或建表失败

加载报告 not authorized 时，确认 SQLite 支持扩展加载；应用连接需要先启用加载。
找不到 libtsfile 时，检查扩展和共享库是否来自同次构建并位于同一目录。

建表失败时，核对绝对路径、源文件内部表名和时间精度。新可写表应使用空或不存在的
独占目录，不能与另一张表的目录重合或嵌套。引用文件时不要同时声明列；从空表开始
时则要声明列、目录和精度。

### 查看建表语句时为什么没有列定义

`sqlite_schema.sql` 保存的是创建虚拟表时的 SQL，文件建表不会在这里展开推断列。
使用 `PRAGMA table_info(表名)` 查看实际结构。较旧 SQLite 若不识别 sqlite_schema，
可以使用兼容名称 sqlite_master。

## 7. 关闭、重开和管理数据文件

退出 SQLite 后，重新打开同一个数据库并加载扩展，即可继续使用持久表。列结构、
热数据和写入边界会保留，不需要重复 CREATE。使用 temp 创建的表随连接消失，其
热数据也会丢弃，已经生成或引用的 TsFile 则保留。

保留 SQLite 数据库、源文件和各表的自有段目录。不要单独移动或改写仍被表引用的
文件，也不要把目录中新出现的文件当作自动加入表的数据。

可写目录中的 `.tsfile-owner` 记录归属，绑定数据库路径及表名。复制或移动 SQLite
数据库后，不能直接复用原目录继续写入；迁移前应规划数据和路径的处理。不要直接
修改名称含 `_tsfile$` 的内部表来改路径或改数据。

确认不再需要某张逻辑表时，才执行 `DROP TABLE 表名`。这会删除该表的 SQLite 热数据
和登记，保留外部源文件、已封存文件及目录归属标记。后续归档或删除文件之前，先确认
没有其他表还在引用它们。

当前版本不自动迁移早期原型数据库，也不支持原地修改列结构或把只读表切换为可写表。
查询和导出会在内存中收集数据，导出还会重写完整表；处理大表前应按实际数据量评估
内存和执行时间。封存与导出均由调用方主动执行，没有后台定时封存。
