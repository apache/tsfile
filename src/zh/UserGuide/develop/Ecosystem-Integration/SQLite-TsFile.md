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

# SQLite TsFile 扩展

本文面向需要在 SQLite 中读写 Apache TsFile 的数据工程师和平台开发者。`tsfile_sqlite` 是 SQLite 的可加载扩展，注册虚拟表模块 `tsfile_hybrid`，把“SQLite 热数据 + TsFile 冷数据”组织成一张可以用普通 SQL 查询的逻辑表：新写入的数据留在 SQLite 中，可以更新和删除；封存后的数据写入 TsFile，仍然可以查询但不再可修改。

**先记住三件事：** 用 `.load` 加载扩展；用 `CREATE VIRTUAL TABLE ... USING tsfile_hybrid(...)` 建表；用 `tsfile_seal` 把热数据封存为 TsFile。当前版本面向本地、表模型 TsFile。

## 快速开始

### 加载扩展

在 SQLite CLI 或应用中加载扩展；每次重新打开连接都需要加载：

```sql
.load /path/to/tsfile_sqlite
```

### 创建可写表并写入

```sql
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  temperature DOUBLE FIELD,
  directory='/data/sensor-segments',
  timestamp_precision='ms'
);

INSERT INTO sensor VALUES
  (1000, 'd1', 21.5),
  (2000, 'd1', 22.0),
  (3000, 'd2', 19.0);
```

`time` 保存毫秒时间戳，`device` 标识设备，`temperature` 保存测量值。`directory` 是这张表封存数据时使用的独占目录，扩展会创建它。此时三行数据都在 SQLite 中，尚未封存。

### 封存为 TsFile

```sql
SELECT tsfile_seal('main.sensor', 200);
```

返回本次封存的行数；参数是单个输出文件的大小上限，单位 MB。封存后这些行仍然可查询，但不能再修改。

### 查询

```sql
SELECT time, device, temperature FROM sensor WHERE device = 'd1';
```

## 读取与查询

### 打开已有 TsFile（只读）

```sql
CREATE VIRTUAL TABLE history USING tsfile_hybrid(
  file='/data/export/sensor-0001.tsfile',
  source_table='sensor'
);

SELECT * FROM history;
```

`file` 指定文件路径（也可以是目录），`source_table` 指定文件内部的表名。不提供 `directory` 的表只读，不需要声明列。

### 基于已有 TsFile 继续写入

```sql
CREATE VIRTUAL TABLE continued USING tsfile_hybrid(
  file='/data/export/sensor-0001.tsfile',
  source_table='sensor',
  directory='/data/continued-segments'
);

INSERT INTO continued VALUES (4000, 'd1', 23.0);
```

历史数据从 TsFile 读取，新增数据写入 SQLite 热区。写入边界按设备计算：新时间必须不早于该设备在源文件中的最大时间加一；新出现的设备不受旧设备边界限制。

### 条件与下推

时间范围、TAG 等值条件和 FIELD 比较条件会下推到 TsFile 读取层；无法下推的表达式由 SQLite 在结果集上完成。

带 `device` 等设备条件是最有效的裁剪方式：数据按设备组织，设备过滤可以跳过其他设备的数据，而全局时间范围查询即使只返回少量行，也可能需要扫描大量数据。业务查询应尽量带上设备条件。

## 写入与封存

### 建表模式

- **从空表开始**：声明列、独占 `directory` 和时间精度，可写。
- **只读查询**：`file=` 且不提供 `directory`。
- **继续写入**：`file=` 加 `directory=`，在已有 TsFile 之上追加。

### 热区写入、更新与删除

```sql
BEGIN;
INSERT INTO sensor VALUES (4000, 'd2', 20.0);
UPDATE sensor SET temperature = 22.5 WHERE time = 2000 AND device = 'd1';
DELETE FROM sensor WHERE time = 3000;
COMMIT;
```

尚未封存的数据可以修改；若 UPDATE 或 DELETE 实际命中已封存的行，整条语句会失败。

### 封存（seal）

```sql
SELECT tsfile_seal('main.sensor', 200);
```

封存当前全部热行，返回封存行数。seal 同步执行，可以参与显式事务（`BEGIN IMMEDIATE; ... ROLLBACK/COMMIT;`）。封存后，各设备的新数据必须不早于该设备本次最大时间加一。

### 合并冷段（compact）

```sql
SELECT tsfile_compact('main.sensor', 50, 200);
```

`50` 是参与合并的小文件阈值，`200` 是目标文件大小上限，单位都是 MB。只有小于阈值的小文件参与合并；目标大小不能小于小文件阈值。

### 导出（export）

```sql
SELECT tsfile_export('main.sensor', '/data/export');
```

export 直接复制已封存的冷段，不复制热区数据。需要交付热数据时先执行 seal。输出目录必须是尚不存在的绝对路径。

## 类型映射与 NULL

| 声明类型 | 写入值 |
| --- | --- |
| BOOLEAN | 整数；0 为假，非 0 为真 |
| INT32、DATE | int32 范围内的整数 |
| INT64、TIMESTAMP | int64 范围内的整数 |
| FLOAT、DOUBLE | 整数或小数 |
| STRING、TEXT | 文本 |
| BLOB | 二进制值，保留长度和零字节 |

TIME 不能为 NULL。TAG 和 FIELD 可以为 NULL；TAG 必须是 STRING。同一时间、相同 TAG 组合的新记录会冲突。NULL、空字符串和文本 `'null'` 各不相同，查找 NULL 使用 `IS NULL`。

## 端到端示例

从空表采集、封存、导出、再回读：

```sql
-- 建表并写入
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  temperature DOUBLE FIELD,
  directory='/tmp/sqlite-demo/segments',
  timestamp_precision='ms'
);
INSERT INTO sensor VALUES (1000,'d1',21.5), (2000,'d1',22.0), (3000,'d2',19.0);

-- 封存并导出
SELECT tsfile_seal('main.sensor', 200);
SELECT tsfile_export('main.sensor', '/tmp/sqlite-demo/export');

-- 只读回读
CREATE VIRTUAL TABLE history USING tsfile_hybrid(
  file='/tmp/sqlite-demo/export', source_table='sensor'
);
SELECT time, device, temperature FROM history ORDER BY time;
```

## 当前限制与排错

### 写入前检查

1. 确认表为可写模式（提供 `directory`）或只读查询。

2. 确认 TIME 列非 NULL，新时间不早于该设备的 watermark。

3. 确认 TAG 为 STRING 类型，列名无重复。

### 已知限制

- 当前版本支持一个本地 TsFile 文件或目录中的表模型表。

- 只读视图的 ASCII 大写 TAG/FIELD 名称可能被小写化，TIME 列名保留；SQLite 标识符大小写不敏感，但下游按精确测量名映射时需留意。

- FIELD 列名的 UTF-8 字节数达到 1018 时，冷读该字段可能失败；建议不超过 1017 字节。

- 不带设备条件的全局时间范围查询可能较慢；业务查询应尽量带上设备条件。

- 长读事务跨 compact 后继续查询旧快照可能报文件缺失或变更，需结束并重新开始事务。

## 能力速查

| 任务 | 入口 |
| --- | --- |
| 加载扩展 | `.load /path/to/tsfile_sqlite` |
| 新建可写表 | `CREATE VIRTUAL TABLE ... USING tsfile_hybrid(...)` |
| 只读查询 TsFile | `file=...` + `source_table=...` |
| 继续写入 | `file=...` + `directory=...` |
| 封存 | `tsfile_seal('main.t', max_mb)` |
| 合并冷段 | `tsfile_compact('main.t', small_mb, target_mb)` |
| 导出 | `tsfile_export('main.t', '/path')` |
| 状态查询 | `tsfile_table_info` / `tsfile_device_info` / `tsfile_schema` |
| 文件校验 | `tsfile_verify('main.t')` |
