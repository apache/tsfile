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

# DuckDB TsFile 扩展

本文面向需要在 DuckDB 中分析或生成 Apache TsFile 的数据工程师、平台开发者和分析开发者。TsFile 扩展已作为 DuckDB Community Extension 发布，扩展把 TsFile 作为 SQL 数据源和 COPY 输出格式，形成“读取 → 查询 → 写回”的完整工作流。

**先记住两件事：** 读取使用 `read_tsfile(path, table_name)`；写入使用 `COPY ... TO ... (FORMAT tsfile)`。当前版本面向本地、表模型 TsFile，写入前请按 TAG 列和时间列排序。

## 快速开始

### 启动 DuckDB 并加载扩展

日常使用优先安装并加载 DuckDB 社区扩展：

```sql
INSTALL tsfile FROM community;
```

```sql
LOAD tsfile;
```

首次使用时安装社区扩展；之后每次启动 DuckDB 只需执行 `LOAD tsfile;`。

### 读取第一张 TsFile

```sql
SELECT *
FROM read_tsfile('/data/measurements.tsfile', 'sensors');
```

`read_tsfile` 的第一个参数是本地文件路径，第二个参数是 TsFile 中保存的表名。一次调用读取一个文件中的一个表模型表。

## 读取与查询

### 选择列

像查询普通 DuckDB 表一样选择需要的列。投影会尽量下推到 TsFile 扫描，减少读取的数据量：

```sql
SELECT time, device_id, temperature
FROM read_tsfile('/data/measurements.tsfile', 'sensors')
LIMIT 10;
```

### 时间过滤

全局 TsFile 时间轴在 DuckDB 中暴露为 `BIGINT`。时间单位由文件或协议约定，常见场景是毫秒。以下形式会下推到扫描器：`=`、`<`、`<=`、`>`、`>=` 和 `BETWEEN`。

```sql
SELECT time, device_id, temperature
FROM read_tsfile('/data/measurements.tsfile', 'sensors')
WHERE time BETWEEN 1700000000000 AND 1700003600000;
```

### TAG 过滤

字符串 TAG 列支持等值、范围、空值判断和 `AND`/`OR` 组合。支持的操作包括 `=`、`!=`、`<`、`<=`、`>`、`>=`、包含边界的 `BETWEEN`、`IS NULL` 和 `IS NOT NULL`。

```sql
SELECT time, device_id, temperature
FROM read_tsfile('/data/measurements.tsfile', 'sensors')
WHERE (device_id = 'device-01' OR device_id = 'device-02')
  AND time BETWEEN 1700000000000 AND 1700003600000;
```

FIELD 过滤以及不支持下推的 TAG 表达式仍会由 DuckDB 在扫描后计算。任意形式的 `NOT (...)` 不会下推。

### 检查过滤是否下推

使用 `EXPLAIN` 查看执行计划。如果下推成功，可以在 `READ_TSFILE` 节点中看到 `Time Range` 和 `TAG Filter`：

```sql
EXPLAIN
SELECT time, device_id, temperature
FROM read_tsfile('/data/measurements.tsfile', 'sensors')
WHERE device_id = 'device-01'
  AND time >= 1700000000000;
```

## 写入 TsFile

### 使用 COPY 输出

写入使用 DuckDB 标准 `COPY` 接口。建议先在子查询中完成清洗、类型转换和排序，再输出 TsFile：

```sql
COPY (
    SELECT time, device_id, temperature, humidity
    FROM measurements
    ORDER BY device_id, time
)
TO '/data/measurements.tsfile'
(
    FORMAT tsfile,
    TABLE_NAME sensors,
    TIME_COLUMN time,
    TAG_COLUMNS (device_id)
);
```

`TIME_COLUMN` 标识时间轴；列在 `TAG_COLUMNS` 中的列会写成 TAG；其余列会写成 FIELD。`TABLE_NAME` 和 `TIME_COLUMN` 推荐使用标识符写法，需要特殊字符时使用双引号。

### 写入选项

|选项|<span style="white-space: nowrap;">是否必需</span>|说明|
|---|---|---|
|`FORMAT tsfile`|是|选择 TsFile COPY 写入器。|
|`TABLE_NAME`|否|输出文件中的本地表名，默认值为 `default_table`。|
|`TIME_COLUMN`|否|时间轴输入列，默认值为 `time`，类型必须是 `BIGINT`。|
|`TAG_COLUMNS`|否|TAG 列名列表；省略时所有非时间列都写成 FIELD。|
|`OVERWRITE true`|否|目标路径已存在且需要替换时使用。保留默认临时文件处理。|

### TAG\_COLUMNS 约束

`TAG_COLUMNS` 接受列名列表，而不是值列表：

```sql
TAG_COLUMNS (device_id, region)
```

- 列必须存在于输入查询中。

- 列类型必须是 `VARCHAR`。

- TAG 值可以为 NULL。

- TAG 列不能同时是 `TIME_COLUMN`。

- 列名匹配不区分大小写。

如果不需要 TAG，可以只写时间列和 FIELD：

```sql
COPY (
    SELECT time, temperature, humidity
    FROM measurements
    ORDER BY time
)
TO '/data/field-only.tsfile'
(
    FORMAT tsfile,
    TABLE_NAME sensors,
    TIME_COLUMN time
);
```

### 类型映射与 NULL

|DuckDB 类型|TsFile 类型|说明|
|---|---|---|
|BOOLEAN|BOOLEAN|FIELD NULL 会保留。|
|INTEGER|INT32|FIELD NULL 会保留。|
|BIGINT|INT64|FIELD NULL 会保留；也用于时间轴。|
|FLOAT|FLOAT|FIELD NULL 会保留。|
|DOUBLE|DOUBLE|FIELD NULL 会保留。|
|VARCHAR|STRING|FIELD 和 TAG 均支持 NULL。|
|BLOB|BLOB|FIELD NULL 会保留。|
|TIMESTAMP\_NS|TIMESTAMP|读取时对应 DuckDB `TIMESTAMP_NS`。|

## 端到端示例

下面的流程从已有 TsFile 读取设备 `a` 的一段时间数据，转换列名后写出新的 TsFile，再回读验证：

```sql
LOAD tsfile;

COPY (
    SELECT time,
           s0 AS device_id,
           s2 AS value,
           CAST(s8 AS VARCHAR) AS day
    FROM read_tsfile('test/data/simple_table_t1.tsfile', 'test')
    WHERE s0 = 'a'
      AND time BETWEEN 1760106022000 AND 1760106024000
    ORDER BY device_id, time
)
TO '/tmp/tsfile_subset.tsfile'
(
    FORMAT tsfile,
    TABLE_NAME subset,
    TIME_COLUMN time,
    TAG_COLUMNS (device_id)
);

SELECT time, device_id, value, day
FROM read_tsfile('/tmp/tsfile_subset.tsfile', 'subset')
ORDER BY device_id, time;
```

查询结果会返回 3 行，时间分别为 `1760106022000`、`1760106023000` 和 `1760106024000`。

## 当前限制与排错

### 写入前检查

1. 确认 `TIME_COLUMN` 存在且类型为 `BIGINT`。

2. 确认 TIME 列没有 NULL；TAG 列可以包含 NULL。

3. 确认输入按所有 TAG 列、再按时间列排序。

4. 目标文件已存在时，保留临时文件处理并设置 `OVERWRITE true`。

### 已知限制

- 当前版本支持一个本地 TsFile 文件中的一个表模型表。

- FIELD 过滤不会下推；任意 `NOT (...)` TAG 表达式不会下推。

- DATE FIELD 写入暂时关闭，以避免依赖本地时区的转换问题；已有 TsFile 中的 DATE 仍可读取。需要写出时可先转换为 `VARCHAR`。

- 使用 `USE_TMP_FILE false` 直接写入时，目标路径必须是新路径。

## 能力速查

|任务|入口|
|---|---|
|读取 TsFile|`read_tsfile('/path/file.tsfile', 'table')`|
|查看列与数据|`SELECT ... FROM read_tsfile(...)`|
|下推时间 / TAG 条件|在 `WHERE` 中使用支持的比较与逻辑组合|
|检查下推结果|`EXPLAIN SELECT ...`|
|写入 TsFile|`COPY (...) TO 'file.tsfile' (FORMAT tsfile, ...)`|
|替换已有文件|默认临时文件处理 \+ `OVERWRITE true`|
