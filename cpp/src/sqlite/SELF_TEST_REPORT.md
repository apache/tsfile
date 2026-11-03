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

# SQLite + TsFile 扩展自测报告

日期：2026-11-03

分支：`experiment-sqlite`（基于 `develop` 的 `931a5d3`）

构建和手工测试步骤见 [TESTING.md](TESTING.md)。

## 1. 测试环境

### 1.1 本地环境

- 机器：Apple M5 Pro，32GB 内存。
- 系统：macOS 26.6.2。
- 编译器：Apple Clang（Command Line Tools）。
- SQLite：Homebrew SQLite 3.53.3，支持 load extension。
- 构建：CMake Release，`BUILD_SQLITE_EXTENSION=ON`、`TSFILE_BUILD_SHARED=ON`、`BUILD_TEST=ON`。
- 产物：`build/develop/lib/tsfile_sqlite.dylib` 和 `build/develop/test/lib/TsFile_Sqlite_Test`。

### 1.2 远端环境

- 机器：datadev，32 核，62GB 内存。
- 系统：Ubuntu 24.04，Linux 6.8，gcc 13.3。
- SQLite：3.45.1（`libsqlite3-dev`）。
- 构建：CMake Release + Ninja，关闭 ANTLR4/SIMD/SNAPPY/LZ4/LZOKAY/ZLIB/ZSTD。
- 数据目录：`/data/colin/tsfile-sqlite-bench`。

树莓派 4B（8GB，aarch64）已完成依赖安装和源码上传，本轮未完成构建。

### 1.3 构建与执行命令

```bash
cmake -S cpp -B build/develop -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON
cmake --build build/develop --target tsfile_sqlite TsFile_Sqlite_Test -j 8
DYLD_LIBRARY_PATH=build/develop/lib \
  build/develop/test/lib/TsFile_Sqlite_Test
```

完整依赖安装和平台差异见 [TESTING.md](TESTING.md)。

### 1.4 SQL 输入约定

- 每个自动化用例使用独立的 `:memory:` SQLite 数据库和 `mkdtemp` 临时目录。
- 下文的 `<TMP>` 表示该用例的临时目录，`<FILE>` 表示运行时生成的 TsFile 文件名。
- 测试夹具默认创建 `sensor` 表：

```sql
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  temperature DOUBLE FIELD,
  directory='<TMP>/sensor',
  timestamp_precision='ms'
);
```

- 用例中未重新声明表结构时，均使用这个 `sensor` 表。
- “预期输出”给出具体行数、值或 SQLite 错误码；“验证”记录实际执行结果。

## 2. 自动化自测

### 2.1 建表与声明

#### 2.1.1 `SqlDeclarationsAndNullableLogicalKeys`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE modern USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG, value DOUBLE FIELD,
  directory='<TMP>/modern', timestamp_precision='ms');

INSERT INTO modern VALUES(1,NULL,1.5),(1,'',2.5),(1,'null',3.5);
INSERT INTO modern VALUES(1,NULL,9);          -- 重复 NULL 键
UPDATE modern SET value=4 WHERE device IS NULL;
UPDATE modern SET device=NULL WHERE device=''; -- 与 NULL 行冲突
```

**构造方式**：同一时间戳下构造 NULL、空字符串、字符串 `'null'` 三种 device，再写入重复 NULL 键；随后更新 NULL 行，并尝试把空字符串行改成 NULL。

**预期输出**：前三行插入成功，`count(*)=3`；三种键分别命中 1 行；重复 NULL 插入返回 `SQLITE_CONSTRAINT`；更新 NULL 行成功；把空字符串改成 NULL 返回 `SQLITE_CONSTRAINT`。

**验证**：`IS NULL`、`=''`、`='null'` 各 1 行；两次约束错误均发生。实际：通过。

#### 2.1.2 `NoTagsAndInvalidDeclarations`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE times USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/times', timestamp_precision='ms');
INSERT INTO times VALUES(1,2);
INSERT INTO times VALUES(1,3);                -- 同一时间重复

CREATE VIRTUAL TABLE bad USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE,
  directory='<TMP>/bad', timestamp_precision='ms'); -- 缺少 FIELD

CREATE VIRTUAL TABLE bad USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/bad', timestamp_precision='ms', timestamp_precision='us');
```

**构造方式**：创建无 TAG 表，测试同一时间只能一行；再构造缺少列类别和重复精度的非法声明。

**预期输出**：第二次同一时间插入返回 `SQLITE_CONSTRAINT`；两个非法建表返回非 OK。

**验证**：约束错误和两次建表失败均发生。实际：通过。

#### 2.1.3 `QuotedSchemaAndRejectedEmptyPrecision`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE "odd.table" USING tsfile_hybrid(
  "event time" TIMESTAMP TIME,
  "sensor value" DOUBLE FIELD,
  directory='<TMP>/quoted', timestamp_precision='ms');

INSERT INTO "odd.table" VALUES(1,3.5);
SELECT tsfile_seal('main."odd.table"',2);
SELECT tsfile_export('main."odd.table"','<TMP>/quoted-output');

CREATE VIRTUAL TABLE inferred USING tsfile_hybrid(
  file='<TMP>/quoted-output/<FILE>.tsfile',
  source_table='odd.table');

CREATE VIRTUAL TABLE invalid USING tsfile_hybrid(
  file='<TMP>/quoted-output/<FILE>.tsfile',
  source_table='odd.table', timestamp_precision='');   -- 空精度
```

**构造方式**：用双引号创建带空格和点的表/列，写入一行后 seal、export，再用导出文件推断 schema。

**预期输出**：导出文件可读回 1 行；schema 保留 `event time`；空精度建表失败。

**验证**：导出计数 1；推断表查询 1 行；空精度返回非 OK。实际：通过。

#### 2.1.4 `AttachedDatabaseUsesItsOwnShadowTables`

**输入（SQL）**

```sql
ATTACH ':memory:' AS aux;
CREATE VIRTUAL TABLE aux.attached USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG, reading INT32 FIELD,
  directory='<TMP>/attached', timestamp_precision='ms');
INSERT INTO aux.attached VALUES(1,'d0',7);

SELECT count(*) FROM aux."attached_tsfile$hot";
SELECT count(*) FROM main.sqlite_master WHERE name='attached_tsfile$hot';
```

**构造方式**：在 `aux` 数据库中创建虚拟表并插入一行。

**预期输出**：`aux` 热表 1 行；`main` 中不存在同名影子表。

**验证**：计数为 1 和 0。实际：通过。

### 2.2 热区 CRUD 与类型

#### 2.2.1 `HotCrudAndSealRollback`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1.5),(2,'d0',2.5);
BEGIN;
SELECT tsfile_seal('main.sensor',2);
ROLLBACK;

SELECT count(*) FROM sensor;
SELECT count(*) FROM "sensor_tsfile$segments";

SELECT tsfile_seal('main.sensor',2);
SELECT count(*) FROM sensor WHERE time=1;
SELECT count(*) FROM sensor WHERE time=2;
INSERT INTO sensor VALUES(1,'d0',9.0);
```

**构造方式**：写入两行，在事务内 seal 后回滚；随后重新 seal，并写入早于 watermark 的时间。

**预期输出**：回滚后 2 行且段数 0；重新 seal 后段数 1；旧时间写入返回 `SQLITE_CONSTRAINT`。

**验证**：计数 2/0/1/1/1；旧时间插入失败。实际：通过。

#### 2.2.2 `Int32RangeAndColdRowsAreImmutable`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE ints USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG, reading INT32 FIELD,
  directory='<TMP>/ints', timestamp_precision='ms');

INSERT INTO ints VALUES(1,'d0',2147483648);   -- 越界
INSERT INTO ints VALUES(1,'d0',42);
SELECT tsfile_seal('main.ints',2);
UPDATE ints SET reading=43 WHERE time=1;
DELETE FROM ints WHERE time=1;
SELECT count(*) FROM ints WHERE time=1 AND reading=42;
```

**构造方式**：写越界 INT32，再写合法值并 seal，随后修改和删除冷行。

**预期输出**：越界返回 `SQLITE_MISMATCH`；冷行 UPDATE/DELETE 返回 `SQLITE_READONLY`；合法值仍为 42。

**验证**：错误码符合，查询 1 行。实际：通过。

#### 2.2.3 `NullAndBlobSurviveSeal`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE payload USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG,
  payload BLOB FIELD, note TEXT FIELD,
  directory='<TMP>/payload', timestamp_precision='ms');

INSERT INTO payload VALUES(1,'d0',X'000102','');
INSERT INTO payload VALUES(2,'d0',NULL,NULL);
SELECT tsfile_seal('main.payload',3);

SELECT count(*) FROM payload WHERE time=1 AND hex(payload)='000102';
SELECT count(*) FROM payload WHERE time=2 AND payload IS NULL AND note IS NULL;
SELECT count(*) FROM payload WHERE time=1 AND length(note)=0;
```

**构造方式**：写入二进制、空字符串和全 NULL 行，seal 后查询。

**预期输出**：三种值在 seal 前后保持一致，三个查询各 1 行。

**验证**：三个计数均为 1。实际：通过。

#### 2.2.4 `RowidNamedColumnsAndBooleanValues`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE aliases USING tsfile_hybrid(
  time TIMESTAMP TIME, rowid INT64 FIELD, oid INT64 FIELD,
  _rowid_ INT64 FIELD, flag BOOLEAN FIELD,
  directory='<TMP>/aliases', timestamp_precision='ms');

INSERT INTO aliases VALUES(1,-1,88,77,4294967296),(2,-1,55,44,0);
UPDATE aliases SET rowid=999 WHERE time=1;
DELETE FROM aliases WHERE time=2;
SELECT count(*) FROM aliases WHERE time=1 AND rowid=999 AND oid=88 AND _rowid_=77 AND flag=1;
SELECT tsfile_seal('main.aliases',3);
SELECT count(*) FROM aliases WHERE time=1 AND rowid=999 AND flag=1;
```

**构造方式**：业务列使用 `rowid`、`oid`、`_rowid_` 名称，并写入超 32 位布尔值。

**预期输出**：业务 rowid 不等于内部定位；更新、删除和 seal 后数据正确。

**验证**：更新后查询 1 行；删除后 1 行；seal 后仍 1 行。实际：通过。

#### 2.2.5 `BusinessColumnsDoNotCarryManagementCommands`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE commands USING tsfile_hybrid(
  time TIMESTAMP TIME, _tsfile_command STRING FIELD,
  directory='<TMP>/commands', timestamp_precision='ms');
INSERT INTO commands VALUES(1,'seal');
SELECT count(*) FROM commands WHERE _tsfile_command='seal';
SELECT count(*) FROM pragma_table_xinfo('commands');
SELECT tsfile_seal('main.commands',2);
```

**构造方式**：插入字符串 `'seal'` 作为普通业务列值。

**预期输出**：该值只作为数据返回；不触发管理操作；表列数为 2；seal 正常。

**验证**：查询 1 行、列数 2、seal 返回 1。实际：通过。

#### 2.2.6 `AllFieldTypesRoundTripAtExtremes`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE alltypes USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG,
  b BOOLEAN FIELD, i32 INT32 FIELD, i64 INT64 FIELD,
  f FLOAT FIELD, d DOUBLE FIELD, txt TEXT FIELD, s STRING FIELD,
  payload BLOB FIELD, day DATE FIELD, stamp TIMESTAMP FIELD,
  directory='<TMP>/alltypes', timestamp_precision='ms');

INSERT INTO alltypes VALUES
 (1,'d0',1,2147483647,9223372036854775807,1.5,2.25,'text','s',
  X'00FF',2147483647,9223372036854775807),
 (2,'d0',0,-2147483648,-9223372036854775808,-1.5,-2.25,'','',
  X'',-2147483648,-9223372036854775808),
 (3,'d0',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

SELECT tsfile_seal('main.alltypes',64);

SELECT count(*) FROM alltypes WHERE b=1;
SELECT count(*) FROM alltypes WHERE i32=2147483647;
SELECT count(*) FROM alltypes WHERE i32=-2147483648;
SELECT count(*) FROM alltypes WHERE i64=9223372036854775807;
SELECT count(*) FROM alltypes WHERE i64=-9223372036854775808;
SELECT count(*) FROM alltypes WHERE f=1.5;
SELECT count(*) FROM alltypes WHERE d=-2.25;
SELECT count(*) FROM alltypes WHERE txt='text';
SELECT count(*) FROM alltypes WHERE txt='';
SELECT count(*) FROM alltypes WHERE s='';
SELECT count(*) FROM alltypes WHERE hex(payload)='00FF';
SELECT count(*) FROM alltypes WHERE length(payload)=0;
SELECT count(*) FROM alltypes WHERE day=2147483647;
SELECT count(*) FROM alltypes WHERE stamp=-9223372036854775808;
SELECT count(*) FROM alltypes WHERE time=3 AND b IS NULL AND i32 IS NULL
  AND i64 IS NULL AND f IS NULL AND d IS NULL AND txt IS NULL
  AND s IS NULL AND payload IS NULL AND day IS NULL AND stamp IS NULL;
SELECT count(*) FROM tsfile_schema('main.alltypes');
```

**构造方式**：覆盖 10 种 FIELD 类型的最小值、最大值、空值、空字符串、空 BLOB 和全 NULL 行。

**预期输出**：每个单值查询 1 行；全 NULL 行 1 行；schema 12 列。

**验证**：所有查询和 schema 计数符合。实际：通过。

### 2.3 查询与下推

#### 2.3.1 `FieldPredicatesFilterHotAndColdRows`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES
 (1,'d0',1.5),(2,'d0',2.5),(3,'d1',3.5),(4,'d1',NULL),(5,'d0',5.0);

SELECT count(*) FROM sensor WHERE temperature>2.0;
SELECT tsfile_seal('main.sensor',2);
SELECT count(*) FROM sensor WHERE temperature>2.0;
SELECT count(*) FROM sensor WHERE temperature=2.5;
SELECT count(*) FROM sensor WHERE temperature>=2.0 AND temperature<5.0;
SELECT count(*) FROM sensor WHERE temperature>=2.0 AND device='d1';
SELECT count(*) FROM sensor WHERE time=4 AND temperature IS NULL;
SELECT count(*) FROM sensor WHERE temperature IS NULL;
SELECT count(*) FROM sensor WHERE temperature>100;
```

**构造方式**：先查询热区，seal 后用相同条件查询冷区。

**预期输出**：`>2.0` 热/冷均为 3；`=2.5` 1；区间 2；加 TAG 1；NULL 条件 1；`>100` 0。

**验证**：全部计数符合，热冷一致。实际：通过。

#### 2.3.2 `FieldPredicatesSupportTypedFields`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE typed USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG,
  label TEXT FIELD, qty INT32 FIELD, flag BOOLEAN FIELD,
  directory='<TMP>/typed', timestamp_precision='ms');

INSERT INTO typed VALUES
 (1,'d0','alpha',10,1),(2,'d0','beta',20,0),(3,'d1',NULL,30,1);
SELECT tsfile_seal('main.typed',2);

SELECT count(*) FROM typed WHERE label='alpha';
SELECT count(*) FROM typed WHERE label='ALPHA';
SELECT count(*) FROM typed WHERE qty>=20;
SELECT count(*) FROM typed WHERE flag=1;
SELECT count(*) FROM typed WHERE label='alpha' AND qty=10 AND flag=1;
SELECT count(*) FROM typed WHERE label IS NULL;
```

**构造方式**：写入文本、整数、布尔字段，seal 后做等值、范围和组合过滤。

**预期输出**：1/0/2/2/1/1 行。

**验证**：全部符合。实际：通过。

#### 2.3.3 `TimeBoundaryQueriesAndWatermarks`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES
 (9223372036854775807,'max',1.0),
 (-9223372036854775808,'min',2.0),
 (-1,'neg',3.0),(0,'zero',4.0),(1,'one',5.0);
SELECT tsfile_seal('main.sensor',64);

SELECT count(*) FROM sensor WHERE time=9223372036854775807;
SELECT count(*) FROM sensor WHERE time=-9223372036854775808;
SELECT count(*) FROM sensor WHERE time>=0 AND time<=1;
SELECT count(*) FROM sensor WHERE time>0 AND time<1;
SELECT count(*) FROM sensor WHERE time<0;
SELECT count(*) FROM sensor WHERE time BETWEEN -9223372036854775808 AND 0;
SELECT watermark IS NULL FROM tsfile_device_info('main.sensor') WHERE device_key='max';
SELECT watermark=-9223372036854775807 FROM tsfile_device_info('main.sensor') WHERE device_key='min';

INSERT INTO sensor VALUES(0,'max',9.0);             -- 应失败
INSERT INTO sensor VALUES(-9223372036854775808,'min',9.0); -- 应失败
INSERT INTO sensor VALUES(-9223372036854775807,'min',9.0); -- 应成功
```

**构造方式**：五个设备分别写入 `INT64_MIN`、`-1`、`0`、`1`、`INT64_MAX`，seal 后查询区间和设备边界。

**预期输出**：边界查询正确；最大设备 watermark 为 NULL；最小设备 watermark 为 `INT64_MIN+1`；旧时间被拒绝，`INT64_MIN+1` 成功。

**验证**：计数 5/1/1/2/0/2/3；两次约束失败；一次插入成功。实际：通过。

#### 2.3.4 `QueryOperatorAndResidualMatrix`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE ops USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG,
  temperature DOUBLE FIELD, label TEXT FIELD,
  directory='<TMP>/ops', timestamp_precision='ms');
INSERT INTO ops VALUES
 (1,'d0',1.5,'alpha'),(2,'d0',2.5,'beta'),(3,'d0',3.5,'gamma'),
 (4,'d1',4.5,'delta'),(5,'d1',NULL,NULL),(6,'d1',6.5,'epsilon');
SELECT tsfile_seal('main.ops',64);

SELECT count(*) FROM ops WHERE temperature=2.5;
SELECT count(*) FROM ops WHERE temperature<>2.5;
SELECT count(*) FROM ops WHERE temperature>3 AND temperature<6;
SELECT count(*) FROM ops WHERE temperature BETWEEN 2 AND 4;
SELECT count(*) FROM ops WHERE temperature IN (1.5,6.5);
SELECT count(*) FROM ops WHERE temperature IS NULL;
SELECT count(*) FROM ops WHERE temperature IS NOT NULL;
SELECT count(*) FROM ops WHERE NOT (temperature>3);
SELECT count(*) FROM ops WHERE device='d0' OR temperature>5;
SELECT count(*) FROM ops WHERE label LIKE 'a%';
SELECT count(*) FROM ops WHERE label LIKE '%ta';
SELECT count(*) FROM ops WHERE label GLOB 'a*';
SELECT group_concat(time,',') FROM
 (SELECT time FROM ops ORDER BY time DESC LIMIT 2);
SELECT group_concat(time,',') FROM
 (SELECT time FROM ops ORDER BY time DESC LIMIT 2 OFFSET 2);
SELECT count(*) FROM
 (SELECT device,count(*) AS cnt FROM ops GROUP BY device HAVING cnt=3);
```

**构造方式**：6 行冷数据，包含 NULL 温度和文本标签，覆盖比较、组合、空值、文本、排序分页和聚合。

**预期输出**：1/4/2/2/2/1/5/2/4/1/2/1/`6,5`/`4,3`/2。

**验证**：逐项符合；OR 查询验证了稳定冷行号。实际：通过。

#### 2.3.5 `MultiTagNullEmptyCombinations`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE multitag USING tsfile_hybrid(
  time TIMESTAMP TIME, t1 STRING TAG, t2 STRING TAG, value DOUBLE FIELD,
  directory='<TMP>/multitag', timestamp_precision='ms');
INSERT INTO multitag VALUES
 (1,NULL,NULL,1.0),(1,'',NULL,2.0),(1,NULL,'',3.0),
 (1,'','',4.0),(1,'a','b',5.0);
SELECT tsfile_seal('main.multitag',64);

SELECT count(*) FROM multitag WHERE t1 IS NULL AND t2 IS NULL;
SELECT count(*) FROM multitag WHERE t1='' AND t2 IS NULL;
SELECT count(*) FROM multitag WHERE t1 IS NULL AND t2='';
SELECT count(*) FROM multitag WHERE t1='' AND t2='';
SELECT count(*) FROM multitag WHERE t1='a' AND t2='b';
SELECT count(*) FROM tsfile_device_info('main.multitag');

INSERT INTO multitag VALUES(1,NULL,NULL,9.0);  -- 重复
INSERT INTO multitag VALUES(2,NULL,NULL,9.0);  -- 不同时间
SELECT hot_rows FROM tsfile_device_info('main.multitag')
 WHERE device_key='NULL,NULL';
```

**构造方式**：同一时间写入双 TAG 的 NULL/空/普通值 5 种组合，再测试重复组合。

**预期输出**：5 个组合各 1 行；设备数 5；重复组合约束失败；不同时间成功且该设备热行 1。

**验证**：计数和错误符合。实际：通过。

### 2.4 seal

#### 2.4.1 `SealSplitsOutputBySizeLimit`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE big USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG, payload BLOB FIELD,
  directory='<TMP>/big', timestamp_precision='ms');

WITH RECURSIVE seq(x) AS (
  VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x<12
)
INSERT INTO big
SELECT x*1000, 'd'||(x%2), randomblob(200000) FROM seq;

SELECT tsfile_seal('main.big',1);
SELECT file_count, hot_rows FROM tsfile_table_info('main.big');
SELECT count(*) FROM big;

INSERT INTO big VALUES(13000,'d0',randomblob(1200000));
SELECT tsfile_seal('main.big',1);  -- 单行超过 1MB
```

**构造方式**：12 行、每行 200KB，用 1MB 上限 seal；再插入 1.2MB 单行并 seal。

**预期输出**：首次返回 12，生成至少 2 个且每个不超过 1MB 的文件；查询 12 行；单行超限返回 `SQLITE_CONSTRAINT`，热行保留。

**验证**：首次 4 个文件、每个约 603KB；第二次失败且 `time=13000` 仍为热行。实际：通过。

#### 2.4.2 `NoTagSealingAndWholeStatementColdFailures`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE notags USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/notags', timestamp_precision='ms');
INSERT INTO notags VALUES(1,1.5),(2,2.5);
SELECT tsfile_seal('main.notags',2);
UPDATE OR FAIL notags SET value=9;
DELETE FROM notags;
INSERT INTO notags VALUES(3,3.5),(2,8);
SELECT tsfile_seal('main.notags',2);
INSERT INTO notags VALUES(3,3.5);
SELECT tsfile_seal('main.notags',4);
SELECT tsfile_seal('main.notags',5);
SELECT watermark FROM tsfile_table_info('main.notags');
```

**构造方式**：无 TAG 表 seal 两行，尝试修改冷行，再构造包含旧时间的多行插入和空 seal。

**预期输出**：冷行 UPDATE/DELETE 返回 `SQLITE_READONLY`；多行插入因一行旧时间整体失败；空 seal 0；后续 seal 1；watermark 4。

**验证**：错误码、行数和 watermark 符合。实际：通过。

#### 2.4.3 `SealPreservesPreexistingTemporarySymlink`

**输入（SQL）**

```sql
-- 前置：创建悬空符号链接
-- <TMP>/tsfilesensor-<pid>-0.tmp -> <TMP>/missing
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);
SELECT count(*) FROM sensor;
```

**构造方式**：在 seal 前占用预期的 `.tmp` 文件名，且指向不存在的目标。

**预期输出**：seal 跳过该名字，不覆盖符号链接；数据仍可查。

**验证**：符号链接仍存在且类型不变；查询 1 行。实际：通过。

#### 2.4.4 `CommitNeverOverwritesExistingDirectoryEntries`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1.5);
BEGIN;
SELECT tsfile_seal('main.sensor',2);
-- 在 COMMIT 前，在最终路径创建符号链接
COMMIT;
SELECT count(*) FROM sensor WHERE time=1 AND temperature=1.5;
SELECT hot_rows FROM tsfile_table_info('main.sensor');
```

**构造方式**：事务内 seal 产生 `.tmp`，在最终 `.tsfile` 路径放置符号链接后提交。

**预期输出**：COMMIT 返回错误；符号链接不被覆盖；热行保留。

**验证**：COMMIT 非 OK；`lstat` 仍为链接；查询 1 行、`hot_rows=1`。实际：通过。

### 2.5 compact

#### 2.5.1 `CompactMergesSmallSealedFiles`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1);
SELECT tsfile_seal('main.sensor',2);
INSERT INTO sensor VALUES(2,'d0',2);
SELECT tsfile_seal('main.sensor',2);
INSERT INTO sensor VALUES(3,'d0',3);
SELECT tsfile_seal('main.sensor',2);

SELECT tsfile_compact('main.sensor',10,20);
SELECT count(*) FROM "sensor_tsfile$segments";
SELECT count(*) FROM sensor;
SELECT watermark FROM tsfile_table_info('main.sensor');
SELECT count(*) FROM tsfile_verify('main.sensor') WHERE status='OK';
SELECT tsfile_compact('main.sensor',10,20);
SELECT tsfile_compact('main.sensor',20,10);
```

**构造方式**：三次单行 seal 生成 3 个小文件，再按 10MB/20MB 合并。

**预期输出**：compact 返回 1，段数和文件数降为 1，数据 3 行，watermark 4；verify OK；第二次 0；目标小于阈值报错。

**验证**：计数和返回值符合。实际：通过。

#### 2.5.2 `CompactRollbackKeepsOriginalFiles`

**输入（SQL）**

```sql
-- 三次 seal 生成 3 个文件
BEGIN;
SELECT tsfile_compact('main.sensor',10,20);
ROLLBACK;

SELECT count(*) FROM "sensor_tsfile$segments";
SELECT count(*) FROM sensor;
SELECT tsfile_compact('main.sensor',10,20);
```

**构造方式**：事务内 compact 后回滚，再重新 compact。

**预期输出**：回滚后 3 个段、3 行、3 个文件保留；重新 compact 返回 1。

**验证**：回滚后 3/3/3；再次 compact 1。实际：通过。

#### 2.5.3 `CompactThresholdAndChangedFile`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE bigrow USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG, payload BLOB FIELD,
  directory='<TMP>/compact-edge', timestamp_precision='ms');

INSERT INTO bigrow VALUES(1,'d0',X'00');
SELECT tsfile_seal('main.bigrow',64);
INSERT INTO bigrow VALUES(2,'d0',randomblob(1200000));
SELECT tsfile_seal('main.bigrow',64);

SELECT tsfile_compact('main.bigrow',1,2);
-- 覆盖其中一个 TsFile
SELECT tsfile_compact('main.bigrow',10,10);
```

**构造方式**：生成一个小文件和一个 1.2MB 文件；先用 1MB 阈值测试跳过，再修改文件测试一致性校验。

**预期输出**：阈值过滤后只剩小文件，返回 0；文件被修改后 compact 报错，原文件保留。

**验证**：第一次 0 且 2 个文件；第二次非 OK，`file_count=2`。实际：通过。

#### 2.5.4 `CompactCanSplitIntoMultipleOutputFiles`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE chunks USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG, payload BLOB FIELD,
  directory='<TMP>/chunks', timestamp_precision='ms');
INSERT INTO chunks VALUES(1,'d0',randomblob(600000));
SELECT tsfile_seal('main.chunks',64);
INSERT INTO chunks VALUES(2,'d0',randomblob(600000));
SELECT tsfile_seal('main.chunks',64);

SELECT tsfile_compact('main.chunks',1,1);
SELECT file_count FROM tsfile_table_info('main.chunks');
SELECT count(*) FROM chunks;
SELECT count(*) FROM tsfile_verify('main.chunks') WHERE status='OK';
```

**构造方式**：两个约 600KB 文件，目标 1MB；合并总量约 1.2MB，触发拆分。

**预期输出**：compact 返回 2，输出 2 个文件，数据 2 行，verify 两个 OK。

**验证**：2/2/2/2。实际：通过。

### 2.6 export

#### 2.6.1 `ExportCopiesSealedFilesAndRoundTrips`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,NULL,1.5),(2,'d0',2.5);
SELECT tsfile_export('main.sensor','<TMP>/export-early');  -- 未 seal

SELECT tsfile_seal('main.sensor',2);
SELECT tsfile_export('main.sensor','<TMP>/export');

CREATE VIRTUAL TABLE exported USING tsfile_hybrid(
  file='<TMP>/export/<FILE>.tsfile', source_table='sensor');
SELECT count(*) FROM exported;
SELECT count(*) FROM exported WHERE device IS NULL AND temperature=1.5;

INSERT INTO sensor VALUES(3,'d0',3.5);
SELECT tsfile_export('main.sensor','<TMP>/export');  -- 路径已存在
SELECT tsfile_export('main.sensor','<TMP>/export-hot'); -- 只导出已封存

BEGIN;
SELECT tsfile_export('main.sensor','<TMP>/export-txn');
ROLLBACK;
```

**构造方式**：先直接 export 热区表，再 seal/export，并用导出文件建只读表；测试重复路径和显式事务。

**预期输出**：首次 0 且目录空；seal 后 1；只读表 2 行；热区不导出；重复路径失败；事务内失败。

**验证**：返回值和错误符合。实际：通过。

#### 2.6.2 `ExportCountsEverySealedFile`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1);
SELECT tsfile_seal('main.sensor',2);
INSERT INTO sensor VALUES(2,'d0',2);
SELECT tsfile_seal('main.sensor',2);
SELECT tsfile_export('main.sensor','<TMP>/multi-export');
```

**构造方式**：两次 seal 生成两个自有文件后导出。

**预期输出**：返回 2，输出目录 2 个 TsFile。

**验证**：返回值 2，文件数 2。实际：通过。

#### 2.6.3 `ExportRejectsChangedSourceAndDuplicateNames`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);
CREATE VIRTUAL TABLE changed USING tsfile_hybrid(
  file='<FILE>', source_table='sensor');
-- 覆盖源文件内容
SELECT tsfile_export('main.changed','<TMP>/changed-export');

CREATE VIRTUAL TABLE dupa USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG, value DOUBLE FIELD,
  directory='<TMP>/dupA', timestamp_precision='ms');
INSERT INTO dupa VALUES(1,'d0',1.0);
SELECT tsfile_seal('main.dupa',2);
-- 把文件重命名为 dirA/same.tsfile；在 dirB 放同名文件
CREATE VIRTUAL TABLE dupsrc USING tsfile_hybrid(
  file='<TMP>/dupA', source_table='dupa');
INSERT INTO "dupsrc_tsfile$segments"
VALUES('<TMP>/dupB/same.tsfile',0,1,'dupa','x',<size>,0,0);
SELECT tsfile_export('main.dupsrc','<TMP>/duplicate-export');
```

**构造方式**：先修改只读源文件再导出；再构造两个不同目录、相同 basename 的段登记。

**预期输出**：两次导出都失败，目标目录不产生。

**验证**：两个 export 非 OK，目标目录不存在。实际：通过。

#### 2.6.4 `ExportFailureRetainsCommittedSealAndManagementGuards`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1);
SELECT tsfile_seal('main.sensor',2) FROM sensor;  -- 行查询中调用
SELECT hot_rows FROM tsfile_table_info('main.sensor');
SELECT tsfile_export('main.sensor','/tmp/<240字符路径>');
SELECT hot_rows FROM tsfile_table_info('main.sensor');

SELECT tsfile_seal('main.sensor',2);
SELECT tsfile_export('main.sensor','<TMP>/retry');
```

**构造方式**：在结果行上下文中调用 seal，再用超长路径 export，最后按正常流程重试。

**预期输出**：行查询调用被拒绝；超长路径失败且不自动 seal；重试成功。

**验证**：前两步错误、hot_rows 保持 1；seal 后 export 1。实际：通过。

#### 2.6.5 `DirectorySourceAndReadonlyExport`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1);
SELECT tsfile_seal('main.sensor',2);
INSERT INTO sensor VALUES(2,'d0',2);
SELECT tsfile_seal('main.sensor',2);

CREATE VIRTUAL TABLE history_dir USING tsfile_hybrid(
  file='<TMP>/sensor', source_table='sensor');
SELECT count(*) FROM history_dir;
SELECT mode, device_count, watermark, file_count
FROM tsfile_table_info('main.history_dir');
SELECT count(*) FROM tsfile_device_info('main.history_dir');
SELECT tsfile_export('main.history_dir','<TMP>/directory-export');
```

**构造方式**：以包含两个 TsFile 的目录创建只读表，查询并导出。

**预期输出**：2 行；mode=readonly，file_count=2；watermark 为 NULL；设备 1 个；export 返回 2。

**验证**：计数和 export 返回值符合，输出 2 个文件。实际：通过。

#### 2.6.6 `RawMultiTablePrecisionAndExportIsolation`

**输入（SQL）**

```sql
-- 前置：C++ TsFileWriter 生成包含 selected/unrelated/empty 的 raw.tsfile
CREATE VIRTUAL TABLE raw USING tsfile_hybrid(
  file='<TMP>/raw.tsfile', source_table='selected');
SELECT timestamp_precision FROM tsfile_table_info('main.raw');

CREATE VIRTUAL TABLE missing_precision USING tsfile_hybrid(
  file='<TMP>/raw.tsfile', source_table='selected',
  directory='<TMP>/missing-precision');           -- 应失败

CREATE VIRTUAL TABLE renamed USING tsfile_hybrid(
  file='<TMP>/raw.tsfile', source_table='selected',
  timestamp_precision='ms', directory='<TMP>/renamed');
SELECT watermark FROM tsfile_table_info('main.renamed');
INSERT INTO renamed VALUES(11,NULL,2.5);
SELECT tsfile_seal('main.renamed',2);
SELECT tsfile_export('main.renamed','<TMP>/selected-export');

CREATE VIRTUAL TABLE empty_source USING tsfile_hybrid(
  file='<TMP>/raw.tsfile', source_table='empty',
  timestamp_precision='ms', directory='<TMP>/empty');
SELECT count(*) FROM empty_source;
SELECT tsfile_export('main.empty_source','<TMP>/empty-output');
INSERT INTO empty_source VALUES(-9223372036854775808,NULL,1);
SELECT tsfile_seal('main.empty_source',1);
```

**构造方式**：用原始 TsFile 中的多张表测试推断、可写续表、空表和导出隔离。

**预期输出**：raw 精度 unknown；缺少精度的可写表失败；renamed watermark 11；导出只含 renamed；空表 export 0、可写后 seal 1。

**验证**：各查询、错误和导出文件内容符合。实际：通过。

### 2.7 管理函数边界

#### 2.7.1 `ManagementFunctionGuardsAndValidation`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1.5);

SELECT tsfile_seal('main.sensor','bad');
SELECT tsfile_seal('main.missing',2);
SELECT tsfile_seal('main.sensor',0);

SELECT tsfile_compact('main.sensor','bad',20);
SELECT tsfile_compact('main.sensor',0,20);
SELECT tsfile_compact('main.sensor',20,10);
SELECT tsfile_compact('main.sensor',9223372036854775807,9223372036854775807);
SELECT tsfile_compact('main.missing',1,2);

SELECT tsfile_export('main.sensor',1);
SELECT tsfile_export('main.missing','<TMP>/guard-missing');
SELECT tsfile_export('main.sensor','<TMP>/guard-parent-missing/child');
SELECT tsfile_export('main.sensor','<TMP>/sensor/inside');

SELECT tsfile_seal('main.sensor',2) FROM sensor;
SELECT tsfile_compact('main.sensor',1,2) FROM sensor;
SELECT tsfile_export('main.sensor','<TMP>/row-export') FROM sensor;

SELECT tsfile_seal('main.sensor',2);
CREATE VIRTUAL TABLE readonly USING tsfile_hybrid(
  file='<FILE>', source_table='sensor');
SELECT tsfile_seal('main.readonly',2);
SELECT tsfile_compact('main.readonly',1,2);
SELECT tsfile_export('main.readonly','<TMP>/readonly-export');
```

**构造方式**：逐项调用管理函数，输入错误类型、缺表、非法大小、非法路径、行查询上下文和只读表。

**预期输出**：参数错误 `SQLITE_MISUSE`；缺表错误；非法大小 `SQLITE_CONSTRAINT`；只读 seal/compact 为 `SQLITE_READONLY`；只读 export 成功。

**验证**：所有返回码符合；只读 export 返回 1。实际：通过。

### 2.8 事务与回滚

#### 2.8.1 `ManagementSealRespectsTransactionsAndSavepoints`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1),(2,NULL,2),(3,'d0',3);
BEGIN;
SELECT tsfile_seal('main.sensor',2);
SAVEPOINT outer_point;
INSERT INTO sensor VALUES(4,'d0',4);
SAVEPOINT inner_point;
INSERT INTO sensor VALUES(5,'d0',5);
ROLLBACK TO inner_point;
RELEASE inner_point;
ROLLBACK TO outer_point;
RELEASE outer_point;
ROLLBACK;

SELECT hot_rows FROM tsfile_table_info('main.sensor');
SELECT tsfile_seal('main.sensor',4);
SELECT count(*) FROM sensor;
SELECT count(*) FROM tsfile_verify('main.sensor') WHERE status='OK';
```

**构造方式**：外层事务中 seal，再用两层 savepoint 插入新行并逐层回滚。

**预期输出**：回滚到内层/外层的热行数分别为 1/0；整体回滚后恢复 3；重新 seal 后 0；verify OK。

**验证**：各阶段 `hot_rows` 为 1/0/3/0；最终 3 行。实际：通过。

#### 2.8.2 `RollbackPastTableCreationPreservesOuterTransaction`

**输入（SQL）**

```sql
CREATE TABLE keep(value);
BEGIN;
INSERT INTO keep VALUES(42);
SAVEPOINT creation;
CREATE VIRTUAL TABLE transient USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/transient', timestamp_precision='ms');
INSERT INTO transient VALUES(1,1.5);
SELECT tsfile_seal('main.transient',2);
ROLLBACK TO creation;
RELEASE creation;
COMMIT;

SELECT value FROM keep;
SELECT count(*) FROM sqlite_master WHERE name='transient';
```

**构造方式**：在外层事务和 savepoint 内创建、写入、seal 一个新虚拟表，然后回滚到创建前。

**预期输出**：keep=42；transient 不存在；临时目录被回收。

**验证**：查询 42；`sqlite_master` 0；目录不存在。实际：通过。

#### 2.8.3 `DiagnosticsAndInt64Exhaustion`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(9223372036854775807,'d0',1);
SELECT tsfile_seal('main.sensor',2);
SELECT tsfile_export('main.sensor','<TMP>/max');
SELECT watermark IS NULL FROM tsfile_device_info('main.sensor');
SELECT watermark IS NULL FROM tsfile_table_info('main.sensor');
INSERT INTO sensor VALUES(9223372036854775807,'d0',2);
SELECT tsfile_export('main.sensor','<TMP>/again');
-- 删除登记文件
SELECT count(*) FROM tsfile_verify('main.sensor') WHERE status='MISSING';
SELECT * FROM sensor;
```

**构造方式**：写入 `INT64_MAX` 并 seal，使设备 watermark 变为 NULL；再删除源文件并 verify。

**预期输出**：设备/表 watermark 均为 NULL；同设备最大时间写入失败；再次 export 成功；verify MISSING；查询失败。

**验证**：NULL 判断、约束错误、export 1、MISSING 1、查询非 OK。实际：通过。

### 2.9 重连与文件校验

#### 2.9.1 `PersistentReconnectAndConcurrentWatermark`

**输入（SQL）**

```sql
ATTACH '<TMP>/persistent.db' AS persisted;
CREATE VIRTUAL TABLE persisted.data USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/persisted', timestamp_precision='ms');
INSERT INTO persisted.data VALUES(1,1.5),(2,2.5);
SELECT tsfile_seal('persisted.data',2);
-- 第二个连接：读取、seal、写入旧/新时间
INSERT INTO data VALUES(2,8);    -- 旧时间，应失败
INSERT INTO data VALUES(3,3.5);  -- 新时间，应成功
SELECT count(*) FROM persisted.data;
DETACH persisted;
ATTACH '<TMP>/persistent.db' AS persisted;
SELECT count(*) FROM persisted.data;
SELECT hot_rows, watermark FROM tsfile_table_info('persisted.data');
```

**构造方式**：主连接 seal 后，第二个 SQLite 连接读写同一数据库，再断开重连。

**预期输出**：第二连接旧时间失败、新时间成功；重连后 3 行、hot_rows 1、watermark 3。

**验证**：计数和边界符合。实际：通过。

#### 2.9.2 `ReconnectedFileTableRestoresDeviceBoundaries`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(2000,'d1',1);
SELECT tsfile_seal('main.sensor',2);
ATTACH '<TMP>/file-reconnect.db' AS persisted;
CREATE VIRTUAL TABLE persisted.continued USING tsfile_hybrid(
  file='<FILE>', source_table='sensor', timestamp_precision='ms',
  directory='<TMP>/file-reconnect-hot');
INSERT INTO persisted.continued VALUES(2001,'d1',2);
INSERT INTO persisted.continued VALUES(1500,'d1',3); -- 应失败
DETACH persisted;
ATTACH '<TMP>/file-reconnect.db' AS persisted;
SELECT count(*) FROM persisted.continued;
INSERT INTO persisted.continued VALUES(1000,'d1',4); -- 应失败
INSERT INTO persisted.continued VALUES(1500,'new',5); -- 应成功
SELECT count(*) FROM persisted.continued;
```

**构造方式**：文件模式可写表写入合法时间，断开重连后测试同设备和 new 设备。

**预期输出**：重连后同设备旧时间失败，新设备旧时间成功；总数 3。

**验证**：两次约束错误和最终计数符合。实际：通过。

#### 2.9.3 `SourceSchemaPersistsWhenFileBecomesUnavailable`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1);
SELECT tsfile_seal('main.sensor',2);
ATTACH '<TMP>/source.db' AS persisted;
CREATE VIRTUAL TABLE persisted.source USING tsfile_hybrid(
  file='<FILE>', source_table='sensor', directory='<TMP>/source-hot');
INSERT INTO persisted.source VALUES(2,NULL,2);
DETACH persisted;
-- 重命名源文件
ATTACH '<TMP>/source.db' AS persisted;
SELECT count(*) FROM pragma_table_info('source','persisted');
SELECT count(*) FROM tsfile_verify('persisted.source') WHERE status='MISSING';
SELECT * FROM persisted.source;
SELECT hot_rows FROM tsfile_table_info('persisted.source');
-- 恢复源文件
SELECT count(*) FROM persisted.source;
```

**构造方式**：源文件表在连接期间被移走，再重连。

**预期输出**：schema 仍为 3 列；verify MISSING；查询失败；恢复后 2 行。

**验证**：3 列、MISSING 1、查询错误、hot_rows 1、恢复后 2 行。实际：通过。

#### 2.9.4 `SourceValidationAndPreserveUnknownFiles`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);

CREATE VIRTUAL TABLE bad USING tsfile_hybrid(
  file='<FILE>', source_table='missing');
CREATE VIRTUAL TABLE bad USING tsfile_hybrid(
  file='<FILE>', source_table='sensor', timestamp_precision='ns');
CREATE VIRTUAL TABLE bad USING tsfile_hybrid(
  time TIMESTAMP TIME, file='<FILE>', source_table='sensor');

-- 在自有目录创建 unrelated.tsfile
SELECT tsfile_seal('main.sensor',3);
```

**构造方式**：用错误的 source_table、冲突精度和显式列创建文件表；同时在目录放未知文件。

**预期输出**：三次建表失败；未知 `.tsfile` 不被删除。

**验证**：三次返回非 OK；未知文件仍存在。实际：通过。

#### 2.9.5 `VerifyReportsUnregisteredCorruptAndMismatch`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);

-- 在自有目录写入 unknown.tsfile
SELECT count(*) FROM tsfile_verify('main.sensor') WHERE status='UNREGISTERED';

-- 用另一张表的合法 TsFile 覆盖 sensor 的登记文件
CREATE VIRTUAL TABLE other USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG, temperature DOUBLE FIELD,
  directory='<TMP>/other', timestamp_precision='ms');
INSERT INTO other VALUES(7,'x',9.5);
SELECT tsfile_seal('main.other',2);
-- 复制 other 的 TsFile 覆盖 sensor 文件
SELECT count(*) FROM tsfile_verify('main.sensor') WHERE status='MISMATCH';

-- 再把登记文件写成垃圾
SELECT count(*) FROM tsfile_verify('main.sensor') WHERE status='CORRUPT';
SELECT * FROM tsfile_verify('main.missing');
```

**构造方式**：依次制造未登记文件、有效但 schema/内容不匹配的文件、无法解析的文件。

**预期输出**：三种状态各 1 行；缺表调用报错。

**验证**：UNREGISTERED/MISMATCH/CORRUPT 各 1；缺表非 OK。实际：通过。

### 2.10 目录、诊断与模块生命周期

#### 2.10.1 `DirectoryOwnershipAndCreationRollback`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE clash USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>', timestamp_precision='ms');
CREATE VIRTUAL TABLE nested USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/nested', timestamp_precision='ms');

BEGIN;
CREATE VIRTUAL TABLE aborted USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/rollback', timestamp_precision='ms');
ROLLBACK;

CREATE TABLE "collision_tsfile$config"(x);
CREATE VIRTUAL TABLE collision USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/collision', timestamp_precision='ms');
SELECT count(*) FROM pragma_table_info('collision_tsfile$config') WHERE name='x';
```

**构造方式**：使用其他表目录、嵌套目录、事务回滚和影子表名冲突。

**预期输出**：前两种建表失败；回滚目录被删除；冲突建表失败且原 config 表保留。

**验证**：错误、目录状态和 `x` 列计数符合。实际：通过。

#### 2.10.2 `CopiedDatabaseCannotShareWritableDirectory`

**输入（SQL）**

```sql
ATTACH '<TMP>/original.db' AS owner;
CREATE VIRTUAL TABLE owner.data USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/owned', timestamp_precision='ms');
INSERT INTO owner.data VALUES(1,1.5);
DETACH owner;

-- 复制 original.db 为 copy.db
ATTACH '<TMP>/copy.db' AS cloned;
INSERT INTO cloned.data VALUES(2,2.5);  -- 应失败
DETACH cloned;

ATTACH '<TMP>/original.db' AS renamed_schema;
INSERT INTO renamed_schema.data VALUES(2,2.5);
SELECT count(*) FROM renamed_schema.data;
```

**构造方式**：复制 SQLite 数据库文件后，用副本写同一目录。

**预期输出**：副本写入被拒绝；原数据库继续可写，最终 2 行。

**验证**：副本错误；原库计数 2。实际：通过。

#### 2.10.3 `DeviceInfoReportsPerDeviceWatermarks`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1),(5,'d1',2);
SELECT max_time, hot_rows FROM tsfile_device_info('main.sensor') WHERE device_key='d1';
SELECT tsfile_seal('main.sensor',2);
SELECT count(*) FROM tsfile_device_info('main.sensor');
SELECT watermark FROM tsfile_device_info('main.sensor') WHERE device_key='d0';
SELECT watermark FROM tsfile_device_info('main.sensor') WHERE device_key='d1';
SELECT max_time FROM tsfile_device_info('main.sensor') WHERE device_key='d0';
SELECT hot_rows FROM tsfile_device_info('main.sensor') WHERE device_key='d1';
SELECT device_count, watermark FROM tsfile_table_info('main.sensor');
```

**构造方式**：两个设备分别在时间 1 和 5 写入，seal 前后查询。

**预期输出**：seal 前 d1 max_time=5、hot_rows=1；seal 后设备数 2，watermark 2/6，表级 watermark 2。

**验证**：各字段符合。实际：通过。

#### 2.10.4 `DiagnosticsExposeSourceAndEmptyNoTagDevices`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);
SELECT directory IS NOT NULL, source_file IS NULL, source_table IS NULL
FROM tsfile_table_info('main.sensor');

CREATE VIRTUAL TABLE source_info USING tsfile_hybrid(
  file='<FILE>', source_table='sensor');
SELECT source_file, source_table, source_max_time, mode
FROM tsfile_table_info('main.source_info');

CREATE VIRTUAL TABLE empty_notags USING tsfile_hybrid(
  time TIMESTAMP TIME, value DOUBLE FIELD,
  directory='<TMP>/empty-notags', timestamp_precision='ms');
SELECT device_count, watermark FROM tsfile_table_info('main.empty_notags');
SELECT device_key, watermark, max_time, hot_rows
FROM tsfile_device_info('main.empty_notags');
INSERT INTO empty_notags VALUES(5,1.5);
SELECT max_time, hot_rows FROM tsfile_device_info('main.empty_notags');

SELECT * FROM tsfile_table_info('main.missing');
SELECT * FROM tsfile_device_info('main.missing');
```

**构造方式**：分别构造自有表、只读文件表、无 TAG 空表并查询诊断字段。

**预期输出**：自有表 directory 非空、source 为 NULL；只读表 source 字段正确、mode=readonly；空无 TAG 表 device_count=1，watermark=INT64_MIN，device_info 1 行占位；插入后 max_time=5、hot_rows=1；缺表报错。

**验证**：字段和计数符合。实际：通过。

#### 2.10.5 `SchemaFunctionListsTsfileTypes`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE typed_schema USING tsfile_hybrid(
  time TIMESTAMP TIME, group_id STRING TAG, device_id STRING TAG,
  d DOUBLE FIELD, i64 INT64 FIELD, i32 INT32 FIELD,
  flag BOOLEAN FIELD, label TEXT FIELD, payload BLOB FIELD, day DATE FIELD,
  directory='<TMP>/schema', timestamp_precision='ms');

SELECT cid, name, tsfile_type, category, sqlite_type, not_null
FROM tsfile_schema('main.typed_schema');
SELECT tsfile_type FROM tsfile_schema('main.typed_schema') WHERE name='time';
SELECT category FROM tsfile_schema('main.typed_schema') WHERE name='group_id';
SELECT sqlite_type FROM tsfile_schema('main.typed_schema') WHERE name='d';
SELECT not_null FROM tsfile_schema('main.typed_schema') WHERE name='time';
SELECT group_concat(name, ',') FROM tsfile_schema('main.typed_schema');
SELECT * FROM tsfile_schema('main.missing');
```

**构造方式**：创建 TIME + 双 TAG + 7 FIELD 的表，查询 schema。

**预期输出**：10 行；time 为 TIMESTAMP/TIME/INTEGER/not_null=1；TAG 为 STRING/TAG；DOUBLE 为 REAL；顺序为声明顺序；缺表报错。

**验证**：各字段、顺序和错误符合。实际：通过。

#### 2.10.6 `SchemaFunctionWorksForInferredFileTables`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);
CREATE VIRTUAL TABLE history_schema USING tsfile_hybrid(
  file='<FILE>', source_table='sensor');

SELECT count(*) FROM tsfile_schema('main.history_schema');
SELECT tsfile_type, category FROM tsfile_schema('main.history_schema')
WHERE name='time';
SELECT tsfile_type, category FROM tsfile_schema('main.history_schema')
WHERE name='device';
SELECT tsfile_type, category FROM tsfile_schema('main.history_schema')
WHERE name='temperature';
```

**构造方式**：用导出/封存文件推断 schema。

**预期输出**：3 列；time 为 TIMESTAMP TIME；device 为 STRING TAG；temperature 为 DOUBLE FIELD；device not_null=0。

**验证**：各字段符合。实际：通过。

#### 2.10.7 `HybridModuleLifecycleAndTagUpdates`

**输入（SQL）**

```sql
ALTER TABLE sensor RENAME TO sensor_renamed;

INSERT INTO sensor VALUES(1,'d0',1),(2,'d0',2);
UPDATE sensor SET device='d1' WHERE time=2 AND device='d0';
SELECT count(*) FROM sensor WHERE device='d0';
SELECT count(*) FROM sensor WHERE device='d1' AND time=2;
SELECT max_time, hot_rows FROM tsfile_device_info('main.sensor') WHERE device_key='d1';

DELETE FROM sensor WHERE time=2 AND device='d1';
SELECT hot_rows FROM tsfile_device_info('main.sensor') WHERE device_key='d1';
INSERT OR IGNORE INTO sensor VALUES(1,'d0',9);
INSERT OR FAIL INTO sensor VALUES(1,'d0',9);

SELECT tsfile_seal('main.sensor',2);
DROP TABLE sensor;
SELECT count(*) FROM sqlite_master WHERE name='sensor';
SELECT count(*) FROM sqlite_master WHERE name LIKE 'sensor_tsfile$%';
-- 检查已封存文件和 .tsfile-owner 仍存在
```

**构造方式**：尝试 RENAME；修改 TAG；删除热行；测试 IGNORE/FAIL；seal 后 DROP。

**预期输出**：RENAME 失败；TAG 修改后查询和 device_info 正确；删除后 hot_rows=0；IGNORE 成功、FAIL 失败；DROP 后影子表全消失，文件和目录标记保留。

**验证**：各查询和文件状态符合。实际：通过。

#### 2.10.8 `TempTableAndQuotedReservedIdentifiers`

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE temp.tempsensor USING tsfile_hybrid(
  time TIMESTAMP TIME, device STRING TAG, value DOUBLE FIELD,
  directory='<TMP>/temp', timestamp_precision='ms');
INSERT INTO temp.tempsensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('temp.tempsensor',2);
SELECT count(*) FROM temp.tempsensor;
SELECT mode FROM tsfile_table_info('temp.tempsensor');
DROP TABLE temp.tempsensor;

CREATE VIRTUAL TABLE "weird table" USING tsfile_hybrid(
  "select" TIMESTAMP TIME, "group" STRING TAG, "order" DOUBLE FIELD,
  directory='<TMP>/quoted-reserved', timestamp_precision='ms');
INSERT INTO "weird table" VALUES(1,'g',2.5);
SELECT tsfile_seal('main."weird table"',2);
SELECT count(*) FROM "weird table"
 WHERE "select"=1 AND "group"='g' AND "order"=2.5;
SELECT name || ':' || cid || ':' || sqlite_type || ':' || category
FROM tsfile_schema('main."weird table"') WHERE name='select';
```

**构造方式**：在 temp 库建表、seal、drop；再创建带空格表名和保留字列名的表。

**预期输出**：temp 表可管理、可 seal；保留字表查询 1 行；schema 返回 `select:0:INTEGER:TIME`。

**验证**：各操作成功。实际：通过。

### 2.11 外部文件

#### 2.11.1 `ExternalFileInferredSchemaAndAppendBoundary`

**输入（SQL）**

```sql
INSERT INTO sensor VALUES(1,'d0',1.5),(2,'d1',2.5);
SELECT tsfile_seal('main.sensor',3);

CREATE VIRTUAL TABLE history USING tsfile_hybrid(
  file='<FILE>', source_table='sensor');
SELECT count(*) FROM history;
SELECT type FROM pragma_table_info('history') WHERE name='time';
SELECT type FROM pragma_table_info('history') WHERE name='temperature';

CREATE VIRTUAL TABLE continuation USING tsfile_hybrid(
  file='<FILE>', source_table='sensor', directory='<TMP>/continuation');
INSERT INTO continuation VALUES(2,'d1',9);   -- 同一设备旧时间，应失败
INSERT INTO continuation VALUES(3,NULL,3.5);
UPDATE continuation SET temperature=4.5 WHERE time=3;
SELECT count(*) FROM continuation;
SELECT tsfile_seal('main.continuation',4);
SELECT count(*) FROM continuation WHERE time=3 AND device IS NULL;
```

**构造方式**：先 seal 源表，再分别创建只读 history 和可写 continuation。

**预期输出**：history 2 行、time 为 INTEGER、temperature 为 REAL；只读写入失败；同一设备旧时间失败；新设备成功；seal 后共 3 行。

**验证**：schema、错误和计数符合。实际：通过。

## 3. 手工 SQL 冒烟自测

以下测试使用 Homebrew `sqlite3` 加载扩展，在持久数据库上按用户操作顺序执行。

### 3.1 建表与写入

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE smoke USING tsfile_hybrid(
  time TIMESTAMP TIME, group_id STRING TAG, device_id STRING TAG,
  v1 DOUBLE FIELD, v2 INT64 FIELD, v3 FLOAT FIELD, v4 INT32 FIELD,
  v5 BOOLEAN FIELD,
  directory='<TMP>/smoke', timestamp_precision='ms');
INSERT INTO smoke VALUES
 (1000,'g0','d1',21.5,1,1.5,10,1),
 (2000,'g0','d1',22.0,2,2.5,20,0),
 (3000,'g1','d2',19.0,3,3.5,30,1),
 (4000,'g1','d2',20.0,4,4.5,40,0);
SELECT count(*) FROM smoke;
```

**构造方式**：创建 2 TAG、5 FIELD 的表，插入 4 行。

**预期输出**：`count(*)=4`。

**验证**：实际返回 4。通过。

### 3.2 热区更新和删除

**输入（SQL）**

```sql
UPDATE smoke SET v1=22.5 WHERE time=2000 AND device_id='d1';
DELETE FROM smoke WHERE time=4000 AND device_id='d2';
SELECT count(*) FROM smoke WHERE v1=22.5;
SELECT count(*) FROM smoke;
```

**构造方式**：更新一行、删除一行，再查询。

**预期输出**：更新值命中 1 行；剩余 3 行。

**验证**：实际为 1 和 3。通过。

### 3.3 热区 FIELD 过滤

**输入（SQL）**

```sql
SELECT count(*) FROM smoke WHERE v1>20;
```

**构造方式**：在热区执行 FIELD 比较。

**预期输出**：2 行（21.5 和 22.5）。

**验证**：实际 2 行。通过。

### 3.4 seal 与冷热合并查询

**输入（SQL）**

```sql
SELECT tsfile_seal('main.smoke',64);
SELECT count(*) FROM smoke;
SELECT count(*) FROM smoke WHERE v1>20;
```

**构造方式**：seal 3 行热数据后重新查询。

**预期输出**：seal 返回 3；总数 3；`v1>20` 为 2。

**验证**：实际 3/3/2。通过。

### 3.5 时间+TAG+FIELD 组合下推

**输入（SQL）**

```sql
SELECT count(*) FROM smoke
WHERE time>=1000 AND time<3000 AND group_id='g0' AND v1>20;
```

**构造方式**：冷区组合条件查询。

**预期输出**：2 行。

**验证**：实际 2。通过。

### 3.6 `tsfile_table_info`

**输入（SQL）**

```sql
SELECT hot_rows, device_count, file_count, watermark
FROM tsfile_table_info('main.smoke');
```

**构造方式**：seal 后查询表级状态。

**预期输出**：0、2、1、2001。

**验证**：实际值符合。通过。

### 3.7 `tsfile_device_info`

**输入（SQL）**

```sql
SELECT device_key, watermark, max_time
FROM tsfile_device_info('main.smoke');
```

**构造方式**：seal 后查询两个设备。

**预期输出**：`g0,d1` watermark=2001；`g1,d2` watermark=3001。

**验证**：实际两行值符合。通过。

### 3.8 `tsfile_schema`

**输入（SQL）**

```sql
SELECT cid, name, tsfile_type, category, sqlite_type, not_null
FROM tsfile_schema('main.smoke');
```

**构造方式**：查询 8 列的原始类型和类别。

**预期输出**：8 列，TIME/TAG/FIELD 和 SQLite 映射正确。

**验证**：实际 8 行，顺序和类型符合。通过。

### 3.9 export 与只读回读

**输入（SQL）**

```sql
SELECT tsfile_export('main.smoke','<TMP>/smoke-export');

CREATE VIRTUAL TABLE history USING tsfile_hybrid(
  file='<TMP>/smoke-export/<FILE>.tsfile', source_table='smoke');
SELECT count(*) FROM history;
SELECT count(*) FROM history WHERE v1>20;
INSERT INTO history VALUES(5000,'g0','d1',1.0,1,1.0,1,1);
```

**构造方式**：导出已封存文件，创建只读表并尝试写入。

**预期输出**：export 返回 1；只读表 3 行；过滤 2 行；写入返回错误。

**验证**：实际 1/3/2/错误。通过。

### 3.10 目录源与 compact

**输入（SQL）**

```sql
CREATE VIRTUAL TABLE history_dir USING tsfile_hybrid(
  file='<TMP>/smoke', source_table='smoke');
SELECT count(*) FROM history_dir;
SELECT mode FROM tsfile_table_info('main.history_dir');
SELECT tsfile_compact('main.smoke',10,20);
```

**构造方式**：以自有目录创建只读表；对只有一个文件的表 compact。

**预期输出**：3 行、mode=readonly、compact 返回 0。

**验证**：实际 3/readonly/0。通过。

### 3.11 verify 与错误路径

**输入（SQL）**

```sql
SELECT count(*) FROM tsfile_verify('main.smoke') WHERE status='OK';
INSERT INTO smoke VALUES(1000,'g0','d1',9.0,9,9.0,9,1);
INSERT INTO smoke VALUES(1000,'g1','d2',9.0,9,9.0,9,1);
DELETE FROM history WHERE time=1000;
SELECT * FROM tsfile_schema('main.missing');
```

**构造方式**：检查 verify 状态，触发重复键、旧时间和只读删除，再调用缺表 schema。

**预期输出**：verify 1；三个错误操作被拒绝；缺表报错。

**验证**：实际符合。通过。

手工冒烟共 35 项检查，全部通过。

## 4. 性能与内存自测

### 4.1 本地 macOS

**输入**：1000 设备，每设备 1 万行，共 1000 万行；每 100 万行 seal。

**构造方式**：C++ 驱动使用 prepared INSERT 逐行写入，每 100 万行提交并 seal；采样进程 RSS。

**预期输出**：写入稳定，seal 成功，内存不随已封存文件数增长。

**验证**：写入约 16.3 万行/秒；单次 seal 约 0.92 秒；峰值 RSS 约 1.1GB；输出约 61MB/10 个文件。

### 4.2 datadev 每 10 万行 seal

**输入**：1000 设备，每轮每设备 100 行，累计 10 万行 seal。

**构造方式**：逐设备轮转写入 100 个时间点，每轮提交并 seal；持续采样。

**预期输出**：写入持续，seal 10 万行成功，内存稳定。

**验证**：约 8.6 万行/秒；单次 seal 约 0.33 秒；峰值 RSS 约 146MB；运行 2804 轮（约 2.804 亿行）后手动停止，文件约 4.1GB。

### 4.3 datadev 单次 1000 万行 seal

**输入**：1000 万行热数据。

**构造方式**：写入后单次 seal，分别测物化版本和流式版本。

**预期输出**：流式版本内存有界且耗时相当或更短。

**验证**：流式 34.3 秒/4.7GB，物化 46.1 秒/11.0GB；流式输出 3 个文件约 114MB。

## 5. 问题回归

### 5.1 seal 内存放大

**输入**：1000 万行热数据。

**构造方式**：原实现一次性读取全部热行，修复后按 400 万行分块读取。

**预期输出**：峰值内存从线性增长降为有界。

**验证**：峰值从 11.0GB 降到 4.7GB，耗时从 46.1 秒降到 34.3 秒。

### 5.2 TEXT FIELD 条件误判

**输入**：`label='alpha'` 和 `label='ALPHA'`。

**构造方式**：字段下推后执行文本等值查询。

**预期输出**：只作为 FIELD 条件处理，大小写敏感。

**验证**：分别命中 1 和 0。通过。

### 5.3 回滚后 DETACH 锁定

**输入**：约束失败后的写入，然后 `DETACH`。

**构造方式**：触发错误，再 DETACH。

**预期输出**：DETACH 成功。

**验证**：修复后返回 OK。通过。

### 5.4 DROP 遗留设备影子表

**输入**：已封存的表执行 `DROP TABLE`。

**构造方式**：DROP 后检查 `sqlite_master` 和目录。

**预期输出**：hot/segments/devices/config 全部删除，文件和 `.tsfile-owner` 保留。

**验证**：影子表 0 行；文件和标记存在。通过。

### 5.5 冷数据 OR 查询丢行

**输入**：`device='d0' OR temperature>5`。

**构造方式**：seal 后执行 MULTI-INDEX OR；两次扫描按过滤结果生成行号。

**预期输出**：两个分支的行都返回，共 4 行。

**验证**：修复前 3 行；改为基于段、时间和 TAG 的稳定冷行号后 4 行。通过。

## 6. 未覆盖范围

函数入口、参数校验、只读/缺表、路径、状态码和事务回滚已经覆盖。仍无法在
常规单元测试中稳定复现的是操作系统级故障注入：磁盘写满、fsync 失败、
rename 与提交之间进程崩溃、权限变化导致 staging 清理失败。这些路径未计入
本轮通过结果。

## 7. 结论

- 47 个自动化用例全部通过，35 项手工 SQL 检查全部通过。
- 类型极值、时间边界、谓词矩阵、多 TAG 空值、compact 多输出、temp 表、
  引号/保留字标识符和模块生命周期均已覆盖。
- 补测发现并修复了冷数据 OR 查询行号不稳定导致丢行的问题。
- 流式 seal 已消除大热区 seal 的主要 OOM 风险；剩余风险集中在查询结果物化、
  compact 内存和低内存边缘设备的块大小上。
- 当前版本可以进入评审/联调。

## 附录 A：实际执行输出

以下输出来自 2026-11-03 在本机对当前工作树的实际执行。

### A.1 自动化测试

执行命令：

```bash
DYLD_LIBRARY_PATH=build/develop/lib \
  build/develop/test/lib/TsFile_Sqlite_Test
```

实际输出末尾：

```text
[----------] 47 tests from TsFileSqliteTest (200 ms total)

[----------] Global test environment tear-down
[==========] 47 tests from 1 test suite ran. (200 ms total)
[  PASSED  ] 47 tests.
```

### A.2 手工 SQL 冒烟

执行命令：

```bash
bash /tmp/sqlite_selftest.sh
```

实际输出：

```text
PASS  initial row count  -> 1
PASS  row count after insert  -> 4
PASS  update/delete  -> 1
PASS  query after delete  -> 3
PASS  field filter hot  -> 2
PASS  seal return rows  -> 3
PASS  query cold  -> 3
PASS  field filter cold  -> 2
PASS  time+tag+field pushdown  -> 2
PASS  table_info hot_rows  -> 0
PASS  table_info device_count  -> 2
PASS  table_info file_count  -> 1
PASS  table_info watermark  -> 2001
PASS  device_info rows  -> 2
PASS  device d1 watermark  -> 2001
PASS  device d2 watermark  -> 3001
PASS  schema column count  -> 8
PASS  schema time type  -> TIMESTAMP:TIME
PASS  schema tag type  -> STRING:TAG
PASS  schema field type  -> DOUBLE:FIELD
PASS  schema order  -> time,group_id,device_id,v1,v2,v3,v4,v5
PASS  export files  -> 1
PASS  exported file exists  -> 1
PASS  readonly file query  -> 3
PASS  readonly file field filter  -> 2
PASS  readonly insert rejected  (rejected: Error in 5th command line argument: table is readonly)
PASS  directory source query  -> 3
PASS  readonly table_info mode  -> readonly
PASS  compact no-op  -> 0
PASS  verify sensor  -> 1
PASS  verify history  -> 1
PASS  duplicate key rejected  (rejected: Error in 5th command line argument: row is outside the mutable watermark or violates NOT NULL)
PASS  watermark rejected  (rejected: Error in 5th command line argument: row is outside the mutable watermark or violates NOT NULL)
PASS  readonly delete rejected  (rejected: Error in 5th command line argument: table is readonly)
PASS  missing table rejected  (rejected: Error in 5th command line argument: TsFile table not found: main.missing)
----------------------------------------
PASS=35 FAIL=0
```

### A.3 代表性 SQL 结果集

以下命令在独立的 `:memory:` 数据库中执行，`<TMP>` 使用 `/tmp/tsfile-report-output`。

#### A.3.1 2.1.1 可空逻辑键

实际输出：

```text
3
1
1
1
4.0
duplicate NULL key:
Error in 5th command line argument: constraint failed
```

#### A.3.2 2.2.6 全部字段类型

实际输出：

```text
3
3|2|2|2|2|2|2|2|2|2|2
|0||
12
```

#### A.3.3 2.3.3 时间边界

实际输出：

```text
5
5
2
0
3
max||9223372036854775807
min|-9223372036854775807|-9223372036854775808
neg|0|-1
one|2|1
zero|1|0
```

#### A.3.4 2.3.4 谓词矩阵

实际输出：

```text
6
1
4
2
2
2
1
2
4
2
6,5
```

#### A.3.5 2.10.3 设备状态

实际输出：

```text
2
d0|2|1|0
d1|6|5|0
2|2
```

#### A.3.6 2.10.5 schema 函数

实际输出：

```text
0|time|TIMESTAMP|TIME|INTEGER|1
1|group_id|STRING|TAG|TEXT|0
2|device_id|STRING|TAG|TEXT|0
3|d|DOUBLE|FIELD|REAL|0
4|i64|INT64|FIELD|INTEGER|0
5|i32|INT32|FIELD|INTEGER|0
6|flag|BOOLEAN|FIELD|INTEGER|0
7|label|TEXT|FIELD|TEXT|0
8|payload|BLOB|FIELD|BLOB|0
9|day|DATE|FIELD|INTEGER|0
```

#### A.3.7 2.6.1 seal 与 export

实际输出：

```text
2
1
0|2|1|2
-rw-r--r--  1 colin  wheel  723  ...  tsfilesensor-84694-0.tsfile
```

