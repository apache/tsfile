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

# 树莓派 SQLite + TsFile 查询性能测试

日期：2026-11-04  
分支：`experiment-sqlite`  
脚本：[pi_query_bench.py](pi_query_bench.py)  
原始数据：[results/pi-query-20261104/](results/pi-query-20261104/)

## 1. 摘要

本轮在 Raspberry Pi 4B 上补测查询性能，覆盖热区、冷段和冷热混合三种路径。数据规模与写入报告一致：D500-M15、2,000,000 行冷数据、50k seal、37 个段文件；查询时额外写 50,000 行热数据形成混合场景。

核心结论：

- **点查、latest、单设备范围查询的延迟在约 0.1 秒量级**；热区点查约 38ms，冷段点查约 108ms，单设备冷范围查询约 111–132ms。
- **不带设备条件的全局时间范围查询非常慢**：冷段 1k/10k/100k 行范围都约 28 秒，每次查询实际读取约 410MB，几乎与返回行数无关。
- **`LIMIT 1` 在冷段约 55 秒**，比全表聚合还慢；`xFilter` 会先把全部命中行物化，LIMIT 没有下推。
- **`count(*)` 全表约 33.5 秒，而直接读 `_segments.row_count` 约 0.035 毫秒**。TsFile/segment 元数据已经有精确行数，但目前 `count(*)` 走不到这条路径。
- **TAG 过滤明显好于全局时间范围**：10 个设备的 count/avg 约 1.1–1.2 秒，因为设备级统计可以跳过其他设备。

## 2. 环境与数据

| 项目 | 配置 |
| --- | --- |
| 机器 | Raspberry Pi 4 Model B Rev 1.5，8GB |
| CPU | Cortex-A72 ×4 @1.8GHz，aarch64 |
| 系统 | Ubuntu 24.04.4 LTS |
| SQLite | 3.45.1，`enable_load_extension` 可用 |
| 扩展 | `/root/tsfile-src/cpp/build/pi/lib/tsfile_sqlite.so` |
| 表 | `time TIMESTAMP TIME`、`device STRING TAG`、15 个 `DOUBLE FIELD` |
| 冷数据 | 2,000,000 行，500 设备，每设备 4,000 行 |
| 段文件 | 37 个，合计 254.5MiB |
| 热数据 | 预热阶段 200,000 行；混合阶段再写 50,000 行 |
| seal | 50,000 行触发，`seal_max_mb=1024` |
| SQLite DB/WAL | 39.6MiB / 39.8MiB |
| 写入阶段 | 590.1s（含 37 次 seal） |
| 进程 RSS | 均值约 750MB，峰值约 2.33GB |

写入阶段和查询阶段在同一个进程、同一个连接中完成；查询延迟包含 SQLite、扩展、Python `fetch` 和结果物化，不是纯 C++ 扩展延迟。

## 3. 测试方法

脚本先写 200,000 行热数据，跑热区查询；再 seal 并继续写到 2,000,000 行，跑冷段查询；最后追加 50,000 行热数据，跑混合查询。

每个查询先执行 1 次预热，再按配置次数重复；记录 p50/p95/p99、返回行数、RSS 峰值和 `/proc/self/io` 的 `rchar`/`read_bytes`。结果集用 `fetchall()` 或 `fetchone()` 完整取出，因此包含客户端取数成本。每组配置只运行一次，页面缓存为热缓存。

查询类型：

| 名称 | SQL 形状 |
| --- | --- |
| point | 固定 device + time，取 1 个测点 |
| latest | 固定 device，`ORDER BY time DESC LIMIT 1` |
| range_1k/10k/100k | 全局时间范围，取 `time,m00000`，`ORDER BY time` |
| tag_10 | 10 个 device 的 `count(*)` + `avg(m00000)` |
| field_gt99 | `m00000 > 99.0` 的 `count(*)` |
| aggregate | 全表 `count(*)` + `avg(m00000)` |
| limit1 | `SELECT * FROM sensor LIMIT 1` |
| metadata_count | 直接查 `sensor_tsfile$segments` 的 `SUM(row_count)`，不是正常查询路径，仅用于对比 |

## 4. 结果

### 4.1 主矩阵 p50 延迟

| 查询 | 热区 200k 行 | 冷段 2M 行 | 混合 2.05M 行 |
| --- | ---: | ---: | ---: |
| point | 37.6ms | 108.0ms | 119.1ms |
| latest | 38.4ms | 110.1ms | 120.0ms |
| range_1k（全局时间） | 40.0ms | **27.9s** | **28.0s** |
| range_10k（全局时间） | 102.7ms | **27.9s** | **28.0s** |
| range_100k（全局时间） | 744.6ms | **28.7s** | **28.5s** |
| tag_10 | 367.6ms | 1.09s | 1.20s |
| field_gt99 | 37.5ms | **28.4s** | **28.6s** |
| aggregate | 915.4ms | **34.9s** | **33.8s** |
| limit1 | 1.33s | **55.4s** | **55.8s** |
| metadata_count | — | 0.0ms | — |

### 4.2 冷段读取量

`/proc/self/io` 的 `read_bytes` 在页面缓存下基本为 0，因此看 `rchar`。下表是每次查询的平均 `rchar`：

| 查询 | 每次查询读取 |
| --- | ---: |
| point | 约 1.3MB |
| latest | 约 1.3MB |
| range_1k（全局） | 约 410MB |
| range_10k（全局） | 约 410MB |
| range_100k（全局） | 约 410MB |
| tag_10 | 约 12.8MB |
| field_gt99 | 约 383MB |
| aggregate | 约 368MB |
| limit1 | 约 1.17GB |

全局时间范围查询的读取量与范围大小基本无关，说明它不是按范围裁剪，而是把每个设备的 chunk 都读了一遍再过滤。

### 4.3 设备条件对比

在同一份冷数据上补测了带 `device` 条件的查询：

| 查询 | p50 |
| --- | ---: |
| point（device+time） | 127.5ms |
| latest（device） | 127.3ms |
| device_count（单个 device 的 count） | 117.7ms |
| device_range_1k（device + 时间范围，返回 1000 行） | 111.0ms |
| device_range_all（device + 全时间范围，返回 4100 行） | 131.7ms |
| device_avg（单个 device 的 count+avg） | 120.0ms |
| global_count（全表 count） | **33.5s** |
| metadata_count（`SUM(row_count)`） | **0.035ms** |

带 device 条件后，单设备范围查询从约 28 秒降到约 0.11 秒。差异不是数据量，而是查询能否利用设备级/chunk 级统计跳过其他设备。

### 4.4 P0 段级时间裁剪复测

P0 给 `_segments` 增加了 `min_time`/`max_time`，`read_cold` 在打开段文件前先按查询时间范围跳过无关段。用同一形状的数据（D500-M15、2,000,000 行、37 个段）复测关键查询：

| 查询 | 优化前 | P0 后 | 读取量变化 |
| --- | ---: | ---: | ---: |
| 全局时间范围 1k | 27.9s | **0.737s** | 410MB → 12.0MB |
| 全局时间范围 10k | 27.9s | **0.748s** | 410MB → 12.0MB |
| 全局时间范围 100k | 28.7s | **1.307s** | 410MB → 13.8MB |
| 单设备点查 | 108ms | 91ms | 1.3MB |
| latest | 110ms | 92.8ms | 1.3MB |
| 单设备时间范围 1k | 111ms | **21.6ms** | 0.25MB |
| metadata count | 0.035ms | 0.038ms | 0.0001MB |

说明：

- 1k/10k 范围通常落在第一个 200k 行段内，所以延迟接近：两者都只打开一个段；
- 100k 范围仍在同一个 200k 行段内，但返回行更多，延迟升到约 1.3s；
- `latest` 没有时间条件，P0 不会跳过段，只有设备过滤生效，所以只从 110ms 降到 93ms；
- 这次复测只跑关键查询，不是完整的热/冷/混合矩阵；原始数据见 [results/pi-query-20261104/p0-results.json](results/pi-query-20261104/p0-results.json)。

### 4.5 P0 的成本

P0 的写入侧成本是一次额外的内存扫描：`write_limited_files` 在写每个输出文件前，对已经读入内存的 `Row` 求一次 min/max。没有额外打开 TsFile、没有额外文件 I/O，也不改变 TsFile 格式。

在 Pi 上对 D100-M15、200,000 行、4 次 seal 做前后对比（两次运行）：

| 版本 | 写入 s | seal s | 总墙钟 s |
| --- | ---: | ---: | ---: |
| pre-P0 | 38.0 / 37.8 | 10.67 / 10.71 | 50.65 / 50.57 |
| P0 | 38.1 / 39.1 | 8.68 / 10.39 | 49.02 / 51.52 |

差异在 SD 卡和运行顺序的波动范围内，没有观察到可测的写入回退。其他成本：

- **存储**：`_segments` 每段多两个 INTEGER 列，37 个段只有约几百字节到 1KB；不是每行存储。
- **查询读取**：每个段多读 `min_time`/`max_time` 两个整数，37 个段量级；相比跳过的 410MB 数据可以忽略。
- **连接/迁移**：`xConnect` 执行一次 `PRAGMA table_info` 检查；老库第一次连接会 `ALTER TABLE` 补两列。`_segments` 只有每段一行，DDL 很轻。
- **source 文件导入**：min/max 在已有的逐行扫描里顺便累计，没有额外扫描。
- **compact**：输出文件需要同样的 min/max 扫描，但 compact 本来就把行读入内存，额外成本与 seal 同级。

限制：老数据库的已有段在迁移后 `min_time/max_time` 为 `NULL`，不会获得段级跳过；需要重新 seal/compact 或后续做一次 metadata backfill。

## 5. 分析

### 5.1 为什么全局时间范围查询约 28 秒

表按设备组织，每个设备的 4,000 行跨越整个时间轴。一个全局时间范围（例如最早的 1,000 个时间点）对大多数设备来说，chunk 的 `start_time/end_time` 都会与该范围重叠，设备级 time filter 无法整块跳过；于是 500 个设备的所有 chunk 都被读取，最后只留下 1,000 行。

因此“范围越小越快”在这里不成立：1k、10k、100k 的冷查询都读取约 410MB，p50 都在 28 秒左右。带 `device=?` 后，其他 499 个设备可以被设备级统计跳过，延迟降到约 0.11 秒。

这不是一个简单的“加索引”问题；需要 page 级时间统计、按时间分段的布局，或者把全局时间范围查询改成按设备并行/分区扫描。

### 5.2 `LIMIT 1` 为什么比聚合还慢

`xFilter` 会把所有命中行 `push_back` 到 `HybridCursor::rows`，SQLite 的 `LIMIT` 在 `xFilter` 之后才生效。`SELECT * FROM sensor LIMIT 1` 仍然物化全部 2,050,000 行、17 列，因此比只做 count/avg 的聚合更慢（55.4s vs 34.9s）。

这和 QR-09 的现象一致：LIMIT 不会减少扩展侧的读取和物化。要改善，需要把冷查询改成流式 cursor，在 `xNext` 中按 block 取数，并在达到 LIMIT 后停止。

### 5.3 `count(*)` 能不能用统计信息

可以，而且差距很大：

- `_segments` 已经保存每个 segment 的精确 `row_count`；无过滤的 `count(*)` 只需要 `热行数 + SUM(row_count)`。
- TsFile 的 chunk/statistic 还包含 `count_`、`start_time_`、`end_time_`、min/max，数值列还有 `sum_value_`。带时间/数值过滤的 count/avg 在原理上可以用这些统计做裁剪和聚合。
- 但当前 SQLite 虚拟表 API 不会把 `COUNT(*)`/`AVG()` 下推给 `xFilter`；SQLite 仍然逐行调用 `xNext`。所以即使有统计信息，普通 `SELECT count(*) FROM sensor` 还是走全量物化。

可行方向：

1. 增加 `tsfile_count(table, filters)` / `tsfile_aggregate(table, filters)` 管理函数，直接读 `_segments` 和 TsFile metadata；
2. 或在 `_segments` 中缓存每个 segment 的时间范围、行数、数值列的 count/sum，让 count/avg 不打开 TsFile；
3. 对带过滤的 count，需要处理 chunk 边界（部分 chunk 命中）和 aligned 表的 NULL 语义，不能简单把整个 chunk 的 count 相加。

本次的 `metadata_count`（0.035ms）只证明了无过滤 count 的潜力；`avg` 和带过滤 count 仍需单独设计。

### 5.4 TAG/FIELD 过滤

- TAG 过滤 10 个设备约 1.1s：设备级统计能跳过其余 490 个设备，因此比全局时间范围好得多。
- FIELD 过滤 `m00000 > 99.0` 仍约 28s：每个设备的 chunk 都包含大于 99 和小于 99 的值，chunk 级 min/max 无法整块排除；需要 page 级统计或流式过滤。
- 单设备 `count/avg` 约 120ms，说明设备过滤是当前最有效的裁剪条件。

## 6. 建议

按收益排序：

1. **为 `count(*)` / `avg()` 增加元数据快路径**：无过滤时直接使用 `_segments.row_count`（以及缓存 sum/count）；这是 33.5s → 0.035ms 量级的差距。
2. **提供设备条件优先的查询建议/接口**：全局时间范围在按设备存储下是坏形状；文档应建议 `device=?` 或设备集合过滤，或提供按时间分区的查询入口。
3. **实现流式冷查询 + LIMIT 下推**：避免 `LIMIT 1` 物化全部 2M 行。
4. **增加 page 级时间/数值统计**：让全局范围查询和 FIELD 过滤能跳过页，而不是只依赖 chunk 级统计。
5. **缓存/预计算 aggregate 统计**：在 seal 时把每个 segment 的 count/sum/min/max 写入 `_segments`，避免重复打开 TsFile。

## 7. 局限

- 每组查询只运行一次；查询顺序固定，页面缓存为热缓存。
- 延迟包含 Python `fetchall()`/`fetchone()` 和结果物化；不是纯 C++ reader 的延迟。
- 只测单连接、无并发、无写入并发；没有测 SQLite planner 的其他计划。
- 全局时间范围查询是刻意保留的对照，实际业务应优先带 device 条件。
- `rchar` 包含重复查询和缓存影响；`read_bytes` 在页面缓存下为 0，不能代表真实磁盘读取。
- 数据布局为 500 设备 × 4,000 行，chunk 覆盖整个设备时间轴；不同 chunk/page 大小会改变结果。

## 8. 复现

```bash
python3 cpp/src/sqlite/bench/pi_query_bench.py \
  --devices 500 --measurements 15 --rows 2000000 \
  --hot-prefix 200000 --extra-hot 50000 \
  --batch-rows 1000 --seal-rows 50000 --seal-max-mb 1024 \
  --db /root/tsfile-bench/query-main/bench.db \
  --dir /root/tsfile-bench/query-main/segments \
  --ext /root/tsfile-src/cpp/build/pi/lib/tsfile_sqlite.so \
  --out /root/tsfile-bench/query-main.json \
  --clean --keep --quiet
```

设备条件对比是在同一份数据上单独执行的，结果见 `results/pi-query-20261104/query-main.json` 和本文 4.3 节。
