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

# TsFileDataFrame 2880 窗口冷缓存性能报告

日期：2026-08-14

## 1. 结论

这轮结果不能概括成单一的“新实现更快”。在每组测试前都清理 Linux OS page cache、随机读取 2880 行窗口时，新 Runtime 同时呈现出两个方向相反且都很明确的结果：

- DataFrame 冷构造从 46.24–49.11 秒降至 0.54–0.55 秒，快 83.9–90.4 倍。
- 随机查询后的总 PSS 从 5.50/10.92/21.77 GiB 降至 153.96/245.31/413.54 MiB，低 36.5–53.9 倍；4 进程 PSS 只占机器物理内存的 0.643%，旧实现为 34.666%。
- 首次随机查询延迟回退 2.74–4.10 倍。
- 后续随机查询 p50 回退 4.74–6.06 倍，聚合 QPS 下降 81.1%–82.8%。

因此，持久化 mmap 索引和进程本地 Runtime 已经实现预期的启动时间与多进程内存目标，但严格冷缓存下的按需查询路径存在稳定且显著的性能回退。当前结果不支持宣称“查询性能没有下降”。在完成冷路径定位和优化前，不建议把本实现按“查询性能无回退”验收。

## 2. 被测环境与数据规模

### 2.1 机器

|项目|值|
|---|---:|
|主机|`vm-127`（`192.168.99.110`）|
|CPU|32 vCPU，AMD EPYC 9J14，2 sockets × 16 cores|
|物理内存|67,417,718,784 bytes（约 62.79 GiB）|
|swap|4 GiB；所有正式测试中均为 0 使用|
|系统|Ubuntu/Linux 6.8.0-136-generic，x86_64|
|Python|3.13.14|

### 2.2 数据

`/data/lotsa` 是多个独立 schema 数据集的集合，不能整体作为一个 DataFrame。本次继续选择其中最大的完整数据集 `/data/lotsa/buildings_900k`。

|项目|`/data/lotsa` 全部数据|本次数据集 `buildings_900k`|
|---|---:|---:|
|独立数据集目录|116|1|
|TsFile 数|647|61|
|TsFile bytes|555,096,347,047|65,856,302,628|
|TsFile GiB|516.97|61.33|
|逻辑序列数|不跨 schema 聚合|1,792,328|

索引文件规模如下：

|索引|bytes|MiB|占 TsFile 数据比例|
|---|---:|---:|---:|
|PyPI 2.4.0 `.tsfidx`|248,139,282|236.64|0.377%|
|新 Runtime `.tsidx`|506,136,064|482.69|0.769%|

### 2.3 实现

|标识|实现|
|---|---|
|旧实现|隔离 venv 中通过 `pip install tsfile` 安装的 PyPI `tsfile==2.4.0`|
|新实现|`feature/tsfile-dataframe-runtime`，本地验收提交 `b4e61644e`，包版本 `2.3.2.dev0`|

## 3. 测试方法

### 3.1 固定随机 workload

随机种子为 `20260814`。为了让 2880 行窗口在两个实现上都可执行，先用 PyPI 2.4.0 从固定随机候选中筛选公共 workload：

- 共检查 517 条不同的候选序列。
- 接受 516 条能够完整返回 2880 行的序列。
- 剔除 1 条；旧实现读取它时报告跨 shard 重复时间戳。
- 516 条序列分给 4 个 worker，每个 worker 129 条：1 条用于首次查询，另外 128 条用于随机查询。
- worker 之间没有重复序列。1 进程使用 worker 0；2 进程使用 worker 0–1；4 进程使用 worker 0–3，因此 1/2/4 进程 workload 是嵌套可比的。
- 新旧实现及全部三轮测试的 worker workload SHA-256 完全一致。

4 个 worker 的 workload SHA-256 为：

```text
worker 0: 7d86fb92d6f83c96d03886df8a7cbd159de1f2d833b2796564d0fe030c5b5d5b
worker 1: 553043633e37501a546ff4c07dd76ee667a59102dde12965bee9545f2b6ca190
worker 2: 83755d3fb5daa66be368ac55deef0692ca6e8510585c3438842ba2355d18eacb
worker 3: d5667a7c4942469ad0e426ada42e2c0e60b39e2c0b029d87a9e0d627d00d89d9
```

### 3.2 冷缓存与执行顺序

每一个“实现 × 进程数 × 轮次”都是独立测试组。每组启动 worker 之前，由 root 执行：

```text
os.sync()
write 3 to /proc/sys/vm/drop_caches
```

共执行 18 个正式测试组：2 个实现 × 1/2/4 进程 × 3 轮。每次清理后 `/proc/meminfo` 中 `Cached` 为 97,536–98,536 KiB（95.25–96.23 MiB），所有组的 swap 使用量均为 0。

每组内部顺序为：

1. 清理 OS cache。
2. spawn 1/2/4 个 worker；每个 worker 在清理后才 import 被测包并构造 DataFrame。
3. 所有 worker 构造完成后，采集查询前 RSS/PSS/USS/FD。
4. barrier 同步后，每个 worker 查询自己的第一条随机序列。
5. 再次 barrier 同步后，每个 worker依次查询另外 128 条不同的随机序列。
6. 清理临时结果对象并执行 Python GC，在 DataFrame 和 Runtime 仍存活时采集查询后内存。

单次查询计时包含 `dataframe[position]`、读取 `series[:2880]` 和结果物化，不提前调用 `len(series)`，也不提前构造 PreparedSeries。全部首次查询和随机查询都实际返回 2880 行。

报告数值取三轮中位数。首次查询的 p50 是组内 worker 首次查询 p50，再取三轮中位数；随机查询分位数在每组的全部 `128 × 进程数` 次查询上计算，再取三轮中位数。聚合 QPS 使用“总查询数 / 最慢 worker 的纯查询 wall time”，不包含查询完成后的 Python GC。

## 4. 冷构造结果

|进程数|PyPI 2.4.0 构造 p50|新 Runtime 构造 p50|加速|
|---:|---:|---:|---:|
|1|46,238.57 ms|551.19 ms|83.89×|
|2|47,627.85 ms|548.70 ms|86.80×|
|4|49,111.47 ms|543.32 ms|90.39×|

这里的“构造 p50”是每个并发组内 worker 构造延迟的中位数。旧实现把完整 metadata catalog 保存在每个进程的 Python heap；新实现打开并映射已有 `.tsidx`。两者构造语义不同，但都代表应用调用 `TsFileDataFrame(dataset)` 后可开始查询的实际成本。

## 5. 内存结果

下面是每个 worker 完成 1 次首次查询和 128 次随机查询后，DataFrame/Runtime 仍保持存活时的进程组总量。PSS 最接近该进程组对机器物理内存的实际占用，因此“物理内存占比”以 PSS 计算；RSS 会在不同进程中重复统计共享 file-backed 页，USS 只统计私有页。

|进程数|实现|总 RSS|总 PSS|总 USS|PSS/物理内存|总 FD|
|---:|---|---:|---:|---:|---:|---:|
|1|PyPI 2.4.0|5,634.98 MiB|5,626.21 MiB|5,621.88 MiB|8.751%|70|
|1|新 Runtime|162.67 MiB|153.96 MiB|149.63 MiB|0.239%|27|
|2|PyPI 2.4.0|11,254.58 MiB|11,190.89 MiB|11,141.01 MiB|17.406%|140|
|2|新 Runtime|322.94 MiB|245.31 MiB|181.30 MiB|0.382%|54|
|4|PyPI 2.4.0|22,463.25 MiB|22,288.24 MiB|22,235.87 MiB|34.666%|280|
|4|新 Runtime|642.50 MiB|413.54 MiB|326.59 MiB|0.643%|108|

按 PSS 比较，新 Runtime 分别减少 36.54×、45.62×、53.90×；按 USS 比较，分别减少 37.57×、61.45×、68.08×。

新 Runtime 的查询前 PSS 为 106.31/157.82/258.57 MiB，随机查询后增至 153.96/245.31/413.54 MiB。这部分增长来自本次 129 条/worker 随机序列访问后仍存活的 Reader、PreparedSeries 和 file-backed working set。即便采用查询后的保守数字，4 进程仍只占 0.643% 物理内存。

## 6. 首次随机查询

首次查询发生在冷 cache 下构造 DataFrame 之后，且每个 worker 选择不同的随机序列。

|进程数|PyPI 2.4.0 p50|新 Runtime p50|新/旧|延迟变化|
|---:|---:|---:|---:|---:|
|1|4.170 ms|11.424 ms|2.74×|+174.0%|
|2|4.310 ms|13.724 ms|3.18×|+218.4%|
|4|4.259 ms|17.447 ms|4.10×|+309.6%|

首次查询随新 Runtime 的并发进程数增加而变慢，1→4 进程从 11.424 ms 增至 17.447 ms；这符合多个进程同时从冷文件系统读取按需 metadata/data 页时的 I/O 竞争特征。

## 7. 随机查询

|进程数|实现|p50|p95|p99|max|聚合 QPS|
|---:|---|---:|---:|---:|---:|---:|
|1|PyPI 2.4.0|1.245 ms|1.462 ms|1.557 ms|1.584 ms|803.72|
|1|新 Runtime|7.540 ms|10.329 ms|11.675 ms|14.164 ms|138.10|
|2|PyPI 2.4.0|1.295 ms|1.567 ms|1.683 ms|1.878 ms|1,523.80|
|2|新 Runtime|7.829 ms|12.372 ms|14.142 ms|14.886 ms|263.77|
|4|PyPI 2.4.0|1.475 ms|2.193 ms|2.744 ms|3.218 ms|2,563.97|
|4|新 Runtime|6.993 ms|16.554 ms|18.576 ms|21.036 ms|483.72|

随机查询 p50 的新/旧比值为 6.06×、6.04×、4.74×；聚合 QPS 分别下降 82.8%、82.7%、81.1%。

两个实现的并发扩展都正常：从 1 到 4 进程，旧实现 QPS 提升 3.19×，新 Runtime 提升 3.50×。问题不是进程并发失效，而是新 Runtime 每次冷随机访问的绝对延迟较高。

## 8. 结果解释

### 8.1 已验证的收益

- mmap Dataset Index 避免了每个进程各自反序列化并常驻完整 catalog。
- 1/2/4 进程构造延迟没有随进程数放大，且保持在约 0.55 秒。
- 4 进程完成 516 条不同序列的随机访问后，总 PSS 仍只有 413.54 MiB，而旧实现为 21.77 GiB。
- 新 Runtime 的 1→4 进程吞吐扩展比例不低于旧实现。

### 8.2 冷查询回退的判断

结合当前实现结构，最可能的解释是：旧实现用约 5.5 GiB/进程的代价在 DataFrame 构造阶段把 metadata catalog 和 Reader 状态放入内存；新 Runtime 只 mmap 紧凑 Dataset Index，首次访问某条随机序列时仍需按需读取/解析 TsFile 中的 TimeseriesMetadata，并在没有真实 page locator 的情况下走按行查询 fallback。清理 OS page cache 后，这些按需读取直接暴露为随机 I/O 延迟。

这是基于架构与结果的推断，不是 I/O trace 的最终归因。已有 256 行、暖 OS cache 测试中，新 Runtime 的查询 p50/p95/p99 曾比 PyPI 快约 10%–14%，但由于窗口长度和 cache 条件都不同，不能用它抵消本报告的冷缓存结果。它只说明当前回退更可能集中在冷启动的 metadata/page 定位路径，而不是所有稳定态查询路径。

### 8.3 对使用场景的含义

- 对短生命周期、只做少量查询的任务，新 Runtime 仍可能更快完成：旧实现约 46–49 秒的构造时间远高于查询本身，新 Runtime 约 0.55 秒即可进入查询。
- 对长生命周期、低内存要求的多进程服务，新 Runtime 的内存优势成立，但冷随机查询的延迟目标尚未满足。
- 对 OS cache 稳定命中的服务，需要补一组同为 2880 行的暖缓存对照，不能直接沿用此前 256 行结果。

## 9. 建议的下一步

1. 在 PreparedSeries 首次查询路径增加分段计时或 I/O trace，区分 Dataset Index lookup、TimeseriesMetadata range read/parse、SSI/page 定位、page read/decompress 和结果 merge。
2. 将实际 SSI 扫描结果发布到 `PagePositionIndex`，或把足够的 page locator 持久化进索引，避免 2880 行首次窗口走完整 fallback。
3. 评估对 TimeseriesMetadata 和 page locator 的有界预取/缓存，继续保留 ReaderSessionPool 与 PreparedSeriesCache 的硬上限，不能用恢复 5.5 GiB/进程 catalog 的方式换回查询速度。
4. 优化后严格复用本报告的 workload plan、2880 行窗口、三轮 cache drop 和 1/2/4 进程口径复测。
5. 建议验收条件同时约束：4 进程 PSS 不超过 1 GiB，且冷随机 p50/p95 不劣于 PyPI 2.4.0；若二者不能同时达到，应在设计中明确可接受的内存—延迟 Pareto 点。

## 10. 复现与原始结果

基准脚本：`python/tests/benchmark_tsfile_dataframe_cold_cache.py`

生成固定 workload plan：

```bash
/root/tsfile_pypi_baseline_20260814/bin/python \
  /root/tsfile_dataframe_cold_cache_benchmark.py \
  /data/lotsa/buildings_900k \
  --sample-count 128 \
  --query-rows 2880 \
  --seed 20260814 \
  --plan-worker-count 4 \
  --generate-workload-plan /tmp/tsfile-dataframe-2880-workload-plan.json
```

正式运行参数的核心部分为：

```bash
python /root/tsfile_dataframe_cold_cache_benchmark.py \
  /data/lotsa/buildings_900k \
  --processes 1,2,4 \
  --sample-count 128 \
  --query-rows 2880 \
  --seed 20260814 \
  --workload-plan /tmp/tsfile-dataframe-2880-workload-plan.json \
  --drop-os-cache \
  --output RESULT.json
```

远端原始文件：

```text
/tmp/tsfile-dataframe-2880-workload-plan.json
/tmp/tsfile-cold-cache-pypi-2.4.0-2880.json
/tmp/tsfile-cold-cache-pypi-2.4.0-2880-trial2.json
/tmp/tsfile-cold-cache-pypi-2.4.0-2880-trial3.json
/tmp/tsfile-cold-cache-feature-2880.json
/tmp/tsfile-cold-cache-feature-2880-trial2.json
/tmp/tsfile-cold-cache-feature-2880-trial3.json
```

第一轮 JSON 由修正 QPS 字段之前的脚本生成，其逐查询延迟、worker `random_wall_ms`、内存和 cache 数据均有效。本报告对全部三轮统一使用 `query_count / max(worker.random_wall_ms)` 重新计算 QPS；第二、三轮脚本已直接输出该修正值。
