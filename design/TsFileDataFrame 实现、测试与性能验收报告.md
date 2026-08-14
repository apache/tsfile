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

# TsFileDataFrame 实现、测试与性能验收报告

## 1. 范围与版本

本次实现基于 Apache `origin/develop` 的 `5681978b84a0d9ee26e1eab94f78afaccf93607a`，独立 worktree 为 `/Users/colin/dev/tsfile/worktrees/tsfile-dataframe-runtime`，分支为 `feature/tsfile-dataframe-runtime`。

|提交|内容|
|---|---|
|`506f2a8bb`|加入并冻结 Format、Runtime、Reader/Prepared 与主设计四份文档|
|`a03a7f1d3`|实现 Dataset Index、C++ prepared path、进程本地 Runtime、Cython merge、Python 生命周期与测试|

权威设计仍分别位于以下四份文档：

- [Dataset Index 文件格式与查询设计](<./Dataset Index 文件格式与查询设计.md>)
- [TsFileDataFrame Runtime 设计](<./TsFileDataFrame Runtime 设计.md>)
- [TsFile Reader 与 Prepared Query Context 设计](<./TsFile Reader 与 Prepared Query Context 设计.md>)
- [TsFileDataFrame 持久化 mmap 索引与进程本地 Runtime 技术设计](<./TsFileDataFrame 持久化 mmap 索引与进程本地 Runtime 技术设计.md>)

## 2. 实现清单

### 2.1 Dataset Index

- C++ 与 Python 共用 Format v1 的 64-byte Header、14 个 SectionDirectory entry 和 14 个业务 section。
- 实现 CRC32C、record width/count、offset/length 溢出、字符串引用、外键、连续 span 与名称索引校验。
- Builder 写入同目录临时文件，完成校验和 fsync 后原子 rename；Reader 使用只读 mmap。
- Runtime 通过 typed view 懒读取表、设备、列、逻辑序列、文件 span 和精确 metadata locator，不恢复完整 Python metadata tree。

### 2.2 C++ Reader 与 Prepared Query Context

- `TimeseriesIndex` 记录其在 TsFile 中的精确 source offset/length。
- `prepare_series` 校验 FileGeneration 和 range，在 PreparedSeries 独立 `PageArena` 中定点反序列化 metadata。
- aligned time/value metadata 独立读取并校验 chunk cardinality；普通与 aligned 查询均复用现有 SSI/ChunkReader/ResultSet。
- C wrapper 与 Cython 提供不透明 PreparedSeries handle 的 prepare/query/free 生命周期。
- `PagePositionIndex` 已实现并发无空洞发布与二分查找，但实际 SSI page offset 尚未回填，按行查询仍走正确性 fallback；2880 行冷缓存结果表明该缺口需要作为性能验收项继续处理。

### 2.3 Runtime 与 Python

- 每次独立 DataFrame 构造创建独立 DatasetRuntime；不同进程和不同 DataFrame 不共享 Python/C++ 对象。
- ReaderSessionPool 严格执行 `TSFILE_DATAFRAME_MAX_OPEN_FILES`，只淘汰无 active use 的 Reader。
- PreparedSeriesCache 以映射 identity、FileGeneration 和 locator 唯一确定 entry，并对首次 prepare 做 single-flight。
- DataFrame view 与惰性 Timeseries 各自持有 RuntimeLease；close 只关闭当前 handle，最后一个对象 lease 才触发 teardown。
- 重叠跨文件 merge 使用 typed Cython kernel；重复 timestamp 保持抛错语义。

## 3. 测试结果

|层次|配置与覆盖|结果|
|---|---|---|
|C++ 正式构建与聚合回归|Release shared library；Snappy、LZ4、LZO、Zlib enabled；ANTLR4 disabled|127 suites、758 tests：757 passed、1 个外部数据可选测试 skipped|
|Dataset Index C++|写入/mmap/lookup、版本与 checksum 拒绝、CRC32C、真实 Python 大索引跨语言打开|远端 7/7 通过；真实索引全校验 4.66 秒|
|Prepared C++|PagePositionIndex 顺序发布、失败不推进、64 writer 并发无空洞发布|2/2 通过|
|Python 完整回归|原有 Reader/Writer/Dataset API，加 format/runtime/lifecycle/stale locator/cache/merge 测试|181/181 通过|
|远端 smoke|4 个 TsFile、1.50 GB、145,063 条逻辑序列，冷构建及 1/2 进程查询|通过；构建 14.43 秒|

聚合回归中唯一 skipped 的用例是未设置 `TSFILE_DATASET_INDEX_TEST_PATH` 的外部索引测试；随后以 `buildings_900k/.tsfile_dataframe_index.tsidx` 设置该变量单独执行，7/7 全部通过。

## 4. 大数据集基准

### 4.1 数据与环境

`/data/lotsa` 是 116 个独立数据集目录的集合，共 647 个 TsFile、约 519 GiB。不同目录可能属于不同 schema，因此本次不把整个目录误当成一个 DataFrame，而选择其中最大的完整数据集：

|项目|值|
|---|---:|
|数据集|`/data/lotsa/buildings_900k`|
|TsFile 数|61|
|TsFile 总 bytes|65,856,302,628|
|逻辑序列数|1,792,328|
|机器|32 CPU，62 GiB RAM，4 GiB swap|
|系统|Ubuntu/Linux 6.8.0-136-generic，x86_64|
|Python|3.13.14|

基准脚本为 `python/tests/benchmark_tsfile_dataframe_runtime.py`。内存取自 `/proc/<pid>/smaps_rollup`；USS 定义为 `Private_Clean + Private_Dirty`。热测没有 drop 全机 page cache，结果明确代表稳定索引和暖 OS cache 场景。

### 4.2 冷构建

|指标|结果|
|---|---:|
|冷构建 wall time|205.13 秒|
|索引长度|506,136,064 bytes（约 482.69 MiB）|
|索引/原数据比例|约 0.77%|
|峰值 RSS|11,250,788 KiB（约 10.73 GiB）|
|swap|0|

冷构建峰值包含 61 个文件的旧 metadata catalog bridge、全局排序、section bytes 与最终校验。它是一次性构建成本，不代表热启动进程的常驻内存。

### 4.3 1/2/4 进程热启动与查询

> 本节保留的是早期 256 行、首条序列、暖 OS cache 微基准，只能说明稳定热路径和 mmap 内存共享，不能代表随机序列或冷缓存查询性能。严格冷缓存的 2880 行结果见下一节；涉及“查询是否回退”的判断以后者为准。

每个 worker 独立构造 DataFrame，映射同一索引，选择首条逻辑序列，执行一次 256 行查询，再在相同 PreparedSeries 上重复 20 次。采样发生在 DataFrame、Reader 和一个 PreparedSeries 保持存活时。

|进程数|组 ready|DataFrame 构造中位数|首次查询中位数|重复查询中位数|重复查询最大 p95|总 RSS|总 PSS|总 USS|总 FD|
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
|1|815.01 ms|529.26 ms|1.261 ms|0.300 ms|0.337 ms|116.64 MiB|84.95 MiB|57.98 MiB|12|
|2|867.98 ms|527.26 ms|1.261 ms|0.302 ms|0.331 ms|233.01 MiB|144.19 MiB|99.45 MiB|24|
|4|923.49 ms|523.28 ms|1.204 ms|0.300 ms|0.345 ms|465.93 MiB|251.13 MiB|198.09 MiB|48|

结论：

- DataFrame 热构造保持在约 0.53 秒，2/4 进程并发没有明显延迟放大。
- 483 MiB 索引没有按进程复制成同等私有内存；4 进程总 USS 约 198 MiB，约 49.5 MiB/进程。
- RSS 会在每个进程中重复计入共享 file-backed 页；评估机器实际容量应使用 PSS/USS，而不是简单相加 RSS。
- 首次查询包含 Reader open、generation 校验和 PreparedSeries 构建；随后 256 行窗口约 0.30 ms。
- 总 FD 包含 Python multiprocessing pipe、stdio、mmap file 和 Reader FD，不等同于 ReaderSession 数；Reader 硬上限由独立压力测试验证。

### 4.4 2880 行随机序列冷缓存对照

2026-08-14 补充了 PyPI 2.4.0 与新 Runtime 的严格冷缓存对照。每个实现的 1/2/4 进程分别运行三轮；每组启动前执行 `sync` 和 `drop_caches=3`，并复用同一份固定随机 workload。全部查询均返回 2880 行。

三轮中位数显示：

- 新 Runtime 冷构造快 83.9–90.4 倍。
- 查询后 PSS 低 36.5–53.9 倍；4 进程 PSS 为 413.54 MiB，对比旧实现 21.77 GiB。
- 首次随机查询慢 2.74–4.10 倍。
- 后续随机查询 p50 慢 4.74–6.06 倍，聚合 QPS 低 81.1%–82.8%。

完整方法、数据表、workload hash、结果解释和原始 JSON 路径见 [TsFileDataFrame 2880 窗口冷缓存性能报告](<TsFileDataFrame 2880 窗口冷缓存性能报告.md>)。这一结果表明内存与构造目标已经达到，但冷随机查询性能尚未通过无回退验收。

## 5. 复现方式

在已构建的 Python 包目录运行：

```bash
python tests/benchmark_tsfile_dataframe_runtime.py \
  /data/lotsa/buildings_900k \
  --mode multiprocess \
  --processes 1,2,4 \
  --query-rows 256 \
  --query-repeats 20 \
  --output /tmp/tsdf-buildings-hot.json
```

若索引不存在，先将 `--mode` 改为 `build`。构建会创建 `.tsfile_dataframe_index.tsidx` 和零长度 lock 文件；已有旧格式 `.tsfile_dataframe_index.tsfidx` 不会被覆盖。

## 6. 剩余项与发布判断

当前 v1 的 Format、Runtime、prepared Reader、Python API、生命周期、跨文件 merge 和多进程 mmap 功能目标均已实现，并通过正确性与多进程内存验收。但 2880 行随机序列冷缓存基准发现 4.74–6.06 倍的 p50 查询回退，因此性能验收尚未完成。

把 SSI 扫描得到的真实 page offset 发布到 `PagePositionIndex`，或提供等价的持久化 page locator/有界预取，已经不是单纯的可选优化。若发布标准包含“冷随机查询不回退”，则该项及其 I/O profiling 应作为发布前工作；优化后必须复用固定 workload 和 cache-drop 口径重新验收。
