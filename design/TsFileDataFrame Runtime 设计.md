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

# TsFileDataFrame Runtime 设计

## 1. 文档边界

本文是 DatasetRuntime、对象/查询 lease、ReaderSessionPool、PreparedSeriesCache 和跨文件 merge 的权威设计。

- [主技术设计](<./TsFileDataFrame 持久化 mmap 索引与进程本地 Runtime 技术设计.md>)只保留背景、目标、总体架构、跨文档契约、端到端流程与公开 API。
- [Dataset Index 文件格式与查询设计](<./Dataset Index 文件格式与查询设计.md>)定义索引的字节布局、校验规则与版本演进。
- [TsFile Reader 与 Prepared Query Context 设计](<./TsFile Reader 与 Prepared Query Context 设计.md>)定义 C++ Reader、PreparedSeries 表示、prepared 查询接口、PagePositionIndex、查询局部对象和 ResultSet 生命周期。

每次独立构造 TsFileDataFrame 都创建独立 Runtime；只有由该 DataFrame 派生的 view 与惰性对象共享它。Runtime 不提供进程级注册或跨独立 DataFrame 的资源仲裁，也不增加通用查询规划对象。

## 2. 现有实现边界

|层级|现有类型|接入后的职责|
|---|---|---|
|Python Dataset|TsFileDataFrame、Timeseries、AlignedTimeseries|保留现有查询语义与返回类型；对象只通过 lease 持有 Runtime。|
|Python 单文件访问|TsFileSeriesReader|保留单文件模型识别和普通查询能力；Dataset 快速路径由 Runtime 提供 locator。|
|Cython|TsFileReaderPy、ResultSetPy|持有原生 Reader/ResultSet，管理 Python 生命周期和异常映射。|
|C++|TsFileReader、TableResultSet|执行单文件读取；prepared 接口只增加定点准备与查询能力。|

Runtime 是内部实现边界。除 Timeseries 增加显式生命周期方法外，TsFileDataFrame 构造参数、索引/loc 语义和结果对象类型保持不变。

## 3. Runtime 对象模型

### 3.1 FileGeneration

Runtime 使用以下值标识索引映射中某个 TsFile 的不可变 generation：

```text
FileGeneration = (
    mapped_index_identity,
    file_id,
    file_size,
    file_fingerprint,
)
```

规范化 path 来自对应 TsFileRecord，但不单独充当 generation。当前数据集模型是静态的：活动 Runtime 不接受索引热切换、文件原地更新或 generation 切换。重新生成文件或索引后，调用者需要重新构造 TsFileDataFrame。

### 3.2 DatasetRuntime

```text
DatasetRuntime {
    mapped_index: MappedDatasetIndex
    readers: ReaderSessionPool(max_open_files)
    prepared: PreparedSeriesCache<(FileGeneration, locator_id), PreparedSeries>
    merge_kernel: Cython DatasetMergeKernel
    object_leases: count
    query_leases: count
    accepting_leases: bool
}
```

|成员|职责|
|---|---|
|MappedDatasetIndex|只读映射并校验 Dataset Index；按 id、span 和 locator 路由。|
|ReaderSessionPool|按 FileGeneration 管理已经打开的 ReaderSession，并严格执行 max_open_files。|
|PreparedSeriesCache|以 (FileGeneration, locator_id) 复用独立所有权的 PreparedSeries。|
|DatasetMergeKernel|在 Cython 中完成跨文件有序拼接、k-way merge、重复时间戳检查和多列对齐。|
|lease 状态|线性化对象关闭、查询开始和 Runtime 最终 teardown。|

MappedDatasetIndex 不持有 TsFileReader。ReaderSessionPool 和 PreparedSeriesCache 也彼此独立：ReaderSession 被回收不会使 PreparedSeries 失效。

## 4. 对象 lease 与关闭语义

### 4.1 RuntimeLease

以下仍可访问 Runtime 的对象各持有一个独立、幂等释放的 RuntimeLease：

- 独立构造的 TsFileDataFrame；
- 由列表、切片或其他索引操作产生的 DataFrame view；
- 仍可发起惰性读取的 Timeseries。

完全物化的 AlignedTimeseries 不再访问 Runtime，因此不持有 RuntimeLease。未来若增加惰性 iterator，iterator 自身必须持有 RuntimeLease，或者确保每个已产出的惰性 Timeseries 各自持有 lease。

DataFrame.close() 只标记当前 handle 已关闭并释放它自己的 lease；它不能使其他 view 或 Timeseries 失效。Timeseries 提供公开、幂等的 close() 和上下文管理器：

```python
with dataframe["root.sg.d.s"] as series:
    values = series[:]
```

finalizer 只作为遗漏 close() 时的兜底，不能作为 Runtime 可确定关闭的主要机制。

### 4.2 QueryLease

每次读取必须先从发起查询的 handle 获取 QueryLease。获取 QueryLease 与该 handle 的 close() 在同一状态锁下原子化：

- close 先成功：后续查询被拒绝；
- 查询先取得 QueryLease：该查询允许完整结束，close 不提前释放其 Runtime 资源。

查询持有 QueryLease、ReaderSession lease 和 PreparedSeries lease，直到 ResultSet 被完整消费并关闭。释放顺序为 ResultSet、PreparedSeries lease、ReaderSession lease、QueryLease。

### 4.3 Runtime 最终 teardown

最后一个 RuntimeLease 释放后，Runtime 停止接受新的对象 lease 和查询。已经取得的 QueryLease 继续完成；当活动 QueryLease 归零后，Runtime 按以下顺序关闭：

1. 释放 PreparedSeriesCache 及其独立 arena；
2. 关闭 ReaderSessionPool 中的所有 Reader；
3. 解除 Dataset Index 映射。

每个 lease 和 close() 都必须幂等，异常路径不得重复减计数或提前 teardown。

## 5. ReaderSessionPool

### 5.1 ReaderSession 的范围

ReaderSession 绑定一个 FileGeneration，并持有一次打开后的 FD、ReadFile、executor/decoder、Reader arena 和本次查询所需的执行状态。它不是只包装 FD，也不拥有 PreparedSeries 的长期内存。

### 5.2 acquire/release

1. acquire(FileGeneration) 查找 generation 完全一致的现有 session；命中后增加 active use。
2. 未命中时，pool 从 Dataset Index 解析 path，打开 TsFileReaderPy，并复核 file_size 与 file_fingerprint。
3. 已打开 session 数达到 max_open_files 时，优先关闭 active use 为 0 的最旧空闲 session。
4. 如果所有 session 都在使用，acquire 等待 release；任何时候都不得临时突破 max_open_files。
5. release 把 active use 减一；归零的 session 进入空闲集合，供复用或回收。

ReaderSession 的创建和复用都必须校验 FileGeneration。校验失败直接拒绝当前查询，不尝试在活动 Runtime 内重建索引或切换文件 generation。

### 5.3 唯一的 v1 资源硬限制

v1 只确认 max_open_files 是 Runtime 的资源硬限制。它直接约束同时打开的 ReaderSession 数量。

除 max_open_files 外，本阶段不定义其他资源预算、进程级总 FD 配额或 DataFrame 内存上限。不同的独立 Runtime 不共享 Reader，也不做全局资源仲裁。

## 6. PreparedSeriesCache

PreparedSeries 的内存表示和 Reader 接口由 [Reader 子文档](<./TsFile Reader 与 Prepared Query Context 设计.md>)定义。Runtime 只冻结以下缓存契约：

- key 固定为 (FileGeneration, locator_id)，不得以 Reader identity、path 或逻辑序列名替代；
- 同 key 的首次 prepare 使用 single-flight，避免并发重复构建；
- cache 是 PreparedSeries 的长期强 owner，查询通过 lease 临时固定 entry；
- PreparedSeries 自带 PageArena 和索引对象，不指向 Reader arena、ReadFile、executor 或 decoder；
- ReaderSession 淘汰或关闭不使 cache entry 失效；
- FileGeneration 不匹配时绝不复用。

v1 不定义 entry 数量上限、字节预算、LRU 淘汰或“超预算则不入 cache”的分支。已成功插入的 PreparedSeries 保留到 Runtime 最终 teardown。是否增加淘汰策略，必须以实现后的占用数据和 benchmark 为依据，另行设计。

## 7. Cython DatasetMergeKernel

第一版跨文件 merge 实现在 typed Cython DatasetMergeKernel 中，不先增加 C++ 类或 C ABI。

实现约束：

- 输入是单文件 ResultSet 已消费得到的 typed timestamp/value array parts；
- 使用 typed memoryview、NumPy C API、C scalar 和预分配输出；
- 可释放 GIL 的纯计算段使用 nogil；
- 已按时间排序且互不重叠的片段走连续复制快速路径；
- 存在重叠时执行有序 k-way merge，并检查同一逻辑序列的重复时间戳；
- loc 多字段结果按时间戳并集对齐，缺失值按现有 Dataset 语义填充；
- Python 层只负责 span 路由、公开结果对象构造和异常映射，不执行逐行 heap merge。

该内核不保存长期查询状态，也不承担跨文件 Planner 职责。只有 profiling 证明 Cython 仍是瓶颈时，才考虑把同一窄接口下沉到 C++。

## 8. 端到端流程

### 8.1 初始化

1. 按现有规则展开、排序并规范化输入 .tsfile 路径。
2. 校验或构建 Dataset Index；构建流程完成临时文件校验、fsync 和原子发布。
3. 只读映射索引并建立 typed view。
4. 创建独立 DatasetRuntime、ReaderSessionPool、空 PreparedSeriesCache 和初始 RuntimeLease。
5. 构造完成后开放查询；初始化期间不允许数据访问。

初始化不预先打开所有 Reader，也不预先构建 PreparedSeries。

### 8.2 按时间读取单个逻辑序列

1. handle 原子获取 QueryLease。
2. MappedDatasetIndex 根据逻辑序列和时间范围定位 SeriesFileSpan。
3. 对每个 span 获取并校验 ReaderSession。
4. 以 (FileGeneration, locator_id) 获取或构建 PreparedSeries。
5. ReaderSession 执行 queryPrepared，完整消费并关闭 ResultSet。
6. DatasetMergeKernel 拼接或归并各文件片段。
7. 构造现有 Timeseries/NumPy/pandas 结果并释放各级 lease。

### 8.3 按行读取

流程与按时间读取相同，但单文件执行使用 queryPreparedByRow。可选 PagePositionIndex 由 PreparedSeries 持有，并在重复按行查询时增量扩展；具体记录和并发规则见 Reader 子文档。

### 8.4 loc 多字段读取

Runtime 为各字段分别定位 spans 和 PreparedSeries，读出 typed parts 后交给 DatasetMergeKernel 做时间戳并集对齐，最后构造现有 AlignedTimeseries 或 pandas.DataFrame。完全物化的返回对象不持有 RuntimeLease。

## 9. 公开 API

TsFileDataFrame 构造函数继续只接收现有 paths 和 show_progress：

```python
df = TsFileDataFrame(paths, show_progress=False)
```

不增加 memory budget、prepared cache budget、schema fingerprint、runtime reuse 或 index view 参数。索引、loc、list_timeseries 和 list_timeseries_metadata 的公开语义保持不变。

生命周期 API 的唯一新增项是惰性 Timeseries 的幂等 close() 与上下文管理器。DataFrame/view 原有 close 语义改为“只关闭当前 handle”；其他持有独立 RuntimeLease 的对象继续可用。

## 10. 错误与安全约束

|场景|行为|
|---|---|
|索引映射或 section 校验失败|仅在初始化阶段进入重建；重建失败则构造失败。|
|打开 Reader 时 generation 不匹配|拒绝查询；不热切换、不原地重建。|
|达到 max_open_files 且全部 Reader 活跃|等待 release；不突破上限。|
|同 key 并发 prepare|single-flight；等待同一次构建结果。|
|prepare 失败|不发布半初始化 entry；错误返回所有等待者。|
|ResultSet 尚未消费完|ReaderSession 与 PreparedSeries lease 继续存活。|
|handle 已 close|拒绝新查询；其他 handle 不受影响。|
|Runtime 正在 teardown|拒绝新 lease；已开始查询继续完成。|
|重复时间戳或非法对齐关系|终止当前查询并映射为现有 Python 异常。|

## 11. 验收要点

- 两次独立构造的 TsFileDataFrame 拥有两个互不共享的 Runtime。
- 关闭源 DataFrame 后，仍持 lease 的 view 或 Timeseries 可以继续读取。
- Timeseries.close() 和上下文管理器能够确定性释放最后一个对象 lease。
- ReaderSessionPool 在压力下从不超过 max_open_files；无空闲 Reader 时等待。
- ReaderSession 被回收后，同 FileGeneration 的 PreparedSeries 仍可由新 session 复用。
- PreparedSeriesCache v1 没有虚构的 entry/bytes 预算或 LRU 语义。
- 跨文件 merge 首版位于 typed Cython，Python 不做逐行归并。
- 最后一个对象 lease 和所有 QueryLease 结束后，Reader、PreparedSeries 与 mmap 均被释放。
