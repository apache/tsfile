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

# TsFileDataFrame 持久化 mmap 索引与进程本地 Runtime 技术设计

# 0\. 文档信息

|设计日期|状态|设计结论|
|---|---|---|
|2026\.08\.14|Final|Dataset 是静态 sealed TsFile 文件集合；TsFileDataFrame 通过只读 MappedDatasetIndex 按需访问持久化统计与路由，并由每次独立构造产生的 DatasetRuntime 管理 Reader、PreparedSeries、对象/查询 lease 和跨文件执行。v1 只对同时打开的 Reader 数量设置硬限制。|



# 1\. 设计背景

`Dataset` 是磁盘上的静态 TsFile 文件集合。`TsFileDataFrame` 是用户可见的跨文件逻辑视图，提供选择、切片与查询，并产出训练使用的 dataframe 或 batch。海量多文件数据集如果为每个 TsFile 和训练进程恢复完整 schema、device、series、物理 series 段与统计对象，会产生与 series 数量线性相关的初始化时间和私有内存。新设计把不可变统计与路由持久化为 mmap-friendly Dataset Index，并由进程本地 Runtime 负责按需 Reader、PreparedSeries 和跨文件执行。

这些工作会在每个训练 worker、分布式训练进程和重复创建的 DataFrame 中独立发生。操作系统 Page Cache 可以缓存原始文件页，却不能共享当前 Python 统计与路由对象图，也不能消除逐文件元数据解析和跨文件 merge。

## 1\.1 元数据对象化问题

```text
TsFile metadata bytes
  → per-file Python MetadataCatalog / tuple / dict / list
  → root DataFrame catalog 与跨文件 routing/statistics
```

当前实现把不可变统计与跨文件路由展开为 tuple、dict、list、string 等 Python 对象图。单条记录包含对象头、指针和容器容量，布局不紧凑；这些对象也不是 file-backed，独立训练进程不能像 mmap 干净页一样共享。新设计把统计与路由写入紧凑的 Format v1 文件，DataFrame 通过只读 mmap typed view 按需访问，只在构造公开返回值时创建必要的 Python 对象。

## 1\.2 训练场景中的放大

若 Dataset 包含 N 个 TsFile 文件、D 个 device、S 个物理 series 段，并由 P 个训练进程独立加载，初始化总工作量接近 `P × O(N + D + S)`。物理 series 段表示一个逻辑 series 在一个 TsFile 文件中的记录；同一逻辑 series 可以跨多个 TsFile 文件。实测 `buildings_900k` 包含 61 个 TsFile 文件、1,792,328 条 series，单进程加载后 PSS 约 6\.31 GiB；按当前实现外推，8 个训练进程约为 50\.5 GiB。每个进程还会持有自己的 Reader、文件描述符、native metadata 和 Python catalog，因此，当 PyTorch DataLoader 使用 spawn 方式创建多个子进程时，每个子进程都会重新加载 DataFrame；它与其他训练进程一样会继续放大私有内存。

## 1\.3 当前 TsFileDataFrame 的源码与 Python 对象占用

当前实现的常驻 Python 对象直接来自 `python/tsfile/dataset/metadata.py` 与 `python/tsfile/dataset/dataframe.py`。下面保留实际字段定义，省略与内存布局无关的方法。

```python
class SeriesStats(NamedTuple):
    length: int
    min_time: int
    max_time: int
    timeline_length: int
    timeline_min_time: int
    timeline_max_time: int

@dataclass(**_DATACLASS_SLOTS)
class TableEntry:
    table_name: str
    tag_columns: Tuple[str, ...]
    tag_types: Tuple[TSDataType, ...]
    field_columns: Tuple[str, ...]
    _field_index_by_name: Dict[str, int] = field(init=False, repr=False)

@dataclass(**_DATACLASS_SLOTS)
class DeviceEntry:
    table_id: int
    tag_values: Tuple[Any, ...]
    min_time: int
    max_time: int

@dataclass(**_DATACLASS_SLOTS)
class MetadataCatalog:
    table_entries: List[TableEntry] = field(default_factory=list)
    device_entries: List[DeviceEntry] = field(default_factory=list)
    table_id_by_name: Dict[str, int] = field(default_factory=dict)
    device_id_by_key: Dict[Tuple[int, tuple], int] = field(default_factory=dict)
    series_stats_by_ref: Dict[Tuple[int, int], SeriesStats] = field(default_factory=dict)
```

```python
# DataFrame-wide logical key: (device_idx, global_field_idx).
SeriesRefKey = Tuple[int, int]
# One physical occurrence in a TsFile shard: (reader, local_device_id, local_field_idx).
SeriesRef = Tuple[object, int, int]

@dataclass(**_DATACLASS_SLOTS)
class _DataFrameCatalog:
    model: Optional[str] = None
    table_entries: Dict[str, TableEntry] = field(default_factory=dict)
    devices: List[DeviceKey] = field(default_factory=list)
    device_index: Dict[DeviceKey, int] = field(default_factory=dict)
    device_time_bounds: List[Tuple[Optional[int], Optional[int]]] = field(default_factory=list)
    series: List[SeriesRefKey] = field(default_factory=list)
    series_shards: Dict[SeriesRefKey, List[SeriesRef]] = field(default_factory=dict)

class TsFileDataFrame:
    def __init__(self, paths, show_progress=True):
        self._paths = _expand_paths(paths)
        self._show_progress = show_progress
        self._readers: Dict[str, object] = {}
        self._index = _DataFrameCatalog()
        self._is_view = False
        self._root = None
        self._closed = False
        self._load_metadata()
```

`TsFileSeriesReader` 为每个 TsFile 建立一个 `MetadataCatalog`；`_register_reader()` 把各文件目录再次合并到 `_DataFrameCatalog`。因此同一个物理 series 至少对应单文件目录中的 `SeriesRefKey + SeriesStats`，以及跨文件目录中的 `series` 项、`series_shards` 键、list 和 `SeriesRef` tuple。

```python
import sys
from tsfile.dataset.metadata import SeriesStats

N = 100_000
reader = object()
series_stats_by_ref = {}
series = []
series_shards = {}

for i in range(N):
    device_id = 10_000 + i
    ref = (device_id, 0)
    stats = SeriesStats(
        1_000_000 + i,
        1_700_000_000 + i,
        1_800_000_000 + i,
        2_000_000 + i,
        1_600_000_000 + i,
        1_900_000_000 + i,
    )
    series_stats_by_ref[ref] = stats
    series.append(ref)
    series_shards[ref] = [(reader, device_id, 0)]

def retained_bytes(root):
    seen = set()
    stack = [root]
    total = 0
    while stack:
        obj = stack.pop()
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        total += sys.getsizeof(obj)
        if isinstance(obj, dict):
            stack.extend(obj.keys())
            stack.extend(obj.values())
        elif isinstance(obj, (list, tuple)):
            stack.extend(obj)
    return total

sample_ref = series[-1]
sample_stats = series_stats_by_ref[sample_ref]
sample_route_list = series_shards[sample_ref]
sample_route = sample_route_list[0]

print(sys.getsizeof(sample_ref))
print(sys.getsizeof(sample_stats))
print(sys.getsizeof(sample_route_list))
print(sys.getsizeof(sample_route))
print(sys.getsizeof(series_stats_by_ref) / N)
print(sys.getsizeof(series) / N)
print(sys.getsizeof(series_shards) / N)
print(retained_bytes(series_stats_by_ref) / N)
print(retained_bytes({"series": series, "series_shards": series_shards}) / N)
print(retained_bytes({
    "stats": series_stats_by_ref,
    "series": series,
    "shards": series_shards,
}) / N)
```

```text
SeriesRefKey tuple                         56 B
SeriesStats tuple                          88 B
单元素 SeriesRef list                      64 B
SeriesRef tuple                            64 B
series_stats_by_ref dict 浅层摊销          52.42960 B/entry
series list 浅层摊销                        8.00984 B/entry
series_shards dict 浅层摊销                52.42960 B/entry
单文件 series_stats_by_ref 保留对象图      408.42988 B/series
跨文件 series + series_shards 保留对象图   272.44273 B/series
两个目录去重后的合计                       596.87272 B/series
N = 100,000 时合计                         59,687,272 B
                                               56.922 MiB
```

这些数值来自上面明确给定的真实 Python 类型、字段值和容器容量。它们只计算 Python 可达对象；`TableEntry`、`DeviceEntry`、tag/path 字符串会按实际数据内容另行计入，Cython/C\+\+ Reader 与分配器占用由下一节的进程 PSS 实测覆盖。这样可以把“Python 对象逐项计算”和“整个进程实际占用”分开，避免把未测量部分分摊成推测比例。

## 1\.4 远程数据规模与内存实测

在 `192.168.99.20:/data/tsfile_datasets/TimeBench_TsFile/lotsa` 上进行了只读盘点和 DataFrame 加载实测。目录包含 130 个数据集、1,145 个 TsFile，原始数据约 1\.018 TiB；已有索引覆盖 18,330,657 个 series fragment，磁盘大小约 1\.955 GiB。

|数据集|TsFile|series|初始化|加载后 PSS|增量 PSS/series|
|---|---|---|---|---|---|
|`cmip6_1935`|14|434,176|32\.45 秒|1\.61 GiB|约 3\.72 KiB|
|`buildings_900k`|61|1,792,328|140\.77 秒|6\.31 GiB|约 3\.65 KiB|

按 3\.65～3\.72 KiB/series 外推，完整 `lotsa` 对象化约需 64～66 GiB/进程，8 个进程约需 512～528 GiB。该结果说明本设计不仅要缩短启动时间，还必须避免恢复完整 Python/native catalog 对象图。

# 2\. 设计目标

1. 为每个 Dataset 建立一个持久化、可重建的外部索引文件，统一描述 schema、device/tag、logical series、fragment、聚合统计和 metadata locator。

2. 索引采用紧凑、只读、mmap\-friendly 的二进制格式；多个训练进程映射同一 inode，由操作系统共享干净文件页。

3. DataFrame 直接在 mmap view 上完成 len、lookup、prefix range、stats 和 fragment routing，不恢复全量 Python catalog。

4. 通过进程本地 ReaderSessionPool 快速切换文件，并通过 FileGeneration-bound PreparedSeries 复用活跃 series 的执行元数据。

5. 保持 C++ Reader 的单文件职责；跨文件 merge 第一版由 typed Cython 窄内核完成，不把 dataset root 或 Python 对象引入 libtsfile 核心。

6. 通过 FileGeneration、fingerprint、原子发布、构建锁和打开时校验保证静态 Dataset 的一致性与崩溃安全。

7. v1 严格限制同时打开的 Reader 数量，并观测索引、PreparedSeries、查询临时输出和结果内存；本阶段不承诺 DataFrame 总内存硬上限。

# 3\. 技术设计

TsFileDataFrame 面向静态 TsFile 文件集合提供跨文件逻辑视图。整体设计分成一个主文档和三个权威子文档：

- [Dataset Index 文件格式与查询设计](<./Dataset Index 文件格式与查询设计.md>)：二进制格式、记录布局、排序、校验和物理定位。
- [TsFileDataFrame Runtime 设计](<./TsFileDataFrame Runtime 设计.md>)：DatasetRuntime、FileGeneration、RuntimeLease/QueryLease、ReaderSessionPool、PreparedSeriesCache、Cython DatasetMergeKernel 和关闭安全。
- [TsFile Reader 与 Prepared Query Context 设计](<./TsFile Reader 与 Prepared Query Context 设计.md>)：C++ Reader、PreparedSeries、prepared 接口、PagePositionIndex、查询局部状态和 ResultSet。

端到端链路是：MappedDatasetIndex 定位逻辑序列、文件 span 和 metadata locator；DatasetRuntime 获取匹配 FileGeneration 的 ReaderSession 与 PreparedSeries；Reader 创建查询局部执行状态和 ResultSet；Cython DatasetMergeKernel 归并单文件数组片段；Python 构造现有公开结果对象。

## 3\.2 架构设计

### 3\.2\.1 独立 Runtime 与 lease 所有权

<!-- tag-comment-scope-B-start mode="block" hash="sha256:6f9771f476032e05294e66d114deb5f669c9c3be33975b1f558448a04f5c7664" -->
每次独立构造 TsFileDataFrame 都创建独立 DatasetRuntime。初始 DataFrame、派生 view 与惰性 Timeseries 分别持有自己的 RuntimeLease；任一 DataFrame 的 close() 只关闭当前 handle。最后一个对象 lease 释放且活动 QueryLease 归零后，Runtime 才完成最终 teardown。不同独立 DataFrame 之间不复用 Runtime、ReaderSession 或 PreparedSeries，也不做进程级 registry、跨 Runtime 复用或全局资源仲裁。
<!-- tag-comment-scope-B-end mode="block" -->
<!-- tag-comment-thread-B
cmd2 {"author":"colin","kind":"human","createdAt":"2026-08-13T14:40:11.754Z","body":"现在没有所谓的processresourcegovernor\n"}
cmd4 {"author":"codex","kind":"codex","createdAt":"2026-08-13T14:47:18.877Z","body":"对，当前实现没有 ProcessResourceGovernor，已经确认的 Runtime 设计也不引入它。这里出现这个名字是旧方案残留，不需要把一个不存在的组件写成“明确不做”的架构边界；正文直接说明每次独立构造 DataFrame 创建独立 DatasetRuntime，不做进程级 registry、跨 Runtime 复用或全局资源仲裁即可。\n\n同一段里的“根 DataFrame 唯一拥有、子视图通过 `_root` 依附根对象”也已经落后：初始 DataFrame、view 和惰性 Timeseries 应各自持有 RuntimeLease；任一 DataFrame 的 close 只关闭当前 handle，最后一个对象 lease 释放且活动 QueryLease 结束后才 teardown Runtime。更新正文时这两处应一起改掉。"}
-->

MappedDatasetIndex 直接在当前版本的 SectionDirectory 上完成名称查找、定长记录读取与 span 遍历，不恢复完整元数据树。DatasetRuntime 负责 mapping、ReaderSessionPool、PreparedSeriesCache、DatasetMergeKernel 和 lease 状态。对象关系和关闭算法以 Runtime 子文档为准。

### 3\.2\.2 职责边界

|对象/组件|职责|
|---|---|
|TsFileDataFrame|保持现有 Python Dataset 查询 API、选择语义和返回类型；管理当前 handle 的 RuntimeLease。|
|MappedDatasetIndex|只读映射并校验 Dataset Index，返回 Table、Device、Column、LogicalSeries、TsFile 与 locator/span 的 typed view；不持有 Reader。|
|DatasetRuntime|绑定一次独立 DataFrame 构造，管理 FileGeneration、对象/查询 lease、ReaderSessionPool、PreparedSeriesCache 和 DatasetMergeKernel。|
|ReaderSessionPool / ReaderSession|按 FileGeneration 获取 ReaderSession；session 持有 FD、ReadFile、executor/decoder、Reader arena 和执行状态；max_open_files 是 v1 唯一 Runtime 资源硬限制。|
|PreparedSeriesCache|以 (FileGeneration, locator_id) 为 key，single-flight 构建并长期持有独立所有权的 PreparedSeries；v1 不定义 entry/byte 上限或 LRU。|
|TsFileReader / ResultSet|负责单文件定点 prepare、prepared 查询、解码和现有 ResultSet；查询期间由 Runtime 同时固定 QueryLease、ReaderSession lease 和 PreparedSeries lease。|
|DatasetMergeKernel|第一版在 typed Cython 中完成连续拼接、k-way merge、重复时间戳检查和多列时间戳对齐。|

### 3\.2\.3 逻辑序列与物理定位

Format v1 的查找与路由链路为 TableNameIndex/TableRecord → DeviceNameIndex/DeviceRecord → ColumnNameIndex/ColumnSchema → LogicalSeries → SeriesFileSpan → SeriesLocator；TsFileRecord 提供 file\_id 对应的源文件记录。设计中不存在 MetaTree、SeriesSegment 或 local\_series\_id。

- 时间范围查询使用 SeriesFileSpan 的时间边界和 prefix\_max\_time 跳过不相交文件，再展开命中的 SeriesLocator。

- 非对齐序列由 SeriesLocator 指向 value TimeseriesMetadata；对齐序列由 DeviceFileSpan 提供共享 time metadata，SeriesLocator 提供 value metadata，并校验二者属于同一 file、device 与 aligned group。

- 位置路由必须区分 row\_count 与 point\_count：aligned row 位置以共享时间轴为准，value point\_count 只表示非空值数量，不能代替行数。

- 不重叠文件可利用累计 count 直接定位；存在时间重叠时由 Cython DatasetMergeKernel 执行有序归并，并保持重复时间戳错误语义。

## 3\.3 Dataset Index 文件与数据结构

[Dataset Index 文件格式与查询设计](<./Dataset Index 文件格式与查询设计.md>)



## 3\.4 进程本地 Native Runtime

[TsFileDataFrame Runtime 设计](<./TsFileDataFrame Runtime 设计.md>)

## 3\.5 Reader 与 Prepared Query Context

[TsFile Reader 与 Prepared Query Context 设计](<./TsFile Reader 与 Prepared Query Context 设计.md>)

跨层契约只有以下几点：Runtime 向 ReaderSession 提供已验证的 FileGeneration 和 locator；Reader 在独立 arena 中构建 FileGeneration-bound PreparedSeries，并为每次查询创建局部 SSI/decoder/ResultSet；PreparedSeries 不持有 Reader 内部指针，可跨同 generation 的 ReaderSession 复用；可选 PagePositionIndex 只优化重复位置查询。Dataset 查询在 ResultSet 完整消费并关闭前同时固定 QueryLease、ReaderSession lease 和 PreparedSeries lease。C++ 表示、prepared 接口和 PagePositionIndex 细节只由 Reader 子文档定义。

## 3\.6 Dataset Index 与运行时加载策略

### 3\.6\.1 Format v1 映射与校验

Dataset Index 的唯一格式定义见 Format 子文档。当前 v1 编码的 `header_size=64`、`directory_entry_size=32` 和 `section_count=14` 是当前 parser 的兼容条件，不是永久架构上限。解析器按 version、size 和 SectionDirectory 校验当前支持的布局；新增可选 section 或扩大结构通过版本化兼容规则演进，不兼容变化升级 major version。

MappedDatasetIndex 只有在 Header、DirectoryEntry、section 边界、记录宽度、字符串引用和外键等全部校验通过后才暴露 typed view。业务 section 的实际位置始终从 SectionDirectory 读取；主文档不维护 offset、record width 或字段表。

### 3\.6\.2 加载后的内存分层

|区域|内容与生命周期|
|---|---|
|Dataset Index mapping|完整索引以只读 file-backed mmap 建立虚拟地址映射，section 不复制到完整 Python catalog。驻留页按访问触发并由 OS 回收；同机进程可共享底层干净页。|
|Runtime 基础对象|每次独立 DataFrame 构造对应的 mapping、ReaderSessionPool、PreparedSeriesCache、DatasetMergeKernel 和 lease 状态。|
|ReaderSessionPool|保存当前打开的 ReaderSession；每个 session 包含 FD、ReadFile、executor/decoder、Reader arena 和执行状态；严格执行内部 max_open_files。|
|PreparedSeriesCache|保存 FileGeneration-bound 的独立序列索引、arena 和可选 PagePositionIndex；v1 不设置 entry/byte 硬限制。|
|查询与结果|QueryLease、ReaderSession/PreparedSeries lease、ResultSet、decoder、typed merge 输入/输出和公开返回对象；随查询或对象生命周期释放。|

### 3\.6\.3 按需访问策略

1. DataFrame 初始化只完成路径展开、索引校验或构建、只读映射、Runtime 和初始 RuntimeLease 创建；ReaderSessionPool 与 PreparedSeriesCache 均为空。

2. 精确名称查找通过 TableNameIndex、DeviceNameIndex 与 ColumnNameIndex；列表与结构化 prefix 查询按 Format v1 的记录顺序扫描需要的 LogicalSeries，并只在构造 Python 返回值时解码 StringOffsets/StringBytes。

3. 查询从 LogicalSeries 进入 SeriesFileSpan，使用时间统计与 prefix\_max\_time 剪枝，再读取命中的 DeviceFileSpan/SeriesLocator；不会在 heap 上恢复完整 MetaTree。

4. 命中具体 FileGeneration 后才获取 ReaderSession；命中 locator 后以 (FileGeneration, locator_id) 获取或构建 PreparedSeries。ReaderSession 淘汰不使 PreparedSeries 失效。

### 3\.6\.4 容量与验收口径

索引精确长度 I 由 Format builder 根据当前版本的 SectionDirectory、记录数量、记录宽度、字符串字节数和对齐规则计算；不能把旧布局的固定小计当作当前版本容量。

```text
mapped_bytes       = I
0 ≤ resident_index_pages(t) ≤ I
observed_private(t) = runtime_base + open_readers
                      + prepared_resident + active_query/merge/results
```

验收时分别记录 mapped_bytes、索引页 RSS/PSS、进程 USS/private dirty、open Reader 数、PreparedSeries 数量及其 PageArena/TimeseriesIndex/ChunkMeta/PagePositionIndex 占用、Cython merge 临时输出和公开结果。只有 max_open_files 是 v1 硬约束；这些内存数据是观测指标，不构成 DataFrame 总内存上限。

## 3\.7 完整执行流程、一致性与跨进程并发

### 3\.7\.1 独立 DataFrame 初始化

1. TsFileDataFrame\.\_expand\_paths 沿用现有语义展开文件或目录，递归收集 \.tsfile，排序并转为绝对路径；公开构造参数仍只有 paths 与 show\_progress。

2. 初始化取得目标 Dataset Index 的构建锁，按 Format 子文档校验当前版本布局及其与输入文件记录的关系。有效时直接映射；缺失或无效时由 DatasetIndexBuilder 构建。

3. Builder 可以并行扫描单文件元数据，但字符串驻留、schema/series 合并、file\_id 分配、section 排序与最终写出必须全局确定。完整临时文件写入并校验后通过 fsync 与原子 rename 发布。

4. MappedDatasetIndex 对稳定索引建立只读 mmap，完成结构校验并创建 typed view；Runtime 不把全部 section 复制为 Python catalog。

5. 当前 DataFrame 创建独立 DatasetRuntime、空 ReaderSessionPool、空 PreparedSeriesCache 和自己的 RuntimeLease。构造函数返回后才开放查询；此时不预先打开 Reader，也不预构建 PreparedSeries。

活动 Runtime 不接收索引重建通知，也不切换到新的 mapped index 或文件 generation；调用者必须重新构造 DataFrame 才能读取新 generation。

### 3\.7\.2 按时间范围读取

1. 当前 handle 原子获取 QueryLease；close 已先完成时拒绝查询，反之查询允许完整结束。

2. MappedDatasetIndex 通过名称索引解析 LogicalSeries，读取 SeriesFileSpan，并使用时间边界与 prefix\_max\_time 跳过不相交文件；命中的 span 展开为 DeviceFileSpan/SeriesLocator。

3. Runtime 按 FileGeneration 获取并校验 ReaderSession，再以 (FileGeneration, locator_id) 查询 PreparedSeriesCache。miss 时调用 prepareSeriesFromLocator；同 key 并发 prepare 由 single-flight 去重。

4. Reader 调用 queryPrepared。整个 ResultSet 消费期间同时持有 ReaderSession lease 和 PreparedSeries lease。

5. 单文件 ResultSet 在当前 Dataset 读取方法中被完整消费并关闭；typed timestamp/value parts 交给 Cython DatasetMergeKernel，执行连续拼接或有序 k-way merge，并完成重复时间戳检查和多字段对齐。

6. ResultSet 关闭后依次释放 PreparedSeries lease、ReaderSession lease 和 QueryLease，最终返回现有 NumPy、Timeseries 或 AlignedTimeseries 结果。

### 3\.7\.3 按位置读取

不重叠文件按 SeriesFileSpan 的 count 与时间统计直接路由；存在重叠时由 Cython DatasetMergeKernel 归并，并保持重复时间戳抛出 ValueError 的语义。aligned 数据的位置语义使用共享时间轴的 row_count，不以 value point_count 代替。

命中单文件后调用 queryPreparedByRow。重复位置查询可使用 PreparedSeries 的增量 PagePositionIndex；没有重复查询时该索引可以保持为空。其定位与并发发布规则见 Reader 子文档。

### 3\.7\.4 一致性、并发与关闭

- 构建锁只串行化指向同一索引文件的初始化。拿到锁后重新校验；若其他进程已发布有效 v1 索引则直接映射。查询阶段不持有构建锁。

- 稳定索引发布后按不可变文件使用。不同 Runtime 和不同进程只通过只读 mmap 与 OS Page Cache 共享文件页；不共享 Reader、PreparedSeries、FD、ResultSet 或 C++ 指针。

- ReaderSessionPool 只回收 active_uses=0 的 idle session。达到 max_open_files 且全部 session 活跃时，acquire 等待 release，不临时超限；session 关闭不影响同 FileGeneration 的 PreparedSeries。

- 任一 DataFrame close 只关闭当前 handle 并释放其 RuntimeLease；其他 view 和 Timeseries 继续可用。最后一个对象 lease 释放后停止接受新查询，等活动 QueryLease 归零，再清空 PreparedSeriesCache、关闭 ReaderSessionPool 并解除 mmap。

## 3\.8 Python API 与兼容性

### 3\.8\.1 公开入口保持不变

```python
df = TsFileDataFrame(paths, show_progress=True)
series = df[0]
window = df.loc[start:end, series_names]
df.close()
```

公开构造签名保持 TsFileDataFrame\(paths: Union\[str, List\[str\]\], show\_progress: bool = True\)。索引路径、校验/构建、mmap 和 max_open_files 都是内部实现；不增加 index_path、RuntimeConfig、memory budget、prepared cache budget 或查询配置对象。

### 3\.8\.2 现有选择与返回类型

|入口|保持的语义与返回|
|---|---|
|df\[int\] / df\[str\]|按位置或完整逻辑序列名选择，返回现有 Timeseries；字符串未命中序列名时沿用元数据列选择语义。|
|df\[slice\] / df\[list\[int\]\] / df\[pandas\.Series\]|创建轻量 TsFileDataFrame 子视图，不复制索引、Reader 或 PreparedSeries cache。|
|df\.loc\[start:end, series\]|沿用 int/slice 时间范围和 str/int 序列选择，按时间戳并集对齐并返回 AlignedTimeseries。|
|list\_timeseries / list\_timeseries\_metadata|分别返回现有 List\[SeriesPath\] 与 pandas\.DataFrame；数据来自 mapped Format v1 记录，仅在构造返回值时创建 Python 字符串和对象。|
|Timeseries|保持惰性读取与数值返回语义；持有 RuntimeLease，并增加幂等 close() 和上下文管理器。|
|AlignedTimeseries|是已经完成读取和对齐的物化结果，不持有 RuntimeLease。|

### 3\.8\.3 子视图、关闭与进程边界

初始 DataFrame、派生 view 与惰性 Timeseries 各自持有 RuntimeLease。关闭初始 DataFrame 不影响其他未关闭 handle；Timeseries 的显式 close()/context manager 是确定释放 lease 的主路径，finalizer 只作兜底。未来若增加惰性 iterator，iterator 自身或每个产出的 Timeseries 也必须持有独立 lease。

每次独立构造 DataFrame 始终创建独立 Runtime；当前阶段不定义新的 pickle/spawn 协议。跨进程可共享的只有 Dataset Index 和 TsFile 的 file-backed OS pages。

### 3\.8\.4 内部类型与格式约束

Python 包不公开 DatasetRuntime、ReaderSessionPool、PreparedSeriesCache、PreparedSeries 或 DatasetMergeKernel。用户仍通过 TsFileDataFrame、Timeseries 与 AlignedTimeseries 完成 Dataset 操作。

Dataset Index 不保存 JSON、pickle、C++ 指针、Python 对象或执行状态。具体 magic、Header、DirectoryEntry、section、字段和兼容规则只由 Format 子文档定义；主文档不复制第二套布局。

## 3\.9 实施结论与发布验收（2026-08-14）

本设计已在 `feature/tsfile-dataframe-runtime` 分支实现。实现从 Apache `origin/develop` 的 `5681978b84a0d9ee26e1eab94f78afaccf93607a` 建立独立 worktree；四份冻结设计位于提交 `506f2a8bb`，主体代码位于提交 `a03a7f1d3`。Format、Runtime、Reader/Prepared 与 Python DataFrame 的实现边界分别落在对应子文档中，本节只汇总发布判断。

|范围|已实现内容|验收|
|---|---|---|
|Dataset Index Format v1|C++/Python 14-section codec、CRC32C、结构与引用校验、原子发布、只读 mmap、名称/series/span 查找、generation 校验|合成 C++ 测试与 506,136,064-byte Python 实际索引的 C++ 完整打开均通过|
|C++ Reader / Prepared|精确 TimeseriesMetadata range、独立 arena、aligned time/value 配对、prepared query C/Cython handle、查询局部 ResultSet|Release shared library 构建通过；C++ 聚合回归 757 passed、1 个外部数据可选测试 skipped；真实索引测试另行 7/7 通过|
|进程本地 Runtime|独立 Runtime、对象/查询 lease、Reader LRU 硬上限、Prepared single-flight、lazy mapped catalog、确定性 close|生命周期、并发、stale generation、Reader cap 与 cache 复用测试通过|
|Python / Cython|公开 DataFrame API 兼容、热启动 mmap 路径、typed overlap merge、view/Timeseries lease|Python 181/181 通过；Snappy/LZ4/LZO/Zlib 数据均由正式构建回归|

`/data/lotsa` 实际是 116 个独立数据集目录，共 647 个 TsFile、约 519 GiB，不应作为一个混合 schema DataFrame 直接加载。本次选择其中最大的完整数据集 `buildings_900k`：61 个 TsFile、65,856,302,628 bytes、1,792,328 条逻辑序列。冷构建耗时 205.13 秒，索引 506,136,064 bytes，峰值 RSS 11,250,788 KiB，无 swap；这是一次性的 metadata object graph 与 section 排序/写出成本。

稳定索引上的 1/2/4 进程热启动结果如下。每个进程构造独立 Runtime，读取同一 file-backed mmap，并执行一次及重复 256 行 prepared 查询。

|进程数|构造中位数|首次查询中位数|重复查询中位数|总 PSS|总 USS|
|---:|---:|---:|---:|---:|---:|
|1|529.26 ms|1.26 ms|0.300 ms|84.95 MiB|57.98 MiB|
|2|527.26 ms|1.26 ms|0.302 ms|144.19 MiB|99.45 MiB|
|4|523.28 ms|1.20 ms|0.300 ms|251.13 MiB|198.09 MiB|

发布结论：核心 Format、Runtime、C++ prepared path、Python API 与 1/2/4 多进程目标已经达到当前 v1 验收条件。`PagePositionIndex` 的并发、无空洞发布结构已经实现，但实际 SSI 扫描尚未向其中回填 page offset；当前按行查询继续走正确的 fallback scan。这是可选性能优化的明确剩余项，不影响索引格式、查询正确性或本次多进程内存结论。
