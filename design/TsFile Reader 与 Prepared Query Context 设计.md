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

# TsFile Reader 与 Prepared Query Context 设计

## 1\. 文档边界

本文是 C++ Reader 与 Prepared Query Context 的权威设计。它定义单个 TsFile 内的定点 metadata 解析、PreparedSeries 所有权、prepared 查询接口、PagePositionIndex、查询局部对象和 ResultSet 生命周期。

相关文档：

- [Dataset Index 文件格式与查询设计](<./Dataset Index 文件格式与查询设计.md>)定义 SeriesLocator、DeviceFileSpan、TsFileRecord 和物理字节范围。
- [TsFileDataFrame Runtime 设计](<./TsFileDataFrame Runtime 设计.md>)定义 FileGeneration、ReaderSessionPool、PreparedSeriesCache、RuntimeLease/QueryLease 和跨文件执行边界。
- [TsFileDataFrame 持久化 mmap 索引与进程本地 Runtime 技术设计](<./TsFileDataFrame 持久化 mmap 索引与进程本地 Runtime 技术设计.md>)定义总体目标和公开 API。

本文不定义 Dataset 路由、跨文件 merge、Runtime cache 淘汰策略、Python DataFrame 选择语义或持久化索引格式。

## 2\. 现有 Reader 边界

|层级|现有对象|职责与所有权|
|---|---|---|
|Cython|TsFileReaderPy|持有原生 TsFileReader；把 Python 参数转换为 C wrapper 调用；用 WeakSet 登记活动 ResultSetPy。|
|Cython|ResultSetPy|持有原生 ResultSet、metadata、TagFilterHandle 和 Reader 弱引用；close 后清空原生指针并标记失效。|
|C\+\+|TsFileReader|持有 ReadFile、TsFileExecutor、TableQueryExecutor 和 Reader 自有 PageArena。|
|C\+\+|TableResultSet|持有 TsBlockReader、RowIterator、当前 TsBlock、结果元数据和列读取器。|

普通单文件查询继续沿用现有公开接口。prepared 接口只服务 DatasetRuntime，不改变公开的 TsFileReaderPy 查询签名和 ResultSetPy 返回类型。

## 3\. FileGeneration 与 locator 输入契约

Runtime 为每次 prepared 操作传入已经验证的 FileGeneration：

```text
FileGeneration = (
    mapped_index_identity,
    file_id,
    file_size,
    file_fingerprint,
)
```

ReaderSession 打开规范化 path 后复核 file\_size 与 file\_fingerprint。活动 Runtime 只访问静态 Dataset，不接受 TsFile 原地更新或 generation 热切换。generation 不匹配时必须在读取 locator 指向的字节前失败。

prepared locator 是从 Dataset Index 复制出的稳定值，不保存 mmap 指针：

```text
PreparedLocator {
    file_id
    locator_id
    layout
    value_metadata_offset
    value_metadata_length
    optional time_metadata_offset
    optional time_metadata_length
    chunk_count_hint
}
```

- non\-aligned 序列只使用 value TimeseriesMetadata range。
- aligned 序列同时使用 DeviceFileSpan 的共享 time range 和 SeriesLocator 的 value range。
- Reader 在反序列化前校验 offset/length 不溢出且不超过当前 TsFile 长度。
- aligned time/value metadata 必须属于同一 file、device 和已验证的 aligned group，并按 chunk ordinal 配对。

## 4\. PreparedSeries 所有权

Prepared Query Context 的具体实现类型是 PreparedSeries。它绑定 FileGeneration 和 locator，不绑定某个 Reader 实例。

```text
PreparedSeries {
    generation: FileGeneration
    locator: PreparedLocator
    arena: owned PageArena
    value_index: ITimeseriesIndex* allocated in arena
    aligned_time_index: optional ITimeseriesIndex* allocated in arena
    page_positions: optional PagePositionIndex
    page_index_state: synchronization state
}
```

不变量：

- PreparedSeries 独占其 PageArena；ITimeseriesIndex、ChunkMetadata 和相关字符串的生命周期不超过该 arena。
- PreparedSeries 不保存指向 Reader 自有 arena、ReadFile、TsFileExecutor、TableQueryExecutor、decoder 或查询局部 SSI 的指针。
- PreparedSeries 不缓存压缩块、解压页、TsBlock、value page 或查询结果。
- ReaderSession 关闭不影响同一 FileGeneration 的 PreparedSeries；重新打开并验证 generation 后可以继续使用。
- PreparedSeriesCache 是长期 owner；一次查询通过 shared ownership/lease 固定对象。本文不定义 cache 的 entry/byte 上限。

## 5\. 私有 prepared 接口

接口可以按现有 C wrapper/Cython 风格实现，下面的签名只冻结语义，不冻结最终命名：

```cpp
int prepareSeriesFromLocator(
    const FileGeneration& generation,
    const PreparedLocator& locator,
    std::shared_ptr<PreparedSeries>& prepared);

int queryPrepared(
    const std::shared_ptr<PreparedSeries>& prepared,
    int64_t start_time,
    int64_t end_time,
    ResultSet*& result);

int queryPreparedByRow(
    const std::shared_ptr<PreparedSeries>& prepared,
    int64_t offset,
    int64_t limit,
    ResultSet*& result);
```

`prepareSeriesFromLocator` 使用当前 ReaderSession 的 ReadFile 读取精确 TimeseriesMetadata range，但把反序列化对象写入 PreparedSeries 自有 arena。`queryPrepared` 和 `queryPreparedByRow` 使用当前 ReaderSession 的 ReadFile/executor/decoder 创建本次查询的局部读取器和现有 ResultSet。

Runtime 可以保留按 device/measurement 的 `prepareSeries` 作为兼容 fallback；Dataset Index 路径以 locator 定点构建为主，不重新遍历文件级 metadata index。

## 6\. 查询局部状态与 ResultSet

每次 prepared 查询独立创建可变状态：

- SSI 或等价查询游标；
- 时间/行过滤条件；
- ChunkReader、PageReader 与 decoder；
- TsBlockReader、RowIterator 和当前 batch；
- ResultSet/TableResultSet。

PreparedSeries 只提供不可变索引和可选的增量 page locator，不共享扫描位置、decoder buffer 或 ResultSet 状态。

Dataset prepared 查询在 Runtime 层同时持有 QueryLease、ReaderSession lease 和 PreparedSeries lease。ResultSetPy 必须在释放 ReaderSession lease 前完整消费并关闭；随后依次释放 PreparedSeries lease、ReaderSession lease 和 QueryLease。ReaderSessionPool 不得回收仍有 active use 的 Reader。

## 7\. PagePositionIndex

### 7\.1 用途

PagePositionIndex 只优化重复的 `queryPreparedByRow`。SeriesLocator/TimeseriesIndex 能定位序列和 chunk，但不能直接回答“全序列第 N 行位于哪个 page”；没有该索引时，每次位置查询都可能重复扫描相同 page header 前缀。

它不用于初始化 ReaderSession，不参与纯时间范围查询，也不是 page value cache。没有重复位置查询时可以一直为空。

### 7\.2 最小记录

```text
PageLocator {
    row_begin
    row_end              // half-open [row_begin, row_end)
    chunk_ordinal
    chunk_header_offset
    page_header_offset
    page_data_offset
    optional value_chunk_header_offset
    optional value_page_header_offset
    optional value_page_data_offset
}
```

- non\-aligned 序列的行数来自该 page 的时间值数量。
- aligned 序列以 time page 的 row count 为准，并保存配对 value page 的位置；value point count 不能替代时间轴行数。
- page offset 不能脱离 FileGeneration、行区间、chunk identity 和 aligned 配对信息单独使用。

### 7\.3 构建与查询

1. 首次位置查询从已有连续前缀末尾开始顺序读取 page header。
2. 每发现一个完整 page，构造完整 PageLocator；只有校验通过后才追加。
3. 目标行已被覆盖时，按 row\_begin 二分找到 PageLocator。
4. 当前 ReaderSession 从记录的 chunk/page 位置创建查询局部 cursor，并只跳过 page 内的剩余行。
5. 目标行尚未覆盖时继续扩展索引；失败时回退为普通扫描，不影响查询语义。

同一 PreparedSeries 的扩展由 entry 级互斥或 single writer 串行化。Reader 只向其他查询发布无空洞、不可变的完整前缀；失败时不得发布半条记录或推进 covered row 边界。

## 8\. Cython 与 C\+\+ 边界

- PreparedSeries、PageArena、ITimeseriesIndex 和 PagePositionIndex 的核心所有权在 C\+\+。
- C wrapper 暴露不透明 PreparedSeries handle，以及 prepare/query/release 所需的窄接口。
- Cython 负责 handle 的 Python 生命周期、错误码到现有异常的映射，并把 ResultSet 包装为现有 ResultSetPy。
- Cython 不把 mmap 指针、Reader 内部指针或 C\+\+ arena 对象暴露给 Python。
- 跨文件 DatasetMergeKernel 属于 Runtime/Cython 子文档边界，不放入单文件 Reader。

## 9\. 错误与关闭安全

|场景|行为|
|---|---|
|FileGeneration 与当前文件不匹配|在解释 locator 前返回 metadata/file identity 错误。|
|metadata range 越界、溢出或反序列化失败|不发布 PreparedSeries；返回现有 MetadataError/FileReadError 映射。|
|aligned time/value 配对不一致|拒绝 fast path，不按 ordinal 猜测。|
|PagePositionIndex 扩展失败|不发布不完整前缀；当前查询回退扫描或返回底层读取错误。|
|ReaderSession 被请求关闭但仍有活动 ResultSet|pool 不回收该 session；先等待 active use 归零。|
|PreparedSeriesCache/Runtime 最终关闭|新查询被 Runtime 拒绝；已有 lease 延迟对象实际释放。|

## 10\. 验收

- locator prepare 不遍历文件级 measurement metadata index。
- 同一 FileGeneration 的 ReaderSession 淘汰并重新打开后，已有 PreparedSeries 仍可查询。
- PreparedSeries 销毁后，其独立 arena、TimeseriesIndex、ChunkMetadata 和 PagePositionIndex 全部释放。
- 并发首次 prepare 由 Runtime cache single\-flight 去重；并发 PagePositionIndex 扩展不产生空洞或重复发布。
- aligned row 查询以 time page 行数为准，并正确处理 value null bitmap。
- ResultSet 消费期间 ReaderSession 与 PreparedSeries 均保持存活。
- 普通单文件 TsFileReaderPy API 与异常类型保持兼容。
- 基准分别记录 prepare 延迟、重复 row window 延迟、PagePositionIndex 容量和 Reader reopen 后的复用收益；v1 不以这些指标声明内存硬上限。
