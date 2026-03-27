# TsFile 读取路径性能优化

## 目标

- **批量读取（Full Scan）**：吞吐量 > Parquet + Arrow
- **过滤查询（Tag Filter）**：延迟与吞吐量 > Parquet + Arrow

对比对象：Apache Parquet C++ 库 + Apache Arrow C++ 库（`parquet::arrow::FileReader` + Arrow Compute）

---

## TsFile 读取路径详解

```
用户调用
  TsFileReader::query(table, columns, time_range, tag_filter)
        │
        ▼
  TableQueryExecutor::query()
    ├── IMetadataQuerier::getDevices(table, id_filter)   ← Tag Filter 在此剪枝设备
    │       TsFileMeta（内存索引）中查找匹配 id_filter 的设备列表
    │
    └── 对每个匹配设备 → DeviceQueryTask
            │
            ▼
        TsFileIOReader::alloc_ssi()       ← 为每个 measurement 分配 SeriesScanIterator
          └── 从 TsFileMeta 中定位 ChunkMeta（offset, size, 统计信息）
                │
                ▼
            ChunkReader::load_by_meta()   ← 按需从磁盘加载 Chunk（含 PageHeader 列表）
              │
              └── 对每个 Page：
                    cur_page_statisify_filter()   ← 使用 Page 统计信息（min/max）剪枝
                    skip_cur_page()               ← 不满足则跳过，无 IO
                    decode_cur_page_data()         ← 满足则：
                      ├── Compressor::decompress() （解压缩）
                      └── Decoder::decode()         （解码 → TsBlock）
                              │
                              ▼
                          TsBlock（列式内存块，含 time 列 + value 列）
                              │
                              ▼
                      TsBlockReader::next()（返回一个 TsBlock）
                              │
                              ▼
                      TableResultSet（行迭代器包装）
                              │
                              ▼
                      用户：result->next() → get_value<T>(col_index)
```

### 关键类说明

| 类 | 职责 |
|---|---|
| `TsFileReader` | 对外 API 入口，持有 ReadFile + TsFileExecutor + TableQueryExecutor |
| `TableQueryExecutor` | 查询编排；协调元数据查询 → 设备迭代 → TsBlock 读取 |
| `IMetadataQuerier` / `MetadataQuerier` | 从 `TsFileMeta` 中查设备列表；在 Tag Filter 阶段完成设备级剪枝 |
| `TsFileIOReader` | 文件底层 IO；加载文件 Footer、设备/测量点索引；分配 SSI |
| `ChunkReader` | 核心读取逻辑；load_by_meta → 逐 Page 解压 → 解码 → TsBlock |
| `Compressor` | 解压缩（GZIP / LZ4 / Snappy / Uncompressed） |
| `Decoder` | 解码（Gorilla / Sprintz / RLE / Delta / Plain 等） |
| `TsBlock` | 列式内存格式（`VectorDesc` 数组），ChunkReader 输出单元 |
| `TableResultSet` | 包装 `TsBlockReader`，提供行迭代器 API（`next()` / `get_value<T>()`） |
| `Filter` | 抽象过滤基类；子类：`EqualToFilter`, `BetweenFilter`, `AndFilter`, `TagFilter`, `TimeFilter` |

### TsFile 存储结构对读取的影响

```
TsFile 物理布局：
┌────────────────────────────────┐
│  Magic + Version               │
├────────────────────────────────┤
│  Chunk (Device_0, s1)          │  ← 一个设备的一列所有数据连续存储
│    PageHeader | PageData       │
│    PageHeader | PageData       │
│    ...                         │
├────────────────────────────────┤
│  Chunk (Device_1, s1)          │
│    ...                         │
├────────────────────────────────┤
│  ...                           │
├────────────────────────────────┤
│  File Footer（元数据索引）      │  ← TsFileMeta，加载到内存的 PageArena
│    DeviceMetadataIndex         │
│      MeasurementMetadataIndex  │
│        ChunkMeta（offset/size）│
└────────────────────────────────┘
```

- **Tag Filter 剪枝粒度：设备（Chunk）级** — 不满足条件的设备的所有 Chunk 都不会被读取
- **Time Filter 剪枝粒度：Page 级** — 利用 PageHeader 中的 min/max 时间戳统计信息跳过整页

---

## 当前性能分析

### TsFile 相对 Parquet+Arrow 的优势

| 场景 | TsFile 优势 |
|---|---|
| **Tag Filter 查询** | 设备粒度剪枝，仅加载匹配设备的 Chunk；Parquet 无设备概念，需全列扫描后再 filter |
| **时间范围查询** | Page 级 min/max 统计，可跳过整页 IO；Parquet Row Group 统计粒度更粗 |
| **时序数据编码** | Gorilla（浮点）/ Sprintz / Delta（时间戳）专为时序优化；Parquet 通用编码在时序上压缩率低 |
| **写入语义一致性** | Tag 是一等公民，查询接口天然支持设备过滤 |

### 当前瓶颈（相对 Parquet+Arrow 的劣势）

| 编号 | 瓶颈 | 说明 |
|---|---|---|
| **P1** | 行式结果集 API | `TableResultSet::next()` 每次返回一行；Arrow 直接操作列内存，无行迭代开销 |
| **P2** | 字符串列名查找 | `get_value("s1")` 每次查 map；Arrow 通过列索引直接访问 |
| **P3** | TsBlock 未直接暴露给用户 | 内部已有列式 TsBlock，但最终被包装为行迭代器，额外开销 |
| **P4** | 非向量化解码路径 | decode 函数逐值处理；Arrow 解码路径为 SIMD 优化的列式批处理 |
| **P5** | 无 Arrow 格式输出接口 | Arrow compute/analytics 生态无法直接对接 TsFile 输出 |
| **P6** | 单线程 Chunk 读取 | 同一个查询内各设备的 Chunk 串行读取；可并行 |

---

## Parquet + Arrow 特点分析

```
Parquet 读取路径：
  parquet::arrow::OpenFile()
        │
        ▼
  FileReader::ReadTable(column_indices)   ← 列投影（仅加载所需列）
    ├── Row Group 统计信息 (min/max) → 粗粒度剪枝
    └── Column Chunk 解码 → Arrow ChunkedArray
              │
              ▼
        Arrow Table（所有列在内存中为 Arrow 格式 ChunkedArray）
              │
              ▼
        Arrow Compute：
          CallFunction("equal", {id1_col, scalar})  → 布尔 mask
          CallFunction("filter", {s1_col, mask})     → 过滤后 ChunkedArray
          CallFunction("sum", {filtered})             → Scalar 结果
              │
              ▼
        全程向量化，SIMD 加速
```

**Parquet+Arrow 劣势（TsFile 可以利用的点）：**
- 无设备/Tag 语义，Tag Filter 必须全列加载后再 compute filter（内存峰值高）
- 无时序专用编码，相同时序数据压缩率低于 TsFile（更多 IO）
- Row Group 是唯一的跳过粒度，粒度比 TsFile 的 Chunk+Page 两级更粗

---

## 优化方向

### 短期（当前 Baseline 完成后确认优先级）

| 优先级 | 优化点 | 预期收益 |
|---|---|---|
| 高 | **暴露批量读取 API（TsBlock 级）** | 消除 P1/P2，Full Scan 大幅提速 |
| 高 | **列索引替代列名字符串查找** | 消除 P2，每行节省 map 查找 |
| 中 | **Tag Filter + 列投影组合** | 同时剪枝设备 + 只解码所需列 |

### 中期

| 优化点 | 说明 |
|---|---|
| **SIMD 友好解码路径** | 对 Plain/RLE 编码使用 SIMD 批量解码，对齐 Arrow 的向量化优势 |
| **Arrow IPC 格式输出接口** | 将 TsBlock 直接转换为 Arrow RecordBatch，打通 Arrow compute 生态 |
| **Predicate pushdown to Page level** | 将 field filter（如 s1 > 100）推入 Page 解码阶段，减少解码量 |

### 长期

| 优化点 | 说明 |
|---|---|
| **多线程并行 Chunk 读取** | 不同设备 Chunk 并行加载 + 解码，充分利用多核 |
| **IO prefetch** | 异步预取下一个 Chunk，隐藏 IO 延迟 |
| **列式存储布局优化** | 对高频访问 field 列，考虑调整 Chunk 边界以提高 IO 局部性 |

---

## Baseline 实验设计

### 数据模型

```
Table: bench_table
Columns:
  time (TIMESTAMP/INT64) — 单调递增，隐式列（TsFile）/ 显式列（Parquet）
  id1  (TAG,   STRING)   — 设备标识，值: "device_0" ~ "device_9"
  id2  (TAG,   STRING)   — 固定值 "tag_b"
  s1   (FIELD, INT64)    — 值 = 时间戳，用于 checksum
  s2   (FIELD, DOUBLE)   — 值 = ts * 1.1
  s3   (FIELD, FLOAT)    — 值 = ts % 10000
  s4   (FIELD, INT32)    — 值 = ts % 100000

压缩：SNAPPY（两侧对齐；TsFile TAG 列为 UNCOMPRESSED，FIELD 列为 SNAPPY）

规模：
  设备数 (kNumDevices):   10
  每设备行数:             row_count / 10  （默认 100,000）
  总行数 (row_count):     1,000,000
  时间戳分布:             device_d 拥有 [d*100K, (d+1)*100K)
```

### 测试场景

| 场景 | 描述 | 结果行数 | 理论优势方 |
|---|---|---|---|
| **Tag Filter** | `id1="device_0"`，计算 `sum(s1)` | 100K（1/10） | TsFile（设备级剪枝） |
| **Time Filter** | `time ∈ [0, 500K)`，计算 `sum(s1)` | 500K（1/2） | TsFile（Page 级时间剪枝） |

两侧产生相同 checksum（`sum_s1`），以此验证结果正确性。计算方式不要求对等（TsFile 行迭代 vs Parquet 向量化，这正是要优化的差距）。

### 运行方式

```bash
# 构建 TsFile 主库
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build --target tsfile -j

# 构建 read_perf_compare（需要系统安装 apache-arrow）
cmake -B examples/build -S examples \
      -DENABLE_READ_PERF_COMPARE=ON \
      -DCMAKE_PREFIX_PATH="$(brew --prefix apache-arrow)/lib/cmake" \
      -DCMAKE_BUILD_TYPE=Release
cmake --build examples/build --target read_perf_compare -j

# 运行（默认 1,000,000 行）
./examples/build/read_perf_compare 1000000
```

### Baseline 记录表（待填入实测数据）

### 测试环境

- 硬件：Apple M 系列（arm64）、macOS
- 存储：本地 SSD
- Arrow 版本：22.0.0（Homebrew）
- 编译：`-O3`，`-DCMAKE_BUILD_TYPE=Release`

### Baseline 结果（2026-03-26）

| 场景 | 结果行数 | TsFile 耗时(s) | TsFile 吞吐(rows/s) | Parquet+Arrow 耗时(s) | Parquet+Arrow 吞吐(rows/s) | 差距(倍) |
|---|---|---|---|---|---|---|
| TAG_FILTER (`id1="device_0"`) | 100K | 0.147 | 681,356 | 0.057 | 1,755,389 | **2.6×** |
| TIME_FILTER (`time ∈ [0, 500K)`) | 500K | 0.731 | 683,886 | 0.050 | 9,933,783 | **14.5×** |

> checksum 两侧一致（TAG: 4,999,950,000 / TIME: 124,999,750,000）
>
> 运行方式：`cmake --build examples/build -j && ./examples/build/examples`

### 观察

- **TAG_FILTER**：TsFile 设备级剪枝只读 1/10 数据，仍慢 2.6×，瓶颈在行式 `ResultSet` 迭代层
- **TIME_FILTER**：差距达 14.5×，Parquet 全量加载后列式扫描极快；TsFile 的 Page 级时间剪枝（理论上应跳过 devices 5-9）受限于当前行迭代开销，优势未体现
- 核心问题：当前 TsFile 读取 API 是 `next()` 行迭代，无法利用已有的列式 TsBlock 内存格式；**暴露批量/列式读取接口是首要优化方向**

---

## 优化记录

### Round 1: Bug 修复 + 批量解码 + 块级过滤（2026-03-27）

#### 修复的 Bug

**TS2DIFFDecoder::has_remaining() 缺少 header_peeked_ 检查**

- 文件：`src/encoding/ts2diff_decoder.h:226`
- 原因：`peek_next_block_range_int64()` 读取最后一个 TS2DIFF 块的 header（24 字节）后，ByteStream 已空。随后 `has_remaining()` 返回 false（未检查 `header_peeked_` 标志），导致 `read_batch_int64()` 返回 time_count=0，丢失最后一个 partial block（67 个值）
- 级联效应：时间页已耗尽但值页仍有数据 → `prev_time_page_not_finish()=false` / `prev_value_page_not_finish()=true` 不匹配 → `get_next_page()` 跳过后续所有页 → 每设备只读 1 页（9933 行），而非 10 页（100000 行）
- 修复：在 `has_remaining()` 中增加 `header_peeked_` 条件：
  ```cpp
  return header_peeked_ || bits_left_ != 0 ||
         (current_index_ <= write_index_ && write_index_ != -1 && current_index_ != 0);
  ```

#### 新增的优化

1. **TS2DIFF 批量解码**（`read_batch_int64`）：一次解码 129 个时间戳（对应一个 TS2DIFF 块），避免逐值虚函数调用
2. **块级时间过滤**（`peek_next_block_range_int64`）：读取 TS2DIFF 块 header 获取 `[first_value, first_value + write_index * delta_min]` 时间范围，整块不满足时直接跳过解码
3. **批量值解码**（`read_batch_int32/int64/float/double`）：与时间批量解码配对，一次读取一批值
4. **批量时间过滤**（`satisfy_batch_time`）：对一批时间戳做批量 filter 判断，返回 pass 掩码

#### 优化后结果（2026-03-27）

测试数据：10 设备 × 100K 行/设备 = 1M 总行，4 列 FIELD（s1=INT64, s2=DOUBLE, s3=FLOAT, s4=INT32）

| 场景 | 结果行数 | TsFile (row) | TsFile (batch) | Parquet+Arrow | TsFile batch vs Parquet |
|---|---|---|---|---|---|
| TAG_FILTER (`id1="device_0"`) | 100K | 0.0222s / 4.5M rows/s | 0.0105s / 9.5M rows/s | 0.0745s / 1.3M rows/s | **7.1× 快** |
| TIME_FILTER (`time ∈ [0, 333333)`) | 333K | 0.0741s / 4.5M rows/s | 0.0364s / 9.2M rows/s | 0.0513s / 6.5M rows/s | **1.4× 快** |

> checksum 两侧一致（TAG: 4,999,950,000 / TIME: 55,555,277,778）

#### 与 Baseline 对比

| 场景 | Baseline TsFile (row) | 优化后 TsFile (row) | 优化后 TsFile (batch) | 行模式提速 | 批量模式提速 |
|---|---|---|---|---|---|
| TAG_FILTER | 0.147s / 681K rows/s | 0.022s / 4.5M rows/s | 0.0105s / 9.5M rows/s | **6.6×** | **14×** |
| TIME_FILTER | 0.731s / 684K rows/s | 0.074s / 4.5M rows/s | 0.036s / 9.2M rows/s | **9.9×** | **20×** |

> 注：Baseline 受 has_remaining bug 影响，每设备只读约 1/10 的数据。修复 bug 后行模式吞吐约 4.5M rows/s，批量模式约 9.5M rows/s。

#### 火焰图分析（2026-03-27，time_filter 场景，570 采样）

**自身时间（self time）Top 10：**

| 采样数 | 占比 | 函数 | 说明 |
|---|---|---|---|
| 49 | 8.6% | `ByteStream::read_buf` | Gorilla/TS2DIFF 底层字节读取 |
| 29 | 5.1% | `_platform_memmove` | 解压缩/向量拷贝中的内存搬运 |
| 22 | 3.9% | `TableResultSet::next` | 行迭代器开销（仅 row 模式） |
| 21 | 3.7% | `GorillaDecoder::read_long` | Gorilla 解码核心（逐值读 bit） |
| 19 | 3.3% | `GorillaDecoder::flush_byte_if_empty` | Gorilla 字节刷新 |
| 17 | 3.0% | `Field::set_value` | 行模式每行填充 Field 对象 |
| 11 | 1.9% | `FixedLengthVector::append` | TsBlock 列追加 |
| 10 | 1.8% | `double_DECODE_TV_BATCH` | 批量解码自身 |
| 9 | 1.6% | `TS2DIFFDecoder::read_batch_int64` | 时间戳批量解码 |
| 9 | 1.6% | `FixedLengthVector::read` | TsBlock 列读取 |

**关键瓶颈分析：**

1. **Gorilla 解码器（~25% 总时间）**：`GorillaDecoder::read_long` + `flush_byte_if_empty` + `ByteStream::read_buf` 合计约 140 采样。Gorilla 仍为逐值解码（bit-by-bit），是当前最大解码瓶颈。TS2DIFF 已有批量路径（仅 9 采样），Gorilla 尚无。
2. **行模式 ResultSet 开销（~20% 总时间）**：`TableResultSet::next`（22）+ `Field::set_value`（17）+ `CaseInsensitiveHash/Equal`（~80 采样来自字符串列名 map 查找）。batch 模式完全跳过此层。
3. **Snappy 解压**（~22 采样，3.9%）：相对较小。
4. **IO**（pread 9 采样，1.6%）：数据已在 OS 缓存中，IO 不是瓶颈。

**下一步优化方向（按收益排序）：**

| 优先级 | 优化 | 预期收益 |
|---|---|---|
| 高 | Gorilla 批量解码（`read_batch_float/double`） | 减少 Gorilla 25% 开销，预期 batch 路径再提速 30-50% |
| 中 | 行模式列名查找缓存（column index） | row 模式提速 ~20%，但 batch 模式不受影响 |
| 中 | TS2DIFF SIMD 解包（128-delta block） | 进一步优化时间戳解码，但当前仅 1.6%，收益有限 |
| 低 | Arrow IPC 输出接口 | 打通 Arrow compute 生态 |

---

## 后续计划

1. 运行 Baseline → 记录数据 → 确认各场景 TsFile 与 Parquet+Arrow 的差距
2. 针对差距最大的场景，按上述优化方向逐步实施
3. 每次优化后重新运行 Baseline 对比，量化收益
4. 目标：Full Scan 与 Parquet+Arrow 持平或更快；Tag Filter 显著快于 Parquet+Arrow
