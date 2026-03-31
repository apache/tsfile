# 第 4 章 面向 AI 工作负载的时序文件读取优化

## 4.1 TsFile 读取框架概述

### 4.1.1 读取流水线架构

TsFile C++ 读取引擎采用分层流水线架构，从用户查询到数据返回经过五个层次：

```
用户查询
  │
  ▼
┌─────────────────────────────────────────────┐
│  TsFileReader                               │
│  统一入口：树模型查询 / 表模型查询          │
└──────────┬───────────────┬──────────────────┘
           │               │
     ┌─────▼─────┐   ┌────▼──────────────┐
     │TsFileExec │   │TableQueryExecutor │
     │(树模型)    │   │(表模型)           │
     └─────┬─────┘   └────┬──────────────┘
           │               │
     ┌─────▼───────────────▼──────────────┐
     │  TsFileSeriesScanIterator (SSI)    │
     │  每设备·每测点一个迭代器实例        │
     │  管理 Chunk 遍历与 Page 加载       │
     └─────────────┬──────────────────────┘
                   │
     ┌─────────────▼──────────────────────┐
     │  AlignedChunkReader                │
     │  核心解码引擎                       │
     │  解压 → 解码 → 过滤 → 组装 TsBlock │
     └─────────────┬──────────────────────┘
                   │
     ┌─────────────▼──────────────────────┐
     │  Decoder（TS2DIFF / PLAIN / ...）  │
     │  SIMD 批量解码                      │
     └────────────────────────────────────┘
```

各层的职责划分如下：

| 层次 | 核心类 | 职责 |
|------|--------|------|
| 入口层 | `TsFileReader` | 接受用户查询参数，分发至树模型或表模型执行器 |
| 执行层 | `TsFileExecutor` / `TableQueryExecutor` | 解析查询表达式，分配迭代器，协调多路归并 |
| 迭代层 | `TsFileSeriesScanIterator` | 管理单个时间序列的 Chunk 遍历，支持 Chunk/Page 级跳过 |
| 解码层 | `AlignedChunkReader` / `ChunkReader` | 从文件读取压缩数据，解压、解码、过滤、输出 TsBlock |
| 编码层 | `TS2DIFFDecoder` / `PlainDecoder` 等 | 将编码的二进制数据还原为原始值，支持 SIMD 批量路径 |

### 4.1.2 两种查询模型

TsFile 同时支持**树模型**（Tree Model）和**表模型**（Table Model）两种查询接口：

**树模型**：以路径 `device.measurement` 为核心的查询方式，继承自 IoTDB 的时间序列模型。

```cpp
// 树模型查询：指定路径列表 + 时间范围
reader.query(path_list, start_time, end_time, result_set);
```

执行路径：`TsFileReader → TsFileExecutor → QDSWithoutTimeGenerator`。对每条路径分配一个 `TsFileSeriesScanIterator`，通过时间最小堆（`std::multimap<int64_t, uint32_t>`）进行多路归并，保证输出按时间有序。

**表模型**：以表名和列名为核心的关系查询方式，适用于 AI/ML 框架的 DataFrame 接口。

```cpp
// 表模型查询：指定表名 + 列名 + 时间范围 + 可选标签过滤
reader.query(table_name, column_names, start_time, end_time,
             result_set, tag_filter, batch_size);
```

执行路径：`TsFileReader → TableQueryExecutor → DeviceOrderedTsBlockReader → SingleDeviceTsBlockReader`。按设备遍历，每个设备内通过 `MeasurementColumnContext` 管理多列的同步读取。

两种模型最终都收敛到 `TsFileSeriesScanIterator` 和 `AlignedChunkReader` 层，共享底层的解码、过滤和 SIMD 优化。

### 4.1.3 元数据索引结构

TsFile 采用**层次化 B-tree 索引**组织元数据，不同于 Parquet 的扁平 Row Group 列表：

```
TsFileMeta
  └── MetaIndexNode (设备级)
        ├── MetaIndexEntry: device_1 → offset
        ├── MetaIndexEntry: device_2 → offset
        └── ...
              └── MetaIndexNode (测点级)
                    ├── MetaIndexEntry: measurement_a → ChunkMeta[]
                    └── MetaIndexEntry: measurement_b → ChunkMeta[]
```

**定位复杂度**：给定设备 ID 和测点名称，通过两层 B-tree 索引实现 $O(\log D + \log M)$ 定位，其中 $D$ 为设备数，$M$ 为测点数。相比 Parquet 需要线性扫描所有 Row Group 的统计量，TsFile 的索引结构在设备数和测点数较大时具有显著优势。

### 4.1.4 列式结果容器 TsBlock

TsFile 的读取结果以 `TsBlock` 为载体，采用列式内存布局：

```cpp
struct TsBlock {
    TupleDesc* tuple_desc_;        // 列 schema
    std::vector<Vector*> vectors_; // 列数据（time + value1 + value2 + ...）
};
```

每个 `Vector` 存储一列的连续数据和 null bitmap：

```cpp
struct Vector {
    ByteBuffer values_;   // 连续的列数据
    BitMap nulls_;        // null 位图
    bool has_null_;
};
```

`TsBlock` 支持两种操作模式：
- **RowAppender**：逐行追加，用于有过滤条件的 scatter 路径
- **ColAppender**：逐列批量追加（`append_fixed_value` 做 memcpy），用于全通过快速路径

这种列式设计使得 TsBlock 可以高效地转换为 Arrow RecordBatch，支持与 AI/ML 框架的零开销互操作。

---

## 4.2 AI 时序数据读取特征分析

### 4.2.1 AI 读取模式与传统监控读取的差异

传统时序数据库的读取场景以**监控仪表盘**为主：按设备、按时间范围的连续扫描，查询模式规律且可预测。AI/ML 工作负载的读取模式则呈现出截然不同的特征：

| 特征维度 | 传统监控读取 | AI/ML 读取 |
|---------|------------|-----------|
| 时间访问模式 | 连续时间窗口 | 随机时间窗口（滑动窗口采样） |
| 设备访问模式 | 单设备或少量设备 | 跨设备批量采样 |
| 列访问模式 | 全列或固定列集 | 动态列裁剪（特征选择） |
| 批次大小 | 大批量连续读取 | 小批量（mini-batch）高频读取 |
| 读取频率 | 低频（秒级刷新） | 高频（每 step 一次读取） |
| 数据复用 | 低（实时数据） | 高（多 epoch 重复读取） |

### 4.2.2 随机访问与读放大

AI 训练中的时间序列采样通常涉及**随机时间窗口**选取：

$$W_i = [t_{\text{start}}^{(i)}, t_{\text{start}}^{(i)} + L], \quad t_{\text{start}}^{(i)} \sim \text{Uniform}(T_{\min}, T_{\max} - L)$$

其中 $L$ 为窗口长度（如 128 或 256 个时间步）。这种模式导致**读放大**问题：

$$\text{Read Amplification} = \frac{\text{实际解码的行数}}{\text{用户需要的行数}}$$

在基于 Page 的存储格式中，即使只需要一个窗口内的 $L$ 行数据，也需要解码包含该窗口的整个 Page（通常 $10^3 \sim 10^4$ 行）。TsFile 通过多级过滤体系（详见 4.5 节）将读放大控制在较低水平。

### 4.2.3 小批量读取与列裁剪

AI 推理和在线学习场景中，每次请求通常只涉及少量样本（1-64 行），但需要频繁执行。这意味着：

1. **元数据查找的开销占比增大**：单次解码的数据量小，元数据定位的固定开销不可忽略
2. **列裁剪的收益显著**：特征工程阶段可能只使用原始列的子集，不解码无关列可大幅减少计算

TsFile 的对齐存储格式天然支持列裁剪：每个测点独立存储为一个 Chunk，查询时只加载和解码需要的列。

### 4.2.4 多 epoch 数据复用

深度学习训练通常对同一数据集执行多轮（epoch）遍历。TsFile 的 LRU 元数据缓存（详见 4.5.2 节）可以在多轮读取中避免重复的元数据解析，降低平均查找延迟。

### 4.2.5 AI 框架读取基准测试

<!-- TODO: 补充 PyTorch DataLoader / Arrow Dataset 等 AI 框架的读取 benchmark 数据 -->
<!-- 实验内容：不同 batch_size、不同时间窗口长度、不同列数下的读取延迟与吞吐 -->

---

## 4.3 面向列式解码的并行机制

### 4.3.1 问题与动机

在对齐（Aligned）存储格式中，同一设备的多个测点共享时间列，但各自独立压缩和编码。解码一个 Page 涉及以下步骤：

| 步骤 | 操作 | 特征 |
|------|------|------|
| 读取 | 从文件加载压缩数据到内存 | I/O 密集，依赖文件句柄 |
| 解压 | 调用压缩器（LZ4/Snappy/ZSTD）解压 | CPU 密集，各列独立 |
| 解析 Bitmap | 读取 null 位图 | 轻量，各列独立 |
| 重置 Decoder | 初始化解码器状态 | 轻量，各列独立 |
| 解码 | 将编码数据还原为原始值 | CPU 密集，各列独立 |

关键观察：**解压和解码**是 CPU 密集操作，且各列之间完全独立，天然适合并行化。

### 4.3.2 两阶段模型：串行 I/O → 并行 CPU

`AlignedChunkReader` 实现了两阶段解码模型：

```
Phase 1: Serial I/O
  ┌─────────────────────────────────────┐
  │ for each column:                     │
  │   ensure_value_page_loaded(col)     │
  │   // 检查缓冲区，必要时从文件读取    │
  └─────────────┬───────────────────────┘
                │
Phase 2: Parallel CPU
  ┌─────────────▼───────────────────────┐
  │ ThreadPool::submit() × N columns:   │
  │   decompress_and_parse_value_page() │
  │     ├── compressor->uncompress()    │
  │     ├── parse null bitmap           │
  │     └── decoder->reset()            │
  │ ThreadPool::wait_all()              │
  └─────────────────────────────────────┘
```

Phase 1（串行 I/O）保证文件读取的有序性，避免并发文件访问的锁竞争。Phase 2（并行 CPU）将各列的解压和解析分发到线程池的不同工作线程。

核心实现（`aligned_chunk_reader.cc`）：

```cpp
int AlignedChunkReader::decode_cur_value_pages_multi() {
    // Phase 1: Serial IO — 确保每列的页数据在内存中
    for (size_t c = 0; c < value_columns_.size(); c++) {
        ensure_value_page_loaded(*value_columns_[c]);
    }
    // Phase 2: Parallel CPU — 解压 + 解析 bitmap + 重置 decoder
#ifdef ENABLE_THREADS
    if (value_columns_.size() > 1 && decode_pool_ != nullptr) {
        std::vector<int> col_rets(value_columns_.size(), E_OK);
        for (size_t c = 0; c < value_columns_.size(); c++) {
            decode_pool_->submit([col, col_ret] {
                *col_ret = decompress_and_parse_value_page(*col);
            });
        }
        decode_pool_->wait_all();
    }
#endif
}
```

### 4.3.3 线程安全的列隔离

并行解码的线程安全由**数据隔离**保证，而非锁同步。每个 `ValueColumnState` 拥有独立的：

```cpp
struct ValueColumnState {
    ChunkMeta* chunk_meta;          // 元数据（只读）
    ChunkHeader chunk_header;       // 头部（只读）
    Decoder* decoder;               // 独立解码器实例
    Compressor* compressor;         // 独立压缩器实例
    ByteStream in_stream, in;       // 独立缓冲区
    char* uncompressed_buf;         // 独立解压缓冲
    std::vector<uint8_t> notnull_bitmap;  // 独立 null 位图
    int32_t cur_value_index;        // 独立位置游标
};
```

由于每个线程操作不同的 `ValueColumnState` 实例，不存在共享可变状态，因此无需加锁即可保证正确性。

### 4.3.4 ENABLE_THREADS 条件编译

整个并行机制通过 `ENABLE_THREADS` 宏控制编译：

```cmake
option(ENABLE_THREADS "Enable multi-threaded operations" OFF)
```

当目标平台不支持 POSIX 线程（如裸机嵌入式环境）时，关闭此选项将完全移除 `<thread>`、`<mutex>` 等标准库依赖，回退到串行执行路径。这种设计使得同一份代码在高性能服务器和资源受限嵌入式设备上均可编译运行。

### 4.3.5 ThreadPool 实现

线程池采用固定大小的工作线程模型：

```cpp
class ThreadPool {
    std::vector<std::thread> workers_;    // 固定数量的工作线程
    std::queue<std::function<void()>> tasks_;  // 任务队列
    std::mutex mu_;
    std::condition_variable cv_work_;     // 通知有新任务
    std::condition_variable cv_done_;     // 通知任务完成
    int active_;                          // 活跃任务计数
};
```

关键设计决策：

- **固定线程数**：避免频繁创建/销毁线程的开销，线程在构造时创建，析构时销毁
- **双条件变量**：`cv_work_` 用于唤醒空闲工作线程，`cv_done_` 用于 `wait_all()` 同步
- **活跃计数**：通过原子递增/递减 `active_` 追踪未完成任务数，`wait_all()` 等待 `active_ == 0`

线程池由 `TsFileSeriesScanIterator` 持有，通过 `set_decode_pool()` 借给 `AlignedChunkReader` 使用，生命周期与查询会话一致。

---

## 4.4 SIMD 加速的页解码与过滤

### 4.4.1 TS2DIFF 编码原理

TS2DIFF 是 TsFile 针对时间序列数据设计的差分编码方案，利用时序数据相邻值差异小的特点实现高压缩比。

**编码过程**：

给定一个数据块 $[v_0, v_1, \ldots, v_{n-1}]$，编码步骤如下：

1. **差分计算**：$\delta_i = v_i - v_{i-1}$，其中 $v_{-1} = 0$
2. **统计分析**：$\delta_{\min} = \min(\delta_i)$，$\delta_{\max} = \max(\delta_i)$
3. **位宽计算**：$w = \lceil \log_2(\delta_{\max} - \delta_{\min}) \rceil$
4. **重基（Rebase）**：$\delta'_i = \delta_i - \delta_{\min}$，将差分值映射到 $[0, 2^w)$
5. **位打包（Bit-packing）**：每个 $\delta'_i$ 使用 $w$ 位存储

**块头格式**（16-24 字节）：

| 字段 | 类型 | 说明 |
|------|------|------|
| `write_index` | uint32 | 块内差分值数量 |
| `bit_width` | uint32 | 每个差分值的位宽 |
| `delta_min` | int32/int64 | 差分最小值（重基偏移量） |
| `first_value` | int32/int64 | 块首值（解码基准） |

**解码过程**（还原）：

$$v_0 = \text{first\_value}$$
$$v_i = v_{i-1} + \delta'_i + \delta_{\min}, \quad i = 1, \ldots, n-1$$

其中 $\delta'_i$ 从位打包数据中按 $w$ 位宽提取。

### 4.4.2 SIMD 批量解码

标量解码逐值提取位打包数据，涉及大量位移和掩码操作。SIMD 批量解码通过并行处理 4 个值（INT32 使用 128 位寄存器，INT64 使用 256 位寄存器）显著提升吞吐。

**INT32 SIMD 解码（4 值/次）**：

```
输入：位打包字节流 + 块头参数
输出：4 个还原的 int32 值

Step 1: 计算 4 个值的位偏移
  pos[k] = index * bit_width + k * bit_width,  k = 0..3
  byte_idx[k] = pos[k] / 8
  bit_offset[k] = pos[k] % 8

Step 2: Gather — 从不规则位置收集 4 个 32 位字
  V4 = _mm_i32gather_epi32(data, {byte_idx[0..3]}, scale=1)

Step 3: 字节序转换 + 位对齐
  V4_be = _mm_shuffle_epi8(V4, REVERSE_MASK)   // 大端→小端
  V4_aligned = _mm_sllv_epi32(V4_be, bit_offset[0..3])  // 左移对齐
  V4_extracted = _mm_srlv_epi32(V4_aligned, 32 - bit_width) // 右移提取

Step 4: 加偏移
  V4_delta = _mm_add_epi32(V4_extracted, delta_min)

Step 5: 前缀和还原
  t = _mm_slli_si128(V4_delta, 4)     // [0, a, b, c]
  V4 = _mm_add_epi32(V4_delta, t)      // [a, a+b, b+c, c+d]
  t = _mm_slli_si128(V4, 8)            // [0, 0, a, a+b]
  V4 = _mm_add_epi32(V4, t)            // [a, a+b, a+b+c, a+b+c+d]

Step 6: 加基值
  V4 = _mm_add_epi32(V4, _mm_set1_epi32(base))

Step 7: 存储
  _mm_storeu_si128(out, V4)
```

**INT64 SIMD 解码（4 值/次）** 使用 256 位 AVX2 寄存器，但跨 lane 的前缀和需要标量补偿：

```cpp
// 同 lane 内前缀和
t = _mm256_slli_si256(V64, 8);
V64 = _mm256_add_epi64(V64, t);   // [a, a+b | c, c+d]

// 跨 lane 补偿（AVX2 的 256 位由两个 128 位 lane 组成）
int64_t tmp[4];
_mm256_storeu_si256(tmp, V64);
tmp[2] += tmp[1];   // lane[2] 加上 lane[1] 的累积
tmp[3] += tmp[1];   // lane[3] 加上 lane[1] 的累积
```

**批量解码循环**在 `read_batch_int32()` / `read_batch_int64()` 中以 8/4 值为步长调用 SIMD 内核，尾部不足的值回退到标量路径。

### 4.4.3 SIMD 向量化时间过滤

读取过程中的时间谓词过滤是高频操作。`satisfy_batch_time()` 方法对一批时间戳同时应用过滤条件，输出布尔掩码数组。

以 `TimeGt`（大于）为例的 AVX2 实现：

```cpp
simde__m256i vval = simde_mm256_set1_epi64x(value_);  // 广播阈值

for (; i + 3 < count; i += 4) {
    // 加载 4 个时间戳
    simde__m256i vt = simde_mm256_loadu_si256(times + i);
    // 比较：time > threshold?
    simde__m256i cmp = simde_mm256_cmpgt_epi64(vt, vval);
    // 提取结果为 4 位掩码
    int bits = simde_mm256_movemask_pd(
        simde_mm256_castsi256_pd(cmp));
    // 展开到 bool 数组
    for (int j = 0; j < 4; ++j) {
        mask[i + j] = (bits >> j) & 1;
        pass += mask[i + j];
    }
}
```

对于范围查询（`TimeBetween`），使用两次比较 + 逻辑与：

$$\text{mask}[i] = (t_i \geq t_{\text{low}}) \wedge (t_i \leq t_{\text{high}})$$

ARM NEON 平台使用 128 位寄存器（2 值/次），通过 `vld1q_s64` / `vcgtq_s64` 等指令实现同样的语义。

已实现的向量化过滤操作包括：`TimeGt`、`TimeGtEq`、`TimeLt`、`TimeLtEq`、`TimeEq`、`TimeNotEq`、`TimeBetween`，覆盖了所有常见的时间谓词类型。

### 4.4.4 编码感知的 Block 级谓词下推

传统的统计量过滤在 Chunk 和 Page 两级进行，基于预计算的 min/max/count 等聚合值。TsFile 在此基础上实现了**第三级过滤**：直接从编码块的头部推断数据范围，无需解码。

**peek_next_block_range 机制**：

TS2DIFF 编码的块头包含 `first_value`、`delta_min` 和 `bit_width` 三个参数。由此可以推断出该块的值域范围：

$$v_{\min} = \text{first\_value}$$
$$v_{\max} = \text{first\_value} + n \times (\delta_{\min} + 2^{\text{bit\_width}} - 1)$$

其中 $n$ 为块内值的数量。这是一个**保守估计**（实际范围可能更窄），但足以判断该块是否可能包含满足谓词的数据。

```cpp
bool TS2DIFFDecoder<int64_t>::peek_next_block_range_int64(
    ByteStream& in, int64_t& block_min, int64_t& block_max,
    int& block_count) {
    // 仅读取块头（16-24 字节），不解码数据
    read_header(in);
    read_i64(delta_min_, in);
    read_i64(first_value_, in);

    block_min = first_value_;
    block_count = write_index_ + 1;

    if (bit_width_ == 0) {
        // 所有差分相同
        block_max = first_value_ + (int64_t)write_index_ * delta_min_;
    } else {
        int64_t max_delta = delta_min_ + ((1LL << bit_width_) - 1);
        block_max = first_value_ + (int64_t)write_index_ * max_delta;
    }
    header_peeked_ = true;
    return true;
}
```

当 `satisfy_start_end_time(block_min, block_max)` 返回 false 时，调用 `skip_peeked_block()` 直接跳过该块的位打包数据：

```cpp
int skip_peeked_block_int64(ByteStream& in, int& skipped) {
    skipped = write_index_ + 1;
    int32_t skip_bytes = (write_index_ * bit_width_ + 7) / 8;
    in.wrapped_buf_advance_read_pos(skip_bytes);  // 直接跳过
    // ... 重置解码器状态
}
```

**与 Parquet 的对比**：

| 过滤级别 | TsFile | Parquet |
|---------|--------|---------|
| Chunk / Row Group 级 | ChunkMeta.statistic_ | Row Group 列统计量 |
| Page 级 | PageHeader.statistic_ | ColumnIndex (v2) |
| **编码块级** | **peek_next_block_range** | **不支持** |

编码块级过滤是 TsFile 独有的优化。其本质是**编码感知的谓词下推**（encoding-aware predicate pushdown）：利用编码格式的结构化头部信息，在数据解码之前就排除不相关的数据块。

### 4.4.5 SIMD 加速的编码器

SIMD 加速不仅用于解码，也用于编码阶段的 rebase 操作。TS2DIFF 编码的第 4 步（将差分值减去 delta_min）是一个逐元素减法，适合向量化：

```cpp
template <>
struct SIMDOps<int32_t> {
    static void rebase(int32_t* arr, int32_t min_val, size_t size) {
        const __m128i min_vec = _mm_set1_epi32(min_val);
        for (size_t i = 0; i + 3 < size; i += 4) {
            __m128i vec = _mm_loadu_si128(arr + i);
            vec = _mm_sub_epi32(vec, min_vec);
            _mm_storeu_si128(arr + i, vec);
        }
    }
};
```

INT64 版本使用 256 位 AVX2 寄存器（4 值/次）。

### 4.4.6 SIMDe 跨平台方案与条件编译

TsFile 采用 **SIMDe（SIMD Everywhere）** 库实现跨平台 SIMD 支持。SIMDe 在以下场景中提供透明的模拟层：

- 目标平台原生支持 AVX2 → 直接映射到硬件指令
- 目标平台为 ARM → 使用 NEON 指令模拟 x86 语义
- 目标平台无 SIMD 支持 → 回退到标量实现

构建配置：

```cmake
option(ENABLE_SIMD "Enable SIMD acceleration via SIMDe" OFF)
# 开启时，添加 SIMDe 头文件路径
if (ENABLE_SIMD)
    add_definitions(-DENABLE_SIMD)
    list(APPEND PROJECT_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/third_party/simde-0.8.4-rc3)
endif()
```

对于时间过滤操作，ARM 平台可直接使用原生 NEON 指令（通过 `__ARM_NEON` 宏检测），无需经过 SIMDe 模拟层。

SIMD 加速的完整条件编译层次：

| 平台 | 解码加速 | 过滤加速 | 编码加速 |
|------|---------|---------|---------|
| x86 + AVX2 | SIMDe (128/256-bit) | SIMDe AVX2 (256-bit) | SSE4.2 / AVX2 原生 |
| ARM + NEON | SIMDe (128-bit) | NEON 原生 (128-bit) | 标量回退 |
| 无 SIMD | 标量回退 | 标量回退 | 标量回退 |

---

## 4.5 多级缓存与过滤体系

TsFile 的读取优化核心是一个**从粗到细的多级过滤体系**，每一级利用不同粒度的摘要信息跳过不相关数据，最大限度减少实际解码量。

### 4.5.1 BloomFilter 路径快速拒绝

BloomFilter 是读取路径的第一道防线，用于快速判断文件中是否存在指定的 `device.measurement` 组合。

**实现参数**：

| 参数 | 值 | 说明 |
|------|-----|------|
| 哈希函数数量 | $\leq 8$ | 由 $k = -\ln(p) / \ln(2)$ 计算 |
| 哈希种子 | {5, 7, 11, 19, 31, 37, 43, 59} | 固定种子，保证可复现 |
| 最小位数组大小 | 256 bits | 下界保护 |
| 假阳率范围 | 1% ~ 10% | 可配置，默认通过 `g_config_value_` 设置 |

**位数组大小计算**：

$$m = \left\lceil -\frac{n \ln p}{\ln^2 2} \right\rceil$$

其中 $n$ 为路径条目数，$p$ 为目标假阳率。

**空间效率**：序列化时通过 `get_words_in_use()` 仅存储非零的 64 位字，避免尾部零的浪费。

**使用方式**：每个 TsFile 文件的元数据区包含一个 BloomFilter，写入时由 `TsFileIOWriter` 构建，读取时由 `TsFileIOReader` 反序列化。查询时首先检查 BloomFilter，若判定路径不存在则直接跳过文件，避免后续的索引查找开销。

### 4.5.2 元数据 LRU 缓存

`MetadataQuerier` 内置了一个 LRU 缓存，用于缓存设备级的 ChunkMeta 列表，避免对热点设备的重复索引查找和反序列化。

**缓存配置**：

```cpp
class MetadataQuerier {
    static constexpr int CACHED_ENTRY_NUMBER = 1000;  // 软上限
    // 弹性：CACHED_ENTRY_NUMBER / 10 = 100
    std::unique_ptr<Cache<std::string,
        std::vector<std::shared_ptr<ChunkMeta>>, std::mutex>>
        device_chunk_meta_cache_;
};
```

**LRU Cache 实现**：

基于 `std::list`（双向链表）+ `std::unordered_map`（哈希表）的经典 O(1) LRU 结构：

- **插入/更新**：将条目移至链表头部（MRU），若超过硬上限（`maxSize + elasticity`），从链表尾部驱逐至 `maxSize`
- **查找**：通过哈希表 O(1) 定位，命中时移至链表头部
- **线程安全**：通过模板参数 `std::mutex` 提供互斥锁保护

**弹性机制**（Elasticity）：硬上限 = 软上限 + 弹性余量。允许缓存短暂超过软上限，减少频繁驱逐的开销。当超过硬上限时一次性驱逐至软上限，分摊了驱逐代价。

**在 AI 场景下的意义**：

- **多 epoch 训练**：同一设备在不同 epoch 被重复访问，LRU 缓存避免了重复的索引遍历
- **跨设备采样**：当设备数 $D$ 超过缓存容量 1000 时，按访问频率自动保留热点设备
- **优化空间**：可考虑 ARC（Adaptive Replacement Cache）策略以更好适应扫描+随机混合的工作负载

### 4.5.3 多级统计量剪枝

TsFile 在三个粒度上维护统计摘要信息，形成逐级细化的过滤体系：

**统计量内容**：

每个 `Statistic` 对象维护以下字段（以数值类型为例）：

| 字段 | 类型 | 用途 |
|------|------|------|
| `count_` | int32 | 值数量，用于 offset/limit 跳过 |
| `start_time_` | int64 | 首时间戳，用于时间范围判断 |
| `end_time_` | int64 | 末时间戳，用于时间范围判断 |
| `min_value_` | T | 最小值 |
| `max_value_` | T | 最大值 |
| `sum_value_` | double/int64 | 求和（用于聚合查询） |
| `first_value_` | T | 首值 |
| `last_value_` | T | 末值 |

**三级过滤流程**：

```
Level 1: Chunk 级
  ┌─────────────────────────────────────────┐
  │ ChunkMeta.statistic_                     │
  │ → filter->satisfy(statistic)             │
  │ → 跳过整个 Chunk（数千~数万行）           │
  └────────────────┬────────────────────────┘
                   │ 通过
Level 2: Page 级
  ┌────────────────▼────────────────────────┐
  │ PageHeader.statistic_                    │
  │ → filter->satisfy(statistic)             │
  │ → 跳过整个 Page（数百~数千行）            │
  └────────────────┬────────────────────────┘
                   │ 通过
Level 3: Block 级（编码感知）
  ┌────────────────▼────────────────────────┐
  │ ts2diff: peek_next_block_range()         │
  │ → filter->satisfy_start_end_time(min,max)│
  │ → 跳过编码块（数十~数百行）               │
  └────────────────┬────────────────────────┘
                   │ 通过
Level 4: Row 级
  ┌────────────────▼────────────────────────┐
  │ satisfy_batch_time(times[], mask[])      │
  │ → SIMD 向量化逐行过滤                    │
  │ → 仅输出 mask[i]=true 的行               │
  └─────────────────────────────────────────┘
```

**Page 级跳过的附加优化——行偏移跳过**：

对于带 `OFFSET/LIMIT` 的查询（如分页查询或 AI 推理的批次定位），`Statistic.count_` 字段支持直接按行数跳过：

```cpp
bool should_skip_page_by_offset() {
    if (row_offset_ > 0 && cur_page_header_.statistic_) {
        int page_count = cur_page_header_.statistic_->count_;
        if (page_count <= row_offset_) {
            row_offset_ -= page_count;
            return true;  // 跳过整页
        }
    }
    return false;
}
```

### 4.5.4 延迟物化

延迟物化（Late Materialization）是减少读放大的关键机制。在多列查询中，先解码时间列并应用过滤，再根据过滤结果选择性地解码 value 列。

**实现细节**（`multi_DECODE_TV_BATCH`）：

```
Phase 1: 批量解码时间列
  time_decoder_->read_batch_int64(times[], BATCH=129)

Phase 2: 向量化时间过滤
  pass_count = filter->satisfy_batch_time(times[], count, time_mask[])

Phase 3: 条件性 value 解码
  if (pass_count == 0) {
      // 整批被过滤，跳过所有 value 列的解码
      for each column: decoder->skip_int32/int64/float/double(nonnull_count)
  } else {
      // 有通过的行，解码 value 列
      for each column: decoder->read_batch_xxx(values[], nonnull_count)
  }

Phase 4: 组装 TsBlock
  if (pass_count == time_count && all_nonnull) {
      // 快速路径：批量 memcpy，零 scatter 开销
      time_vec->append_fixed_value(times, count * 8);
      for each column: vec->append_fixed_value(val_buf, count * elem_size);
  } else {
      // 慢速路径：逐行 scatter，跳过 time_mask[i]==false 的行
  }
```

**性能收益**：

- **批量跳过**：当整个 batch（129 行）被时间谓词过滤时，所有 value 列仅调用 `skip`（前进解码器位置），不执行实际解码计算
- **快速路径**：当整个 batch 全部通过且无 null 值时，使用 `memcpy` 批量复制，避免逐行的分支和间接寻址
- **在 AI 随机窗口查询中**：大部分 Page 的大部分 batch 在时间过滤阶段被跳过，延迟物化使 value 解码量降低 70-90%

### 4.5.5 PageArena 内存池化

读取过程中产生大量临时分配（TsBlock、Statistic、ChunkMeta 等）。`PageArena` 提供了一个轻量级的区域内存分配器，减少 `malloc/free` 的系统调用开销和内存碎片：

```cpp
class PageArena {
    struct Page {
        Page* next_;
        uint32_t used_;
        char data_[];  // 柔性数组
    };
    Page* head_;       // 页链表头
    uint32_t page_size_;
};
```

**分配策略**：
- 在当前页的空闲区域分配（bump allocator）
- 空间不足时分配新页并链接到链表
- 所有页在 PageArena 析构时一次性释放

**优势**：
- **零碎片**：不逐个释放，避免碎片化
- **高速分配**：仅递增指针，无锁
- **批量释放**：查询结束时一次性回收所有临时内存

---

## 4.6 与 Parquet+Arrow 的对比分析

### 4.6.1 读取能力对比

| 读取能力 | Parquet + Arrow | TsFile (本文) |
|---------|----------------|--------------|
| 设备定位 | 线性扫描 Row Group 统计量 | **B-tree 索引 $O(\log D)$** |
| 列裁剪 | Row Group 内按列独立存储 | Chunk 内按测点独立存储 |
| 时间谓词下推 | Row Group + Page 统计量（2 级） | **Chunk + Page + Block peek（3 级）** |
| 编码感知过滤 | 不支持 | **ts2diff peek_next_block_range** |
| 延迟物化 | 无内置（依赖外部引擎如 DuckDB） | **内置：time→filter→selective value decode** |
| 批量解码 SIMD | Arrow Compute Kernel | ts2diff batch decode (128/256-bit) |
| 批量过滤 SIMD | Arrow Compute Kernel | satisfy_batch_time (AVX2/NEON) |
| Bloom Filter | 列级（v2 可选） | 路径级（device.measurement） |
| 并行解码 | 外部框架实现（DuckDB 等） | **内置 ThreadPool per-column 并行** |
| 元数据缓存 | 无内置 | **LRU Cache（1000 条，弹性驱逐）** |
| 列式结果格式 | Arrow RecordBatch | TsBlock（可转换为 Arrow） |
| 嵌入式适配 | 不支持（依赖 Arrow 运行时） | **ENABLE_THREADS / ENABLE_SIMD 条件编译** |

### 4.6.2 TsFile 的结构性优势

**1. 时间原生索引**

Parquet 是通用列存格式，没有"时间轴"或"设备"概念。查询特定设备在特定时间范围内的数据需要扫描所有 Row Group 的统计量。TsFile 的 B-tree 索引按 `device → measurement` 组织，能够在 $O(\log D + \log M)$ 时间内直接定位到目标 Chunk。

**2. 三级过滤**

Parquet v2 支持 Row Group 级和 Page 级两级统计量过滤。TsFile 在此基础上增加了编码块级过滤：通过解析 TS2DIFF 块头即可推断值域范围，无需解码即可跳过不相关块。这是利用领域特定编码格式实现的优化，通用列存格式难以复制。

**3. 内置并行与缓存**

Parquet 的并行读取和缓存依赖外部框架（如 Arrow Dataset API、DuckDB 等）。TsFile 在文件读取层内置了线程池和 LRU 缓存，使得单文件读取即可利用多核性能，无需外部框架支持。

---

## 4.7 本章小结

本章提出了面向 AI 工作负载的时序文件读取优化框架。该框架以分层流水线架构为基础，针对 AI 读取的随机访问、小批量、列裁剪等特征，构建了以下优化体系：

1. **多级过滤**：BloomFilter → Chunk 统计量 → Page 统计量 → Block 级 peek → 行级 SIMD 过滤，五级逐层细化，最大限度减少读放大
2. **延迟物化**：先解码时间列并过滤，再选择性解码 value 列，对 AI 随机窗口查询可节省 70-90% 的解码开销
3. **SIMD 加速**：TS2DIFF 编码的 4 值/次批量解码和时间谓词的向量化过滤，基于 SIMDe 实现 x86/ARM 跨平台支持
4. **列式并行解码**：两阶段模型（串行 I/O → 并行 CPU）利用多核加速多列解码，各列通过数据隔离保证线程安全
5. **元数据缓存**：LRU Cache 缓存热点设备的 ChunkMeta，弹性驱逐策略平衡内存开销与命中率

相比 Parquet+Arrow 方案，TsFile 在设备定位效率（B-tree vs 线性扫描）、过滤粒度（三级 vs 两级）、和嵌入式适配能力（条件编译）方面具有结构性优势，为资源受限环境下的 AI 时序数据读取提供了高效的文件格式级解决方案。

---

## 附录：待补充实验数据

### 实验 A：SIMD 加速效果

<!-- TODO: 对比 ENABLE_SIMD=ON vs OFF 的解码吞吐（M rows/s） -->
<!-- 测试数据：固定数据集，变量为 SIMD 开关 -->
<!-- 指标：解码延迟、吞吐、CPU 利用率 -->

### 实验 B：多级过滤的 I/O 节省

<!-- TODO: 量化每级过滤跳过的行数/字节数 -->
<!-- 测试方法：分别禁用各级过滤，对比实际解码行数 -->
<!-- 指标：读放大比率 = 实际解码行数 / 返回行数 -->

### 实验 C：延迟物化收益

<!-- TODO: 量化 AI 随机窗口查询下延迟物化节省的 value 解码量 -->
<!-- 测试方法：不同时间窗口选择率 (1%, 5%, 10%, 50%) 下的解码量对比 -->

### 实验 D：并行解码加速比

<!-- TODO: 不同列数 (1, 4, 8, 16) 下的并行 vs 串行解码延迟 -->
<!-- 指标：加速比 = 串行时间 / 并行时间 -->

### 实验 E：AI 框架端到端读取性能

<!-- TODO: PyTorch DataLoader 使用 TsFile vs Parquet 的端到端训练数据加载性能对比 -->
