# 资源约束下的 TsFile 读写优化——论文结构草案

> 状态：讨论用草案，各章节标注置信度和待确认项
> 核心叙事：以资源约束维度为轴，依次呈现内存受限、CPU 并行、向量化加速、AI 接口四个维度的优化方案

---

## 第 1 章 绪论（略）

---

## 第 2 章 背景与 TsFile 概述（略）

---

## 第 3 章 内存受限下的 TsFile 读写优化

> **约束**：可用内存严格有限（数十 MB），写入数据量可达数亿行
> **置信度**：高——现有 sec3.md 内容扎实，主要补充读侧

### 3.1 TsFile 写入过程内存模型

- 三分模型：$M_{\text{total}} = M_{\text{init}} + M_{\text{data}} + M_{\text{meta}}$
- 各模块内存量化公式（每行字节数 $s$，每次 flush 元数据增量 $b$）
- 关键洞察：`TSFILE_WRITER_META` 单调递增，flush 不释放，close 才释放
- `ModStat` 编译期内存追踪框架（`ENABLE_MEM_STAT`）

### 3.2 写入内存实验

- 顺序写入 vs 混合写入的内存曲线对比
- 峰值分析、flush 后残留差异、设备切换锯齿效应
- 公式验证：误差 5-15%，来源分析

### 3.3 内存约束下的自适应参数配置

- EOQ 最优策略（总量已知）：$F_{\text{opt}} = \sqrt{R \times D \times b / s}$
- 对半分预算策略（流式，总量未知）
- 可行性检查：构造时报错，说明最小可用配置
- 安全裕度设计（SAFETY_FACTOR = 0.85）

### 3.4 两级刷盘与文件轮转控制

- 第一级：flush 释放 $M_{\text{data}}$
- 第二级：rotate 释放 $M_{\text{meta}}$
- 与底层 `chunk_group_size_threshold_` 的协同（双重保险）
- 动态设备数追踪（`observed_max_devices`）

### 3.5 MemConstrainedWriter 接口设计

- `WritingStrategy` / `WriteStats` 数据结构
- 核心接口：`write_table()`、`compute_strategy()`（静态）
- 使用示例与约束说明

### 3.6 读取路径内存管理

> **状态**：已确认可用 ModStat 追踪（alloc 层不区分读写，读取路径同样接入）；约束接口只控制 batch_size，不控制并发列数。

#### 3.6.1 读取内存模型

读取全程内存由以下组件构成：

| 组件 | 公式 | 释放时机 |
|------|------|---------|
| `TsBlock` 输出缓冲 | $\text{batch\_size} \times s_{\text{row}}$ | 每批次返回后 |
| 解压缓冲（per column） | $N_{\text{cols}} \times C_{\text{page}}$ | 每 Page 解码后 |
| Decoder / Compressor 对象 | $N_{\text{cols}} \times c_{\text{dec}}$（小量） | 查询结束 |
| `PageArena` 临时分配 | 随查询涉及的 Chunk/Page 数增长 | 查询结束一次性回收 |
| LRU 元数据缓存 | $\leq 1000 \times \bar{b}_{\text{meta}}$（固定上限） | 进程生命周期 |

其中：
- $s_{\text{row}} = 8 + \sum_{\text{cols}} \text{sizeof}(\text{col\_type})$（时间戳 8 字节 + 各列）
- $C_{\text{page}}$：每列解压缓冲，由 Page 大小决定（约 64 KB，可配置）

整体估算：

$$M_{\text{read}} \approx M_{\text{fixed}} + \text{batch\_size} \times s_{\text{row}} + N_{\text{cols}} \times C_{\text{page}}$$

其中 $M_{\text{fixed}}$ 包含 LRU 缓存、Decoder 对象等固定开销。

关键结论：**读取内存主要由列数和 batch_size 驱动**，远比写侧结构简单（无元数据单调增长问题）。

#### 3.6.2 ModStat 实验

实验设计：固定设备数和时间范围，变化以下两个维度：
- 列数 $N_{\text{cols}}$：1 / 4 / 8 / 16
- batch_size：1K / 4K / 16K / 64K 行

记录各 `AllocModID` 的峰值内存，验证上述量化公式。

#### 3.6.3 资源约束读取接口

给定内存上限 $M_{\text{limit}}$ 和查询 schema，自动推导 batch_size：

$$\text{batch\_size} = \left\lfloor \frac{M_{\text{limit}} - M_{\text{fixed}} - N_{\text{cols}} \times C_{\text{page}}}{s_{\text{row}}} \right\rfloor$$

接口形式：

```cpp
// 纯计算：给定内存上限和 schema，返回建议的 batch_size
int64_t compute_read_batch_size(
    int64_t mem_limit_bytes,
    const std::vector<ColumnSchema>& columns
);

// 或作为 query() 的重载，内部自动使用推导的 batch_size
reader.query(table_name, columns, start_time, end_time,
             result_set, filter, mem_limit_bytes);
```

用户不再需要手动调参，只需声明内存上限，系统保证每批次内存不超限。

> **注**：并发解码（`ENABLE_THREADS`）不引入额外内存消耗——各列解压缓冲已独立分配，并发只影响 CPU 调度，不影响内存总量。因此约束接口无需感知线程数。

### 3.7 本章小结

---

## 第 4 章 CPU 并行化：多线程读写框架

> **约束**：内存充足，目标是充分利用多核 CPU
> **置信度**：中高——读写两侧并行模型已确认，实验数据待补充

### 4.1 统一并行模型

读写两侧共享同一个并行设计哲学：

**主线程负责串行的协调性工作**（无法或不值得并行化的部分）：
- 写侧：Tablet 拆分、设备路由、内存检查、flush 落盘
- 读侧：元数据查找、文件 I/O（顺序读取，保证有序性）、TsBlock 组装

**线程池负责 CPU 密集的 per-measurement 任务**（完全独立，天然可并行）：
- 写侧：各测点（measurement）的编码任务
- 读侧：各列（column）的解压 + 解码任务

**并行粒度统一在测点/列层面**，而非设备层面。这一设计的关键推论：
- 线程数应配置为 field 列数（或其合理上限），而非固定值
- 线程安全由**数据隔离**保证（每个测点/列拥有独立的 ChunkWriter / ValueColumnState），无需锁同步
- 条件编译 `ENABLE_THREADS` 关闭后完全移除线程依赖，回退到串行路径（嵌入式适配）

### 4.2 ThreadPool 设计

- 可配置大小的工作线程模型（建议 = field 列数）
- 双条件变量：`cv_work_`（唤醒空闲线程）/ `cv_done_`（`wait_all()` 同步）
- 活跃计数 `active_`：原子递增/递减，`wait_all()` 等待 `active_ == 0`
- 写侧使用 `submit()` + `future.get()` 获取结果；读侧使用 `submit()` + `wait_all()`

### 4.3 写入路径并行化

**执行模型**：

```
主线程
  │
  ├─ split_tablet_by_device()        串行：Tablet 按 TAG 列分组
  │
  ├─ 对每个设备：
  │    ├─ time_write_column_batch()  串行：时间列写入（共享）
  │    └─ 对每个 field 列：
  │         └─ pool.submit(          并行：各测点独立编码
  │              value_write_column_batch(col_i))
  │    └─ pool.wait_all()
  │
  └─ check_memory_and_may_flush()
       └─ flush()                    串行：单线程落盘
```

- 线程安全保证：每个测点拥有独立的 `ValueChunkWriter` 实例，编码缓冲、统计对象均不共享
- flush 保持串行：`end_encode_chunk()` → 磁盘 I/O → `reset()` 三步不可打断，flush 期间无新编码任务提交

**flush 串行瓶颈分析与双缓冲可行性**：

`FLUSH_CHUNK` 宏将 CPU 工作（`end_encode_chunk()`）与磁盘 I/O（`flush_chunk()`）串行绑定，`reset()` 必须等 I/O 完成后才能释放 ChunkWriter 供下一批写入。这意味着 flush 期间线程池完全空闲，是潜在的吞吐瓶颈。

理论上可通过**双缓冲**消除此瓶颈：每个 ChunkWriter 持有 active/sealed 两套 ByteStream，flush 触发时 swap 两套缓冲，主线程立即可将新数据写入 active，后台 I/O 线程读 sealed 写盘。其可行性条件已满足（各 ChunkWriter 实例独立，ByteStream 支持 swap），工程实现不在本文范围内，作为后续优化方向。

### 4.4 读取路径并行化

**两阶段模型**：

```
主线程（Phase 1：串行 I/O）
  │
  ├─ 对每个 column：
  │    └─ ensure_value_page_loaded(col)   从文件读取压缩数据到内存
  │
  └─ 提交并行任务（Phase 2：并行 CPU）
       ├─ pool.submit(decompress_and_parse(col_0))
       ├─ pool.submit(decompress_and_parse(col_1))
       │    ├─ compressor->uncompress()
       │    ├─ parse null bitmap
       │    └─ decoder->reset()
       └─ pool.wait_all()
```

- Phase 1 串行保证文件访问有序，避免并发文件句柄竞争
- Phase 2 各列任务完全独立（`ValueColumnState` 独立实例），无锁
- 代码路径：`AlignedChunkReader::decode_cur_value_pages_multi()`

### 4.5 两级并发配置

并发控制采用**编译期 + 运行时**两级门控，在 `global.cc` 初始化，通过 `global.h` 的 setter/getter 暴露：

```
编译期（ENABLE_THREADS）
  OFF → 不引入任何线程库，完全串行，适合裸机嵌入式
  ON  → 线程基础设施可用，由运行时配置决定是否启用
          │
          ▼
运行时（g_config_value_）
  parallel_read_enabled_   = true   读取并行开关
  parallel_write_enabled_  = true   写入并行开关
  read_thread_count_       = 4      读取线程数上限（1-64）
  write_thread_count_      = 6      写入线程数上限（1-64）
```

**有效线程数** = $\min(\texttt{thread\_count}, N_{\text{field}})$：配置的上限与实际任务数取小，避免空转线程的调度开销。

**配置建议**：
- 高性能服务器：`thread_count` = CPU 核心数，`enabled` = true
- 列数固定的场景：`thread_count` = field 列数，精确匹配任务数
- 嵌入式环境：编译时关闭 `ENABLE_THREADS`，运行时配置无意义

### 4.6 实验评估

固定 field 列数，对比三种工作模式：

| 模式 | 配置 | 说明 |
|------|------|------|
| **串行基线** | `ENABLE_THREADS=OFF` 或 `enabled=false` | 无线程调度开销 |
| **线程不足** | `thread_count` < field 列数 | 线程复用，部分并行 |
| **线程充足** | `thread_count` $\geq$ field 列数 | 每列一个线程，最大并行 |

分别在**写入**和**读取**两个路径上采集上述三种模式的吞吐量，field 列数取 4 / 8 / 16 作为代表场景。

### 4.7 本章小结

---

## 第 5 章 SIMD 向量化加速

> **约束**：目标平台支持 SIMD 指令（x86 AVX2 / ARM NEON）
> **状态**：范围已确认——时间过滤（不含值过滤）；写侧差分计算和位打包待实现；批量化接口已就绪

### 5.1 批量化接口：SIMD 的前提

SIMD 操作的基本要求是**连续内存数组**。TsFile 写入路径已构建完整的批量化接口层：

```
TsFileWriter::value_write_column_batch()   // Tablet 列指针 → 连续数组
  └── ValueChunkWriter::write_batch()      // 管理 Page 边界分拆
        └── ValuePageWriter::write_batch() // 全量通过 or null 散列
              └── encoder->encode_batch()  // 编码器批量入口 ← SIMD 发力点
```

读取路径同理，`read_batch_int32()` / `read_batch_int64()` 以连续数组为输出目标。

**批量化的意义**：逐值接口（`write(t, v)`）在编码器内部无法形成连续数组，SIMD 无从下手；批量接口将整列数据以裸指针传入，使编码/解码核心可直接操作向量寄存器宽度对齐的内存块。

### 5.2 SIMDe 跨平台方案

- SIMDe（SIMD Everywhere）库：透明模拟层，同一份代码在 x86/ARM/无 SIMD 平台上均可编译
- 三级回退：AVX2 原生 → NEON 模拟 → 标量回退
- 条件编译：`ENABLE_SIMD` 关闭后完全不引入 SIMD 头文件依赖
- ARM 时间过滤路径可直接使用原生 NEON（`__ARM_NEON` 宏检测），绕过 SIMDe 模拟层

### 5.3 写入路径的 SIMD 加速

TS2DIFF 编码的三个阶段均可向量化，批量化接口使这三步直接操作连续数组：

**Step 1：差分计算**（待实现）

$$\delta_i = v_i - v_{i-1}, \quad i = 1, \ldots, n-1$$

相邻差是典型的向量减法：加载 $[v_1, v_2, \ldots, v_n]$ 和 $[v_0, v_1, \ldots, v_{n-1}]$，一次 `_mm256_sub_epi64` 得到 4 个差分值。

**Step 2：Rebase**（已实现）

$$\delta'_i = \delta_i - \delta_{\min}$$

- INT32：SSE4.2 `_mm_sub_epi32`（4 值/次）
- INT64：AVX2 `_mm256_sub_epi64`（4 值/次）

**Step 3：位打包**（待实现）

将 $\delta'_i$ 以 $w$ 位宽紧密打包写入字节流。SIMD 实现通过 scatter + blend 指令将多个值的位域压缩到连续字节，与解码路径的 gather 操作对称。

### 5.4 读取路径的 SIMD 加速

#### 5.4.1 TS2DIFF 批量解码

TS2DIFF 解码是编码的逆过程，批量解码以 4/8 值为步长：

- INT32（128-bit）：Gather → 字节序转换 → 位对齐提取 → 加偏移 → 前缀和还原 → 加基值
- INT64（256-bit）：同 lane 前缀和 + 跨 lane 标量补偿（AVX2 lane 隔离限制）
- 尾部不足一个向量宽度的值回退到标量路径

#### 5.4.2 编码感知的 Block 级谓词下推

直接从 TS2DIFF 块头推断值域，无需解码：

- `peek_next_block_range()`：读取 16-24 字节块头，计算 $[v_{\min}, v_{\max}]$
- `skip_peeked_block()`：直接跳过位打包数据（`advance_read_pos`）
- 与 Parquet 对比：TsFile 独有的第三级过滤（Chunk → Page → Block），Parquet 仅支持两级

#### 5.4.3 时间谓词的向量化过滤

`satisfy_batch_time()` 对连续时间戳数组应用谓词，输出布尔掩码：

- x86 AVX2：`_mm256_cmpgt_epi64` + `movemask_pd`，4 值/次
- ARM NEON：`vcgtq_s64`，2 值/次（原生，不经 SIMDe）
- 覆盖全部时间谓词：`TimeGt`、`TimeGtEq`、`TimeLt`、`TimeLtEq`、`TimeEq`、`TimeNotEq`、`TimeBetween`
- **不做值过滤**：只关注时间维度的过滤，与 TsFile 的时序语义一致

### 5.5 全链路视图

```
写入路径                              读取路径
──────────────────────────────────────────────────────
encode_batch(values[], n)            read_batch(out[], n)
     │                                      │
     ▼                                      ▼
差分计算 δ[]    [SIMD 待实现]        位解包 + 字节序  [SIMD 已实现]
     │                                      │
     ▼                                      ▼
Rebase δ'[]     [SIMD 已实现]        前缀和还原       [SIMD 已实现]
     │                                      │
     ▼                                      ▼
位打包写入      [SIMD 待实现]        加基值输出       [SIMD 已实现]
     │
     ▼                              satisfy_batch_time(times[], mask[])
Page ByteStream                          [SIMD 已实现，仅时间过滤]
```

### 5.6 实验评估

对比 `ENABLE_SIMD=ON` vs `OFF`，分别测写入编码吞吐和读取解码 + 过滤吞吐：

| 实验 | 路径 | 变量 | 指标 |
|------|------|------|------|
| 编码吞吐 | 写入 | SIMD ON/OFF × 数据类型 | M rows/s |
| 解码吞吐 | 读取 | SIMD ON/OFF × 数据类型 | M rows/s |
| 时间过滤吞吐 | 读取 | SIMD ON/OFF × 选择率 | M rows/s |
| 跨平台对比 | 读写 | x86 AVX2 vs ARM NEON vs 标量 | 加速比 |

### 5.7 本章小结

---

## 第 6 章 面向 AI 负载的优化成果应用

> **定位**：本章是前三章优化成果的应用出口。第 3-5 章分别在内存、CPU、指令集三个维度完成了底层优化；本章展示这些优化如何通过 Arrow C Data Interface 以最小开销透传到 AI/ML 生态，使底层的列式高效读取、多线程解码、SIMD 加速最终呈现在 Python DataFrame 接口上。
> **状态**：核心实现已确认——Arrow C Data Interface 双向桥接 + Python DataFrame API，已通过测试

### 6.1 AI 工作负载的时序数据访问特征

- 与传统监控读取的差异：随机时间窗口 vs 连续扫描，小批量/多 epoch vs 大批量单次
- 列裁剪收益：特征选择阶段只读部分测点
- 读放大问题：随机窗口访问触发整 Page 解码，多级过滤体系的缓解效果

### 6.2 Arrow C Data Interface 桥接架构

AI/ML 生态（pandas、PyTorch、polars 等）普遍以 Apache Arrow 为数据交换格式。TsFile 通过实现 **Arrow C Data Interface**（而非依赖 pyarrow C++ 库）实现了轻量的跨语言互操作。

```
C++ TsFile                         Python / pyarrow
─────────────────────────────────────────────────────
TsBlock (列式内存)                  pa.RecordBatch
     │                                   ▲
     │  TsBlockToArrowStruct()           │  pa.RecordBatch._import_from_c(
     ▼                                   │      array_ptr, schema_ptr)
ArrowArray + ArrowSchema  ──────────────┘
(Arrow C Data Interface)
     │
     ▼
pa.RecordBatch → pd.DataFrame
```

**设计选择**：Arrow C Data Interface 是纯 C ABI 的标准（`ArrowArray` / `ArrowSchema` 结构体），不依赖 Arrow C++ 运行时，与 TsFile 的极简依赖原则一致。

### 6.3 TsBlock → Arrow 转换（读取路径）

`TsBlockToArrowStruct()` 将一个 TsBlock 转换为 Arrow struct array：

**内存模型**：

| 情况 | 操作 | 内存开销 |
|------|------|---------|
| 非 null 数值列 | `memcpy` Vector ByteBuffer → Arrow buffer | 1× 数据大小 |
| 含 null 数值列 | scatter 非 null 值 + 分配 null bitmap（位反转） | 1× 数据大小 + bitmap |
| 字符串列 | 重组为 Arrow offset+data 格式 | 1× 数据大小 + offset 数组 |
| DATE 列 | YYYYMMDD → days-since-epoch 逐值转换 | 1× int32 数组 |

**"单拷贝"语义**：转换过程中每列数据最多经历一次 `memcpy`，无中间格式。pyarrow 的 `_import_from_c()` 接受指针所有权，本身是零拷贝。整体链路：**1 次 memcpy（C++侧）+ 0 次拷贝（Python 侧）**。

**null bitmap 位语义差异**：TsFile 内部 bitmap 以 `1=null`，Arrow 以 `1=valid`，转换时逐字节取反（`~byte`）。

**类型映射**：

| TsFile 类型 | Arrow format string | 说明 |
|------------|--------------------|----|
| INT32 | `"i"` | 直接映射 |
| INT64 | `"l"` | 直接映射 |
| TIMESTAMP | `"tsn:"` | 纳秒时间戳 |
| FLOAT | `"f"` | 直接映射 |
| DOUBLE | `"g"` | 直接映射 |
| BOOLEAN | `"b"` | 字节→位打包 |
| STRING/TEXT | `"u"` | offset+data 格式 |
| DATE | `"tdD"` | YYYYMMDD→天数转换 |

### 6.4 Arrow → Tablet 转换（写入路径）

`ArrowStructToTablet()` 将 Arrow struct array 转换回 Tablet 供写入：

- 数值列（INT32/INT64/FLOAT/DOUBLE）：`set_column_values()` 直接使用 Arrow buffer 指针，有效避免额外拷贝
- 字符串列：逐行从 Arrow offset+data 格式提取
- DATE 列：days-since-epoch → YYYYMMDD 逐值转换
- 支持可选的 `TableSchema` 参数：当已注册 schema 时，以 schema 类型为准（优先于 Arrow format 推断）

### 6.5 Python DataFrame API

基于上述 Arrow 桥接层，Python 侧提供两个核心接口：

**读取：`to_dataframe()`**

```python
to_dataframe(
    file_path,
    table_name=None,       # 默认使用文件中第一张表
    column_names=None,     # 列裁剪：None = 全列
    start_time=None,       # 时间范围过滤
    end_time=None,
    max_row_num=None,      # 行数限制（LIMIT 语义）
    as_iterator=False      # True → Iterator[DataFrame]，流式读取
)
```

- `as_iterator=True`：分批 yield DataFrame，内存峰值受 `batch_size` 控制，适合大文件流式处理
- 列裁剪直达 C++ 读取层，未指定的列不被解码

**写入：`write_dataframe()` / `dataframe_to_tsfile()`**

```python
# 已有 schema 的写入
with TsFileTableWriter(path, schema) as writer:
    writer.write_dataframe(df)

# schema 自动推断写入
dataframe_to_tsfile(df, path, table_name, time_column, tag_column)
```

- `write_dataframe` 支持大小写不敏感的列名匹配
- `dataframe_to_tsfile` 从 pandas dtype 自动推断 TsFile 类型

### 6.6 实验评估

| 实验 | 变量 | 指标 |
|------|------|------|
| 端到端读取吞吐 | 列数 × batch_size | M rows/s，与 Parquet+pyarrow 对比 |
| Arrow 转换开销占比 | 行数 × 列数 | 转换耗时 / 总查询耗时 |
| 迭代器模式内存峰值 | 文件大小 × batch_size | 峰值内存 MB |
| 写入吞吐 | 行数 × 列数 | M rows/s |

### 6.7 本章小结

---

## 附录：各章节依赖关系与实验工作量评估

| 章节 | 依赖现有工作 | 待新增实现 | 待采集实验 |
|------|------------|-----------|-----------|
| 第 3 章 内存 | sec3.md 全部 + PageArena/LRU 代码 | 读侧内存分布分析 | 无（写侧已有） |
| 第 4 章 多线程 | AlignedChunkReader 并行解码、ThreadPool | 写侧并行粒度确认 | 写/读并行加速比 |
| 第 5 章 SIMD | TS2DIFF 解码、时间过滤、rebase | 值过滤/差分/位打包 SIMD（可选） | 全链路 ON vs OFF 对比 |
| 第 6 章 AI 接口 | TsBlock 设计、多级过滤、LRU | 方向待定 | 端到端 benchmark |

---

> **下一步**：逐章讨论，从第 3 章开始确认内容范围和实验设计。
