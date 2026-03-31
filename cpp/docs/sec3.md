# 第 3 章 面向嵌入式的资源受限写入优化

## 3.1 问题分析与设计目标

嵌入式设备、容器限额节点和边缘计算终端等资源受限环境对 TsFile 写入提出了严格的内存约束。在这些场景中，可用内存通常仅为数十 MB，而写入的时间序列数据量可能达到数亿行。用户面临以下困境：

- 不知道每次应写入多少行（批次大小 $F$）；
- 不知道写入多少次后需要调用 `flush()` 将数据落盘；
- 不知道何时内存将超出上限，需要关闭当前文件并打开新文件。

现有的 `TsFileWriter` / `TsFileTableWriter` 将上述三个决策完全交给用户，使用者需要深入理解 TsFile 内部的内存模型才能正确配置参数。

**设计目标**：

1. **约束驱动**：用户只需声明内存上限和 schema，系统自动推导所有写入参数。
2. **零决策负担**：用户只调用 `write_table()`，flush / close / 文件轮转全部自动处理。
3. **构造时可行性校验**：约束不可满足时立即报错，并说明最小可用配置。
4. **运行时可观测**：提供当前内存用量估算、已写行数、已创建文件数等运行时状态。
5. **零运行时监控开销**：不依赖原子操作级别的内存追踪，仅使用轻量整数计算。

---

## 3.2 TsFile 写入过程内存模型分析

### 3.2.1 内存组成

通过对写入过程的逐模块内存追踪实验（基于 `ModStat` 编译期统计框架），确认 TsFile 写入过程中的内存由以下模块组成：

| 模块标识 | 说明 | 特征 |
|---------|------|------|
| `CW_PAGES_DATA` | ChunkWriter 已编码的数据页 | **主导项**，占峰值 80-85%，flush 后清零 |
| `DEFAULT` | 通用分配（ChunkWriter 对象、ByteStream 等） | 随活跃设备数分配，设备切换时部分释放 |
| `TABLET` | 用户提交的 Tablet 数据缓冲 | 每个 batch 分配/释放，生命周期短 |
| `PAGE_WRITER_OUTPUT_STREAM` | PageWriter 编码输出流 | 随活跃设备数分配，周期性释放 |
| `STATISTIC_OBJ` | 统计对象（min/max/count 等） | 少量，随 flush 次数线性增长 |
| `ENCODER_OBJ` / `COMPRESSOR_OBJ` | 编码器、压缩器对象 | 少量，随 flush 次数线性增长 |
| `TSFILE_WRITER_META` | ChunkMeta、ChunkGroupMeta 等元数据 | **不可压缩，单调递增，仅 close 时释放** |
| `TS2DIFF_OBJ` | TS_2DIFF 编码差分缓冲 | 少量 |

这些模块可归纳为三类：

$$M_{\text{total}} = M_{\text{init}} + M_{\text{data}} + M_{\text{meta}}$$

| 组成 | 来源 | 释放时机 | 能否控制 |
|------|------|---------|---------|
| $M_{\text{init}}$ | 固定开销（schema 注册、Writer 对象等） | 程序结束 | 不可控 |
| $M_{\text{data}}$ | ChunkWriter 数据页（`CW_PAGES_DATA` + `DEFAULT` + `TABLET` 等） | **flush 后立即释放** | 控制批次大小 $F$ |
| $M_{\text{meta}}$ | ChunkGroup/Chunk 元数据（`TSFILE_WRITER_META`） | **仅 close 时释放** | 控制每文件 flush 次数 $K$ |

**关键洞察**：$M_{\text{meta}}$ 是核心约束来源。只要文件不 close，每次 flush 都会向内存中追加一批不可压缩的元数据。设备数越多、flush 次数越多，$M_{\text{meta}}$ 积压越严重。因此，**文件轮转的根本原因不是磁盘空间，而是为了释放 $M_{\text{meta}}$**。

### 3.2.2 内存量化模型

对于 Table 模式，各量的计算方式如下：

**每行数据的内存字节数**：

$$s_{\text{tablet}} = 8 + \sum_{\text{all cols}} \text{sizeof}(\text{col\_type})$$

$$s_{\text{data}} = \sum_{\text{FIELD cols}} (8 + \text{sizeof}(\text{field\_col\_type}))$$

$$s = s_{\text{tablet}} + s_{\text{data}}$$

其中 $s_{\text{tablet}}$ 是用户 Tablet 对象占用（包含时间戳 8 字节 + 所有列），$s_{\text{data}}$ 是 ChunkWriter 内部对 FIELD 列的编码缓冲（每列 8 字节 page header + 数据本身）。

**每设备每次 flush 的元数据增量**：

$$b = n_{\text{field\_cols}} \times 104 + 96 \quad \text{(bytes/设备/flush)}$$

其中 104 bytes 来自每个 ChunkMeta 的序列化大小，96 bytes 来自 ChunkGroupMeta。

**三类内存的量化公式**：

| 组成 | 公式 | 说明 |
|------|------|------|
| $M_{\text{init}}$ | $\approx 900$ KB | 固定，实测值 |
| $M_{\text{data}}$ | $s \times F$ | $F$ = 当前 flush 区间内已写行数 |
| $M_{\text{meta}}$ | $K \times D \times b$ | $K$ = 已 flush 次数，$D$ = 设备数 |

类型字节数映射：

| TSDataType | 字节数 |
|-----------|-------|
| BOOLEAN | 1 |
| INT32 / FLOAT | 4 |
| INT64 / DOUBLE | 8 |
| STRING | 32（保守估计，实际为变长） |

### 3.2.3 模型验证

#### 编译期内存追踪框架

TsFile C++ 实现了基于编译选项 `ENABLE_MEM_STAT` 的内存追踪框架。其核心组件 `ModStat` 类通过 cache-line 对齐的原子计数器（`ATOMIC_FAA`）为每个分配模块（`AllocModID`）维护实时内存占用统计。所有内存分配通过 `mem_alloc(size, mid)` / `mem_free(ptr)` 接口完成，在分配的 8 字节头中记录模块 ID 和大小，从而实现零侵入的逐模块追踪。

该框架仅在编译时开启，不影响生产构建的性能。

#### 实验设计

实验程序（`experiment/write_memory`）配置如下：

- **Schema**：1 张表（`mem_bench`），2 个 TAG 列（STRING）+ 6 个 FIELD 列（INT64, DOUBLE, FLOAT, INT32, INT64, INT32），SNAPPY 压缩
- **数据规模**：200M 行，10 个设备，每设备 20M 行
- **内存阈值**：50 MB（`memory_threshold` 参数）
- **采集频率**：每 50 万行记录一次各模块内存，输出 CSV
- **写入模式**：两种——顺序写入（按设备逐个写完）和混合写入（每个 Tablet 包含所有设备）

#### 顺序写入模式结果

顺序写入模式下，每个 Tablet 仅包含单个设备的数据，10 个设备依次写完。

![顺序写入内存曲线](../experiment/write_memory/write_memory_chart_sequential.png)

| Flush 事件 | 触发行数 | Flush 前 | Flush 后 | 释放量 |
|-----------|---------|---------|---------|--------|
| #1 | 32.1M | 41.5 MB | 8.1 MB | 33.4 MB |
| #2 | 70.0M | 42.3 MB | 8.1 MB | 34.2 MB |
| #3 | 104.1M | 48.1 MB | 8.2 MB | 39.8 MB |
| #4 | 133.5M | 38.7 MB | 8.0 MB | 30.7 MB |
| #5 | 171.0M | 38.8 MB | 7.4 MB | 31.4 MB |
| #6 (final) | 200.0M | 26.2 MB | 0.1 MB | 26.1 MB |

- **峰值内存**：48.1 MB
- **写入吞吐量**：3.36 M rows/s
- **CPU 利用率**：稳定约 115%

**顺序模式的特征**：在每 20M 行的设备切换边界处，`DEFAULT`（4.5 → 0.8 MB）和 `TABLET`（2.75 → 0.48 MB）出现短暂凹陷，形成锯齿状小波动。这是因为旧设备的 Tablet 对象和临时缓冲区被析构，新设备的尚未分配。`CW_PAGES_DATA` 则不受设备切换影响，持续线性增长直到触发 flush。

#### 混合写入模式结果

混合写入模式下，每个 Tablet 包含全部 10 个设备的数据（每设备 6553 行，按设备分组连续排列）。

![混合写入内存曲线](../experiment/write_memory/write_memory_chart_interleaved.png)

| Flush 事件 | 触发行数 | Flush 前 | Flush 后 | 释放量 |
|-----------|---------|---------|---------|--------|
| #1 | 29.0M | 41.8 MB | 12.9 MB | 29.0 MB |
| #2 | 58.1M | 41.8 MB | 7.4 MB | 34.5 MB |
| #3 | 89.1M | 41.7 MB | 12.8 MB | 28.9 MB |
| #4 | 127.5M | 42.0 MB | 7.4 MB | 34.6 MB |
| #5 | 166.1M | 47.6 MB | 12.2 MB | 35.4 MB |
| #6 | 198.6M | 48.6 MB | 12.0 MB | 36.6 MB |
| #7 (final) | 200.0M | 6.4 MB | 0.1 MB | 6.2 MB |

- **峰值内存**：48.6 MB
- **写入吞吐量**：3.30 M rows/s
- **CPU 利用率**：稳定约 115%

**两种模式的关键差异**：

| 对比项 | 顺序写入 | 混合写入 |
|-------|---------|---------|
| Flush 后残留内存 | ~8 MB | ~12 MB |
| 自动 flush 次数 | 5 次 | 6 次 |
| 设备切换锯齿 | 有（每 20M 行） | 无 |
| `PAGE_WRITER_OUTPUT_STREAM` 峰值 | 0.46 MB（1 个设备活跃） | 4.59 MB（10 个设备同时活跃） |
| 更接近场景 | 批量导入历史数据 | 实时采集（多设备数据交替到达） |

混合模式下 flush 后残留更高（~12 MB vs ~8 MB），因为所有 10 个设备的 ChunkWriter 同时活跃，其 `DEFAULT` 分配和 `PAGE_WRITER_OUTPUT_STREAM` 缓冲在 flush 后仍保留（ChunkWriter 对象本身不销毁，只清空数据页）。

#### 模型验证结论

配置：50 设备，50 FIELD 列 int32，总行数 100,000：

| max_rows | 实测内存 | 公式估算 | 误差 |
|----------|---------|---------|------|
| 10,000 | 11.7 MB | 11.05 MB | 5.5% |
| 8,000 | 10.8 MB | 10.3 MB | 4.6% |
| 6,000 | 10.6 MB | 9.63 MB | 9.1% |
| 5,000 | 11.2 MB | 9.75 MB | 12.9% |

公式误差 5-15%，主要来源：变长字符串开销、schema 结构节点、OS 内存分配粒度。该误差范围可通过安全裕度系数覆盖。

---

## 3.3 资源约束下的自适应参数配置方案

### 3.3.1 EOQ 最优策略（总量已知）

当写入总量 $R$（每设备总行数）和设备数 $D$ 均已知时，问题等价于经济订货量（EOQ）模型：$M_{\text{data}}$ 和 $M_{\text{meta}}$ 存在此消彼长的关系——增大批次大小 $F$ 可减少 flush 次数 $K$，降低 $M_{\text{meta}}$，但增大 $M_{\text{data}}$；反之亦然。

最优解使两者在峰值时刻相等：

$$F_{\text{opt}} = \sqrt{\frac{R \times D \times b}{s}}$$

$$K_{\text{opt}} = \frac{R}{F_{\text{opt}}}$$

$$M_{\min} = M_{\text{init}} + 2 \sqrt{R \times s \times D \times b}$$

当 $M_{\min} \leq M_{\text{limit}}$ 时，所有数据可写入单个文件，$F_{\text{opt}}$ 即为最优批次大小。

### 3.3.2 对半分预算策略（总量未知）

在流式写入场景中，总量未知，无法使用 EOQ 公式。此时采用预算对半分策略，使单个文件容纳的数据量 $F \times K$ 最大化：

$$M_{\text{avail}} = M_{\text{limit}} - M_{\text{init}}$$

$$F = \frac{M_{\text{avail}}}{2 \times s}, \quad K = \frac{M_{\text{avail}}}{2 \times D \times b}$$

该策略将可用内存均匀分配给 $M_{\text{data}}$ 和 $M_{\text{meta}}$，是数学上使 $F \times K$ 最大的分配方案。

### 3.3.3 可行性检查

在构造 Writer 时执行可行性校验：

$$M_{\min} = M_{\text{init}} + s + b$$

若 $M_{\min} > M_{\text{limit}}$，则当前 schema 在给定内存约束下不可行，应立即报错并说明最小可用配置。

### 3.3.4 策略推导算法

```
输入：M_limit, schema, n_devices, total_rows_per_device

1. 从 schema 计算 s, b
2. 可行性检查：M_init + s + b ≤ M_limit，否则报错
3. M_avail = M_limit - M_init
4. 若 total_rows > 0 且 n_devices > 0（总量已知）：
     B = n_devices × b
     F = √(total_rows × B / s)
     K = total_rows / F
     peak = M_init + s×F + K×B
     若 peak ≤ M_limit：返回 {F, K, peak}   // EOQ 最优
     否则退化为对半分
5. 对半分策略：
     D_eff = max(1, n_devices)
     F = M_avail / (2 × s)
     K = M_avail / (2 × D_eff × b)
     返回 {F, max(1, K), M_init + s×F + K×D_eff×b}
```

---

## 3.4 资源感知的主动刷盘与文件轮转控制

### 3.4.1 两级内存控制机制

$M_{\text{data}}$ 和 $M_{\text{meta}}$ 的释放时机不同，因此需要两级控制：

- **第一级：flush 控制**——当 $M_{\text{data}}$ 达到预算时自动 flush，将编码后的数据页写入磁盘，释放 `CW_PAGES_DATA` 等缓冲。
- **第二级：文件轮转控制**——当 $M_{\text{meta}}$ 达到预算时关闭当前文件、打开新文件，释放不可压缩的元数据。

```
write_table(tablet):
  writer->write_table(tablet)                 // M_data 增加
  accumulated_rows += tablet.rows

  if accumulated_rows >= F:                   // 达到数据预算
      flush()                                 // M_data 清零，M_meta 增加 D×b
      flush_count++
      accumulated_rows = 0
      if flush_count × D × b >= rotate_thresh:
          rotate_file()                       // M_meta 清零
```

两者结合后，$M_{\text{total}}$ 始终受控：

| 阶段 | 内存状态 |
|------|---------|
| 写入中（未达 $F$）| $M_{\text{init}} + s \times \text{accumulated} + \text{flush\_count} \times D \times b$ |
| flush 后 | $M_{\text{init}} + \text{flush\_count} \times D \times b$ |
| rotate 后 | $M_{\text{init}}$ |

### 3.4.2 安全裕度设计

由于量化模型存在 5-15% 的估算误差，文件轮转的触发条件引入安全裕度系数：

$$\text{rotate\_thresh} = \text{meta\_budget} \times \text{SAFETY\_FACTOR}$$

其中 $\text{SAFETY\_FACTOR} = 0.85$，即提前 15% 触发轮转。

```
data_budget  = s × F
meta_budget  = M_avail - data_budget
rotate_thresh = meta_budget × 0.85
```

### 3.4.3 关闭时内存峰值分析

`close()` 调用时，$M_{\text{data}}$ 已为 0（最后一次 flush 已完成）。峰值为：

$$M_{\text{at\_close}} = M_{\text{init}} + M_{\text{meta\_final}}$$

由于在 $\text{flush\_count} \times D \times b \geq \text{rotate\_thresh}$ 时触发 rotate：

$$M_{\text{meta\_final}} \leq \text{meta\_budget} \times \text{SAFETY\_FACTOR} \leq \text{meta\_budget} \leq M_{\text{avail}}$$

$$\Rightarrow M_{\text{at\_close}} \leq M_{\text{init}} + M_{\text{avail}} = M_{\text{limit}} \quad \checkmark$$

关闭时不会超出内存上限。

### 3.4.4 已有自动刷盘机制的协同

`TsFileWriter` 内部已有一个自适应的 $M_{\text{data}}$ 检查机制，通过 `chunk_group_size_threshold_` 控制：

```cpp
int TsFileWriter::check_memory_size_and_may_flush_chunks() {
    if (record_count_since_last_flush_ >= record_count_for_next_mem_check_) {
        int64_t mem_size = calculate_mem_size_for_all_group();
        record_count_for_next_mem_check_ =
            record_count_since_last_flush_ *
            chunk_group_size_threshold_ / mem_size;
        if (mem_size > chunk_group_size_threshold_) {
            flush();
        }
    }
}
```

该机制通过动态调整检查间隔实现自适应——当内存增长速度快时检查更频繁，慢时检查更稀疏。`TsFileTableWriter` 在构造时将 `memory_threshold` 参数直接赋值给 `chunk_group_size_threshold_`：

```cpp
TsFileTableWriter(WriteFile* writer_file, T* table_schema,
                  uint64_t memory_threshold = 128 * 1024 * 1024) {
    // ...
    common::g_config_value_.chunk_group_size_threshold_ = memory_threshold;
}
```

在 `MemConstrainedWriter` 的设计中，flush 由上层根据累积行数 $F$ 显式触发。底层的 `chunk_group_size_threshold_` 设为 `data_budget` 值，作为**双重保险**——若单个超大 Tablet 导致 $M_{\text{data}}$ 短暂超出预算，底层机制仍可兜底触发 flush。

### 3.4.5 从公式估算到直接监控的演进

TsFile 已有两个可直接测量内存的接口，可替代公式估算：

**$M_{\text{data}}$ 的直接测量**：`TsFileWriter::calculate_mem_size_for_all_group()` 遍历所有 ChunkWriter，返回编码后的实际数据大小：

```cpp
int64_t estimate_max_series_mem_size() {
    return chunk_data_.total_size()              // 已完成页（编码后字节）
         + page_writer_.estimate_max_mem_size()  // 当前页已编码 + encoder pending 上界
         + page_header_overhead + statistic_sizeof;
}
```

测量的是编码后的实际大小，高效编码时返回值远小于 $s \times F$，天然适应实际编码效率。

**$M_{\text{meta}}$ 的直接测量**：所有元数据均从 `TsFileIOWriter::meta_allocator_`（类型 `PageArena`）分配，只需给 `PageArena` 新增 `get_total_used_bytes()` 方法即可获得精确的 $M_{\text{meta}}$ 大小。

有了直接监控后，运行时控制简化为纯粹的阈值比较：

```
write_table(tablet):
  writer->write_table(tablet)
  if writer->get_data_size() >= data_budget:
      writer->flush()
      if writer->get_meta_size() >= meta_budget:
          rotate_file()
```

原来需要计算的 $F$、$K$、$s$、$b$、$D$ 等参数大部分消失，"最优策略"退化为唯一一个决策：**如何在 $M_{\text{avail}}$ 中分配 data\_budget 和 meta\_budget**。

### 3.4.6 设备数未知时的自适应追踪

当用户未指定设备数（流式写入场景常见）时，系统进入动态追踪模式：

```
// 每次 write_table 后
d_in_tablet = tablet.find_all_device_boundaries().size() - 1
if d_in_tablet > observed_max_devices:
    observed_max_devices = d_in_tablet
    recompute_rotate_thresh()     // D 增大 → K 减小 → thresh 减小

recompute_rotate_thresh():
    D = max(1, observed_max_devices)
    rotate_thresh = meta_budget / (D × b) × SAFETY_FACTOR
```

`observed_max_devices` 在单个文件内单调递增（rotate 后重置为 0），设备数越多阈值越小，天然保守。

---

## 3.5 MemConstrainedWriter 接口设计

### 3.5.1 核心数据结构

```cpp
struct WritingStrategy {
    int64_t rows_per_flush;        // F：每次 flush 的行数
    int64_t flushes_per_file;      // K：每文件最多 flush 次数
    int64_t estimated_peak_mem;    // 按此策略的预估峰值内存（bytes）
};

struct WriteStats {
    int64_t rows_written_total;           // 累计写入行数（跨所有文件）
    int64_t files_completed;              // 已完成（已 close）的文件数
    int64_t current_file_flush_count;     // 当前文件已 flush 次数
    int64_t estimated_current_mem_bytes;  // 当前估算内存
};
```

### 3.5.2 接口设计

```cpp
class MemConstrainedWriter {
   public:
    static constexpr int DEVICES_UNKNOWN = 0;

    MemConstrainedWriter(
        int64_t mem_limit_bytes,
        const std::string& output_dir,
        const std::string& file_prefix,
        const std::shared_ptr<TableSchema>& table_schema,
        int n_devices = DEVICES_UNKNOWN,
        int64_t total_rows_per_device = -1
    );

    ~MemConstrainedWriter();

    int init_status() const;

    // 写入接口（内部自动 flush / close / reopen）
    int write_table(Tablet& tablet);
    int close();

    // 查询建议的 Tablet 行数
    int64_t rows_per_tablet() const;

    // 查询当前策略与运行时状态
    const WritingStrategy& strategy() const;
    const WriteStats& stats() const;
    const std::vector<std::string>& created_files() const;

    // 纯计算接口：不创建文件，仅返回策略
    static WritingStrategy compute_strategy(
        int64_t mem_limit_bytes,
        const std::shared_ptr<TableSchema>& table_schema,
        int n_devices,
        int64_t total_rows_per_device = -1
    );
};
```

文件按 `{output_dir}/{file_prefix}_{index:04d}.tsfile` 命名，如 `/data/output/sensor_0000.tsfile`。

### 3.5.3 write_table 流程

用户提交任意大小的 Tablet，系统内部累计行数，达到 $F$ 时自动 flush，达到 $K$ 次 flush 后自动轮转文件：

```
write_table(tablet):
  if init_status != E_OK: return error

  writer->write_table(tablet)
  accumulated_rows += tablet.rows

  // 动态设备追踪
  if dynamic_mode:
    d = count_devices_in_tablet(tablet)
    if d > observed_max_devices:
        observed_max_devices = d
        recompute_rotate_thresh()

  if accumulated_rows >= F:
      do_flush()

do_flush():
  writer->flush()
  accumulated_rows = 0
  flush_count++

  meta_estimate = flush_count × D_eff × b
  if meta_estimate >= rotate_thresh:
      rotate_file()

rotate_file():
  writer->close()
  file_index++
  writer = new TsFileWriter()
  writer->open(next_file_path)
  writer->register_table(schema)
  flush_count = 0
  if dynamic_mode: observed_max_devices = 0
```

### 3.5.4 使用示例

**基本用法**：

```cpp
auto schema = std::make_shared<TableSchema>("sensor_table", columns);

MemConstrainedWriter writer(
    10 * 1024 * 1024,   // 10 MB 内存上限
    "/data/output",
    "sensor",
    schema,
    50,                 // 50 个设备
    100000              // 每设备总行数（可选）
);

if (writer.init_status() != E_OK) {
    return writer.init_status();   // 内存不足以写入此 schema
}

int F = writer.rows_per_tablet();

while (has_more_data) {
    Tablet tablet(..., F);
    fill(tablet);
    writer.write_table(tablet);   // flush / rotate 全自动
}
writer.close();
```

**仅查询策略**（不创建文件）：

```cpp
auto s = MemConstrainedWriter::compute_strategy(
    10 * 1024 * 1024, schema, 50, 100000);
// s.rows_per_flush     = 每批次建议行数
// s.flushes_per_file   = 每文件建议 flush 次数
// s.estimated_peak_mem = 预估峰值内存
```

### 3.5.5 约束与局限

1. **超大 Tablet 的短暂 $M_{\text{data}}$ 超限**：若单个 Tablet 行数超过 $F$，$M_{\text{data}}$ 会短暂超出预算。底层 `chunk_group_size_threshold_` 作为双重保险兜底。
2. **STRING 列内存估算误差**：使用固定 32 字节估算变长字符串，通过 SAFETY_FACTOR 缓解。
3. **Schema 同构假设**：假设所有设备使用相同的 FIELD 列结构。异构 schema 取最大 FIELD 列数作为保守估计。
4. **`chunk_group_size_threshold_` 是进程全局配置**：多个 Writer 实例并发时会互相覆盖，当前设计仅支持单写入器场景。
5. **单线程写入**：不考虑并发写入。

---

## 3.6 实验与评估

### 3.6.1 实验环境与数据集

- **硬件**：Apple Silicon（macOS Darwin 24.6.0）
- **编译**：Release 模式，`ENABLE_MEM_STAT=ON`
- **Schema**：1 张表，2 TAG（STRING）+ 6 FIELD（INT64, DOUBLE, FLOAT, INT32, INT64, INT32），SNAPPY 压缩
- **数据量**：200M 行，10 个设备
- **内存阈值**：50 MB
- **Batch 大小**：65,536 行
- **采集间隔**：每 50 万行记录一次内存快照（含 wall time、user CPU、sys CPU）

### 3.6.2 内存控制精度

两种写入模式下，50 MB 阈值的实际控制效果：

| 指标 | 顺序写入 | 混合写入 |
|------|---------|---------|
| 设定阈值 | 50 MB | 50 MB |
| 实测峰值 | 48.1 MB | 48.6 MB |
| 峰值 / 阈值 | 96.2% | 97.2% |
| Flush 后最低 | 7.4 MB | 7.4 MB |
| 自动 flush 次数 | 5 | 6 |

内存峰值均未超过设定阈值，说明 `chunk_group_size_threshold_` 的自适应检查机制有效。峰值略低于 50 MB 是因为检查粒度为 batch 级别（64K 行），在接近但未达阈值时即完成当前 batch 并触发 flush。

### 3.6.3 写入吞吐量

| 模式 | 吞吐量 | CPU 利用率 |
|------|--------|-----------|
| 顺序写入 | 3.36 M rows/s | ~115% |
| 混合写入 | 3.30 M rows/s | ~115% |

两种模式吞吐量接近。CPU 利用率 >100% 表明部分编码/压缩工作利用了多核（SNAPPY 压缩开销）。

作为参考，同等规模下 Parquet（Arrow + SNAPPY）的写入吞吐约 6.3 M rows/s。TsFile 写入的主要瓶颈在于 `Tablet::add_value()` 的按列名 string map 查找开销，而非 I/O 或编码本身。

### 3.6.4 行为示例

以 50 设备、50 FIELD 列 int32、$M_{\text{limit}} = 12$ MB 为例：

```
s = 1008 bytes/row
b = 50 × 104 + 96 = 5296 bytes/设备/flush
M_avail ≈ 11.5 MB

对半分策略：
  F = 11.5 MB / (2 × 1008) ≈ 5,694 行
  K = 11.5 MB / (2 × 50 × 5296) ≈ 21 次

rotate_thresh = 5.75 MB × 0.85 / (50 × 5296) ≈ 第 18 次 flush
```

| 事件 | $M_{\text{meta}}$ 估算 | 操作 |
|------|----------------------|------|
| flush 1 | 0.26 MB | 继续 |
| flush 10 | 2.65 MB | 继续 |
| flush 18 | 4.77 MB $\geq$ thresh | **rotate：close 释放 4.77 MB 元数据，重开新文件** |
| flush 1（新文件）| 0.26 MB | 继续 |

文件轮转的本质：每 18 次 flush 必须 close 一次，释放积压的不可压缩元数据。

---

## 3.7 本章小结

本章针对嵌入式环境下 TsFile 写入的内存约束问题，提出了一套完整的资源受限写入优化方案：

1. **内存模型**：将写入过程内存分解为 $M_{\text{init}}$（固定）、$M_{\text{data}}$（周期性，flush 释放）和 $M_{\text{meta}}$（单调递增，close 释放）三部分，并建立了量化公式。通过 `ModStat` 编译期追踪框架进行了实验验证，公式误差在 5-15% 范围内。

2. **参数配置策略**：针对总量已知和未知两种场景，分别给出了 EOQ 最优策略和对半分预算策略，实现了自动推导批次大小 $F$ 和文件轮转频率 $K$。

3. **两级内存控制**：通过 flush（释放 $M_{\text{data}}$）和文件轮转（释放 $M_{\text{meta}}$）的两级控制机制，配合安全裕度设计，保证写入全程内存不超过用户设定的上限。同时与 TsFile 已有的 `chunk_group_size_threshold_` 自适应刷盘机制形成双重保险。

4. **MemConstrainedWriter 接口**：封装了上述所有机制，用户只需提供内存上限即可实现零决策的不间断写入，支持自动 flush、自动文件轮转和动态设备追踪。

5. **实验验证**：在 200M 行、50 MB 阈值的实验中，两种写入模式均将内存峰值控制在阈值以内，写入吞吐达到 3.3 M rows/s，验证了方案的有效性和实用性。
