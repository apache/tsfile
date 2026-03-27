# ConstrainedTsFileWriter 设计文档

## 1. 背景与需求

### 1.1 核心问题：元数据的不可压缩性

TsFile 写入过程中存在两类内存，性质截然不同：

| 内存类型 | 来源 | 是否压缩 | 何时释放 | 能否控制 |
|---------|------|---------|---------|---------|
| M_data  | ChunkWriter 数据页 | 是（写盘时压缩） | flush 后立即释放 | 控制批次大小 F |
| M_meta  | ChunkGroup/Chunk 元数据 | **否** | **只有 close 才释放** | 控制每文件 flush 次数 K |

**M_meta 是核心约束来源**：只要文件不 close，每次 flush 都会向内存中追加一批不可压缩的元数据（chunk header、chunk group header、timeseries index 节点）。设备数越多、flush 次数越多，M_meta 积压越严重。

因此，**文件轮转（close + 重新打开）的根本原因不是磁盘空间，而是为了释放 M_meta**。

### 1.2 现有 API 的问题

`TsFileWriter` / `TsFileTableWriter` 将所有决策完全交给用户：

- 每批写多少行（F）：需要理解内存模型才能计算
- 何时 flush：需要实时感知 M_data
- 何时轮转文件：需要追踪 M_meta 积累量

用户在资源受限环境（嵌入式设备、容器限额、边缘节点）中无法做出这些决策。

### 1.3 内存组成

实验（见同目录 `TsFile 内存控制.pdf`）确认写入过程内存由三部分构成：

```
M_total = M_init + M_data + M_meta
```

| 组成 | 特征 | 公式 |
|------|------|------|
| M_init | 固定 | ≈ 900 KB |
| M_data | 周期性，flush 后释放 | `s × F` |
| M_meta | **单调递增，不可压缩，close 才释放** | `K × D × b` |

其中：

- `F`：每次 flush 写入的行数（批次大小）
- `K`：当前文件已 flush 的次数
- `D`：当前文件已出现的设备数（唯一 TAG 组合数）
- `s = s_tablet + s_data`：每行在内存中的综合字节数
- `b`：每设备每次 flush 新增的元数据字节数（不可压缩）

对于 Table 模式：

```
s_tablet = 8 + Σ sizeof(col_type)              // 所有列（TAG + FIELD）
s_data   = Σ (8 + sizeof(field_col_type))      // 仅 FIELD 列进 ChunkWriter
s        = s_tablet + s_data

b        = n_field_cols × 104 + 96             // chunk_meta + chunk_group_meta（字节）
```

### 1.4 最优策略

**总量已知时**（EOQ 模型最优解）：

```
F_opt = √(R × D × b / s)
K_opt = R / F_opt
M_min = M_init + 2 × √(R × s × D × b)
```

当 `M_min ≤ M_limit` 时，所有数据可写入单个文件。

**总量未知时**（流式，预算对半分，F×K 最大）：

```
M_avail = M_limit - M_init
F = M_avail / (2 × s)
K = M_avail / (2 × D × b)
```

### 1.5 实验验证

配置：50 设备，50 FIELD 列 int32，总行数 100,000

| max_rows | 实测内存 | 公式估算 | 误差 |
|----------|---------|---------|------|
| 10,000   | 11.7 MB | 11.05 MB | 5.5% |
| 8,000    | 10.8 MB | 10.3 MB  | 4.6% |
| 6,000    | 10.6 MB | 9.63 MB  | 9.1% |
| 5,000    | 11.2 MB | 9.75 MB  | 12.9% |

公式误差 5–15%，主要来源：变长字符串开销、schema 结构节点、OS 内存分配粒度。

---

## 2. 设计目标

1. **约束驱动**：用户声明内存上限和 schema，系统自动推导所有参数（F、K）。
2. **零决策负担**：用户只调用 `write_table()`，flush / close / 文件轮转全部自动处理。
3. **任意 Tablet 大小**：用户无需精确提供 F 行的 Tablet，系统自动拆分超大批次。
4. **构造时可行性校验**：约束不可满足时立即报错，并说明最小可用配置。
5. **运行时可观测**：提供估算的当前内存用量、已写行数、已创建文件。
6. **零运行时监控开销**：不依赖 `ModStat`，所有估算仅使用轻量整数计算。

---

## 3. 内存控制机制

### 3.1 两类内存的控制策略

TsFileWriter 内部的 ChunkWriter **本身就是 M_data 的缓冲区**，每次 `write_table()` 都将数据写入 ChunkWriter，M_data 持续积累，直到显式调用 `flush()` 时才落盘并清零。

```
M_data 控制：在内部追踪累计写入行数，达到 F 时 flush()
             → 用户提交任意大小 Tablet，系统计数，F 行后自动刷盘

M_meta 控制：公式估算 flush_count × D × b，达到阈值时 rotate_file()
             → 文件轮转是释放不可压缩元数据的唯一手段
```

```
每次 write_table(tablet)：
  ChunkWriter 积累数据，M_data 增加
  accumulated_rows += tablet.rows

  if accumulated_rows >= F:       // 达到数据预算上限
      flush()                     // M_data 清零，M_meta 增加 D×b
      flush_count++
      accumulated_rows = 0
      if flush_count×D×b >= rotate_thresh:
          rotate_file()           // M_meta 清零
```

两者结合，M_total 始终受控：

```
写入中（未达 F）：M_total = M_init + s×accumulated + flush_count×D×b
flush 后：       M_total = M_init + flush_count×D×b
rotate 后：      M_total = M_init
```

### 3.2 关闭时内存峰值

`close()` 调用时，M_data 已为 0（最后一次 flush 已完成）。峰值为：

```
M_at_close = M_init + M_meta_final
```

由于我们在 `flush_count × D × b >= meta_budget × SAFETY_FACTOR` 时触发 rotate：

```
M_meta_final ≤ meta_budget × SAFETY_FACTOR ≤ meta_budget ≤ M_avail
→ M_at_close ≤ M_init + M_avail = M_limit  ✓
```

关闭时不会超出内存上限，SAFETY_FACTOR=0.85 同时覆盖了序列化开销和公式误差。

### 3.3 已有 auto-flush 机制（分析与结论）

`TsFileWriter` 内部有自适应区间 M_data 检查机制，通过 `chunk_group_size_threshold_` 控制：

```cpp
if (record_count_since_last_flush_ >= record_count_for_next_mem_check_) {
    mem_size = calculate_mem_size_for_all_group();
    record_count_for_next_mem_check_ =
        record_count_since_last_flush_ * chunk_group_size_threshold_ / mem_size;
    if (mem_size > chunk_group_size_threshold_) flush();
}
```

`ConstrainedTsFileWriter` 在 `accumulated_rows_ >= F` 时显式调用 `flush()`，M_data 控制已由上层保证。该内置机制作为**双重保险**保留（设置 `chunk_group_size_threshold_ = data_budget_`），防止单个超大 Tablet 导致 M_data 短暂超限。

> `chunk_group_size_threshold_` 是进程全局配置，多个实例并发时会互相覆盖，当前设计仅支持单写入器场景。

### 3.4 ModStat 方案（排除）

`ModStat` 通过 `ATOMIC_FAA` 追踪各模块内存，排除原因：

- 写入热路径（数据页分配、ByteStream 扩容）调用频繁，嵌入式 CPU 原子操作代价不可忽略
- 仅在 `ENABLE_MEM_STAT` 编译选项下开启，不适合生产路径
- 公式估算（5–15% 误差）加安全裕度已足够，无需字节精度

---

## 4. 策略计算

### 4.1 约束规格

```cpp
struct WriteConstraints {
    int64_t memory_limit_bytes;      // 必填：内存上限

    // 可选：数据规模提示（有则使用 EOQ 最优策略）
    int     n_devices       = 0;     // 0 = 未知，运行时动态追踪
    int64_t total_rows_hint = -1;    // 每设备总行数，-1 = 未知
};
```

### 4.2 可行性检查

构造时执行，失败则 `init_status()` 返回非 E_OK：

```
// 最小可行内存：F=1 行，K=1 次，D=1 设备
M_min = M_INIT_BYTES + s + b
if M_min > memory_limit_bytes:
    ERROR "内存不足：schema 最小写入需 {M_min} 字节，当前限制 {memory_limit_bytes}"
```

### 4.3 策略推导

```
M_avail = memory_limit_bytes - M_INIT_BYTES
D_eff = max(1, n_devices)     // n_devices=0 时用 1 作为初始保守估计

// 尝试 EOQ 最优（总量已知时）
if total_rows_hint > 0 && n_devices > 0:
    B = D_eff × b
    F = √(total_rows_hint × B / s)
    K = total_rows_hint / F
    peak = M_INIT_BYTES + s×F + K×B
    if peak ≤ memory_limit_bytes:
        return {rows_per_flush=F, flushes_per_file=K, estimated_peak_mem=peak}
    // 超出内存，退化为对半分

// 对半分预算（流式最优，F×K 最大）
F = M_avail / (2 × s)
K = M_avail / (2 × D_eff × b)
K = max(1LL, K)
return {rows_per_flush=F, flushes_per_file=K, estimated_peak_mem=M_INIT_BYTES+s×F+K×D_eff×b}
```

### 4.4 安全裕度

```
data_budget  = s × F
meta_budget  = M_avail - data_budget
rotate_thresh = meta_budget × SAFETY_FACTOR   // SAFETY_FACTOR = 0.85

// 每次 flush 后判断
if flush_count × D_eff × b >= rotate_thresh:
    rotate_file()
```

`sizeof_datatype()` 映射：

| TSDataType | 字节数 |
|-----------|-------|
| BOOLEAN   | 1 |
| INT32 / FLOAT  | 4 |
| INT64 / DOUBLE | 8 |
| STRING    | 32（保守估计）|

---

## 5. 接口设计

### 5.1 数据结构

```cpp
struct WriteConstraints {
    int64_t memory_limit_bytes;
    int     n_devices       = 0;
    int64_t total_rows_hint = -1;
};

struct WritingStrategy {
    int64_t rows_per_flush;        // F：每次 flush 的行数
    int64_t flushes_per_file;      // K：每文件最多 flush 次数
    int64_t estimated_peak_mem;    // 预估峰值内存（bytes）
};

struct WriteStats {
    int64_t rows_written_total;           // 累计写入行数（跨所有文件）
    int64_t files_completed;              // 已完成（已 close）的文件数
    int64_t current_file_flush_count;     // 当前文件已 flush 次数
    int64_t estimated_current_mem_bytes;  // 当前估算 M_meta（M_data 已 flush）
};
```

### 5.2 类接口

```cpp
class ConstrainedTsFileWriter {
   public:
    // 构造：立即执行可行性校验
    ConstrainedTsFileWriter(
        const WriteConstraints& constraints,
        const std::string& output_dir,
        const std::string& file_prefix,
        const std::shared_ptr<TableSchema>& schema
    );

    ~ConstrainedTsFileWriter();

    int init_status() const;   // E_OK 表示约束可满足，否则不可写入

    // 写入任意大小 Tablet，内部自动拆分、flush、轮转
    int write_table(Tablet& tablet);
    int close();

    // 观测接口
    const WriteStats&      stats()    const;
    const WritingStrategy& strategy() const;
    const std::vector<std::string>& created_files() const;

    // 纯计算接口：不创建文件，仅返回策略
    static WritingStrategy compute_strategy(
        const WriteConstraints& constraints,
        const std::shared_ptr<TableSchema>& schema
    );

    // 可行性检查：返回 E_OK 或错误码，可选输出错误描述
    static int check_feasibility(
        const WriteConstraints& constraints,
        const std::shared_ptr<TableSchema>& schema,
        std::string* error_msg = nullptr
    );
};
```

### 5.3 文件命名规则

```
{output_dir}/{file_prefix}_{index:04d}.tsfile

例：/data/output/sensor_0000.tsfile
    /data/output/sensor_0001.tsfile
```

### 5.4 用户代码示例

**基本用法**

```cpp
WriteConstraints c;
c.memory_limit_bytes = 10 * 1024 * 1024;  // 10 MB
c.n_devices = 50;

ConstrainedTsFileWriter writer(c, "/data/output", "sensor", schema);
if (writer.init_status() != E_OK) {
    return writer.init_status();   // 内存不足以写入此 schema
}

// 用户可提交任意大小的 Tablet，系统自动拆分
while (has_more_data) {
    Tablet tablet(...);
    fill(tablet);
    writer.write_table(tablet);   // 拆分 + flush + rotate 全自动
}
writer.close();
```

**仅查询策略**

```cpp
auto s = ConstrainedTsFileWriter::compute_strategy(c, schema);
// s.rows_per_flush    = 每批次建议行数
// s.flushes_per_file  = 每文件建议 flush 次数
// s.estimated_peak_mem
```

**运行时观测**

```cpp
const auto& st = writer.stats();
printf("已写 %lld 行，当前文件 flush %lld 次，估算内存 %lld KB\n",
       st.rows_written_total,
       st.current_file_flush_count,
       st.estimated_current_mem_bytes / 1024);
```

---

## 6. 内部实现

### 6.1 模块结构

```
src/writer/
├── tsfile_writer.h/.cc                  （已有）
├── tsfile_table_writer.h/.cc            （已有）
├── constrained_tsfile_writer.h          （新增）
└── constrained_tsfile_writer.cc         （新增）
```

内部直接持有 `TsFileWriter*`（而非 `TsFileTableWriter`），以便在 rotate 时精确控制 `open()` 和 `register_table()`。

### 6.2 内部状态

```cpp
// 固定参数（构造时确定）
WriteConstraints             constraints_;
WritingStrategy              strategy_;
int64_t                      s_;               // 每行综合字节数
int64_t                      b_per_device_;    // 每设备每 flush 的不可压缩元数据增量
int64_t                      meta_budget_;     // 元数据内存预算
int64_t                      rotate_thresh_;   // = meta_budget_ × SAFETY_FACTOR
int                          init_status_;

// 文件管理
TsFileWriter*                writer_;
std::shared_ptr<TableSchema> table_schema_;
int                          file_index_;
std::vector<std::string>     created_files_;
std::string                  output_dir_;
std::string                  file_prefix_;

// 运行时状态
WriteStats                   stats_;
int64_t                      accumulated_rows_;  // 上次 flush 后累计写入的行数

// 动态设备追踪（n_devices == 0 时）
bool     dynamic_mode_;
int64_t  observed_max_devices_;  // 本文件内见到的最多设备数（rotate 后重置）
```

### 6.3 构造与可行性检查

```
ConstrainedTsFileWriter(constraints, output_dir, prefix, schema):
  1. 从 schema 计算 s_, b_per_device_
  2. check_feasibility() → init_status_
     if init_status_ != E_OK: return（不创建文件）
  3. compute_strategy() → strategy_
  4. data_budget_   = s_ × strategy_.rows_per_flush
     meta_budget_   = M_avail - data_budget_
     rotate_thresh_ = meta_budget_ × SAFETY_FACTOR
  5. g_config_value_.chunk_group_size_threshold_ = data_budget_
  6. open_next_file()
```

### 6.4 write_table() 流程

用户提交任意大小的 Tablet，系统内部累计行数，达到 F 时自动 flush。

```
write_table(tablet):
  if init_status_ != E_OK: return init_status_

  writer_->write_table(tablet)              // 数据写入 ChunkWriter，M_data 增加
  accumulated_rows_ += tablet.cur_row_size_
  stats_.rows_written_total += tablet.cur_row_size_

  // 动态设备追踪（每个 Tablet 都更新，取本文件内最大值）
  if dynamic_mode_:
    d = tablet.find_all_device_boundaries().size() - 1
    if d > observed_max_devices_:
        observed_max_devices_ = d
        recompute_rotate_thresh()           // D 增大 → thresh 减小

  if accumulated_rows_ >= strategy_.rows_per_flush:
      do_flush()

do_flush():
  writer_->flush()                          // M_data 落盘，M_meta 增加 D×b
  accumulated_rows_ = 0
  stats_.current_file_flush_count++

  D_eff = dynamic_mode_ ? observed_max_devices_ : constraints_.n_devices
  meta_est = stats_.current_file_flush_count × D_eff × b_per_device_
  stats_.estimated_current_mem_bytes = M_INIT_BYTES + meta_est

  if meta_est >= rotate_thresh_:
      rotate_file()
```

### 6.5 rotate_file() 流程

```
rotate_file():
  1. writer_->close()                          // 写盘，释放 M_meta
  2. created_files_.push_back(current_path)
  3. stats_.files_completed++
  4. delete writer_; writer_ = nullptr
  5. file_index_++
  6. g_config_value_.chunk_group_size_threshold_ = s_ × strategy_.rows_per_flush
  7. writer_ = new TsFileWriter()
  8. writer_->open(make_file_path(file_index_))
  9. writer_->register_table(table_schema_)
  10. stats_.current_file_flush_count = 0
  11. if dynamic_mode_:
        observed_max_devices_ = 0
        recompute_rotate_thresh()
```

### 6.6 动态设备追踪

```
recompute_rotate_thresh():
    D = max(1LL, observed_max_devices_)
    // rotate_thresh 以"flush 次数"表示
    rotate_thresh_ = (int64_t)(meta_budget_ / (D × b_per_device_) × SAFETY_FACTOR)
    rotate_thresh_ = max(1LL, rotate_thresh_)
```

`observed_max_devices_` 在单个文件内单调递增，rotate 后重置为 0。设备数越多，阈值越小，天然保守。

---

## 7. 行为示例

配置：50 设备，50 FIELD 列 int32，memory_limit=12MB

```
s_tablet = 8 + 100×4 = 408 bytes/row   （假设 50 TAG + 50 FIELD 各 4 字节）
s_data   = 50×(8+4) = 600 bytes/row
s        = 1008 bytes/row
b        = 50×104 + 96 = 5296 bytes/设备/flush （不可压缩元数据）
M_avail  ≈ 11.5 MB

对半分策略：
  F     = 11.5MB / (2×1008) ≈ 5,694 行
  K     = 11.5MB / (2×50×5296) ≈ 21 次

data_budget  = 5694 × 1008 ≈ 5.74 MB
meta_budget  ≈ 5.75 MB
rotate_thresh = 5.75MB × 0.85 / (50×5296) ≈ 18.4 → 第 18 次 flush 后触发
```

| 事件 | M_meta 估算 | 操作 |
|------|------------|------|
| flush 1 | 0.26 MB | 继续 |
| flush 10 | 2.65 MB | 继续 |
| flush 18 | 4.77 MB ≥ thresh | **rotate：close 释放 4.77 MB 元数据，重开新文件** |
| flush 1（新文件） | 0.26 MB | 继续 |

文件轮转的本质：每 18 次 flush 必须 close 一次，强制释放积压的不可压缩元数据。

---

## 8. 约束与局限

1. **超大 Tablet 的短暂 M_data 超限**：若单个 Tablet 的行数超过 F，写入时 M_data 会短暂超出 data_budget，随后的 `do_flush()` 会立即释放。`chunk_group_size_threshold_ = data_budget_` 作为双重保险在此场景下触发保护（见 §3.3）。若需严格保证不超限，可在 write_table 前检查 Tablet 行数并返回错误。

2. **STRING 列内存估算误差**：使用固定 32 字节估算变长字符串，通过 SAFETY_FACTOR=0.85 和保守 M_INIT_BYTES 缓解。

3. **Schema 同构假设**：假设所有设备有相同的 FIELD 列结构。异构 schema 取最大 FIELD 列数作为保守估计。

4. **`chunk_group_size_threshold_` 是进程全局配置**：多个实例并发运行时会互相覆盖。当前设计仅支持单写入器场景。

5. **单线程写入**：不考虑并发。

---

## 9. 待确认事项

- [ ] `M_INIT_BYTES=900KB`：是否随注册的时间序列数量显著变化。
- [ ] `b = n_field_cols × 104 + 96`：104 bytes/chunk_meta 和 96 bytes/chunk_group_meta 为近似值，需根据实际序列化大小校准。
- [ ] `find_all_device_boundaries().size() - 1`：确认等于本次 Tablet 中唯一设备数。

---

## 10. 设计演进记录

### 10.1 从公式估算到直接监控

**原始方向**：用公式 `M_data = s × F`、`M_meta = K × D × b` 预测内存，提前计算 F/K/s/b 等参数，通过行数计数驱动 flush 和 rotate。

**转变原因**：TsFile 已有两个精确的内存测量接口，无需估算：

**M_data 的直接测量**：`TsFileWriter::calculate_mem_size_for_all_group()` 已存在，遍历所有 ChunkWriter 求和：

```cpp
// ChunkWriter / ValueChunkWriter / TimeChunkWriter 均实现：
int64_t estimate_max_series_mem_size() {
    return chunk_data_.total_size()              // 已完成页（编码后字节）
         + page_writer_.estimate_max_mem_size()  // 当前页已编码 + encoder pending 上界
         + page_header_overhead
         + statistic_sizeof;
}
```

测量的是**编码后的实际大小**，不是原始数据大小。高效编码（delta、RLE）时返回值远小于 `s × F`，天然适应实际编码效率。

**M_meta 的直接测量**：所有元数据（ChunkMeta、ChunkGroupMeta、Statistic）均从 `TsFileIOWriter::meta_allocator_`（类型 `PageArena`）分配。只需给 `PageArena` 新增一个方法：

```cpp
// page_arena.h 新增
int64_t get_total_used_bytes() const {
    int64_t total = 0;
    Page* p = dummy_head_.next_;
    while (p) {
        total += p->cur_alloc_ - (char*)(p + 1);
        p = p->next_;
    }
    return total;
}

// tsfile_io_writer.h 新增
int64_t get_meta_size() const {
    return meta_allocator_.get_total_used_bytes();
}
```

M_meta 是**不经过数据编码的结构体**，PageArena 的已用字节数即为精确的 M_meta 大小。

### 10.2 监控接口对称性

| | M_data | M_meta |
|---|---|---|
| 存储位置 | ChunkWriter 内部 ByteStream | `meta_allocator_`（PageArena） |
| 测量方法 | `calculate_mem_size_for_all_group()` | `meta_allocator_.get_total_used_bytes()` |
| 精度 | 编码后实际大小 + encoder pending 上界（保守） | 精确 |
| 编码影响 | **已体现**，反映实际编码效率 | **无关**，元数据不经数据编码 |
| 需新增代码 | 已有，直接暴露为 `get_data_size()` | PageArena 新增一个方法 + IOWriter getter |

### 10.3 策略的简化

有了直接监控，原来需要计算的 F、K、s、b、D 等参数大部分消失。"最优策略"退化为**唯一一个决策：如何在 M_avail 中分配 data_budget 和 meta_budget**。

```
M_avail = memory_limit - M_init

// 流式（总量未知）：50/50 是数学最优（最大化单文件容纳数据量）
data_budget = M_avail / 2
meta_budget = M_avail / 2

// 总量已知：EOQ 给出最优分配比（同样只是两个数值）
```

运行时控制变为纯粹的阈值比较：

```
write_table(tablet):
  writer->write_table(tablet)
  if writer->get_data_size() >= data_budget:
      writer->flush()
      if writer->get_meta_size() >= meta_budget:
          rotate_file()
```

去掉的东西：accumulated_rows 计数、observed_max_devices 追踪、find_all_device_boundaries 调用、b_per_device 计算、SAFETY_FACTOR（仅覆盖 M_init 估算误差，可提高到 0.95）。

### 10.4 两层接口定位

监控接口和自动管理层相互独立，服务不同用户：

```
TsFileWriter（已有 + 小幅扩展）
  ├── get_data_size()   ← calculate_mem_size_for_all_group() 直接暴露
  └── get_meta_size()   ← meta_allocator_.get_total_used_bytes()

ConstrainedTsFileWriter（建立在上面两个接口之上）
  └── write_table()     ← 内部用两个阈值自动判断 flush / rotate
```

- **直接使用监控接口**：适合已有自己写入节奏的用户，只需资源可见性，自行决策
- **ConstrainedTsFileWriter**：适合完全不想管写入细节的用户，约束自动执行
