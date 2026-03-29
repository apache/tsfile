# MemConstrainedWriter 设计文档

## 1. 背景与需求

### 1.1 问题描述

TsFile C++ 在资源受限环境（嵌入式设备、容器限额、边缘计算节点）中运行时，用户面临以下困境：

- 不知道每次应写入多少行（`max_rows`）
- 不知道写入多少次后需要调用 `flush()`
- 不知道何时内存将超出上限，需要 `close()` 当前文件并打开新文件

现有的 `TsFileWriter` / `TsFileTableWriter` 将这三个决策完全交给用户，用户需要深入理解 TsFile 内存模型才能正确配置。

### 1.2 内存组成分析

实验（见 `experiment/TsFile 内存控制.pdf`）确认了写入过程的内存由三部分构成：

```
M_total = M_init + M_data(F) + M_meta(K)
```

| 组成 | 特征 | 公式 |
|------|------|------|
| M_init | 固定，程序启动后不变 | ≈ 900 KB |
| M_data | 周期性，flush 后释放 | `s × F`（字节） |
| M_meta | 单调递增，close 才写盘 | `K × D × b`（字节） |

其中：

- `F`：每次 flush 写入的行数
- `K`：当前文件已 flush 的次数
- `D`：当前文件已出现的设备数（唯一 TAG 组合数）
- `s = s_tablet + s_data`：每行在内存中的综合字节数
- `b`：每设备每次 flush 新增的元数据字节数

对于 Table 模式，各量的计算方式：

```
s_tablet = 8 + Σ sizeof(col_type)              // 所有列（TAG + FIELD）
s_data   = Σ (8 + sizeof(field_col_type))      // 仅 FIELD 列进 ChunkWriter
s        = s_tablet + s_data

b        = n_field_cols × 104 + 96             // 每设备每次 flush 的 chunk_meta + chunk_group_meta
```

### 1.3 内存约束下的最优策略

在给定内存上限 `M_limit` 和写入规模（设备数 `D`，总行数 `R`）的情况下：

**总量已知时**（EOQ 模型最优解）：

```
F_opt = √(R × D × b / s)
K_opt = R / F_opt
M_min = M_init + 2 × √(R × s × D × b)
```

当 `M_min ≤ M_limit` 时，F_opt 即为最优批次大小，所有数据可写入单个文件。

**总量未知时**（流式，预算对半分）：

```
M_avail = M_limit - M_init
F = M_avail / (2 × s)          // 数据预算
K = M_avail / (2 × D × b)      // 元数据预算
```

此时 F × K 最大（单个文件容纳的数据量最多），是未知总量场景下的最优策略。

---

## 2. 设计目标

1. 用户只需提供内存上限和 schema，不需要手动计算 F 和 K。
2. 用户只需调用 `write_table()`，flush / close / reopen 全部自动处理。
3. 当设备数未知时，通过动态追踪自适应调整，保证内存不超出上限。
4. 接口风格与现有 `TsFileTableWriter` 保持一致，便于迁移。
5. 零运行时内存监控开销：不依赖 `ModStat`，仅使用轻量的整数计算。

---

## 3. 关于内存监控方式的取舍

### 3.1 ModStat 方案（已排除）

`ModStat` 通过在每次 `mem_alloc` / `mem_free` 时执行一次 `ATOMIC_FAA` 来追踪各模块的内存占用。

排除原因：
- 写入热路径（数据页分配、ByteStream 扩容）中 `mem_alloc` 调用频繁
- `ATOMIC_FAA` 在弱内存模型的嵌入式 CPU 上代价不可忽略
- `ModStat` 用于诊断目的，仅在 `ENABLE_MEM_STAT` 编译选项下开启
- `MemConstrainedWriter` 不需要字节精度，公式估算（5-15% 误差）+ 安全裕度已足够

### 3.2 公式估算方案（采用）

利用公式 `M_meta ≈ flush_count × D × b` 进行轻量估算：

- **F** 在构造时由公式一次性计算，之后固定不变
- **rotate 判断** 在每次 `flush()` 后执行一次整数乘法，开销可忽略
- **D（设备数）** 通过 `Tablet::find_all_device_boundaries()` 高效获取

`find_all_device_boundaries()` 按列进行逐元比较（非按行遍历），返回每个设备在 Tablet 中的行范围边界。`boundaries.size() - 1` 即为本次 Tablet 涉及的设备数。

### 3.3 安全裕度

由于公式存在 5-15% 的估算误差（主要来源：变长字符串开销、schema 结构的 list/map 节点），rotate 触发条件设置为：

```
if meta_estimate >= meta_budget × SAFETY_FACTOR:   // SAFETY_FACTOR = 0.85
    rotate_file()
```

提前 15% 触发 rotate，保证实际内存不超出上限。

---

## 4. 接口设计

### 4.1 数据结构

```cpp
struct WritingStrategy {
    int64_t rows_per_tablet;      // 建议的 Tablet 行数 (F)
    int64_t flushes_per_file;     // 每个文件最多 flush 次数 (K)，-1 = 不限
    int64_t estimated_peak_mem;   // 按此策略的预估峰值内存（bytes）
};
```

### 4.2 类接口

```cpp
class MemConstrainedWriter {
   public:
    // n_devices = DEVICES_UNKNOWN(0) 时进入动态追踪模式
    static constexpr int DEVICES_UNKNOWN = 0;

    MemConstrainedWriter(
        int64_t mem_limit_bytes,           // 内存上限
        const std::string& output_dir,     // 输出目录
        const std::string& file_prefix,    // 文件名前缀
        const std::shared_ptr<TableSchema>& table_schema,
        int n_devices = DEVICES_UNKNOWN,   // 设备数，0 = 未知
        int64_t total_rows_per_device = -1 // 每设备总行数，-1 = 未知
    );

    ~MemConstrainedWriter();

    // 写入接口（内部自动 flush / close / reopen）
    int write_table(Tablet& tablet);

    // 完成写入后调用
    int close();

    // 查询建议的 Tablet 行数，用于创建 Tablet
    int64_t rows_per_tablet() const;

    // 查询当前生效的写入策略
    const WritingStrategy& strategy() const;

    // 本次写入创建的所有文件路径
    const std::vector<std::string>& created_files() const;

    // 纯计算接口：不创建 writer，只返回建议策略
    static WritingStrategy compute_strategy(
        int64_t mem_limit_bytes,
        const std::shared_ptr<TableSchema>& table_schema,
        int n_devices,
        int64_t total_rows_per_device = -1
    );
};
```

### 4.3 文件命名规则

输出文件按 `{output_dir}/{file_prefix}_{index:04d}.tsfile` 命名：

```
/data/output/sensor_0000.tsfile
/data/output/sensor_0001.tsfile
```

### 4.4 用户代码示例

```cpp
auto schema = std::make_shared<TableSchema>("sensor_table", columns);

MemConstrainedWriter writer(
    10 * 1024 * 1024,   // 10 MB 内存上限
    "/data/output",
    "sensor",
    schema,
    50,                 // 50 个设备（已知时填写，未知时省略）
    100000              // 每设备总行数（可选）
);

int F = writer.rows_per_tablet();

while (has_more_data) {
    Tablet tablet(..., F);
    fill(tablet);
    writer.write_table(tablet);   // flush / rotate 全自动
}
writer.close();

for (const auto& path : writer.created_files()) {
    // /data/output/sensor_0000.tsfile ...
}
```

---

## 5. 内部实现

### 5.1 模块结构

```
src/writer/
├── tsfile_writer.h/.cc              （已有）
├── tsfile_table_writer.h/.cc        （已有）
├── mem_constrained_writer.h         （新增）
└── mem_constrained_writer.cc        （新增）
```

`MemConstrainedWriter` 内部持有一个 `TsFileWriter*`，在文件轮换时销毁并重建。直接包装 `TsFileWriter` 而非 `TsFileTableWriter`，以便在 rotate 时精确控制 `open()` 和 `register_table()`。

### 5.2 内部状态

```cpp
// 固定参数（构造时确定，之后不变）
int64_t  mem_limit_bytes_;
int64_t  s_;               // 每行综合字节数 = s_tablet + s_data
int64_t  b_per_device_;    // 每设备每 flush 的元数据增量（字节）
int64_t  meta_budget_;     // 分给元数据的内存预算 = M_avail - s_ × F
WritingStrategy strategy_; // rows_per_tablet 固定

// 运行时状态
TsFileWriter*                writer_;
std::shared_ptr<TableSchema> table_schema_;
int64_t                      flush_count_;      // 当前文件已 flush 次数
int                          file_index_;       // 当前文件编号
std::vector<std::string>     created_files_;

// 动态设备追踪（n_devices == DEVICES_UNKNOWN 时使用）
bool     dynamic_mode_;
int64_t  observed_max_devices_;  // 单次 Tablet 内见过的最多设备数
int64_t  current_k_;             // 动态计算的 K 值
```

### 5.3 write_table() 流程

```
write_table(tablet):
  1. writer_->write_table(tablet)
  2. writer_->flush()
  3. flush_count_++

  4. if dynamic_mode_:
       d_in_tablet = tablet.find_all_device_boundaries().size() - 1
       if d_in_tablet > observed_max_devices_:
           observed_max_devices_ = d_in_tablet
           recompute_k()          // D 增大 → K 减小

  5. effective_k = dynamic_mode_ ? current_k_ : strategy_.flushes_per_file
     meta_estimate = flush_count_ × D_effective × b_per_device_
     if effective_k > 0 && meta_estimate >= meta_budget_ × SAFETY_FACTOR:
         rotate_file()
```

> **为何同时保留 flush_count 判断和 meta_estimate 判断：**
> `meta_estimate` 是主要判断条件（基于实际累积量），`effective_k` 作为硬上限保底，
> 防止极端情况下（如估算偏差较大）超出预算。

### 5.4 rotate_file() 流程

```
rotate_file():
  1. writer_->close()
  2. delete writer_
  3. file_index_++
  4. writer_ = new TsFileWriter()
  5. writer_->open(make_file_path(file_index_))
  6. writer_->register_table(table_schema_)
  7. created_files_.push_back(current path)
  8. flush_count_ = 0
  9. if dynamic_mode_: observed_max_devices_ = 0; recompute_k()
```

### 5.5 动态设备追踪

```
// 每次 write_table 后（仅在 dynamic_mode_ 时）
d_in_tablet = tablet.find_all_device_boundaries().size() - 1

// observed_max_devices_ 跟踪单个 Tablet 内见过的最多设备数
// （rotate 后重置，重新从当前 Tablet 学习）
if d_in_tablet > observed_max_devices_:
    observed_max_devices_ = d_in_tablet
    recompute_k()

recompute_k():
    D = observed_max_devices_
    B = D × b_per_device_
    current_k_ = max(1, meta_budget_ / B)
```

**动态模式的保守性来源**：`observed_max_devices_` 只增不减（单文件内），设备数越多 K 越小。安全裕度（×0.85）进一步确保估算偏低时不会超出上限。

### 5.6 compute_strategy() 计算逻辑

```
inputs: mem_limit, schema, n_devices, total_rows (-1 = 未知)

s_tablet = 8 + Σ sizeof(col_type)              // 全部列（TAG + FIELD）
s_data   = Σ (8 + sizeof(field_col_type))      // 仅 FIELD 列
s = s_tablet + s_data

n_field_cols = count(FIELD columns in schema)
b = n_field_cols × 104 + 96                    // 每设备每 flush 的元数据

M_avail = mem_limit - M_INIT_BYTES             // M_INIT_BYTES = 900 KB

if total_rows > 0 && n_devices > 0:            // 总量已知：EOQ 最优
    B = n_devices × b
    F = √(total_rows × B / s)
    K = total_rows / F
    peak = M_INIT_BYTES + s×F + K×B
    if peak ≤ mem_limit: return {F, K, peak}
    // 超出上限，退化为对半分

// 总量未知，或 EOQ 超限：对半分预算
B = max(1, n_devices) × b                      // n_devices=0 时用 1 作为初始值
F = M_avail / (2 × s)
K = M_avail / (2 × B)
peak = M_INIT_BYTES + s×F + K×B
return {F, K, peak}
```

`sizeof_datatype()` 的映射：

| TSDataType | 字节数 |
|-----------|-------|
| BOOLEAN   | 1     |
| INT32 / FLOAT  | 4 |
| INT64 / DOUBLE | 8 |
| STRING    | 32（保守估计，实际为变长）|

---

## 6. 两种模式行为对比

以实验配置（50 设备，50 FIELD 列 int32，M_limit=12MB）为例：

```
s = 208 + 600 = 808 bytes/row
b = 50×104 + 96 = 5296 bytes/设备/flush
B = 50×5296 = 264,800 bytes/flush

M_avail ≈ 11.5MB

策略（对半分）：F ≈ 7,500 行，K ≈ 22 次
```

**n_devices 已知（=50）：**

| flush 轮次 | meta_estimate | meta_budget×0.85 | 是否 rotate |
|-----------|--------------|-----------------|------------|
| 第 10 次  | 10×264800 = 2.5MB | 4.9MB | 否 |
| 第 19 次  | 19×264800 = 4.8MB | 4.9MB | **是** |

**n_devices 未知（动态追踪，每次 Tablet 含 50 设备）：**

| flush 轮次 | observed_D | K 重算 | meta_estimate | 是否 rotate |
|-----------|-----------|-------|--------------|------------|
| 第 1 次   | 50（首次发现即稳定）| 22 | 0.26MB | 否 |
| 第 19 次  | 50 | 22 | 4.8MB | **是** |

两种模式在设备数稳定后行为一致。

---

## 7. 约束与局限

1. **STRING 列的内存估计误差**：STRING 类型使用固定 32 字节估算，实际为变长。通过 SAFETY_FACTOR=0.85 和 M_INIT_BYTES 的保守取值缓解此误差。

2. **每次 write_table 后立即 flush**：当前设计在每次 `write_table` 后强制 flush。此行为与实验场景一致（每个 Tablet 写完即落盘），是内存受限场景的推荐模式。

3. **Schema 同构假设**：`compute_strategy()` 假设所有设备使用相同的 FIELD 列结构。异构 schema 场景下应取最大 FIELD 列数作为保守估计。

4. **单线程写入**：当前设计不考虑并发写入场景。

---

## 8. 待确认事项

- [ ] `M_INIT_BYTES` 取值：实验测得约 900 KB，是否随注册的时间序列数量显著变化。
- [ ] `b = n_field_cols × 104 + 96` 的精度：104 bytes/chunk_meta 和 96 bytes/chunk_group_meta 为近似值，需根据实际序列化大小校准。
- [ ] `find_all_device_boundaries()` 返回值语义确认：`boundaries.size() - 1` 等于本次 Tablet 中唯一设备数。
