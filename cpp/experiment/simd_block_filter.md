# SIMD 向量化编解码与过滤加速

本文档面向论文第五章同名章节，系统梳理 TsFile C++ 读取路径中所有 SIMD 加速点的设计动机、实现方案与相互关系。

---

## 1. 整体架构：三级谓词下推

TsFile 的时间过滤在三个粒度上依次推进，每一级命中即可跳过后续工作：

```
Level 1: Chunk 级（统计信息）
  └─ ChunkMeta.statistic_.{start_time_, end_time_}
     → 整个 Chunk 不满足则跳过（数万行）

Level 2: Page 级（统计信息）
  └─ PageHeader.statistic_.{start_time_, end_time_}
     → 整个 Page 不满足则跳过（数千行）

Level 3: Block 级（编码感知）
  └─ peek_next_block_range_int64()
     → 整个编码块不满足则跳过（≤129 行）

Level 4: 行级（SIMD 批量）
  └─ satisfy_batch_time()
     → 逐行比对，输出 bool mask

Level 5: 延迟物化
  └─ 仅对通过 mask 的行解码 value
```

第 1–2 级基于预存统计量，无额外计算开销。**第 3 级是本章核心贡献**：利用 TS2DIFF 编码结构，在不完整解码的前提下推断编码块的时间值域，实现编码感知的谓词下推。

---

## 2. TS2DIFF 编码块结构

每个 TS2DIFF 编码块的内存布局：

```
┌─────────────────────────────────────────────────────────┐
│  Header（24 字节）                                        │
│  ├─ write_index_  : int32   (块内 delta 数量，值域 0–128) │
│  ├─ bit_width_    : int32   (残差位宽)                    │
│  ├─ delta_min_    : int64   (最小 delta 值)               │
│  └─ first_value_  : int64   (块内第一个绝对时间戳)         │
├─────────────────────────────────────────────────────────┤
│  Packed Data（variable）                                  │
│  └─ write_index_ 个残差，每个占 bit_width_ bits            │
│     residual[i] = actual_delta[i] - delta_min_           │
│     actual_value[i] = actual_value[i-1] + delta_min_     │
│                       + residual[i]                       │
└─────────────────────────────────────────────────────────┘
```

编码语义：
- `actual_value[0] = first_value_`
- `actual_value[i] = actual_value[i-1] + delta_min_ + residual[i-1]`（i ≥ 1）
- 块内元素数 = `write_index_ + 1`，最大 129

---

## 3. 块级时间范围估算的三种方案

### 3.1 方案 A：宽估计（保守上界）——原始实现

**核心假设**：每个残差都取最大可能值 `2^bit_width_ - 1`。

```
block_min = first_value_
block_max = first_value_ + write_index_ × (delta_min_ + 2^bit_width_ - 1)
```

**实现**（`ts2diff_decoder.h`）：

```cpp
if (write_index_ == 0 || bit_width_ == 0) {
    block_max = first_value_ + (int64_t)write_index_ * delta_min_;
} else if (bit_width_ >= 63) {
    block_max = INT64_MAX;
} else {
    int64_t max_delta = delta_min_ + ((1LL << bit_width_) - 1);
    block_max = first_value_ + (int64_t)write_index_ * max_delta;
}
```

**代价**：仅读取 24 字节 header，O(1)，无额外 I/O。

**精度**：高度保守。当 `bit_width_ = 8` 时，上界比真实值最多偏高
`write_index_ × (255 - avg_residual)` 个单位。若真实残差集中在低值（IoT
常见模式），误差可达理论上界的数倍。

**幻影区间**（false positive 区域）：
```
[true_block_max, estimated_block_max]
= [first_value_ + write_index_*delta_min_ + sum(residuals),
   first_value_ + write_index_*(delta_min_ + 2^bit_width_ - 1)]
```
落在此区间的查询窗口会导致本可跳过的块被完整解码。

---

### 3.2 方案 B：精确残差求和（理论最优，未实现）

**洞察**：由于时间戳单调递增，块内最大值 = 最后一个元素 = 所有 delta 的累加：

```
true_block_max = first_value_ + write_index_ × delta_min_ + sum(residuals)
```

这将问题从"求 max(residuals)"转化为"求 sum(residuals)"——两者需要的数据量相同，但 sum 可以分解为按 bit 平面的 popcount：

```
sum(residuals) = Σ_{b=0}^{bit_width_-1}  2^b × popcount(bit_plane[b])
```

其中 `bit_plane[b]` 是所有残差第 b 位的集合（共 `write_index_` 个 bit）。

**为何未实现**：TS2DIFF 采用顺序 bit-packed 存储（非 bit-plane 存储），第 b 位
的位置散布在 `{b, b+bw, b+2×bw, ...}`，无法用一次 popcount 提取整个位平面，
仍需 O(n) 扫描。更严重的是：

- 对**跳过的块**：在 peek 阶段额外读入全部 packed data，但随后直接 skip，
  I/O 被浪费；
- 对**不跳过的块**：peek 读一遍 packed data，decode 再读一遍，双倍 I/O；
- 有收益的仅为：当前保守估计无法跳过、但精确 sum 估计可以跳过的块（幻影区间内的查询）。

该方案仅在幻影区间命中率高且 packed data 已在 CPU cache 中时才划算，不适合作为通用策略。

---

### 3.3 方案 C：下一块首值预读（已实现）

**关键洞察**：由于时间戳单调递增，下一个编码块的 `first_value_` 严格大于当前块的最后一个值，因此：

```
true_block_max  <  next_block.first_value_   （精确紧上界，差值 = 一个 delta）
```

**实现原理**：读完当前块 header 后，流指针位于 packed data 起点。利用
`get_wrapped_buf() + read_pos()` 直接访问裸内存，**不消耗流**地计算偏移量：

```
next block header offset = packed_bytes = (write_index_ * bit_width_ + 7) / 8
first_value_ 在 header 内偏移 = 4 (write_index_) + 4 (bit_width_) + 8 (delta_min_) = 16 字节
```

```cpp
int32_t packed_bytes = (write_index_ * bit_width_ + 7) / 8;
if (in.remaining_size() >= (uint32_t)packed_bytes + 24) {
    char* next_fv_ptr =
        in.get_wrapped_buf() + in.read_pos() + packed_bytes + 16;
    block_max = (int64_t)common::SerializationUtil::read_ui64(next_fv_ptr);
} else {
    // 最后一个块：退化为方案 A 保守估计
    ...
}
```

**代价分析**：

| 情形 | 额外代价 |
|------|---------|
| 顺序扫描（下一块必然读取） | 实质为零：next_fv_ptr 指向的内存即将被访问 |
| 随机查询（稀疏跳转） | 一次 cache line 预取（~64 字节），通常命中 L1 |
| 最后一块 | 退化为方案 A，无额外代价 |

**精度**：上界误差仅为相邻块之间的一个 delta（通常是采样间隔），比方案 A 的
误差缩小 `write_index_` 倍（对 128 值块，约 2 个数量级）。

**实现约束**：
- 依赖 `ByteStream` 为 wrapped（单连续 buffer）模式，此为 Page 解码路径的不变式；
- 依赖 `SerializationUtil::read_ui64(char*)` 的原始指针重载，零额外依赖；
- 对 `skip_peeked_block_int64` 和 `read_batch_int64` 无任何修改。

### 3.4 方案 C 算法流程图

```
peek_next_block_range_int64(ByteStream& in)
            │
            ▼
  ┌─────────────────────────────────┐
  │ current_index_ ≠ 0              │
  │  OR !has_remaining(in) ?        │
  └────────────┬────────────────────┘
          Yes  │  No
               │
          return false
               │
               ▼
  ┌─────────────────────────────────┐
  │ read_header(in)                 │  ← 读 write_index_, bit_width_ (8B)
  │ read_i64(delta_min_, in)        │  ← 读 delta_min_               (8B)
  │ read_i64(first_value_, in)      │  ← 读 first_value_             (8B)
  └────────────┬────────────────────┘    流指针现在指向 packed data 起点
               │
               ▼
  ┌─────────────────────────────────┐
  │ block_min  = first_value_       │
  │ block_count = write_index_ + 1  │
  │ packed_bytes = (wi×bw + 7) / 8  │
  └────────────┬────────────────────┘
               │
               ▼
  ┌────────────────────────────────────────┐
  │  remaining_size() ≥ packed_bytes + 24? │
  └──────┬─────────────────────────┬───────┘
    Yes  │                         │ No（最后一块）
         │                         ▼
         │             ┌───────────────────────────────┐
         │             │  write_index_==0 OR bw==0 ?   │
         │             └────────┬──────────────────────┘
         │                 Yes  │  No
         │                      │  ┌───────────────────────┐
         │             block_max │  │  bit_width_ ≥ 63 ?    │
         │             = fv +    │  └──────┬───────────────┘
         │             wi×dm     │    Yes  │  No
         │                       │         │
         │                    INT64_MAX    block_max =
         │                                fv + wi×(dm + 2^bw − 1)
         │                         │
         ▼                         │ （保守上界，方案 A）
  ┌─────────────────────────────────────┐
  │  next_fv_ptr =                      │
  │    buf_start + read_pos             │
  │    + packed_bytes          ← 跳过当前块 packed data
  │    + 16                    ← 跳过 next header 前三字段
  │                                     │
  │  block_max = read_ui64(next_fv_ptr) │  ← 裸指针读 8B，流不前进
  └────────────┬────────────────────────┘    （方案 C，精确上界）
               │
               │◄────────────── 两条路径汇合
               ▼
  ┌─────────────────────────────────┐
  │  header_peeked_ = true          │
  │  return true                    │
  └─────────────────────────────────┘
```

---

## 4. 三种方案对比

| 维度 | 方案 A（宽估计） | 方案 B（残差求和） | 方案 C（预读首值） |
|------|---------------|----------------|----------------|
| block_max 精度 | 差（差 n×max_residual） | 精确 | 接近精确（差 1 delta） |
| 额外读取字节 | 0 | 全部 packed data | 8 字节（+ 指针算术） |
| 是否消耗流 | 否 | 是（需 mark/reset） | 否 |
| 对 skip 块的代价 | 0 | packed_bytes（已读但废弃） | 0 |
| 对 decode 块的代价 | 0 | packed_bytes × 2（双读） | 0 |
| 依赖单调递增 | 否 | 否（但只对单调有意义） | 是 |
| 实现状态 | 已实现（原始） | 未实现 | 已实现（本工作） |

---

## 5. SIMD 批量解码：TS2DIFF 向量化位解包

TS2DIFF 的批量解码（`read_batch_int32` / `read_batch_int64`）对 packed data
段采用 SIMD gather + shift 实现向量化位解包，每次解码 4 个值。

### 5.1 INT32 路径（`simd_decode_4_i32`）

同时支持 AVX2（x86）和 NEON（ARM），利用 simde 抽象层实现跨平台。

核心步骤：
1. **计算 4 个值的 bit 偏移**：`pos[k] = (index + k) × bit_width`
2. **SIMD gather**：从不对齐地址一次加载 4 个 4 字节窗口
3. **字节序翻转**：TS2DIFF 为大端序，`_mm_shuffle_epi8` 转换为小端
4. **向量移位提取**：`_mm_sllv_epi32` + `_mm_srlv_epi32` 对齐并屏蔽各字段
5. **加 delta_min_**：向量广播加法
6. **前缀和重建绝对值**：`slli_si128` + `add_epi32` 实现 4 路前缀求和
7. **加 base**：与上一批次末值相加

当 `bit_width > 16` 时切换为 64 位 gather（`_mm256_i32gather_epi64`）路径。

### 5.2 INT64 路径（`simd_decode_4_i64`）

仅支持 AVX2（256 位），每次解码 4 个 INT64 值，处理步骤与 INT32 类似但使用
256 位寄存器（`__m256i`）。由于 AVX2 无原生 64 位前缀和指令，跨 128 位 lane
的累加需要标量辅助（`tmp_buf[2] += tmp_buf[1]`）。

### 5.3 标量尾处理

对每块末尾不足 4（INT64）或 8（INT32）个值的 tail 段，退化为标量
`scalar_read_bits`。对于 write_index_ = 128 的满块，tail 段最多 3 个值，
开销可忽略。

### 5.4 吞吐分析

| 路径 | 每次迭代解码量 | 核心操作 |
|------|------------|---------|
| INT32 AVX2 | 8 值（2× 4路） | gather + shuffle + sllv/srlv + prefix-sum |
| INT32 NEON | 8 值（2× 4路） | vld + vshl + prefix-sum |
| INT64 AVX2 | 4 值 | 256-bit gather + shuffle + sllv + partial scalar prefix |

---

## 6. SIMD 批量时间过滤：`satisfy_batch_time`

### 6.1 设计

`satisfy_batch_time(const int64_t* times, int count, bool* mask)` 对一批
解码后的时间戳（最多 129 个）批量执行谓词判断，输出 `bool mask[]`（1=通过）
并返回通过数量。

已覆盖的 filter 类型及其向量化形式：

| Filter 类型 | AVX2 实现 | NEON 实现 |
|------------|----------|----------|
| `TimeGt(v)` | `cmpgt_epi64(vt, v)` | `vcgtq_s64` |
| `TimeGtEq(v)` | `NOT cmpgt_epi64(v, vt)` | `vcgeq_s64` |
| `TimeLt(v)` | `cmpgt_epi64(v, vt)` | `vcltq_s64` |
| `TimeLtEq(v)` | `NOT cmpgt_epi64(vt, v)` | `vcleq_s64` |
| `TimeEq(v)` | `cmpeq_epi64(vt, v)` | `vceqq_s64` |
| `TimeNotEq(v)` | `NOT cmpeq_epi64(vt, v)` | `veorq + vceqq` |
| `TimeBetween(lo, hi)` | `ge_lo AND le_hi` (±NOT) | `vcgeq AND vcleq` |

AVX2 路径每次处理 4 个 INT64（256 位），NEON 路径每次处理 2 个。

### 6.2 movemask 提取

AVX2 无原生 64 位 movemask，通过 `_mm256_movemask_pd`（将比较结果视为
double 符号位）提取 4 位掩码：

```cpp
static inline int simd_movemask_epi64(simde__m256i v) {
    return simde_mm256_movemask_pd(simde_mm256_castsi256_pd(v));
}
```

### 6.3 单调递增时间戳的进一步优化机会

当时间戳单调递增时，`satisfy_batch_time` 的结果具有**单调分界点**特性：
对于 `TimeGt(v)` / `TimeLt(v)` 等单调谓词，mask 必然是前 k 个 false、
后 (n-k) 个 true 的形式（或反之），等价于在 129 个元素中的二分查找：

```cpp
// 潜在优化（未实现）：二分找分界点，O(log 129) ≈ 7 次比较
int lo = 0, hi = count;
while (lo < hi) {
    int mid = (lo + hi) / 2;
    if (times[mid] > value_) hi = mid; else lo = mid + 1;
}
memset(mask, 0, lo);
memset(mask + lo, 1, count - lo);
return count - lo;
```

此方案 7 次比较替代约 33 次 AVX2 向量操作，对边界块（部分通过）收益显著。
当前未实现，因需在 filter 接口中增加 `assume_monotonic` 语义参数。

---

## 7. 完整热路径流程图

```
                    ┌──────────────────────────────┐
                    │      Page 解码循环入口          │
                    └──────────────┬───────────────┘
                                   │
                    ┌──────────────▼───────────────┐
               ┌────┤   has_remaining(time_in)?     ├────┐
               │ N  └──────────────────────────────┘  Y │
               ▼                                         ▼
          ┌─────────┐     ┌────────────────────────────────────────┐
          │  结  束  │     │  ① peek_next_block_range_int64()       │  ← Level-3
          └─────────┘     │     读 header 24B                       │
                          │     裸指针预读 next.first_value_ 8B      │
                          │     → [block_min, block_max]            │
                          └──────────────┬─────────────────────────┘
                                         │
                          ┌──────────────▼───────────────┐
                     ┌────┤  filter ≠ null && peek 成功?  ├────┐
                     │ N  └──────────────────────────────┘  Y │
                     │                                         ▼
                     │         ┌───────────────────────────────────────────┐
                     │    ┌────┤ satisfy_start_end_time(block_min,block_max)├──┐
                     │    │ N  └───────────────────────────────────────────┘ Y│
                     │    ▼                                                    │
                     │  ┌──────────────────────────────────┐                  │
                     │  │ skip_peeked_block_int64()         │                  │
                     │  │   advance read_pos by packed_bytes│                  │
                     │  │ value_decoder_->skip(block_count) │                  │
                     │  └──────────────────┬───────────────┘                  │
                     │             continue│                                   │
                     │    ┌────────────────┘                                   │
                     └────┴───────────────────────────────────────────────────►┤
                                                                                │
                          ┌─────────────────────────────────────────────────────┘
                          ▼
          ┌───────────────────────────────────────────┐
          │  ② read_batch_int64(times[], 129)          │  ← SIMD 位解包
          │     gather + byte-swap + sllv/srlv         │
          │     prefix-sum 重建绝对时间戳               │
          │     AVX2: 4路×INT64 / NEON: 4路×INT32      │
          └──────────────┬────────────────────────────┘
                         │
          ┌──────────────▼────────────────────────────┐
          │  ③ satisfy_batch_time(times, n, mask[])   │  ← SIMD 谓词
          │     AVX2: cmpgt_epi64，4路×INT64           │
          │     NEON: vcgtq_s64，2路×INT64             │
          │     → pass_count，bool mask[n]             │
          └──────────────┬────────────────────────────┘
                         │
          ┌──────────────▼───────────────┐
     ┌────┤      pass_count == 0 ?       ├────┐
     │ Y  └──────────────────────────────┘  N │
     ▼                                         ▼
  ┌──────────────────────────────┐  ┌──────────────────────────────────────┐
  │ value_decoder_->skip(n)      │  │  ④ read_batch_*(values[], 129)       │  ← 延迟物化
  └──────────────┬───────────────┘  │     SIMD 批量解码                     │
                 │                  │     INT32/INT64/FLOAT/DOUBLE          │
                 │                  └─────────────────┬────────────────────┘
                 │                                     │
                 │                  ┌──────────────────▼────────────────────┐
                 │                  │  ⑤ 输出循环（标量）                    │
                 │                  │  for i in [0, n):                      │
                 │                  │    if block_all_pass OR mask[i]:        │
                 │                  │      row_appender.append(time, value)  │
                 │                  └─────────────────┬────────────────────┘
                 │                                     │
                 └─────────────────────────────────────┘
                                      │
                              （返回循环顶部）
```

各步骤 SIMD 覆盖汇总：

| 步骤 | 函数 | SIMD 状态 | 并行宽度 |
|------|------|----------|---------|
| ① 块级过滤 | `peek_next_block_range_int64` | 裸指针预读，O(1) | — |
| ② 时间批量解码 | `read_batch_int64` | AVX2 ✓ | 4× INT64 |
| ③ 批量时间过滤 | `satisfy_batch_time` | AVX2 ✓ / NEON ✓ | 4×/2× INT64 |
| ④ 值批量解码 | `read_batch_int32/64/float/double` | AVX2 ✓ | 8×INT32 / 4×INT64 |
| ⑤ 输出写入 | 输出循环 | 标量 ✗ | 1 |

---

## 8. 与 Parquet 的对比

| 优化维度 | Parquet + Arrow | TsFile（本工作） |
|---------|----------------|----------------|
| 谓词下推级数 | 2（Row Group + Page） | **3（Chunk + Page + Block）** |
| 编码感知过滤 | 不支持 | **peek_next_block_range**（方案 C） |
| 批量解码 SIMD | Arrow Compute Kernel | TS2DIFF SIMD gather+shift |
| 批量过滤 SIMD | Arrow Compute（AVX2/NEON） | satisfy_batch_time（AVX2/NEON） |
| 利用时间单调性 | 不支持 | 块级精确上界（方案 C） |
| 延迟物化 | 无内置（依赖外部引擎） | 内置：time→filter→selective value decode |
