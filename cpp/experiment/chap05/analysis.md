# Chapter 5 实验结果分析

## 实验环境

| 项目 | 值 |
|------|------|
| 平台 | Apple Silicon (ARM64, NEON) |
| 编译配置 | C1: SIMD OFF, 单线程; C3: SIMD ON, 单线程 |
| 编译器 | Apple Clang, Release (-O2) |
| SIMD 实现 | SIMDe 0.8.4 跨平台库 (ARM NEON / x86 AVX2) |
| E5-1 数据量 | 20M rows/dtype |
| E5-2 数据量 | 20M rows, 10 devices, 8 INT64 fields, TS_2DIFF+SNAPPY |
| E5-4 数据量 | 10,000 blocks × 129 rows = ~129 万点/page, in-memory |
| W0 随机种子 | 42, ni(-100,100), nf(-5,5), nd(-0.5,0.5) |

---

## E5-1: TS_2DIFF 编解码微基准

> 仅测试 INT32/INT64 的 TS_2DIFF 编码。FLOAT/DOUBLE 在 W0 中使用 GORILLA 编码，
> GORILLA 当前无 SIMD 批量解码路径，不纳入对比。

### 编码吞吐量 (T5-1)

| 数据类型 | Per-value (M rows/s) | Batch Scalar (M rows/s) | Batch+SIMD (M rows/s) | Batch/PV |
|----------|---------------------|------------------------|----------------------|----------|
| INT32 | 85 | **122** | 123 | **1.43x** |
| INT64 | 87 | **141** | 137 | **1.62x** |

编码端 `encode_batch` 的优化点:
- **消除串行依赖**: 用相邻差分 `delta[i] = values[i] - values[i-1]` 替代逐值 `value - previous_value_`，允许编译器/SIMD 向量化
- **SIMD 批量差分**: 128-bit NEON 一次算 4 个 INT32 差分 (INT64 为 2 个)
- **SIMD 批量 min/max**: `simde_mm_min/max_epi32` 向量化规约
- **减少函数调用开销**: 无逐值虚函数调用、无逐值分支判断

编码端 SIMD ON vs OFF 差异微小 (123 vs 122)，因为 SIMD 仅作用于 128 个元素的小数组，编译器自动向量化已做了类似优化。主要收益来自批量化本身。

### 解码吞吐量 (T5-2)

| 数据类型 | Per-value (M rows/s) | Batch Scalar (M rows/s) | Batch+SIMD (M rows/s) | Batch/PV | SIMD/PV |
|----------|---------------------|------------------------|----------------------|----------|---------|
| INT32 | 169 | 535 | **1130** | **3.16x** | **6.69x** |
| INT64 | 162 | 739 | 819 | **4.56x** | **5.06x** |

解码端优化层次:

1. **Per-value → Batch (3.2-4.6x)**: `read_batch_int32/64` 直接用 raw pointer 访问 bit-packed 数据，跳过 `ByteStream::read_buf()` 的逐字节原子读、页边界检查和 memcpy 开销。一次性处理整个 block，避免逐值路径的状态机维护。

2. **Batch → Batch+SIMD (INT32 再提 2.1x, INT64 再提 1.1x)**: `simd_decode_4_i32` 使用 128-bit SIMD gather + shift + prefix-sum 一次解 4 个值，2 组共 8 个值。INT32 使用原生 128-bit NEON，效率高。

3. **INT64 SIMD 加速弱于 INT32**: 数据 bit_width 相近 (≈8-9 bits)，但 INT64 SIMD 路径有架构劣势:
   - 使用 256-bit 指令，ARM NEON 无原生 256-bit 支持，SIMDe 拆为 2×128-bit 模拟
   - 每次只解 4 个值 (vs INT32 的 8 个)
   - Prefix sum 需 store→标量修改→load 回退，打断流水线
   - 在 x86 AVX2 (原生 256-bit) 上预期加速比会显著提高

### 编解码对比

编码端批量化加速 (1.4-1.6x) 远小于解码端 (3.2-4.6x)。原因:
- 编码核心的 bit-packing (`write_bits`) 仍是逐值串行写入 ByteStream，`encode_batch` 只优化了前置的 delta 计算和 min/max 查找
- 解码端的 batch 路径则整体绕过了 ByteStream，直接操作原始字节数组

---

## E5-2: 端到端读取 — 逐行 vs 批量+SIMD

### 不同选择率下的端到端读取性能

| 选择率 | Row-by-row (M rows/s) | Batch+SIMD (M rows/s) | 加速比 |
|--------|----------------------|----------------------|--------|
| 1% | 4.10 | 8.45 | 2.1x |
| 10% | 5.96 | 20.62 | **3.5x** |
| 50% | 6.20 | 24.98 | **4.0x** |
| 90% | 5.70 | 24.79 | **4.3x** |
| 100% | 5.87 | 24.53 | **4.2x** |

### 分析

1. **批量+SIMD 端到端加速 2.1-4.3x** — 逐行读取 (`rs->next()` + `get_value()`) 稳定在 ~6 M rows/s，批量读取 (`get_next_tsblock()` + SIMD) 在高选择率下达到 ~25 M rows/s。加速比随选择率增大而增大，因为高选择率下解码量越大，批量化的优势越明显。

2. **低选择率加速较小 (2.1x)** — 1% 选择率时，固定开销 (文件打开、metadata 解析、索引查找) 占比更大，解码本身占比小，批量化的优势被稀释。

3. **端到端 vs 纯解码** — 端到端吞吐量 (~25 M rows/s) 远低于纯解码 (>500 M rows/s)，差距约 20 倍。差距来自 SNAPPY 解压、Page 头解析、文件 I/O、TsBlock 内存分配和 8 列并行解码的综合开销。

---

## E5-4: Block-Level 时间过滤精度

### E5-4a: 跳块率对比 (T5-5, 10,000 blocks/page, ~129 万点)

| bit_width | Plan A 跳块率 | Plan C 跳块率 | 幻影块数 |
|-----------|--------------|--------------|----------|
| 0 | 100.0% | 100.0% | 0 |
| 4 | 0.0% | 100.0% | 9,999 |
| 8 | 0.0% | 100.0% | 9,999 |
| 12 | 0.0% | 100.0% | 9,999 |

### E5-4b: 查询延迟对比 (T5-6, 10% 选择率, 1000 次重复)

| bit_width | Plan A p50 (ms) | Plan C p50 (ms) | 加速比 |
|-----------|----------------|----------------|--------|
| 0 | 0.317 | 0.310 | 1.02x |
| 4 | 0.372 | 0.368 | 1.01x |
| 8 | 0.339 | 0.335 | 1.01x |
| 12 | 0.397 | 0.394 | 1.01x |

### 分析

1. **bit_width=0 时 Plan A 与 Plan C 完全一致** — 采样完全规则时，保守上界 = 精确上界。验证退化路径正确性。

2. **bit_width >= 4 时 Plan A 几乎完全失效** — 10,000 个块中仅 1 个块 (最后一个，无法 lookahead) 被 Plan A 跳过。幻影区间宽度 = `write_index × (2^bit_width - 1) × delta_base`:
   - bw=4: ~32 分钟; bw=8: ~9 小时; bw=12: ~6 天
   - 对任何 jitter > 0 的数据，Plan A 的 block 过滤基本失效

3. **延迟差异 ~1%** — 10% 选择率查询中，窗口覆盖 ~1,000 个块（必须解码），窗口外 ~9,000 个块两种 Plan 都能跳过。幻影块集中在窗口边界，数量有限。

4. **Plan C 的价值在精度而非单次延迟** — Lookahead 只需一次 raw pointer 读 (8 字节)，零成本消除 99.99% 的幻影块。

### Block 级过滤与 Page 大小的关系

TsFile 中 page 是 I/O 和解压的最小单位，page 大小（`page_writer_max_point_num_`，默认 10,000 点）决定了过滤粒度的分层：

| Page 大小 | Page 级过滤 | Block 级过滤价值 |
|-----------|-----------|-----------------|
| 小 (≤10K 点, ~77 blocks) | 粒度细，整页跳过即可 | 低：page 本身时间范围窄，很少部分命中 |
| 大 (100K+ 点, ~775+ blocks) | 粒度粗，query 常部分命中 page | **高：block 过滤填补 page 内过滤空白** |

**大 Page 的写入优势**：高频采集场景（振动传感器 10-100 kHz、车联网毫秒级采样）下，增大 page 可以：
- 提高压缩效率 — 压缩器看到更长的连续数据流
- 减少 page 封口开销 — 更少的 page header/statistics 写入
- 降低元数据占比 — 每个 page header 有固定字节开销

**大 Page 不影响读取粒度**：无论写入时 page 设多大，读取端的过滤能力是等价的：
- 小 page → page 级统计信息直接跳过整个 page
- 大 page → 进入 page 后，block 级过滤 (Plan C) 跳过不需要的 blocks

因此 **大 page + Plan C block 过滤** 是最优组合：写入端享受大 page 的压缩和减少封口开销的效率，读取端靠 block 级精确过滤弥补 page 粒度不足，不牺牲查询选择性。Plan C 的 lookahead 使这种组合成为可能——没有 Plan C，大 page 下的 block 过滤会因幻影区间而完全失效。

---

## 总结

### 各层优化效果汇总

| 层次 | 优化 | INT32 | INT64 |
|------|------|-------|-------|
| 编码 | Per-value → Batch | 85 → **122** M rows/s (**1.43x**) | 87 → **141** M rows/s (**1.62x**) |
| 编码 | Batch → Batch+SIMD | 122 → 123 (~1.0x) | 141 → 137 (~1.0x) |
| 解码 | Per-value → Batch | 169 → **535** M rows/s (**3.16x**) | 162 → **739** M rows/s (**4.56x**) |
| 解码 | Batch → Batch+SIMD | 535 → **1130** M rows/s (**2.11x**) | 739 → **819** M rows/s (**1.11x**) |
| 解码 | Per-value → Batch+SIMD | 169 → **1130** M rows/s (**6.69x**) | 162 → **819** M rows/s (**5.06x**) |
| 端到端 | Row → Batch+SIMD (100%) | ~6 → **25** M rows/s (**4.2x**) | — |
| 过滤 | Plan A → Plan C 跳块率 (bw≥4) | 0% → **100%** | — |

### 关键结论

1. **批量化是最大的加速来源** — 解码端 batch 带来 3-5x 加速，编码端 1.4-1.6x。批量化通过绕过 ByteStream 逐字节开销、消除虚函数调用和串行依赖实现加速。

2. **SIMD 是批量化的增量收益** — 在 batch 基础上，SIMD 对 INT32 解码再提 2.1x（总计 6.7x），对 INT64 受 ARM 256-bit 模拟限制仅 1.1x。编码端 SIMD 收益可忽略。

3. **端到端加速 2-4x** — 逐行读取 ~6 M rows/s 提升到批量+SIMD ~25 M rows/s。解码优化的绝对收益被 I/O、解压等端到端开销稀释。

4. **Plan C lookahead 以零代价消除幻影块** — bit_width≥4 时 Plan A block 过滤完全失效，Plan C 恢复 100% 跳块率。结合大 page 策略（高频采集场景），Plan C 使得 "大 page 高效写入 + block 级精确过滤读取" 成为可能。

### 待补充工作

1. **E5-3 跨平台** — ARM NEON (本次) vs x86 AVX2 (fit39) 性能对比
2. **x86 AVX2 验证** — INT64 SIMD 在 ARM 上受 256-bit 模拟限制 (1.1x)，原生 AVX2 预期显著提高
3. **大规模数据 (W0, 200M rows)** — 当前 E5-2 使用 20M 行，可扩展至 200M 行验证线性扩展性

---

*实验日期: 2026-04-04*
*图表: `plot_all.py` 生成，PDF 格式*
