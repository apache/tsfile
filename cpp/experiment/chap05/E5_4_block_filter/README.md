# E5-4：块级时间过滤精度（方案 A vs 方案 C）

## 状态：TODO

**产出**：T5-5（skip rate × bit_width_ 矩阵）、F5-3（phantom block 数量对比柱状图）、T5-6（查询延迟 × bit_width_）

## 背景

TS2DIFF 编码块的时间上界估算有两种方案：

- **方案 A（保守）**：假设所有残差取最大值，`block_max = first_value + n×(delta_min + 2^bw - 1)`
- **方案 C（预读）**：裸指针读下一块的 `first_value_`，利用时间戳单调递增性质获得精确上界

两者的差距由 **幻影区间** 决定：

```
幻影区间宽度 = write_index_ × (2^bit_width_ - 1) × delta_min_
```

落在幻影区间内的查询起始时间会导致方案 A 无法跳过本应跳过的块（false positive），
方案 C 则能正确跳过。

## 测试层面

**解码器微基准层**（直接操作 `ByteStream` + `TS2DIFFDecoder`，绕过文件 I/O 和解压缩）。

选择解码器层的原因：块级过滤收益发生在 Page 已解压进内存之后，跳过的是 decode CPU
工作而非 I/O，在解码器层测量结果干净、可控、可复现。

## 程序：`block_filter_bench`

新程序，直接构造 `ByteStream` 并调用 decoder 接口，无需读取 TsFile 文件。
通过编译宏 `USE_LOOKAHEAD`（ON/OFF）切换方案 A / 方案 C：

```cpp
// peek_next_block_range_int64 内部
#ifdef USE_LOOKAHEAD
    // 方案 C：裸指针预读 next.first_value_
#else
    // 方案 A：保守上界公式
#endif
```

## 数据生成

在内存中直接生成 TS2DIFF 编码的 Page 数据，无需写文件。控制参数：

| 参数 | 含义 | 取值 |
|------|------|------|
| `target_bw` | 目标 bit_width_（抖动范围 `[0, 2^target_bw - 1]` ms） | 0 / 4 / 8 / 12 |
| `delta_base` | 基础采样间隔（ms） | 1000（1 Hz）|
| `n_blocks` | 每 Page 的 block 数量 | 1000 |
| `block_size` | 每 block 的值数量 | 129（满块）|

生成方式：

```cpp
int64_t ts = start_time;
for each block:
    first_value = ts
    for each delta in [0, write_index_):
        delta = delta_base + (rand() % (1 << target_bw))
        ts += delta
    encode block → packed into ByteStream
```

## 实验参数矩阵

| 变量 | 取值 |
|------|------|
| `target_bw` | 0 / 4 / 8 / 12 |
| 方案 | A（保守）/ C（预读）|
| 查询位置 | Q_phantom（起始时间落在幻影区间内）/ Q_normal（10% 选择率）|

固定：`delta_base = 1000ms`，`n_blocks = 1000`，`block_size = 129`，单线程。

## 两个子实验

### E5-4a：skip rate 对比（对应 T5-5 / F5-3）

**目标**：量化方案 A 的 false positive 规模随 bit_width_ 的增长趋势。

**查询构造（Q_phantom）**：对每个 block i，令
`query_start = true_block_max_i + delta_base/2`（落在幻影区间中点），
统计各方案的 skip 数量。

**需埋点的指标**：
- `blocks_total`：总 block 数
- `blocks_skipped`：peek 判断为不满足、跳过的 block 数
- `phantom_blocks`：A 跳不过但 C 跳得过的 block 数（= A.decoded - C.decoded）

**产出 T5-5**：

| bit_width_ | 幻影区间宽度（s） | A skip rate | C skip rate | phantom block 数/1000 blocks |
|-----------|----------------|------------|------------|------------------------------|
| 0 | 0 | X% | X%（=A）| 0 |
| 4 | ~30 min | X% | Y% | Z |
| 8 | ~9 h | X% | Y% | Z |
| 12 | ~6 d | X% | Y% | Z |

**产出 F5-3**：柱状图，x = bit_width_，y = phantom block 数，对比两个方案的 skip 数量。

### E5-4b：查询延迟对比（对应 T5-6）

**目标**：将 skip rate 差异转化为可感知的延迟改善。

**查询构造（Q_normal）**：标准时间范围查询，选择率约 10%，起始时间不刻意落在幻影区间，
反映真实工作负载下的平均收益。

重复 1000 次取中位数，排除首次运行 cache cold 效应。

**产出 T5-6**：

| bit_width_ | 方案 A 延迟（ms） | 方案 C 延迟（ms）| 加速比 |
|-----------|----------------|----------------|-------|
| 0 | X | X（=A）| 1.0× |
| 4 | X | Y | Z× |
| 8 | X | Y | Z× |
| 12 | X | Y | Z× |

## 输出文件

- `skip_rate_results.csv`：`bw, method, blocks_total, blocks_skipped, phantom_blocks, skip_rate_pct`
- `latency_results.csv`：`bw, method, latency_ms_p50, latency_ms_p95, speedup`
- `plot_skip_rate.py`：F5-3，柱状图

## 预期结论

- `bit_width_=0`（规则采样）：A=C，无差异，验证退化路径正确
- `bit_width_≥4`：方案 C phantom_blocks=0，方案 A 随 bit_width_ 指数增长
- `bit_width_=8`（中等抖动，IoT 常见）：方案 A 有约 250 个 phantom block/1000，
  延迟比方案 C 高 X%（具体数值待实测）
- 幻影区间宽度与 `2^bit_width_` 成正比，在论文中可作为理论分析的实验验证

## 与其他实验的关系

| 实验 | 关注点 |
|------|--------|
| E5-1 | SIMD 解码吞吐（解码器层，与本实验同层） |
| E5-2 | SIMD 过滤 + 延迟物化端到端效果（TsFile 层，W0 规则采样，A=C）|
| **E5-4**（本实验） | 块级过滤方案 A vs C 的机制验证与收益量化 |

E5-2 使用 W0（规则采样，`bit_width_≈0`），方案 A 和 C 结果相同，因此 E5-4 需要
专门的不规则采样数据来体现方案 C 的独立贡献。
