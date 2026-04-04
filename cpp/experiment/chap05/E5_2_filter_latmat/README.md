# E5-2：时间过滤 + 延迟物化

## 状态：TODO

**产出**：T5-3（时间过滤吞吐矩阵）、F5-1（过滤吞吐 vs 选择率曲线）、F5-2（延迟物化解码比例曲线）

## 程序：`filter_bench`

单个程序，通过 `--metric throughput|decode_ratio` 控制输出侧重点（时间过滤吞吐 vs 延迟物化收益）。

## 测试层面

**TsFile 端到端层**（含 I/O + 解压缩 + 解码 + 过滤）。

为保证可复现性，跑前先 warm up（`cat *.tsfile > /dev/null`），让数据进入 OS page cache，
消除磁盘 I/O 波动——本实验关注的是 decode/filter CPU 耗时，不是 I/O 吞吐。

## 编译配置

| 配置 | 内容 | 用途 |
|------|------|------|
| C1 | 标量解码 + 标量过滤 + 提前物化 + 块级过滤方案 A | 完整基线 |
| C3 | SIMD 解码 + SIMD 过滤 + 延迟物化 + 块级过滤方案 C | 完整优化 |

> C3 包含所有优化的叠加效果（SIMD 解码、SIMD 过滤、延迟物化、方案 C 块级过滤）。
> 各优化的单独贡献由 E5-4 的消融实验拆解。

## 实验参数矩阵

| 变量 | 取值 |
|------|------|
| 选择率（W3） | 1% / 10% / 50% / 90% / 100% |
| SIMD | ON / OFF（编译配置控制） |

固定：W0 基线（规则采样，bit_width_≈0），8 FIELD 列，INT64，单线程。
选择率通过调整时间窗口实现：`ts_end = total_rows × selectivity`。

## 两个子实验

### 时间过滤吞吐（对应 T5-3 / F5-1）

- 指标：端到端读取吞吐（M rows/s）
- C1 vs C3，5 个选择率点
- 需埋点：`blocks_total`、`blocks_skipped_by_peek`、`blocks_decoded`

### 延迟物化收益（对应 F5-2）

- 指标：`decode_ratio = 实际解码 value 行数 / 时间窗口内总行数`
- 仅 C3，5–7 个选择率点
- 需埋点：统计真正执行 value block 解码的行数

## 输出

- `filter_results.csv`：`config, selectivity_pct, throughput_mrows_s, speedup, blocks_skipped_pct`
- `latmat_results.csv`：`selectivity_pct, decode_ratio_eager, decode_ratio_late`
- `plot_filter.py`：F5-1，x = 选择率，y = 吞吐，C1/C3 两条线
- `plot_latmat.py`：F5-2，x = 选择率，y = decode_ratio，eager/late 两条线

## 预期结论

- 低选择率（1%–10%）：SIMD 过滤 + 延迟物化效果最显著，value 解码量降低 70%–90%
- 高选择率（90%–100%）：两种配置差异缩小，趋于相同
- W0 为规则采样（bit_width_≈0），块级过滤方案 A 与 C 结果相同；不规则采样下方案 C 的额外收益见 E5-4

## 与其他实验的关系

| 实验 | 测量层面 | 关注点 |
|------|---------|--------|
| E5-1 | 解码器微基准 | SIMD 解码加速比（纯 CPU，无 I/O）|
| **E5-2**（本实验） | TsFile 端到端 | SIMD 过滤 + 延迟物化整体效果 |
| E5-4 | 解码器微基准 | 块级过滤方案 A vs C 的 skip rate |
