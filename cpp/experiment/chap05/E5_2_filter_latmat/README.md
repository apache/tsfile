# E5-2：时间过滤 + 延迟物化

## 状态：TODO

**产出**：T5-3（时间过滤吞吐矩阵）、F5-1（过滤吞吐 vs 选择率曲线）、F5-2（延迟物化解码比例曲线）

## 程序：`filter_bench`

单个程序，通过 `--metric throughput|decode_ratio` 控制输出侧重点（时间过滤吞吐 vs 延迟物化收益）。

## 编译配置

| 配置 | 用途 |
|------|------|
| C1 | 标量，无 SIMD 时间过滤，提前物化（eager materialization） |
| C3 | SIMD 时间过滤 + 延迟物化 |

## 实验参数矩阵

| 变量 | 取值 |
|------|------|
| 选择率（W3） | 1% / 10% / 50% / 90% / 100% |
| SIMD | ON / OFF（编译配置控制） |

固定：W0 基线，8 FIELD 列，INT64，单线程。选择率通过调整时间窗口实现：`ts_end = total_rows × selectivity`。

## 两个子实验

### 时间过滤吞吐（对应 T5-3 / F5-1）

- 指标：端到端读取吞吐（M rows/s，含 I/O + 过滤 + 解码）
- C1 vs C3，5 个选择率点

### 延迟物化收益（对应 F5-2）

- 指标：`decode_ratio = 实际解码 value 行数 / 时间窗口内总行数`
- 仅 C3（延迟物化在 SIMD 过滤后生效），5~7 个选择率点
- 需在程序内埋点，统计真正执行 value chunk 解码的行数

## 输出

- `filter_results.csv`：`simd, selectivity_pct, throughput_mrows_s, speedup`
- `latmat_results.csv`：`selectivity_pct, decode_ratio_eager, decode_ratio_late`
- `plot_filter.py`：F5-1，x = 选择率，y = 吞吐
- `plot_latmat.py`：F5-2，x = 选择率，y = decode_ratio，含 eager/late 两条线

## 预期结论

- 低选择率时 SIMD 过滤 + 延迟物化效果最显著（大量数据被过滤，value 解码量降低 70%–90%）
- 高选择率（接近 100%）时两者几乎无差异
