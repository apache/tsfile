# E3-4：不同内存预算下的写入性能

## 状态：TODO

**产出**：F3-4（柱状图）、T3-7（吞吐+flush+rotate+峰值内存）

## 依赖

**前置条件**：`ConstrainedTsFileWriter` 实现完成（见 `../../design.md` §10.3 方案：使用 `get_data_size()` + `get_meta_size()` 直接监控）。需确认相关代码已合并到 final 分支。

## 程序：`write_budget`

### 编译配置

**C4**（多线程+SIMD 生产配置，不需要 MEM_STAT；峰值内存通过 `getrusage` 测 RSS）

### 实验参数矩阵

| 变量 | 取值 |
|------|------|
| 内存预算 M_limit | 8 / 16 / 32 / 64 / 128 MB |
| 对比组 | (a) ConstrainedTsFileWriter 自动 / (b) 默认配置 / (c) 无限制 |

固定：W0 基线（10 设备，200M 行，SNAPPY）。

### 对比组说明

- **(a) 自动**：`ConstrainedTsFileWriter(memory_limit=M_limit)`，自动推导 F/K
- **(b) 默认**：`TsFileTableWriter` 默认参数（`chunk_group_size_threshold` 默认值）
- **(c) 无限制**：不设内存上限，写完所有数据后一次 flush，作为吞吐上界参考

### 输出

- `write_budget_results.csv`：`config, m_limit_mb, throughput_mrows_s, flush_count, rotate_count, peak_rss_mb`
- `plot_write_budget.py`：x = 内存预算，y = 吞吐（M rows/s），三组对比柱状图

### 预期结论

- 自动配置（a）接近无限制（c）吞吐，并随预算增大而提升（边际递减）
- 默认配置（b）在小预算下明显劣于自动配置，甚至 OOM
