# E4-3：串行占比 α 实测

## 状态：TODO（依赖 E4-1/E4-2 完成）

**产出**：T4-7（压缩算法 × 路径 × 实测 α / 估算 α / 偏差）

## 无独立程序

本实验是对 E4-1/E4-2 结果的后处理，使用 Python 脚本 `calc_alpha.py`。

## 输入

- `../E4_12_throughput/write_*_1t_*.csv`（1 线程基线）
- `../E4_12_throughput/write_*_8t_*.csv`（8 线程）
- 对应的 `read_*` 系列

## 计算方法

由 Amdahl 定律反推 α（取 n=8 线程时的加速比）：

```python
speedup_8 = throughput_8t / throughput_1t
alpha = (1/speedup_8 - 1/8) / (1 - 1/8)
```

## 输出

- `alpha_results.csv`：`compression, mode, speedup_8t, alpha_measured, alpha_estimated, deviation_pct`
- 直接在论文中以表格形式呈现
