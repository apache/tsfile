# E6-2：优化透传效果

## 状态：TODO

**产出**：T6-2（各优化组合吞吐 / 加速比 / 内存峰值）

## 方法

Python `tsfile.to_dataframe()` 端到端，逐步开启底层优化。

## 脚本：`bench_api.py`

### 优化开启顺序

| 配置 | 开启内容 |
|------|---------|
| C1 | 串行 + 标量（基线） |
| C2 | +多线程（第四章） |
| C3 | +SIMD（第五章，替换 C2 而非叠加） |
| C4 | 多线程 + SIMD（第四章+第五章叠加） |

### 前置条件

TsFile Python binding 已编译（`import tsfile` 可用）。C1/C2/C3/C4 各对应一份编译的 `.so`，需在测试中切换加载。具体切换方案（`LD_PRELOAD` / 不同 `.so` 路径 / 子进程隔离）在实现时确定。

### 参数

- 固定：8 FIELD 列，W0 文件，50% 时间过滤选择率

## 输出

- `opt_stack_results.csv`：`config, enabled_opts, throughput_mrows_s, speedup_vs_C1, peak_rss_mb`
- 论文中呈现为累积优化效果表
