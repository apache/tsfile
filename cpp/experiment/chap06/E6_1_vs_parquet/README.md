# E6-1：TsFile vs Parquet 端到端读取

## 状态：TODO

**产出**：T6-1（列数 × 选择率 × 格式 × 吞吐矩阵）

## 方法

Python 层对比：`tsfile.to_dataframe()` vs `pyarrow.parquet.read_table()`。

## 程序

**扩展现有 `../../read_perf/read_benchmark.cpp`**，增加 `--cols` 参数支持 4/8/16 FIELD 列宽。改动最小：在现有 `write_tsfile()` / `write_parquet()` 函数中按列数动态构建 schema。

## 编译配置

**C4**（多线程+SIMD 生产配置）

## 实验参数矩阵

| 变量 | 取值 |
|------|------|
| FIELD 列数（W1） | 4 / 8 / 16 |
| 时间过滤选择率（W3） | 10% / 50% / 100% |
| 格式 | TsFile / Parquet |

固定：W0 基线（10 设备，200M 行，SNAPPY）。

## 数据文件管理

不同列数的 TsFile 和 Parquet 是不同文件，需分别生成：

```
bench_4cols.tsfile / bench_4cols.parquet
bench_8cols.tsfile / bench_8cols.parquet
bench_16cols.tsfile / bench_16cols.parquet
```

## 输出

- `vs_parquet_results.csv`：`cols, selectivity_pct, engine, throughput_mrows_s`
- `plot_vs_parquet.py`：分组图展示对比

## 预期结论

- 设备定位（tag filter）和时间过滤场景：TsFile 优势显著（索引 vs 线性扫描）
- 全量扫描（100% 选择率）：Parquet 可能略优（Arrow 批处理成熟，列存优化好）
