# E4-4：多线程 + SIMD 叠加效果

## 状态：TODO

**产出**：T4-8（四组配置读写吞吐 + 加速比 + 各自乘积 vs 实际叠加）

## 程序

复用 `../E4_12_throughput/throughput_bench`，用四种编译配置各跑一次。

## 实验设计

| 组 | 配置 | 含义 |
|----|------|------|
| A  | C1   | 串行 + 标量（基线） |
| B  | C2   | 并行 + 标量 |
| C  | C3   | 串行 + SIMD |
| D  | C4   | 并行 + SIMD |

固定：W0 基线，8 FIELD 列，4 线程，ZSTD 压缩。读写各测一次。

## run.sh

```bash
for cfg in C1 C2 C3 C4; do
    ${cfg}/throughput_bench --cols 8 --threads 4 --compression zstd \
                            --mode write --out write_${cfg}.csv
    ${cfg}/throughput_bench --cols 8 --threads 4 --compression zstd \
                            --mode read  --out read_${cfg}.csv
done
```

## 输出

- `combined_results.csv`：`config, mode, throughput_mrows_s, speedup_vs_C1`
- 后处理计算列：
  - `speedup_threads_only` = B/A
  - `speedup_simd_only` = C/A
  - `speedup_combined` = D/A
  - `product` = (B/A) × (C/A)
  - `gap` = product − speedup_combined

## 预期结论

叠加加速比 < 各自加速比乘积：SIMD 降低了每线程的计算时间 $T_p$，使串行占比 α 相对上升，多线程对 SIMD 化代码的加速收益下降。
