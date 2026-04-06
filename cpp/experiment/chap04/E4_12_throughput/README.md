# E4-1/E4-2：写入/读取并行吞吐量

## 状态：TODO

**产出**：T4-1（写入加速比）、T4-2（读取加速比）

## 程序：`throughput_bench`

对 `TsFileTableWriter::write_table()`（写）和 `TsFileReader::query()`（读）直接计时，
测量列级并行化在不同配置下的吞吐量与加速比。
编码：INT64 + TS\_2DIFF；压缩：SNAPPY / LZ4。

## 编译配置

| 配置 | 用途 |
|------|------|
| C1 | 串行基线（ENABLE\_THREADS=OFF） |
| C4 | 仅多线程（ENABLE\_THREADS=ON） |

C3（SIMD）和 C5（SIMD+threads）的叠加效果在 E4-4 中测量。

## 实验参数矩阵

| 变量 | 取值 |
|------|------|
| FIELD 列数 | 4 / 8 / 16 |
| 线程数 | 1（串行）/ 2 / 4 / 8 |
| 压缩算法 | SNAPPY / LZ4 |

固定：总行数 5M，batch\_size=65536，单设备（TAG dev=d0）。

## 加速比计算（后处理）

由 CSV 数据可直接计算加速比：
```
speedup(p) = T(1) / T(p)
```

从加速比拟合 Amdahl 串行占比 α：
```
α ≈ (T(p)/T(1) - 1/p) / (1 - 1/p)
```

## 输出

- `write_results_{ON|OFF}.csv`：`cols, threads, parallel, compression, throughput_mrows_s, time_s`
- `read_results_{ON|OFF}.csv`：同上

## 预期结论

- LZ4 压缩：α 较大（I/O 和编解码开销相当），加速比有限（约 1.5–2×）
- SNAPPY 压缩：α 居中，加速比适中
- 列数 ≥ 8、线程数 ≥ 4 时，加速比接近理论上界 1/α
- 实际加速比略低于 Amdahl 理论值（线程调度 + CPU 缓存竞争）

## 与其他实验的关系

- E4-3（alpha）利用本实验的 T(1) 和 T(p) 数据拟合 α
- E4-4（combined）在此基础上叠加 SIMD（C5 vs C4 vs C3 vs C1）
