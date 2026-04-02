# E4-1 / E4-2：写入与读取并行加速比

## 状态：TODO

**产出**：F4-1（写入加速比曲线）、F4-2（读取加速比曲线）、T4-1~T4-3（写入吞吐矩阵）、T4-4~T4-6（读取吞吐矩阵）

## 程序：`throughput_bench`

E4-1（写入）和 E4-2（读取）共用同一个二进制，通过 `--mode write|read` 切换。

### 编译配置

| 配置 | 用途 |
|------|------|
| C1 | 串行+标量基线（threads=1 等效） |
| C2 | 仅多线程（关闭 SIMD，隔离变量） |
| C4 | 多线程+SIMD（作为参考，不用于主加速比计算） |

加速比计算：**以 C2 配置下 threads=1 为基线**，对比 C2 threads=2/4/8。

### 实验参数矩阵（每种压缩算法一张表）

| 变量 | 取值 |
|------|------|
| FIELD 列数（W1） | 4 / 8 / 16 |
| 线程数（W6） | 1 / 2 / 4 / 8 |
| 压缩算法（W2） | LZ4 / SNAPPY / ZSTD |

固定：200M 行，10 设备，TS_2DIFF/GORILLA 编码。

### run.sh 流程

```bash
# 编译 C1/C2/C4
build_config C1 && build_config C2 && build_config C4

# 写入阶段（E4-1）：用 C2 跑不同 threads/cols/compression 组合
for cols in 4 8 16; do
  for threads in 1 2 4 8; do
    for comp in lz4 snappy zstd; do
      C2/throughput_bench --mode write --cols $cols --threads $threads \
                          --compression $comp --out write_${cols}c_${threads}t_${comp}.csv
    done
  done
done

# 读取阶段（E4-2）：先用 C4 写好数据文件，再用 C2 读
C4/throughput_bench --mode write --cols 16 --compression zstd --out bench_data.tsfile
for cols in 4 8 16; do
  for threads in 1 2 4 8; do
    for comp in lz4 snappy zstd; do
      C2/throughput_bench --mode read --cols $cols --threads $threads \
                          --compression $comp --file bench_data_${comp}.tsfile \
                          --out read_${cols}c_${threads}t_${comp}.csv
    done
  done
done
```

### 输出

- 6 个 CSV（写入 × 3 压缩 + 读取 × 3 压缩），每个对应一张吞吐矩阵表
- `plot_speedup.py`：加速比折线图，实测（实线）vs Amdahl 理论（虚线），按压缩算法分子图

### 预期结论

- **写入（E4-1）**：ZSTD 加速比显著高于 LZ4（ZSTD 计算密集，并行收益大）；列数越多加速比越高
- **读取（E4-2）**：Phase 1 串行 I/O 限制了上界；SSD 上 ZSTD 解压场景加速最显著

### E4-3 说明

E4-3（α 实测）**无独立程序**，由 `../E4_3_alpha/calc_alpha.py` 读取本实验 CSV 反推 α 值。
