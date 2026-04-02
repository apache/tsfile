# E5-1：编解码吞吐 SIMD ON/OFF

## 状态：TODO

**产出**：T5-1（编码吞吐）、T5-2（解码吞吐）

## 程序：`codec_bench`

直接对 ChunkWriter（编码）/ ChunkReader（解码）计时，绕过完整读写路径，精确测量 SIMD 加速比，排除 I/O 干扰。单线程运行（C1 vs C3）。

## 编译配置

| 配置 | 用途 |
|------|------|
| C1 | 标量基线（SIMD OFF） |
| C3 | 仅 SIMD ON（无多线程） |

## 实验参数矩阵

| 变量 | 取值 |
|------|------|
| 操作 | 编码（T5-1）/ 解码（T5-2） |
| 数据类型 | INT32 / INT64 / FLOAT / DOUBLE |
| SIMD | ON / OFF（编译配置控制） |

固定：W0 基线规模，SNAPPY 压缩（减少压缩耗时干扰编解码测量）。

## 输出

- `encode_results.csv`：`dtype, simd, throughput_mrows_s, speedup`
- `decode_results.csv`：`dtype, simd, throughput_mrows_s, speedup`

## 预期结论

- 编码加速比受 rebase 在编码总耗时中占比影响（当前仅 rebase 步骤已 SIMD 化）
- 解码加速比约 1.3–2×，INT64 比 INT32 更显著（位宽更宽，SIMD 批处理收益更大）
- FLOAT/DOUBLE（GORILLA 编码）加速比取决于 SIMD 化程度

## 与 E5-3 的关系

E5-3（跨平台对比）复用本程序，在不同硬件平台上运行相同二进制，产出 T5-4。
