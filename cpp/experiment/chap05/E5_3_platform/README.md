# E5-3：跨平台性能对比

## 状态：TODO

**产出**：T5-4（AVX2 / 标量 × 操作 × 吞吐 / 加速比）

## 程序

复用 `../E5_1_codec/codec_bench` 和 `../E5_2_filter_latmat/filter_bench`，不需要新程序。

## 平台情况

| 平台 | 设备 | 编译配置 | 状态 |
|------|------|---------|------|
| x86 + AVX2 | fit39 (i7-10700) | C3 | 可用 |
| x86 + 标量 | fit39（关闭 AVX2 编译选项） | C1 | 可用 |
| ARM + NEON | Mac (Apple Silicon) 或开发板 | 另行编译 | 待定 |

**若无 ARM 设备**：仅做 AVX2 vs 标量对比，在论文 T5-4 脚注中注明。

## 测量项目（fit39 上）

| 操作 | 程序 | 参数 |
|------|------|------|
| 编码吞吐（INT32/INT64） | codec_bench | --mode encode |
| 解码吞吐（INT32/INT64） | codec_bench | --mode decode |
| 时间过滤吞吐（50% 选择率） | filter_bench | --selectivity 0.5 |

## 输出

- `platform_results.csv`：`platform, operation, dtype, throughput_mrows_s, speedup_vs_scalar`
- 论文中为汇总表，无需绘图
