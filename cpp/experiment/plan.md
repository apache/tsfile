# 实验落地计划

本文档是 `experiments-todo.md` 的落地版本，描述文件组织、程序划分和执行流程。
状态标记与 experiments-todo.md 保持同步：✅ 已有数据，TODO 待实验。

---

## 一、文件夹结构

```
experiment/
├── plan.md                       ← 本文档
├── experiments-todo.md           ← 实验需求原始清单（权威来源）
├── design.md                     ← ConstrainedTsFileWriter 设计文档
│
├── common/                       ← 跨实验共用的数据生成逻辑
│
├── chap03/                       ← 第三章：内存建模
│   ├── E3_1_write_model/         ← E3-1a（无 flush 动机实验）；E3-1b 已有
│   ├── E3_2_write_precision/     ← E3-2（已有，write_memory/ 产出）
│   ├── E3_3_read_memory/         ← E3-3（TODO：读取内存模型验证）
│   └── E3_4_write_budget/        ← E3-4（TODO：不同内存预算写入性能）
│
├── chap04/                       ← 第四章：多线程并行化
│   ├── E4_12_throughput/         ← E4-1 写入 + E4-2 读取加速比（共用程序）
│   ├── E4_3_alpha/               ← E4-3 α 实测（后处理脚本）
│   └── E4_4_combined/            ← E4-4 多线程+SIMD 叠加
│
├── chap05/                       ← 第五章：SIMD 向量化
│   ├── E5_1_codec/               ← E5-1 编解码吞吐 SIMD ON/OFF
│   ├── E5_2_filter_latmat/       ← E5-2 时间过滤+延迟物化（合并）
│   └── E5_3_platform/            ← E5-3 跨平台对比
│
├── chap06/                       ← 第六章：AI 负载接口
│   ├── E6_1_vs_parquet/          ← E6-1 TsFile vs Parquet
│   ├── E6_2_opt_stack/           ← E6-2 优化透传（Python）
│   └── E6_3_stream_mem/          ← E6-3 流式迭代内存控制
│
├── write_memory/                 ← 已有实验（E3-1b/E3-2 产出来源）
└── read_perf/                    ← 已有实验（E6-1 read_benchmark 基础）
```

---

## 二、已完成产出（write_memory/ 已有）

| 编号 | 类型 | 描述 |
|------|------|------|
| F3-1 | 图 | 顺序写入内存曲线（有 flush，锯齿形） |
| F3-2 | 图 | 混合写入内存曲线（有 flush） |
| T3-1 | 表 | 各模块内存分布 |
| T3-2 | 表 | 顺序写入 flush 事件 |
| T3-3 | 表 | 混合写入 flush 事件 |
| T3-4 | 表 | 两种写入模式对比 |
| T3-5 | 表 | 写入内存模型精度（batch_size 5K/6K/8K/10K） |

---

## 三、TODO 产出与程序对应

### 需要新开发的程序

| 程序名 | 所在目录 | 涉及产出 | 编译配置 |
|--------|---------|---------|---------|
| `no_flush_bench` | `chap03/E3_1_write_model/` | F3-0 | C5 |
| `read_mem_model` | `chap03/E3_3_read_memory/` | F3-3, T3-6 | C5 |
| `write_budget` | `chap03/E3_4_write_budget/` | F3-4, T3-7 | C4 |
| `throughput_bench` | `chap04/E4_12_throughput/` | F4-1/F4-2, T4-1~T4-6 | C1/C2/C4 |
| `codec_bench` | `chap05/E5_1_codec/` | T5-1, T5-2 | C1/C3 |
| `filter_bench` | `chap05/E5_2_filter_latmat/` | F5-1, F5-2, T5-3 | C1/C3 |
| `stream_mem_bench` | `chap06/E6_3_stream_mem/` | F6-1 | C4 |

### 扩展现有程序

| 程序 | 改动 | 涉及产出 |
|------|------|---------|
| `read_perf/read_benchmark.cpp` | 增加 `--cols` 参数，支持 4/8/16 FIELD 列宽 | T6-1 |

### Python 脚本

| 脚本名 | 所在目录 | 涉及产出 | 说明 |
|--------|---------|---------|------|
| `calc_alpha.py` | `chap04/E4_3_alpha/` | T4-7 | 从 E4-1/E4-2 CSV 反推 α |
| `bench_api.py` | `chap06/E6_2_opt_stack/` | T6-2 | Python `to_dataframe()` 端到端 |

---

## 四、执行顺序（按 experiments-todo.md §七）

### 阶段一：准备
1. fit39 上构建 C1–C5 共 5 个编译目录
2. 用 C5 生成 W0 基线数据（200M 行 → TsFile）
3. 同一份数据转存 Parquet（第六章用）
4. `pip3 install pyarrow pandas`

### 阶段二：按章跑（第三章 → 第五章 → 第四章 → 第六章）

```
第三章
  E3-1a  no_flush_bench（C5）        → F3-0（动机对照）
  E3-3   read_mem_model（C5）        → F3-3, T3-6
  E3-4   write_budget（C4）          → F3-4, T3-7

第五章
  E5-1   codec_bench（C1 vs C3）     → T5-1, T5-2
  E5-2   filter_bench（C1 vs C3）    → F5-1, F5-2, T5-3
  E5-3   filter_bench/codec_bench    → T5-4（跨平台，同程序不同硬件）

第四章
  E4-1/2 throughput_bench（C1/C2/C4）→ F4-1, F4-2, T4-1~T4-6
  E4-3   calc_alpha.py               → T4-7（无需新实验，复用 E4-1/4-2 CSV）
  E4-4   throughput_bench（C1/C2/C3/C4）→ T4-8

第六章
  E6-1   read_benchmark（C4）        → T6-1
  E6-2   bench_api.py（C1/C2/C3/C4）→ T6-2
  E6-3   stream_mem_bench（C4）      → F6-1
```

### 阶段三：填表
将实验 CSV 数据填入各章 LaTeX 表格和图。

---

## 五、全量 TODO 产出清单

| 产出 | 类型 | 章 | 程序 |
|------|------|----|------|
| F3-0 | 图 | 3 | no_flush_bench |
| F3-3 | 图 | 3 | read_mem_model |
| F3-4 | 图 | 3 | write_budget |
| T3-6 | 表 | 3 | read_mem_model |
| T3-7 | 表 | 3 | write_budget |
| F4-1 | 图 | 4 | throughput_bench |
| F4-2 | 图 | 4 | throughput_bench |
| T4-1~T4-3 | 表×3 | 4 | throughput_bench（写入，3 种压缩） |
| T4-4~T4-6 | 表×3 | 4 | throughput_bench（读取，3 种压缩） |
| T4-7 | 表 | 4 | calc_alpha.py |
| T4-8 | 表 | 4 | throughput_bench（4 组配置） |
| T5-1 | 表 | 5 | codec_bench |
| T5-2 | 表 | 5 | codec_bench |
| F5-1 | 图 | 5 | filter_bench |
| F5-2 | 图 | 5 | filter_bench |
| T5-3 | 表 | 5 | filter_bench |
| T5-4 | 表 | 5 | codec_bench + filter_bench |
| T6-1 | 表 | 6 | read_benchmark（扩展） |
| T6-2 | 表 | 6 | bench_api.py |
| F6-1 | 图 | 6 | stream_mem_bench |

**合计 TODO：8 图 + 16 表**（与 experiments-todo.md §八一致）

---

## 六、注意事项

1. **E3-4 依赖 ConstrainedTsFileWriter 完成**：需确认 `get_data_size()` / `get_meta_size()` 接口已合并到 final 分支。

2. **E4-1/4-2 数据文件**：不同压缩算法（LZ4/ZSTD）需要重新生成对应的 TsFile，因为压缩算法在写入时确定。

3. **E6-1 Parquet 宽度**：列数变化时（4/8/16）需重新生成对应宽度的 Parquet 文件。

4. **E5-3 跨平台**：fit39 只有 x86 AVX2；若无 ARM 设备则降级为 AVX2 vs 标量，在论文中注明。

5. **E6-3 10GB 文件**：需挂载 HDD（`/dev/sda`），提前写好数据。

6. **每组实验至少跑 3 次**，CSV 保留每次原始数据，取中位数报告。
