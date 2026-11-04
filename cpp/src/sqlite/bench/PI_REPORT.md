<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# SQLite + TsFile 树莓派写入/seal 性能报告

日期：2026-11-04  
数据：2026-11-03 在 Raspberry Pi 4B 上单进程运行  
被测对象：`experiment-sqlite` 分支的 SQLite loadable extension `tsfile_hybrid`  
原始数据：[results/pi-20261103/](results/pi-20261103/)  
图表脚本：[plot_pi_results.py](plot_pi_results.py)

## 1. 摘要

本报告在树莓派上验证 SQLite + TsFile 混合表在固定写入规模下的写入吞吐、seal 开销和内存行为。每个配置写入 **2,000,000 行**，覆盖 100/500/2,000 台设备与 5/15/30 个测点，共 9 个主配置；另外对 D500-M15 做 20k/50k/100k 行 seal 阈值扫描，对 D2000-M30 做 30k 行 seal 补充。

最重要的结论是：**写入吞吐由测点数决定，内存上限由 seal 触发量和行宽决定。** 测点越多，行/秒越低，但点/秒越高；50k 主矩阵中 M30 的纯写点吞吐为 10.4 万–11.7 万点/秒，代价是 390–541MB 的峰值内存；D2000-M30 改用 30k seal 后纯写点吞吐达到 14.4 万点/秒。把 D2000-M30 的 seal 从 50k 降到 30k，峰值从 541MB 降到 315MB，有效点吞吐基本不变，但 seal 总时间从 246s 增加到 332s。

| 指标 | 结果 |
| --- | --- |
| 固定写入规模 | 2,000,000 行/组；M5/M15/M30 分别为 1,000 万/3,000 万/6,000 万点 |
| 主配置数 | 9 组（100/500/2,000 设备 × 5/15/30 测点） |
| 纯写点吞吐（主矩阵） | 28,600–116,581 点/秒；D2000-M30 30k 补充为 143,692 |
| 有效点吞吐（主矩阵） | 24,457–79,795 点/秒 |
| seal 导致的吞吐损失 | 14.5%–35.6% |
| 峰值 RSS | 77–541MB |
| M30 + 2,000 设备 | 50k seal 541MB；30k seal 315MB |

## 2. 测试环境与方法

### 2.1 环境

| 项目 | 配置 |
| --- | --- |
| 机器 | Raspberry Pi 4 Model B Rev 1.5，8GB |
| CPU | Cortex-A72 ×4 @1.8GHz，aarch64 |
| 存储 | microSD 59.5GB，根分区可用约 38GB |
| 系统 | Ubuntu 24.04.4 LTS，内核 6.8.0-1064-raspi |
| Python | 3.12.3 |
| SQLite | 3.45.1，`enable_load_extension` 可用 |
| 扩展 | `/root/tsfile-src/cpp/build/pi/lib/tsfile_sqlite.so` |
| 扩展 SHA256 | `57145b883bc37b3ae75aadc1198b1e6b68e059e4c8f64943c71873fba79a509` |
| 温度/节流 | 测试前 39.9°C；`get_throttled=0x50000`（发生过欠压/节流，当前位为 0） |

### 2.2 负载与配置

每个配置使用一张独立的虚拟表：

```sql
CREATE VIRTUAL TABLE sensor USING tsfile_hybrid(
  time TIMESTAMP TIME,
  device STRING TAG,
  m00000 DOUBLE FIELD,
  ...
  m<N-1> DOUBLE FIELD,
  directory='<workdir>/segments',
  timestamp_precision='ms'
);
```

- 设备数：100 / 500 / 2,000；每设备测点数：5 / 15 / 30。
- 每配置固定写 2,000,000 行；每行包含该设备全部测点。
- 按设备轮转写入，时间戳每设备严格递增；每 1,000 行一个事务，按批尽快写入。
- 不模拟 1Hz 节拍、不模拟多客户端、不执行查询压测。
- 主矩阵 seal 触发量：50,000 行；组末 seal 剩余热行。
- seal 扫描：固定 D500-M15，触发量 20,000 / 50,000 / 100,000 行。
- 补充：D2000-M30 使用 30,000 行 seal。

所有主配置和补充配置的 `status` 均为 `ok`；seal 后热区归零，单设备行数与抽样值校验通过。另有 D100-M5、200,000 行的冒烟运行，用于验证扩展加载、建表、写入、seal 和校验链路。

### 2.3 指标定义

- **纯写**：只统计 INSERT 批次的时间，不包含 seal。
- **有效**：总墙钟时间，包含 seal 停顿；`有效行/秒 = 写入行数 / 总墙钟时间`。
- **峰值 RSS**：测试进程每 250ms 读取 `/proc/self/status` 的 `VmRSS`，取最大值；包括 Python、SQLite 和扩展。
- **seal 峰值**：每次 seal 前后单独记录的 `rss_peak`，用于区分首次 seal 和稳定状态。
- **TsFile 大小**：该配置所有已封存段文件的合计大小；不是单个文件。

本报告的 MB/MiB 均按 1024 换算；RSS 的 MB 与文件大小的 MiB 是同一口径。

## 3. 结果

### 3.1 写入吞吐

![图 1 设备数与测点数对纯写吞吐的影响](figures/fig1_throughput.png)

*图 1：纯写吞吐（不含 seal）。左：行/秒；右：点/秒。每条线是一个测点档位，横轴是设备数。*

图 1 显示两个方向相反的趋势：

- **行/秒随测点数下降**：M5 为 5,720–7,246 行/秒，M15 为 4,765–5,607，M30 为 3,477–3,886。行越宽，单行写入成本越高。
- **点/秒随测点数上升**：M5 为 28,600–36,231 点/秒，M15 为 71,479–84,105，M30 为 104,310–116,581。宽行虽然单行更慢，但每行包含更多测点，总点吞吐更高。

设备数的影响不是单调的：在 M5 三档中，D2000-M5 的纯写行/秒最高（7,246），D500-M5 反而最低（5,720）。这一差异可能来自批大小、SD 卡状态和运行顺序，不能单独归因于设备数。

![图 2 seal 对有效写入速率的影响](figures/fig2_seal_overhead.png)

*图 2：每个配置的纯写与有效行/秒。浅色为有效（含 seal），深色为纯写；右侧数字是 seal 导致的下降比例。*

seal 会暂停写入，因此有效速率低于纯写速率。下降幅度从 **14.5%**（D100-M5、D500-M5）到 **35.6%**（D2000-M30）：

- M5：14.5%–22.6%；
- M15：23.4%–30.7%；
- M30：26.5%–35.6%。

下降随测点数增加，因为同样 50k 行，M30 需要编码的 cell 数是 M5 的 4 倍多；设备数增加到 2,000 时，M30 的下降进一步扩大。

| 配置 | 纯写行/秒 | 纯写点/秒 | 有效行/秒 | 有效点/秒 | 峰值 RSS | seal 总时间 | TsFile |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| D100-M5 | 5,942 | 29,708 | 5,081 | 25,406 | 77MB | 46.1s | 71.5MiB |
| D100-M15 | 5,076 | 76,138 | 3,860 | 57,904 | 152MB | 103.2s | 213.0MiB |
| D100-M30 | 3,617 | 108,504 | 2,660 | 79,795 | 390MB | 160.3s | 425.3MiB |
| D500-M5 | 5,720 | 28,600 | 4,891 | 24,457 | 82MB | 48.1s | 77.1MiB |
| D500-M15 | 4,765 | 71,479 | 3,652 | 54,780 | 164MB | 107.1s | 226.5MiB |
| D500-M30 | 3,477 | 104,310 | 2,529 | 75,868 | 415MB | 176.3s | 450.5MiB |
| D2000-M5 | 7,246 | 36,231 | 5,606 | 28,031 | 109MB | 69.9s | 114.6MiB |
| D2000-M15 | 5,607 | 84,105 | 3,883 | 58,250 | 233MB | 138.0s | 324.5MiB |
| D2000-M30 | 3,886 | 116,581 | 2,502 | 75,062 | 541MB | 246.3s | 639.4MiB |

### 3.2 内存

![图 3 不同配置的峰值 RSS](figures/fig3_peak_rss.png)

*图 3：50k seal 下各配置的进程峰值 RSS；红色虚线是 500MB 目标。*

峰值内存主要由“seal 行数 × 行宽”决定，并叠加设备数开销：

| 测点数 | D100 | D500 | D2000 |
| --- | ---: | ---: | ---: |
| M5 | 77MB | 82MB | 109MB |
| M15 | 152MB | 164MB | 233MB |
| M30 | 390MB | 415MB | **541MB** |

M5/M15 在 50k seal 下有充足余量；M30 在 100/500 设备时接近但未超过 500MB，在 2,000 设备时超过目标。D2000-M30 改用 30k seal 后峰值为 **315MB**。

![图 4 内存随时间和 seal 次序的变化](figures/fig4_rss_timeline.png)

*图 4：左：D100-M30 前 40 秒的 RSS 轨迹；右：每个配置每次 seal 的峰值 RSS。*

内存不是随累计写入行数线性增长，而是台阶式的：

- 写入阶段 RSS 只缓慢上升：D100-M30 从 14MB 到首次 seal 前 36MB。
- 第一次 seal 把 RSS 抬到 256MB；seal 结束后没有回落到 36MB，而是停在约 253MB。
- 第二次 seal 在已保留的内存基础上继续升到 386MB；之后 38 次 seal 基本维持在 387–390MB。
- 最终的进程峰值是 390MB，但首个 seal 只有 256MB。**做容量评估应看进入平台后的峰值，而不是第一次 seal。**

D500-M15 的 100k seal 也有同样现象：首轮 286MB、第二轮 413MB、之后 415–420MB。首轮峰值会明显低估稳定值。

### 3.3 seal 触发量扫描

![图 5 D500-M15 的 seal 阈值扫描](figures/fig5_seal_threshold.png)

*图 5：固定 D500-M15、2,000,000 行，比较 20k/50k/100k 行 seal 的峰值 RSS、有效行/秒和 seal 总时间。*

| seal 行数 | seal 次数 | 纯写行/秒 | 有效行/秒 | 峰值 RSS | seal 总时间 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 20,000 | 100 | 6,444 | 4,419 | 90MB | 121.8s |
| 50,000 | 40 | 4,787 | 3,657 | 165MB | 108.5s |
| 100,000 | 20 | 2,987 | 2,549 | 420MB | 92.8s |

seal 越频繁，峰值内存越低，但固定开销越高：

- 20k seal 峰值只有 90MB，但需要 100 次 seal，总时间 121.8s，有效行/秒降到 4,419。
- 100k seal 只需 20 次，总时间 92.8s，但峰值升到 420MB，有效行/秒降到 2,549。
- 50k 是中间点：165MB、40 次、108.5s。

需要注意，纯写行/秒也随 seal 阈值变化（6,444 → 4,787 → 2,987），这不是 seal 本身造成的，可能包含运行顺序、SD 卡状态和节流影响；因此不要把三组的纯写差异全部归因于 seal 大小。有效行/秒的下降同时包含 seal 停顿和这些环境因素。

### 3.4 D2000-M30：30k 与 50k 的权衡

![图 6 D2000-M30 的 30k/50k seal 对比](figures/fig6_d2000_tradeoff.png)

*图 6：D2000-M30 在 30k 和 50k seal 下的峰值 RSS、seal 总时间和有效点/秒。*

| seal 行数 | seal 次数 | 纯写点/秒 | 有效点/秒 | 峰值 RSS | seal 总时间 | TsFile |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 30,000 | 67 | 143,692 | 76,192 | **315MB** | 331.9s | 801.1MiB |
| 50,000 | 40 | 116,581 | 75,062 | **541MB** | 246.3s | 639.4MiB |

把 seal 从 50k 降到 30k：

- 峰值 RSS 从 541MB 降到 315MB，满足 <500MB 目标；
- 有效点/秒从 75,062 到 76,192，基本持平（约 +1.5%，在单次运行波动范围内）；
- 代价是 seal 次数从 40 增加到 67，seal 总时间从 246s 增加到 332s（+35%）；
- TsFile 合计从 639.4MiB 增加到 801.1MiB（+25%）；段文件数从 40 增到 67，更多分段会带来额外的元数据和编码开销。

### 3.5 存储规模

每个配置写 2,000,000 行后，TsFile 段文件合计大小如下（MiB）：

| 测点数 | D100 | D500 | D2000 |
| --- | ---: | ---: | ---: |
| M5 | 71.5 | 77.1 | 114.6 |
| M15 | 213.0 | 226.5 | 324.5 |
| M30 | 425.3 | 450.5 | 639.4 |

SQLite 热表本身很小：DB 为 5.5–16.6MiB，WAL 为 5.5–19.6MiB，两者合计 10.9–36.1MiB。写入 200 万行后，绝大部分数据已经在 TsFile 冷段中，热区在组末 seal 后归零。

## 4. 讨论

### 4.1 为什么行/秒下降、点/秒上升

每行包含 `2 + N` 个值（时间、设备 TAG、N 个 FIELD）。N 增大时：

- 单行的值数量和编码工作量增加，所以行/秒下降；
- 每行携带的测点数增加，所以点/秒上升。

在 50k 主矩阵中，M30 的纯写点吞吐约为 M5 的 3.2–3.9 倍，而行吞吐只有 M5 的 0.54–0.61 倍。这个权衡适合“设备数有限、每设备测点多”的场景；如果设备数很多而每设备测点少，行吞吐更重要。

### 4.2 内存要看稳定 seal 峰值

RSS 曲线是台阶式的：写入阶段增长缓慢，seal 时跳升，首轮 seal 后分配器保留内存，第二轮可能继续抬升，之后进入平台。D100-M30 的首轮 seal 峰值 256MB，稳定值 390MB；D500-M15 的 100k seal 首轮 286MB，稳定值 420MB。只看第一次 seal 会低估 30%–50%。

因此容量规划应以“进入平台后的 seal 峰值”为准，并留出设备数增加带来的元数据余量。D2000-M30 的 541MB 就是一个例子：同为 M30，100 设备时 390MB，2,000 设备时 541MB。

### 4.3 seal 频率的取舍

seal 频率同时影响三件事：

- **内存峰值**：seal 行数越少，峰值越低；
- **有效吞吐**：seal 越频繁，固定开销占比越高；
- **文件数量与存储开销**：seal 越频繁，段文件越多，TsFile 总大小越大（D2000-M30 30k 比 50k 多 25%）。

没有一组参数对所有场景都最优。对内存受限的设备，优先保证峰值；对吞吐和存储更敏感的场景，可以适当增大 seal 触发量。

## 5. 建议

以 **<500MB 峰值内存** 为目标：

- 100/500 设备、M5/M15/M30：50k seal 均可，M30 峰值 390/415MB，余量较小但未超限。
- 2,000 设备、M5/M15：50k seal，峰值 109/233MB，余量充足。
- 2,000 设备、M30：使用 **30k seal**，峰值 315MB；如果更看重吞吐和存储，可接受 541MB 时再用 50k。
- 内存非常紧张时可用 20k seal，但要知道它会带来约 100 次 seal 和更高的固定开销；D500-M15 的 20k 峰值为 90MB。

容量评估时使用“平台峰值”，不要用首轮 seal 峰值；同时把 Python/SQLite/扩展的基线内存（本测试约 14–36MB）计入总量。

## 6. 局限

- 每组只运行一次，绝对值受 SD 卡状态、温度和运行顺序影响；应看趋势和数量级，不宜当作精确上限。
- 测试前 `get_throttled=0x50000` 表明 Pi 发生过欠压/节流；当前位为 0，但历史节流可能影响部分结果。
- RSS 每 250ms 采样一次，瞬时峰值可能略高于记录值。
- 吞吐包含 Python 驱动、SQLite 热表和 SD 卡成本，不是扩展的纯 C++ 上限。
- 只测单连接、同步写入；没有并发客户端、查询压测或 latest/range 查询负载。
- TsFile 大小是每个配置所有段文件的合计，不是单个文件；不同 seal 阈值会产生不同文件数，比较时需注意。

## 7. 复现与数据

- 原始 JSON（每组含 `rss_samples`、逐次 seal 记录、批次延迟、文件大小和校验结果）：[results/pi-20261103/](results/pi-20261103/)
- 汇总表：[PI_RESULTS.md](PI_RESULTS.md)、[PI_RESULTS.csv](PI_RESULTS.csv)
- 测试脚本：[pi_write_seal_bench.py](pi_write_seal_bench.py)、[run_pi_matrix.sh](run_pi_matrix.sh)
- 图表脚本：[plot_pi_results.py](plot_pi_results.py)
- 图表矢量版：`figures/*.svg`

在树莓派上重新生成图表：

```bash
python3 cpp/src/sqlite/bench/plot_pi_results.py
```
