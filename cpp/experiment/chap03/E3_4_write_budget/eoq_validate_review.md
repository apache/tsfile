# `eoq_validate.cpp` 审查：混合写入下的最优刷盘次数

## 结论

你研究的场景应当是**混合写入**：

```text
一个 Tablet / 一个 flush 周期内包含多个 device；
writer.flush() 被调用时，会把当前内存中所有活跃 device 的 chunk group 一起写下去。
```

因此，不应该把实验改成“一个 device 写完再 flush”。那会变成另一种写入模式，和你的目标不一致。

在混合写入场景下，EOQ 模型仍然成立，但元数据项应该按：

```text
flush 次数 × 每次 flush 涉及的活跃 device 数 × 单个 device chunk group 元数据开销
```

来估算，而不是简单理解为“每个 device 独立 flush”。

当前 `eoq_validate.cpp` 的方向是对的：它用全局 `rows_since_flush` 控制 flush 周期，符合“混合写入，全局刷盘”的思想。

但代码里仍有几个需要修正或澄清的地方：

- 当前数据生成其实是按 device 顺序写入，不是真正的 interleaved multi-device Tablet；
- 公式中的 `D` 应解释为每次 flush 的活跃 device 数，而不是总 device 数在任何情况下都直接参与；
- 当前 `peak_m_total` 没有在 flush 后更新，可能漏掉元数据峰值；
- `M_init` 不应使用手写默认值，应从 `ModStat` 初始化增量中测得，并同时加入实测值和公式值；
- `K` 需要明确表示 flush 次数，元数据增长应写成 `K * D_active * b`；
- 文件头部注释中的 schema 常量已经过期。

## 混合写入下的模型

混合写入中，flush 周期 `F` 表示：

```text
全局累计写入 F 行后调用一次 writer.flush()
```

如果在一个 flush 周期内有 $D_{\text{active}}$ 个 device 产生了数据，那么这次 flush 会写出约 $D_{\text{active}}$ 个 chunk group。

总行数为：

$$
N_{\text{rows}} = R \cdot D
$$

其中：

- $R$：每个 device 的总行数；
- $D$：总 device 数。

全局 flush 次数近似为：

$$
K(F) \approx \left\lceil \frac{RD}{F} \right\rceil
$$

如果每个 flush 周期内都覆盖全部 device，即 $D_{\text{active}} \approx D$，则元数据项为：

$$
M_{\text{meta}}(F)
\approx
K(F) \cdot D \cdot b
$$

于是总内存可写为：

$$
M(F)
\approx
M_{\text{init}}
+ sF
+ \left\lceil \frac{RD}{F} \right\rceil D b
$$

忽略取整项时：

$$
M(F)
\approx
M_{\text{init}}
+ sF
+ \frac{RD}{F}Db
$$

对应最优 flush 周期：

$$
F_{\text{opt}}
=
\sqrt{\frac{R D^2 b}{s}}
$$

如果定义 $R_{\text{total}} = RD$，也可以写成：

$$
F_{\text{opt}}
=
\sqrt{\frac{R_{\text{total}} D b}{s}}
$$

这个形式更适合混合写入，因为 `F` 是全局行数，而不是单个 device 的行数。

## 与“每 device 独立 flush”模型的区别

如果是每个 device 独立累计 `F` 行再 flush，则模型是：

$$
M(F)
\approx
M_{\text{init}}
+ sF
+ D \left\lceil \frac{R}{F} \right\rceil b
$$

但这不是你现在要研究的场景。

你的场景是：

```text
多个 device 混合写入；
全局累计到 F 行；
一次 flush 写出所有活跃 device 的 chunk group。
```

因此更合理的是：

$$
M(F)
\approx
M_{\text{init}}
+ sF
+ \left\lceil \frac{RD}{F} \right\rceil D_{\text{active}} b
$$

当每个 flush 周期都覆盖全部 device 时：

$$
D_{\text{active}} = D
$$

## 当前代码与混合写入模型的偏差

### 1. 当前数据生成不是严格混合写入

当前代码的写入顺序是：

```cpp
for (int dev = 0; dev < kNumDevices; dev++) {
    for (int64_t off = 0; off < rows_per_dev;) {
        ...
        writer.write_table(tablet);
    }
}
```

这意味着数据是按 device 顺序写入的：

```text
dev_0 写完，再写 dev_1，再写 dev_2 ...
```

虽然 `rows_since_flush` 是全局的，但任意一个 flush 周期内通常只覆盖一个 device，最多在 device 边界处覆盖两个 device。

这并不等价于：

```text
一个 Tablet / 一个 flush 周期内包含很多 device。
```

如果你想研究混合写入，数据生成方式应该改成 interleaved：

```text
每一轮生成多个 device 的行；
把这些行写入同一个或连续几个 Tablet；
全局累计到 F 行后 flush。
```

理想情况下，一个 flush 周期内应覆盖接近全部 device，使：

$$
D_{\text{active}} \approx D
$$

这样实验才对应混合写入下的：

$$
K(F) \cdot D b
$$

### 2. `peak_m_total` 漏掉 flush 后的元数据峰值

当前写入 batch 后会更新：

```cpp
int64_t md = writer.calculate_mem_size_for_all_group();
int64_t mm = writer.calculate_meta_mem_size();
if (md > peak_data) peak_data = md;
if (mm > peak_meta) peak_meta = mm;
if (md + mm > peak_total) peak_total = md + mm;
```

但 flush 后只更新了 `peak_meta`：

```cpp
writer.flush();
flush_count++;
rows_since_flush = 0;

mm = writer.calculate_meta_mem_size();
if (mm > peak_meta) peak_meta = mm;
```

这里应同步更新 `peak_total`：

```cpp
if (mm > peak_total) peak_total = mm;
```

因为 flush 后数据缓冲下降，但元数据上升，总峰值可能出现在 flush 之后。

最终 flush 后也应做同样处理。

### 3. `M_init` 应从 `ModStat` 实测值获取

固定写死：

```cpp
static const int64_t kMInit = 900LL * 1024;
```

不够合理。`M_init` 表示 writer 初始化、schema 注册等固定开销。为了和 `calculate_mem_size_for_all_group()` / `calculate_meta_mem_size()` 的内部内存估算口径一致，它应该从 `ModStat` 的增量中测得，而不是使用进程 RSS 或经验默认值。

建议在每次 run 中记录 writer 初始化前后的 `ModStat` 总量差值：

```cpp
int64_t mod_before_init = total_modstat();
TsFileWriter writer;
writer.init(&wf);
writer.register_table(schema);
int64_t mod_after_init = total_modstat();
int64_t m_init = std::max<int64_t>(0, mod_after_init - mod_before_init);
```

然后把同一个 `m_init` 同时加入实测峰值和公式值：

```cpp
r.peak_m_total = m_init + peak_total;
r.formula_m_total = m_init + r.formula_m_data + r.formula_m_meta;
```

这样比较绝对值时口径一致；同时，最优 `F` 的推导仍可只看变量项，因为 `M_init` 只会整体平移曲线，不改变最小点位置。

由于同一进程内连续扫多个 `F` 时，某些初始化对象可能已被创建或复用，后续 run 的初始化增量可能不稳定。因此建议使用第一次 writer 初始化测得的 `m_init` 作为本次实验的固定初始化开销，并在后续 `F` 配置中复用该值。

## 混合写入下建议的实验设计

建议把实验写成真正的 interleaved multi-device 写入：

```text
while global_rows_written < R * D:
    create tablet with rows from many devices

    for each device in active devices:
        append one or more rows for this device

    writer.write_table(tablet)
    rows_since_flush += tablet.row_count

    if rows_since_flush >= F:
        record active_device_count for this flush window
        writer.flush()
        flush_count++
        rows_since_flush = 0
        clear active device set
```

同时记录：

```cpp
int64_t flush_calls;
int64_t chunk_group_count;
int64_t active_device_sum;
```

其中：

```cpp
active_device_sum += active_devices_in_this_flush_window;
```

则元数据公式可以写成：

```cpp
formula_m_meta = active_device_sum * kB;
```

如果实验设计保证每次 flush 都覆盖全部 device，则有：

```cpp
active_device_sum = flush_count * kNumDevices;
```

此时：

```cpp
formula_m_meta = flush_count * kNumDevices * kB;
```

而不是当前的：

```cpp
formula_m_meta = flush_count * kB;
```

## 当前公式需要改的地方

当前代码中：

```cpp
r.formula_m_data = kSData * F;
r.formula_m_meta = flush_count * kB;
r.formula_m_total = kMInit + r.formula_m_data + r.formula_m_meta;
```

在混合写入模型下，建议改成：

```cpp
r.formula_m_data = kSData * F;
r.formula_m_meta = active_device_sum * kB;
r.formula_m_total = m_init + r.formula_m_data + r.formula_m_meta;
```

如果每次 flush 覆盖全部 device，也可以先简化为：

```cpp
r.formula_m_meta = flush_count * kNumDevices * kB;
```

其中 `m_init` 应从当前运行的 `ModStat` 初始化增量测得，而不是使用固定默认值。

## `F_opt` 也需要按混合写入口径改写

当前代码中：

```cpp
double F_opt_d = std::sqrt((double)R * kNumDevices * kB / kSData);
```

这个公式更接近“每 device 独立 flush”的口径。

如果 `F` 是全局行数，并且每次 flush 覆盖全部 device，则应改成：

```cpp
double total_rows = (double)R * kNumDevices;
double F_opt_d =
    std::sqrt(total_rows * kNumDevices * kB / kSData);
```

也就是：

$$
F_{\text{opt}}
=
\sqrt{\frac{R D^2 b}{s}}
$$

## 文件头部注释需要更新

文件开头目前写的是：

```cpp
Schema: 4 DOUBLE fields, PLAIN + UNCOMPRESSED (incl. timestamp).
  s_data  = 8 + 4*8 = 40 bytes/row
  b       = 4*104 + 96 = 512 bytes/device/flush
```

但代码实际是：

```cpp
static const int kNumFields = 8;
static const int64_t kSData = 8 + kNumFields * 8;  // 72 bytes/row
static const int64_t kB = kNumFields * 104 + 96;   // 928 bytes/device/flush
```

建议改成：

```text
Schema: 8 DOUBLE fields, PLAIN + UNCOMPRESSED.
s_data = 8 + 8*8 = 72 bytes/row
b      = 8*104 + 96 = 928 bytes/device/flush
```

## 推荐论文表述

可以写成：

```text
在混合写入场景下，一个 flush 周期内会同时积累多个 device 的数据。
当全局累计行数达到 F 时，系统调用 flush，并将当前活跃 device 的
chunk group 一并写出。较大的 F 会增加内存中的数据缓冲项 sF，
较小的 F 则会增加 flush 次数和元数据项 K(F)D_active b。
因此总内存峰值关于 F 呈 U 型变化。
```

对应公式：

$$
M(F)
\approx
M_{\text{init}}
+ sF
+ K(F)D_{\text{active}}b
$$

其中：

$$
K(F)
\approx
\left\lceil \frac{RD}{F} \right\rceil
$$

若每次 flush 覆盖全部 device，则：

$$
D_{\text{active}} \approx D
$$

于是：

$$
M(F)
\approx
M_{\text{init}}
+ sF
+ \left\lceil \frac{RD}{F} \right\rceil Db
$$

忽略取整后，最优 flush 周期为：

$$
F_{\text{opt}}
=
\sqrt{\frac{RD^2b}{s}}
$$

## 小结

你说得对：这个实验应该研究混合写入，而不是一个 device 写完再 flush。

因此不应把代码改成每 device 独立 flush。真正需要改的是：

1. 数据生成方式应更接近 interleaved multi-device Tablet；
2. 元数据项应按 `flush_count * active_device_count * b` 估算；
3. 如果每次 flush 覆盖全部 device，则用 `flush_count * kNumDevices * kB`；
4. flush 后要同步更新 `peak_total`；
5. `M_init` 应从 `ModStat` 初始化增量测得，并同时加入实测峰值和公式值；
6. `F_opt` 要按全局 flush 周期推导，即 $\sqrt{RD^2b/s}$；
7. 更新过期注释。

这样才是在你的目标场景下验证“存在最优刷盘次数”。
