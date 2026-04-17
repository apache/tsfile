# 读取内存估算公式误差分析

## 背景

文章中目前给出的读取内存模型为：

$$
M_{\text{read}} \approx
M_{\text{fixed}}
+ \text{batch\_size} \times s_{\text{row}}
+ N_{\text{cols}} \times C_{\text{page}}
$$

对应实验位于 `read_precision.cpp`。实验中用于对比的公式为：

```cpp
int64_t m_data = static_cast<int64_t>(batch_size) * s_row;
int64_t m_page = kTimeCPage;
for (int f = 0; f < n_field; f++) m_page += kFieldCPage[f];
int64_t m_formula = m_data + m_page;
```

实测结果中，一部分误差较小，另一部分误差较大：

```text
N_cols     batch     peak_actual     m_formula    error%
--------------------------------------------------------
     1      1024          352032        148672   136.78%
     1     65536         3738912       1955008    91.25%
     2      1024          209918        192768     8.90%
     4      1024          399480        369152     8.22%
     4     65536         5100792       3465728    47.18%
```

这些结果并不推翻“读取内存是有界的、主要受 `batch_size` 和查询列数驱动”这个趋势判断，但说明当前简化公式低估了若干实现层面的内存项。

## 主要原因

### 1. 实验公式省略了 `M_fixed`

文章公式中包含 `M_fixed`，但 `read_precision.cpp` 里实际计算 `m_formula` 时明确省略了该项：

```cpp
// M_fixed omitted here; we compare variable part only.
```

而 `peak_actual` 的计算方式是：

```cpp
total_modstat() - g_baseline
```

它统计的是查询期间 `ModStat` 记录到的总分配增量，因此包含了查询初始化和读取过程中产生的对象与缓冲，例如：

- `ResultSet` / reader 上下文；
- iterator / column context；
- 元数据结构；
- decoder / compressor 对象；
- `PageArena` 页面；
- 其他查询生命周期内的辅助对象。

因此，在小 `batch_size`、少列数场景下，`m_formula` 本身很小，一个 100 KB 到 200 KB 量级的固定开销就会造成很大的百分比误差。

例如：

```text
N_cols=1, batch=1024
peak_actual = 352032
m_formula   = 148672
diff        = 203360
error       = 136.78%
```

这里的大误差主要来自固定项缺失以及分母较小，而不一定表示趋势模型完全错误。

### 2. TAG / STRING 列的行宽估计偏低

实验代码中当前假设：

```cpp
static const int kTagSize = 8;  // STRING stored as pointer in TsBlock
```

但这与当前 TsBlock 实现不一致。`TupleDesc::get_single_row_len()` 中，`STRING` / `TEXT` / `BLOB` 类型按如下常量计入行宽：

```cpp
DEFAULT_RESERVED_SIZE_OF_STRING + STRING_LEN
```

对应常量为：

```cpp
#define DEFAULT_RESERVED_SIZE_OF_STRING 16
#define STRING_LEN 4
```

因此，每个 TAG / STRING 列在 TsBlock 中更接近按 20 字节每行预留，而不是 8 字节指针。

当前查询固定包含两个 TAG 列，所以每行低估量为：

$$
(20 - 8) \times 2 = 24\ \text{bytes/row}
$$

这个误差会随着 `batch_size` 线性放大。

当 `batch_size = 65536` 时，仅 TAG / STRING 估计偏差就达到：

$$
24 \times 65536 = 1{,}572{,}864\ \text{bytes}
$$

这可以解释大批次下的大部分误差。例如：

```text
N_cols=4, batch=65536
peak_actual = 5100792
m_formula   = 3465728
diff        = 1635064
```

差值 `1,635,064` 与 TAG / STRING 缓冲低估值 `1,572,864` 非常接近。

### 3. TsBlock 的 null bitmap 没有计入

每个 TsBlock `Vector` 都拥有一个 null bitmap：

```cpp
nulls_.init(max_row_num, true);
```

其大小约为：

$$
\left\lceil \frac{\text{batch\_size}}{8} \right\rceil
\quad \text{bytes per output column}
$$

输出列包含：

```text
时间列 + TAG 列 + 查询 FIELD 列
```

因此 null bitmap 的总量近似为：

$$
N_{\text{out-cols}} \times
\left\lceil \frac{B}{8} \right\rceil
$$

其中：

$$
N_{\text{out-cols}} = 1 + N_{\text{tag}} + N_{\text{field}}
$$

这一项通常小于 TAG / STRING 行宽低估造成的误差，但它同样随 `batch_size` 和输出列数增长。

### 4. Page 解压缓冲也略有低估

当前 `m_page` 只包含：

```text
time page payload + selected field page payloads
```

但 value page 解码后的结构还包含：

```text
data_num: 4 bytes
not-null bitmap: ceil(page_points / 8) bytes
value payload
```

因此每个 field value page 还应额外计入：

$$
4 + \left\lceil \frac{N_{\text{page-points}}}{8} \right\rceil
$$

当前实验中 `page_points = 10000`，所以每个 field page 约多出：

$$
4 + \left\lceil \frac{10000}{8} \right\rceil
= 1254\ \text{bytes}
$$

这一项不是主要误差来源，但如果目标是验证公式精度，应当补入。

### 5. TsBlock 和 PageArena 的生命周期不是严格“每批次分配和释放”

文章中目前的表述是：

```text
每个批次开始时分配 TsBlock 缓冲和解压缓冲，
批次数据返回后 TsBlock 缓冲即释放。
```

这个描述与当前实现不完全一致。

在 `SingleDeviceTsBlockReader::init_internal()` 中，`current_block_` 在 reader 初始化时创建：

```cpp
common::TsBlock::create_tsblock(&tuple_desc_, current_block_, block_size)
```

后续读取过程中通常是对该 TsBlock 执行 `reset()` 并复用，而不是每个批次重新分配和释放一次。

另外，`PageArena` 的分配也更接近查询生命周期内持有，直到 reader / result set 关闭时再统一释放。

因此，实际内存变化更适合描述为：

```text
查询初始化阶段形成一部分固定基线；
TsBlock 输出缓冲随 batch_size 分配后通常跨批次复用；
Page 解压缓冲在页面解码期间形成局部峰值；
查询关闭时释放查询生命周期内的对象和 Arena 分配。
```

这仍然支持“读取内存有界且主要受 `batch_size` 和列数控制”的结论，但不宜强调严格的“每批次锯齿形释放”。

## 更贴近实现的修正公式

可以将读取峰值内存估计改写为：

$$
M_{\text{read}} \approx
M_{\text{fixed}}
+ M_{\text{meta}}
+ M_{\text{tsblock}}
+ M_{\text{page}}
$$

其中 TsBlock 部分为：

$$
M_{\text{tsblock}} \approx
B \times s_{\text{row,buf}}
+ N_{\text{out-cols}} \times
\left\lceil \frac{B}{8} \right\rceil
$$

行缓冲宽度为：

$$
s_{\text{row,buf}}
=
8
+ \sum_{\text{field cols}} \text{sizeof(type)}
+ N_{\text{tag}} \times 20
$$

这里的 20 来自当前实现中的：

$$
\text{DEFAULT\_RESERVED\_SIZE\_OF\_STRING}
+ \text{STRING\_LEN}
= 16 + 4
= 20
$$

Page 解码部分可以估为：

$$
M_{\text{page}} \approx
C_{\text{time-page}}
+ \sum_{\text{field cols}}
\left(
C_{\text{value-page}}
+ \left\lceil \frac{N_{\text{page-points}}}{8} \right\rceil
+ 4
\right)
$$

其中：

- `B` 表示 `batch_size`；
- `N_out-cols` 表示输出列数，即时间列、TAG 列和查询 FIELD 列之和；
- `M_fixed` 表示 reader、result set、decoder、compressor 等固定对象开销；
- `M_meta` 表示查询生命周期内的元数据和 `PageArena` 分配。

## 建议修改文章表述

原结论：

```text
读取内存主要由 TsBlock 输出缓冲和 per-column Page 解压缓冲组成，
主要受查询列数和 batch_size 驱动。
```

这个方向仍然成立，但建议将“公式精确预测”改为“刻画主要趋势”：

```text
简化公式能够刻画读取内存随 batch_size 和查询列数增长的主要线性趋势。
但实测峰值通常高于简化估算值，因为当前实现还包含查询生命周期内的固定对象、
元数据分配、每列 null bitmap、TAG/STRING 预留缓冲以及 value page 中的 bitmap/header。
其中，TAG/STRING 列在 TsBlock 中按固定长度的变长缓冲预留空间，
若简单按 8 字节指针估算，会在大 batch_size 场景下显著低估读取内存。
```

生命周期描述建议改为：

```text
读取查询初始化时会分配 result set、reader context、metadata context 和 TsBlock 缓冲。
读取过程中，Page 解压缓冲随页面解码产生并释放，而 TsBlock 缓冲通常跨批次复用。
因此读取内存整体保持有界，主要由 batch_size 和查询列数决定，
但还叠加了查询固定基线以及若干实现相关的 per-column 开销。
```

## 实验代码修正建议

若要降低 `read_precision.cpp` 中的估算误差，可以做以下修改。

### 1. 修正 TAG 行宽

将：

```cpp
static const int kTagSize = 8;
```

改为：

```cpp
static const int kTagSize = 20;
```

或直接写成：

```cpp
static const int kTagSize = DEFAULT_RESERVED_SIZE_OF_STRING + STRING_LEN;
```

### 2. 增加 TsBlock null bitmap

加入：

```cpp
int64_t m_null_bitmap =
    static_cast<int64_t>(1 + kNumTags + n_field) * ((batch_size + 7) / 8);
```

### 3. 增加 value page bitmap/header

加入：

```cpp
int64_t m_page_meta =
    static_cast<int64_t>(n_field) * (4 + ((kPagePoints + 7) / 8));
```

### 4. 明确是否包含 `M_fixed`

如果目标是验证绝对峰值，应加入经验测得的 `M_fixed`：

$$
M_{\text{formula}} =
M_{\text{fixed}}
+ M_{\text{tsblock}}
+ M_{\text{page}}
$$

如果目标只是验证变量项趋势，则应在实验和文章中明确说明：

```text
该公式对比的是读取内存变量项，不包含查询初始化和元数据固定开销。
因此小 batch_size 或少列数场景下百分比误差会被放大。
```

## 小结

当前结果说明，原始简化公式低估了实际读取峰值，但主要原因是实现细节没有计入，而不是“读取内存有界”这一判断错误。

最关键的低估项是 TAG / STRING 列：当前实验把它们按 8 字节估算，但 TsBlock 实际更接近按 20 字节每行预留。该项随 `batch_size` 线性增长，因此会在大批次场景下造成显著误差。

修正 TAG 行宽、null bitmap、page bitmap/header，并说明 `M_fixed` 的处理方式后，公式应能更好解释实验结果。

