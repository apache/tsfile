# 读取内存公式精简建议

## 背景

在修正实验程序后，读取内存估算误差已经显著降低：

```text
N_cols     batch     peak_actual     m_formula    error%
--------------------------------------------------------
     1      1024          168970        175014     3.45%
     1      4096          330250        336294     1.80%
     1     16384          975370        981414     0.62%
     1     65536         3555850       3561894     0.17%
     2      1024          209918        220492     4.80%
     2      4096          383870        394444     2.68%
     2     16384         1079678       1090252     0.97%
     2     65536         3862910       3873484     0.27%
     4      1024          399480        399640     0.04%
     4      4096          623352        623512     0.03%
     4     16384         1518840       1519000     0.01%
     4     65536         5100792       5100952     0.00%
```

当前精确公式能够很好解释实验结果，但它包含若干实现细节，例如 TsBlock null bitmap、value page bitmap/header、TAG/STRING 列的预留缓冲等。

论文正文中如果直接展开这些细节，公式会比较复杂，也容易让模型看起来像是在拟合当前代码实现。因此建议采用“两层公式”的写法：

- 正文使用精简公式，突出主导因素；
- 实验说明中给出修正项，解释为什么实验估算能够达到较低误差。

## 正文推荐公式

正文中建议使用如下精简模型：

$$
M_{\text{read}}
\approx
M_{\text{fixed}}
+ B \cdot s_{\text{row}}
+ C_{\text{page}}
$$

其中：

$$
C_{\text{page}}
=
C_{\text{time}}
+ \sum_{i=1}^{N} C_{\text{value},i}
$$

符号含义如下：

- $B$ 表示 `batch_size`；
- $s_{\text{row}}$ 表示 TsBlock 中单行输出缓冲大小；
- $C_{\text{time}}$ 表示当前 time page 解码缓冲；
- $C_{\text{value},i}$ 表示第 $i$ 个查询列的 value page 解码缓冲；
- $N$ 表示查询的 FIELD 列数；
- $M_{\text{fixed}}$ 表示 reader context、decoder/compressor、metadata、PageArena 基础分配等固定或低阶开销。

这个公式的重点不是逐字节拟合实现，而是表达读取路径的核心规律：

```text
读取内存主要由 batch_size 和查询列数控制，
与文件总行数不呈线性增长关系。
```

## 更贴近实验的公式

实验中为了与 `ModStat` 的实际统计口径一致，可以使用更细的估算式：

$$
M_{\text{read}}
\approx
M_{\text{fixed}}
+ B \cdot s_{\text{row}}
+ N_{\text{out}} \cdot
\left\lceil \frac{B}{8} \right\rceil
+ C_{\text{time}}
+ \sum_{i=1}^{N}
\left(
C_{\text{value},i}
+ \left\lceil \frac{P}{8} \right\rceil
+ 4
\right)
$$

其中：

- $N_{\text{out}}$ 表示输出列数，包括时间列、TAG 列和查询 FIELD 列；
- $\left\lceil B/8 \right\rceil$ 来自 TsBlock 每列的 null bitmap；
- $P$ 表示每个 page 中的点数；
- $\left\lceil P/8 \right\rceil$ 来自 value page 的 not-null bitmap；
- 常数 $4$ 来自 value page 中记录点数的 `data_num` 字段。

对于当前实验中的 TAG / STRING 列，$s_{\text{row}}$ 应按 TsBlock 实际预留空间估算：

$$
s_{\text{row}}
=
8
+ \sum_{\text{field cols}} \text{sizeof(type)}
+ N_{\text{tag}} \times 20
$$

其中：

$$
20
=
\text{DEFAULT\_RESERVED\_SIZE\_OF\_STRING}
+ \text{STRING\_LEN}
= 16 + 4
$$

也就是说，TAG / STRING 列不应按 8 字节指针估算，而应按当前 TsBlock 中的变长列预留缓冲估算。

## 为什么正文可以简化

精确公式中的 null bitmap、page bitmap/header 等项都是实现相关的低阶项。它们会影响小 batch 或少列数场景下的估算误差，但不会改变读取内存的主导趋势。

主导项仍然是：

$$
B \cdot s_{\text{row}}
$$

和：

$$
\sum_{i=1}^{N} C_{\text{value},i}
$$

前者随 `batch_size` 线性增长，后者随查询列数增长。相比之下，bitmap/header 项规模较小，可以在正文中并入 $M_{\text{fixed}}$ 或描述为“实现相关低阶开销”。

因此，正文可以写精简公式；实验部分再说明为了提高估算精度，额外计入了这些实现项。

## 推荐论文表述

可以在正文中写：

```text
读取路径中的内存主要由三部分构成：查询生命周期内的固定开销、
TsBlock 输出缓冲，以及当前正在解码的 Page 缓冲。因此读取峰值内存可近似表示为：
```

$$
M_{\text{read}}
\approx
M_{\text{fixed}}
+ B \cdot s_{\text{row}}
+ C_{\text{page}}
$$

然后补充：

```text
其中，B 为 batch_size，s_row 为输出结果中单行所需的缓冲空间，
C_page 为当前 time page 与查询列 value page 的解码缓冲总量。
该公式表明，读取内存主要受 batch_size 和查询列数控制，
而不随文件总行数单调增长。
```

在实验结果附近可以写：

```text
实验估算中，为了与实现中的内存统计口径保持一致，
我们进一步计入 TsBlock null bitmap、value page bitmap/header，
以及 TAG/STRING 列在 TsBlock 中的固定预留缓冲。
这些修正项不改变模型的主导趋势，但可以显著降低小 batch 和少列数场景下的估算误差。
```

## 实验公式与正文公式的关系

可以将两者关系表述为：

```text
正文公式给出机制模型，强调主导项；
实验公式在机制模型基础上加入实现相关修正项，用于验证估算精度。
```

具体来说：

$$
M_{\text{experiment}}
=
M_{\text{read}}
+ M_{\text{bitmap}}
+ M_{\text{page-meta}}
+ M_{\text{string-reserve}}
$$

其中：

- $M_{\text{bitmap}}$ 对应 TsBlock null bitmap；
- $M_{\text{page-meta}}$ 对应 value page 中的 bitmap 和 `data_num`；
- $M_{\text{string-reserve}}$ 对应 TAG / STRING 列的固定预留缓冲修正。

严格来说，TAG / STRING 预留缓冲也可以直接并入 $s_{\text{row}}$，而不是作为额外项。

## 建议取舍

建议采用如下写法：

1. 正文只保留精简公式：

$$
M_{\text{read}}
\approx
M_{\text{fixed}}
+ B \cdot s_{\text{row}}
+ C_{\text{page}}
$$

2. 表格中的 `m_formula` 使用实验精确公式。

3. 在实验说明中解释：

```text
表中公式值使用了实现感知的修正公式，
额外计入 TsBlock null bitmap、value page bitmap/header 和 TAG/STRING 预留缓冲。
```

4. 不建议在正文主公式中展开所有 bitmap/header 项，否则会削弱模型的可读性。

## 小结

当前实验结果已经说明，修正实现细节后，读取内存估算误差可以控制在很小范围内。论文中应避免把主公式写得过于复杂。

更合适的组织方式是：

- 用精简公式解释机制和趋势；
- 用实现修正公式解释实验精度；
- 明确说明修正项不改变主导趋势，只影响小 batch、少列数场景下的误差。

