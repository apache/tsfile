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

# 文件格式

本文描述 TsFile 第 4 版的语言无关二进制文件格式。文件版本字节为 `0x04`。

<TsFileStructureTree locale="zh" />

## 范围与术语

本规范定义独立实现之间交换完整 TsFile v4 所需的二进制字节。文件包含四个组织层级：

- **Page** 是可以独立压缩和解码的最小数据单元。
- **Chunk** 包含一个物理列的多个 Page，并确定这些 Page 的数据类型、编码和压缩方式。
- **Chunk Group** 将一组连续 Chunk 关联到同一个设备标识符。
- **文件**由顺序写入的数据区、索引以及文件级元数据组成。

“对齐”表示多个测量共享一个物理时间列；“非对齐”表示每个测量携带自己的时间流。
对齐方式改变 Page 载荷的物理布局，但不改变时间戳和值本身的语义。

除非明确使用半开区间，本规范中的字节区间均包含首尾。绝对偏移从文件第一个字节开始
计算。除非另有说明，序列化长度不包含保存该长度的字段自身。

## 物理区域

设文件长度为 `F`，文件尾四字节记录的 `TsFileMetadata` 长度为 `L`，
`meta_offset` 为 `TsFileMetadata` 内保存的值：

| 区域 | 包含首尾的字节区间 | 定位方式 |
| --- | --- | --- |
| 头部魔数 | `0 … 5` | 固定位置 |
| 版本 | `6` | 固定位置 |
| 数据区 | `7 … meta_offset - 1` | 在分隔符前结束 |
| 分隔符 | `meta_offset` | 值为 `0x02`，由 `TsFileMetadata` 引用 |
| 索引元数据 | `meta_offset + 1 … F - 11 - L` | 由元数据偏移划分 |
| `TsFileMetadata` | `F - 10 - L … F - 11` | 根据文件尾长度反向定位 |
| 元数据长度 | `F - 10 … F - 7` | 相对文件尾的固定位置 |
| 尾部魔数 | `F - 6 … F - 1` | 相对文件尾的固定位置 |

索引元数据区可以按实现选择的物理顺序保存 Timeseries Metadata 和 Metadata Index Node。
它们之间的关系由绝对偏移确定，而不是由物理相邻关系确定。

## 写入模型

上例中，数据按设备分成多个 Chunk Group。一个 Chunk 保存一个测量列，每个 Chunk
包含一个或多个 Page。非对齐 Chunk 在同一个 Page 中保存时间流和值流；对齐数据由
一个 Time Chunk 和若干 Value Chunk 共享时间列。

文件元数据位于数据之后，写入器可以先顺序写入所有 Chunk Group，再写入索引和
Footer。读取器通常先读取文件尾，从 `TsFile Metadata size` 定位
`TsFileMetadata`，再通过其中的索引查找所需 Chunk。

写入器可以选择 Page 大小、Chunk Group 刷新阈值、元数据索引扇出、编码和压缩方式。
这些选择会影响性能和物理边界，但读取器只需根据文件中的标记、长度和元数据解析最终
布局，不需要获得写入器配置。

## 元数据优先读取算法

随机访问读取器无需扫描数据区即可打开完整文件：

1. 校验文件尾的六字节魔数。
2. 在 `F - 10` 读取 `L`，计算 `file_metadata_pos = F - 10 - L`。
3. 反序列化 `TsFileMetadata`，并验证 `meta_offset` 指向 `0x02`。
4. 可以先用完整路径查询布隆过滤器。阴性结果可以确定路径不存在；阳性结果仍需查索引。
5. 依次遍历表/设备索引和测量索引，定位目标 Timeseries Metadata 区间。
6. 根据时间和值谓词选择 Chunk Metadata 中记录的 Chunk 偏移。
7. 跳转到 Chunk Header，利用可用的 Page Statistics，仅读取和解码所需 Page。

因此，读取元数据的开销与选中的设备、测量、Chunk 和 Page 数量相关，而不是与整个文件
大小线性相关。

## 结构不变量

读取外部提供的长度并进行内存分配之前，合规读取器至少需要验证以下关系：

- 头部和尾部魔数相同，且版本字节受支持。
- `0 <= L <= F - 10`，计算得到的 `file_metadata_pos` 位于 `meta_offset` 之后。
- `meta_offset` 是文件内的绝对偏移，且对应字节为 `0x02`。
- 每个 Chunk 恰好结束在其 Chunk Header 之后的 `data_size` 字节处。
- 每个非空 Page 均位于所属 Chunk 内，磁盘载荷恰好包含 `compressed_size` 字节。
- 同一索引节点内的条目偏移单调不减，且不超过 `end_offset`。
- 对齐 Value Page 与相同序号的 Time Page 对应，其 `position_count` 等于该 Time Page 的
  位置数。

长度或偏移不满足上述关系时即为格式错误。读取器不得把长度限定的 Page 载荷内部偶然出现
的标记字节解释为顶层标记。

文件中只有一个用于结束数据区的顶层 `0x02` separator。它位于全部元数据之前，并由
`meta_offset` 指向。`TsFileMetadata` 前没有单独的 separator 或 marker，它的位置由
文件尾的长度字段确定。

设文件总长度为 `F`，`TsFileMetadata` 的序列化长度为 `L`：

```text
metadata_size_pos = F - 6 - 4
file_metadata_pos = F - 6 - 4 - L
```

文件头和文件尾的魔数字节均为：

```text
54 73 46 69 6c 65    # "TsFile"
```

## Format Details

- [Binary Representation](Binary-Representation.md)
- [Configurations](Configurations.md)
- [Extensibility](Extensibility.md)
- [Metadata](Metadata.md)
- [Types](Types.md)
- [Bloom Filter](Bloom-Filter.md)
- [Data Pages](DataPages/README.md)
  - [Chunks](DataPages/Chunks.md)
  - [Compression](DataPages/Compression.md)
  - [Encodings](DataPages/Encodings.md)
  - [Encryption](DataPages/Encryption.md)
  - [Checksumming](DataPages/Checksumming.md)
  - [Error Recovery](DataPages/Error-Recovery.md)
- [Nulls](Nulls.md)
- [Format Versions](Format-Versions.md)
