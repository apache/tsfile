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

TsFile 按列保存时序数据。写入时，数据按设备分组依次追加，索引则放在文件尾部。
读取时通常反过来：先从文件尾找到索引，定位目标时间序列，再跳转到保存数据的 Chunk。

本文介绍版本字节为 `0x04` 的 TsFile v4 布局，以 2026-10-26 的 Java 参考实现为依据，
说明各结构的存储方式及相互关系。编码章节列出了编码编号，并展开 `PLAIN` 的字节布局；
其他值编码算法的完整位布局不在本文范围内。

## 范围与术语

文件中的数据分为三个层级：

- **Page** 保存一批编码后的值，是独立压缩的单位。
- **Chunk** 保存一个物理列的 Page，头部记录数据类型、编码和压缩方式。对齐的
  Value Chunk 的 `data_size` 可以为 `0`，此时 Header 仍存在，只是不含 Page。
- **Chunk Group** 保存同一设备的一批 Chunk。随着数据分批刷出，一个设备可以在文件中
  出现多次，对应多个 Chunk Group。

**非对齐数据**的每个测量列各自保存时间戳和值。**对齐数据**把共享的时间戳放在
Time Chunk 中，各测量列分别放在 Value Chunk 中，再用位图记录哪些位置有值。
两种布局的细节见[数据页](DataPages/README.md)。

<TsFileStructureTree locale="zh" />

## 物理区域

文件以六字节魔数 `TsFile` 和一字节版本号开头。Page AEAD 文件随后还有一个可选的
文件加密头。数据区由唯一的 `0x02` 分隔符结束，之后是索引和文件元数据。
最后十个字节保存元数据长度和尾部魔数。

下表使用以下记号：

- `F`：文件总长度。
- `L`：`TsFileMetadata` 的序列化长度，由文件尾的四字节长度字段保存。
- `E`：可选文件加密头的总序列化长度；不存在时为 `0`。
- `data_start = 7 + E`：数据区的起点。
- `meta_offset`：分隔符的偏移量，保存在 `TsFileMetadata` 中。

| 区域 | 包含首尾的字节区间 | 定位方式 |
| --- | --- | --- |
| 头部魔数 | `0 … 5` | 固定位置 |
| 版本 | `6` | 固定位置 |
| 可选文件加密头 | `7 … 6 + E` | 仅 Page AEAD 存在，marker 为 `0x06` |
| 数据区 | `data_start … meta_offset - 1` | 在分隔符前结束 |
| 分隔符 | `meta_offset` | 值为 `0x02`，由 `TsFileMetadata` 引用 |
| 索引元数据 | `meta_offset + 1 … F - 11 - L` | 由元数据偏移划分 |
| `TsFileMetadata` | `F - 10 - L … F - 11` | 根据文件尾长度反向定位 |
| 元数据长度 | `F - 10 … F - 7` | 相对文件尾的固定位置 |
| 尾部魔数 | `F - 6 … F - 1` | 相对文件尾的固定位置 |

偏移量从文件的第一个字节开始计算。上表的区间包含首尾；其他位置使用的 `[start, end)`
表示左闭右开区间。除非另有说明，长度不包含长度字段自身。`E = 0` 时，文件加密头对应
的区间不存在。

索引元数据区中的 Timeseries Metadata 和索引节点可以交错存放，读取时应跟随偏移量，
不依赖它们的物理排列顺序。`TsFileMetadata` 自身没有标记，需要通过文件尾保存的长度
反向定位。

## 写入模型

写入器先追加各个 Chunk Group，结束文件时再写索引和 Footer。Page 大小、刷出阈值和
索引扇出会影响结构边界，但这些边界都能从头部和偏移量中读出。时间单位和非对齐时间
编码仍需读写双方约定，详见[配置](Configurations.md)。

头部和尾部魔数使用相同的字节：

```text
54 73 46 69 6c 65    # "TsFile"
```

## 元数据优先读取算法

读取器无需扫描整个数据区，就能定位目标序列：

1. 校验头部魔数和版本，检查偏移 `7` 处是否有[文件加密头](DataPages/Encryption.md#file-encryption-header)，
   据此确定 Chunk Header 和 Page 载荷的解析方式。
2. 校验尾部魔数，在 `F - 10` 处读取 `L`，计算 `file_metadata_pos = F - 10 - L`。
3. 读取 `TsFileMetadata`，检查 `meta_offset` 是否指向分隔符。
4. 可以先用完整路径查询[布隆过滤器](Bloom-Filter.md)。只有确认字符集、路径构造规则和
   哈希行为与写入端一致时，才能据此判断路径不存在。
5. 依次查找表/设备索引、测量索引，定位目标序列的元数据。
6. 根据序列和 Chunk 的统计信息选择 Chunk，跳转后再用可用的 Page 统计信息缩小范围。
7. 读取选中 Page 的载荷，按需解密，然后解压和解码。

这样可以避免读取无关 Page 的数据。单 Page Chunk 不保存 Page Statistics，筛选时要使用
Chunk 或序列级的统计信息。

## 结构不变量

读取时需要检查的主要关系包括：

- 头尾魔数和版本有效。
- 可选文件加密头只在偏移 `7` 处出现一次，且长度和版本有效。
- `0 <= L <= F - 10`，且 `file_metadata_pos` 位于分隔符之后。
- 一个 Chunk 的全部 Page 恰好占据完整 Chunk Header 之后的 `data_size` 字节。
- 每个非空 Page 的载荷恰好占据 `compressed_size` 字节，且没有越过 Chunk 边界。
- 同一索引节点内的偏移量单调不减，且不超过 `end_offset`。
- 对齐 Value Page 和相同序号的 Time Page 覆盖同一组位置，且 `position_count` 等于该
  Time Page 的位置数。

不满足这些关系即为格式错误。

通用的长度和偏移量检查见[二进制表示](Binary-Representation.md)，Page AEAD 额外要求的
认证检查见[加密](DataPages/Encryption.md)。标记字节只在结构边界处有含义，不能把 Page
载荷中碰巧相同的字节当成标记。

<div id="format-details"></div>

## 阅读顺序与相关章节

初次阅读可以先看[二进制表示](Binary-Representation.md)、[Chunk 与 Chunk Group](DataPages/Chunks.md)
和[数据页](DataPages/README.md)，再通过[元数据](Metadata.md)了解索引查找过程。

- [数据类型](Types.md)与[编码](DataPages/Encodings.md)
- [空值](Nulls.md)与[压缩](DataPages/Compression.md)
- [布隆过滤器](Bloom-Filter.md)
- [加密](DataPages/Encryption.md)与[校验和](DataPages/Checksumming.md)
- [错误恢复](DataPages/Error-Recovery.md)
- [配置](Configurations.md)、[格式版本](Format-Versions.md)与[扩展机制](Extensibility.md)
