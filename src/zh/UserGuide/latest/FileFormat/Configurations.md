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

# 配置

TsFile 的大多数配置属于写入策略。它们控制缓冲、Page 边界、Chunk Group 边界以及
元数据索引的形状，但不会作为额外字段写入文件。读取器根据标记、长度字段、
Chunk Header 和已序列化的元数据确定实际布局。

下列名称表示文件格式层面的概念。不同接口暴露的配置名称可能不同，但它们对磁盘字节的
影响以及读取端是否需要原始配置是一致的。

| 概念配置 | 可观察的文件影响 | 配置值是否直接写入 | 读取器是否需要原始配置 |
| --- | --- | --- | --- |
| Chunk Group 大小 | 设备分组的刷出位置 | 否 | 否 |
| Page 大小目标 | Page 边界和 Statistics 粒度 | 否 | 否 |
| 每 Page 最大点数 | Page 边界 | 否 | 否 |
| 元数据索引扇出 | 索引树深度和节点宽度 | 否 | 否 |
| 布隆过滤器误判率 | 最终位数和哈希函数数 | 否；生成结果会写入 | 否 |
| 值编码 | Page 编码流 | 是，每个 Chunk 的 `encoding_type` | 否 |
| 压缩 | Page 磁盘负载 | 是，每个 Chunk 的 `compression_type` | 否 |
| 时间精度和时区 | `int64` 时间戳的解释 | 否 | 是，由上层协议提供 |
| 非对齐时间编码 | 每个非对齐 Page 的时间编码流 | 否 | 是 |
| 加密 | Page 磁盘负载和 Footer 属性 | 属性会写入；密钥上下文可能在外部 | 加密时需要 |

“不直接写入”表示文件中没有以该阈值或比率命名的字段，但配置造成的布局结果仍可能被
观察到。

## Chunk Group Size

Chunk Group 大小表示写入器在刷出当前设备的各个 Chunk 之前允许缓冲的数据量。
较大的 Chunk Group 可以减少标记与元数据开销，并有利于顺序 I/O，但需要更多写入内存；
较小的 Chunk Group 可以降低峰值内存，并更早地将数据写入输出。

该配置是刷出阈值，并不表示精确的磁盘占用大小。文件格式不规定默认值。
Chunk Group 的实际边界由 `0x00` Chunk Group Header 标记以及下一个顶层标记确定。

## Data Page Size

Data Page 大小是单个序列写入器在内存中累计数据量的目标值。较小的 Page 可以提供
更细粒度的 Statistics 和数据跳过能力；较大的 Page 可以减少 Page Header 开销并可能
提高压缩率，但需要更多缓冲空间，数据跳过粒度也更粗。

该阈值不等于精确的序列化 Page 大小。达到最大点数、显式刷出或需要维持对齐 Page
边界时，写入器都可以提前结束当前 Page。实际 Page 边界由 Page Header 中的长度确定。
文件格式不规定默认的 Page 大小。

## Maximum Points per Page

除 Page 大小目标外，写入器还可以限制一个 Page 中的逻辑位置数，以防大量紧凑值让
单个 Page 包含过多数据点。参考写入配置使用 `10,000` 点。

对于对齐序列，Time Page 及其对应的各个 Value Page 必须覆盖相同的逻辑位置范围，
因此写入器会为这些对齐列使用共同的 Page 边界。

该上限不会作为独立字段写入文件。读取器使用 Page 边界；对于对齐 Value Page，
还会使用 `position_count`。

## Metadata Index Fanout

元数据索引扇出限制一个内部元数据索引节点可以包含的子节点数量。较大的扇出形成
层数更少但节点更大的树；较小的扇出形成层数更多但节点更小的树。该值必须至少为 `2`，
参考写入配置使用 `256`。

配置的上限不会写入文件。每个元数据索引节点都会保存节点类型、子节点和结束偏移，
因此读取器无需知道写入配置即可遍历最终生成的树。

## Bloom Filter False-positive Rate

布隆过滤器误判率控制路径查找的空间与准确率权衡。误判率越低，需要的位数和哈希函数
越多。标准写入器将该值限制在 `0.01` 到 `0.10` 之间，默认使用 `0.05`。

误判率本身不会写入文件。最终生成的过滤器字节、位数和哈希函数数量保存在
`TsFileMetadata` 中，因此读取器不需要写入端的配置。详见
[Bloom Filter](Bloom-Filter.md)。

## Value Encoding and Compression

写入器可以按物理类型选择默认值编码，并选择默认压缩方式。这些默认值属于 Schema
与写入策略，不是读写端必须共享的配置：

- Chunk Header 中的 `encoding_type` 标识值编码；对于对齐 Time Chunk，它标识时间编码。
- Chunk Header 中的 `compression_type` 标识该 Chunk 内所有 Page 使用的压缩方式。

因此读取器直接使用 Chunk Header，不需要知道写入器的默认值。支持的标识符及其
载荷格式见 [Encodings](DataPages/Encodings.md) 和
[Compression](DataPages/Compression.md)。

## Time Precision and Time Zone

物理时间戳是有符号 64 位整数。TsFile v4 不在文件中保存时间单位或时区；毫秒、微秒、
纳秒等时间精度以及时区语义由使用该文件的上层协议确定。

## Non-aligned Time Encoding

非对齐 Chunk Header 的 `encoding_type` 描述值流编码，不描述同一 Page 内的时间流编码。
非对齐时间流编码由读写配置共同确定，默认值为 `TS_2DIFF`。

对齐 Time Chunk 不受这一限制；其时间编码保存在 Time Chunk Header 的
`encoding_type` 中。

## Encryption

Page 数据是否加密以及所用算法由 `TsFileMetadata` 的 `encryptLevel`、`encryptType`
和 `encryptKey` 属性描述。打开加密文件所需的密钥材料仍然保存在文件之外。
详细格式见 [Encryption](DataPages/Encryption.md)。

## Implementation Settings

内存分配、内存检查频率、批大小、并行执行、线程数、存储后端以及关闭文件时的同步
策略都不定义 TsFile 字节。它们属于具体实现与部署范围，不属于本文件格式规约。
