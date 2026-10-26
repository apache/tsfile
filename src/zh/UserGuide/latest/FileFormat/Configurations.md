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

多数配置影响写入器如何分组和缓冲数据。读取器无需知道原来的 Page 大小目标或刷出阈值，
因为最终形成的边界已经记录在文件中。时间单位、非对齐时间编码等少数信息则仍需由
交换文件的应用共同约定。

下表区分了这些情况。不同语言接口中的配置名称可能不同。

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
| 加密 | 文件加密头、Chunk 认证、Page 载荷或旧版 Footer 属性 | provider/profile 或旧版属性会写入；密钥上下文在外部 | 加密时需要 |

例如，文件不会保存“Page 大小目标”这个配置值，但会记录每个 Page 的实际长度。

<div id="chunk-group-size"></div>

## Chunk Group 大小

Chunk Group 大小表示写入器在刷出当前设备的各个 Chunk 之前允许缓冲的数据量。
较大的 Chunk Group 可以减少标记与元数据开销，并有利于顺序 I/O，但需要更多写入内存；
较小的 Chunk Group 可以降低峰值内存，并更早地将数据写入输出。

该配置是刷出阈值，并不表示精确的磁盘占用大小。文件格式不规定默认值。
分组从 `0x00` Chunk Group Header 开始，到下一个分组、操作索引记录或分隔符结束；
中间的 Chunk 标记表示当前分组仍在继续。

<div id="data-page-size"></div>

## Page 大小

Data Page 大小是单个序列写入器在内存中累计数据量的目标值。较小的 Page 可以提供
更细粒度的 Statistics 和数据跳过能力；较大的 Page 可以减少 Page Header 开销并可能
提高压缩率，但需要更多缓冲空间，数据跳过粒度也更粗。

该阈值不等于精确的序列化 Page 大小。达到最大点数、显式刷出或需要维持对齐 Page
边界时，写入器都可以提前结束当前 Page。实际 Page 边界由 Page Header 中的长度确定。
文件格式不规定默认的 Page 大小。

<div id="maximum-points-per-page"></div>

## 每个 Page 的最大点数

除 Page 大小目标外，写入器还可以限制一个 Page 中的逻辑位置数，以防大量紧凑值让
单个 Page 包含过多数据点。常见取值为 `10,000` 点。

对于对齐序列，Time Page 及其对应的各个 Value Page 必须覆盖相同的逻辑位置范围，
因此写入器会为这些对齐列使用共同的 Page 边界。

该上限不会作为独立字段写入文件。读取器使用 Page 边界；对于对齐 Value Page，
还会使用 `position_count`。

<div id="metadata-index-fanout"></div>

## 索引扇出

索引扇出就是一个内部索引节点最多能包含多少个子节点。较大的扇出形成
层数更少但节点更大的树；较小的扇出形成层数更多但节点更小的树。该值应至少为 `2`，
常见取值为 `256`。

配置的上限不会写入文件。每个元数据索引节点都会保存节点类型、子节点和结束偏移，
因此读取器无需知道写入配置即可遍历最终生成的树。

<div id="bloom-filter-false-positive-rate"></div>

## 布隆过滤器误判率

布隆过滤器有时会把不存在的路径判为“可能存在”，导致一次额外的索引查找。降低目标
误判率可以减少这种情况，代价是使用更多位和哈希函数。标准写入器将该值限制在 `0.01` 到 `0.10` 之间，默认使用 `0.05`。

误判率本身不会写入文件。最终生成的过滤器字节、位数和哈希函数数量保存在
`TsFileMetadata` 中，因此读取器不需要写入端的配置。详见
[布隆过滤器](Bloom-Filter.md)。

<div id="value-encoding-and-compression"></div>

## 值编码与压缩

写入器可以按物理类型选择默认值编码，并选择默认压缩方式。这些默认值属于 Schema
与写入策略，不是读写端必须共享的配置：

- Chunk Header 中的 `encoding_type` 标识值编码；对于对齐 Time Chunk，它标识时间编码。
- Chunk Header 中的 `compression_type` 标识该 Chunk 内所有 Page 使用的压缩方式。

因此读取器直接使用 Chunk Header，不需要知道写入器的默认值。支持的标识符及其
载荷格式见 [编码](DataPages/Encodings.md) 和
[压缩](DataPages/Compression.md)。

<div id="time-precision-and-time-zone"></div>

## 时间精度与时区

物理时间戳是有符号 64 位整数。TsFile v4 不在文件中保存时间单位或时区；毫秒、微秒、
纳秒等时间精度以及时区语义由使用该文件的上层协议确定。

<div id="non-aligned-time-encoding"></div>

## 非对齐时间编码

非对齐 Chunk Header 的 `encoding_type` 描述值流编码，不描述同一 Page 内的时间流编码。
非对齐时间流编码由读写配置共同确定，默认值为 `TS_2DIFF`。

对齐 Time Chunk 不受这一限制；其时间编码保存在 Time Chunk Header 的
`encoding_type` 中。

<div id="encryption"></div>

## 加密

加密方式有两处描述来源：Page AEAD 使用可选的文件加密头，旧版加密使用 Footer 属性。
它们记录 Page 字节应如何解释，取得密钥仍需外部环境。AEAD 文件的旧版 Footer 属性虽然
写为未加密状态，实际模式仍以文件加密头为准。布局和判断规则见[加密](DataPages/Encryption.md)。

<div id="implementation-settings"></div>

## 与格式无关的配置

内存分配、内存检查频率、批大小、并行执行、线程数、存储后端以及关闭文件时的同步
策略都不定义 TsFile 字节。它们属于具体实现与部署范围，不属于本文件格式规约。
