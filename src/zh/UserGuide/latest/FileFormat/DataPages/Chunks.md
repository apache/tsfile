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

# Chunk 与 Chunk Group

TsFile 数据区由 marker 引导的 Chunk Group、Chunk 和 Operation Index Range 组成。
没有文件加密头时从偏移 `7` 开始；若偏移 `7` 处保存总序列化长度为 `E` 的 Page AEAD
[FileEncryptionHeader](Encryption.md#file-encryption-header)，数据区从 `7 + E` 开始。
该可选加密头只允许出现一次，并位于数据区之前。

顶层语法为：

```text
FilePrefix  := magic[6] version[1] FileEncryptionHeader?
DataSection := (ChunkGroup+ OperationIndexRange?)*
ChunkGroup  := ChunkGroupHeader Chunk*
Separator   := byte(0x02)
```

每个批次包含一个或多个 ChunkGroup，随后可选一条 OperationIndexRange。数据区之后紧接
`0x02` 分隔符，它不属于任何 Chunk Group。Chunk Group 自身没有总长度字段。读取完一个 Chunk 后，要看下一个标记：如果仍是 Chunk，就继续读取；如果是新的
Chunk Group、操作索引记录或分隔符，当前分组就结束了。

```text
File
└── Chunk Group                 一个设备的一批数据
    ├── Non-aligned Chunk       自带时间流的一个测点列
    │   └── Page(s)
    └── Aligned Chunk Set       多个测点列共享时间列
        ├── Time Chunk
        │   └── Time Page(s)
        └── Value Chunk(s)
            └── Value Page(s)
```

<div id="markers"></div>

## 标记字节

| 值 | 名称 | 后续结构 |
| ---: | --- | --- |
| `0x00` | `CHUNK_GROUP_HEADER` | `DeviceID` |
| `0x01` | `CHUNK_HEADER` | 多页、非对齐 Chunk |
| `0x02` | `SEPARATOR` | 数据区结束，元数据区开始 |
| `0x03` | `VERSION` | v3 起不再使用，编号保留 |
| `0x04` | `OPERATION_INDEX_RANGE` | 两个 `int64` |
| `0x05` | `ONLY_ONE_PAGE_CHUNK_HEADER` | 单页、非对齐 Chunk |
| `0x06` | `ENCRYPTION_HEADER` | 仅位于偏移 `7` 的文件加密头；不属于 Chunk |
| `0x41` | `VALUE_CHUNK_HEADER` | 多页、对齐 Value Chunk |
| `0x45` | `ONLY_ONE_PAGE_VALUE_CHUNK_HEADER` | 单页、对齐 Value Chunk |
| `0x81` | `TIME_CHUNK_HEADER` | 多页、对齐 Time Chunk |
| `0x85` | `ONLY_ONE_PAGE_TIME_CHUNK_HEADER` | 单页、对齐 Time Chunk |

Chunk marker 的高两位表示列角色，低 6 位表示页数类别：

```text
bit 7 = 1: aligned time column
bit 6 = 1: aligned value column
low 6 bits = 0x01: more than one page
low 6 bits = 0x05: one page or no page
```

`data_size = 0` 的对齐 Value Chunk 可以没有 Page。

只有读完前一个结构，才能解释下一个标记。Page 载荷可以包含任意字节，其中看似标记的
值也只是数据。

<div id="chunk-group-header"></div>

## Chunk Group 头部

```text
ChunkGroupHeader :=
    byte       marker = 0x00
    DeviceID   device_id

DeviceID :=
    UVarInt    segment_count
    VarString  segment[segment_count]
```

DeviceID 由一组有序的字符串片段组成，单个片段可以为 null。序列化时会去掉末尾的 null，
但至少要有一个非 null 片段。`segment_count = 0` 是一种特殊表示，读取后得到一个仅含
空字符串片段的设备标识。

Chunk Group Header 之后的 Chunk 属于该设备，直到下一个 Chunk Group Header、Operation
Index Range 或 Separator。

`segment_count` 统计片段数量，每个片段另有自己的字节长度。比较设备标识时应保留片段
边界；用于显示的点号拼接可能产生歧义，因为片段自身也可以含点号。

<div id="chunk-header"></div>

## Chunk 头部

marker 是 Chunk Header 的第一个字段：

```text
ChunkHeader :=
    byte       chunk_type
    VarString  measurement_id
    UVarInt    data_size
    byte       data_type
    byte       compression_type
    byte       encoding_type
    int64      chunk_ordinal?
    int32      chunk_authentication_size?
    byte       chunk_authentication[chunk_authentication_size]?
```

最后三个字段只出现在 Page AEAD 文件中，由文件加密头决定是否存在；Chunk marker
本身不变。序号、认证证明及校验规则见[AEAD Chunk 头部](Encryption.md#aead-chunk-header)。
这些后缀字节和其他 Header 字段一样，不计入 `data_size`。

Chunk Header 不包含 Statistics 字段。第一个 Page Header 紧跟在 Chunk Header 之后；
当该 Page 非空且 Chunk 包含多个 Page 时，其 Statistics 位于两个长度字段之后，因此这些
Statistics 字节在物理上紧邻 Chunk Header，但属于 Page Header。Chunk 级 Statistics 保存在
文件尾的 Chunk Metadata 中；单 Page Chunk 不保存 Page Statistics，改用 Chunk 或序列级
Statistics。详见[数据页](README.md)和[统计信息](../Metadata.md#statistics)。

`data_size` 是 Chunk 中全部 Page Header 和已保存 Page 数据的总长度，不包括 Chunk
Header。读取器从 Chunk Header 结束处读取恰好 `data_size` 字节。

若 `H` 是 Chunk Header 后第一个字节的偏移，则下一结构从以下位置开始：

```text
next_structure_offset = H + data_size
```

所有 Page Header 的序列化长度与相应 `compressed_size` 载荷长度之和必须等于
`data_size`。因此，该值既是快速跳过 Chunk 的长度，也是校验 Page 不越界的边界。

对齐 Time Chunk 的 `measurement_id` 是空字符串，`data_type` 是 `VECTOR (0x06)`。
对齐 Value Chunk 和非对齐 Chunk 的 `data_type` 是测量值类型。

对齐数据按 Chunk 连续存储，不按 Time Page、Value Page 交替存储。Time Chunk 保存完整
的 Time Page 序列，每个 Value Chunk 保存自己的 Value Page 序列，Page 通过序号对应。

“对齐 Chunk 集”指一个 Time Chunk 及跟随它的 Value Chunk，并没有单独的集合头。
它们之间有以下关系：

- Time Chunk 位于使用该时间列的 Value Chunk 之前。
- 每个 Value Chunk 标识一个测量；Time Chunk 使用空 `measurement_id` 和物理类型标记
  `VECTOR (0x06)`。
- Value Page `i` 覆盖 Time Page `i` 的逻辑位置。
- 某个测量在该集合中完全没有数据时，可以不存在对应 Value Chunk；已存在的对齐 Value
  Chunk 也可以具有 `data_size = 0`。

非对齐 Chunk 的每个逻辑数据点都包含时间戳和值，因此每个 Page 解码后的时间流和值流
必须具有相同的元素数量。

## Chunk 扫描状态机

流式读取器或恢复读取器可以使用以下状态机扫描数据区：

1. 校验文件前缀，检查偏移 `7`，并消费可选的 `0x06` 加密头。在解析 Chunk 前确定
   provider/profile，再从 `data_start = 7 + E` 读取数据区第一个标记；无加密头时 `E = 0`。
   后续或重复出现的 `0x06` 均无效。
2. 遇到 `0x00` 时读取 DeviceID，并进入对应 Chunk Group。
3. 遇到 Chunk 标记时读取完整 Header，包括模式要求的 AEAD 后缀，并在使用 Chunk 身份
   之前验证位置证明；可以解析 Page，也可以直接向后移动 `data_size` 字节。
4. 遇到 `0x04` 时消费后续 16 字节 Operation Index Range。
5. 遇到 `0x02` 时停止；下一个字节属于索引元数据区。
6. 顶层标记无法识别，或声明边界超出可用字节时，判定文件不完整或损坏。

<div id="operation-index-range"></div>

## 操作索引范围

```text
OperationIndexRange :=
    byte   marker = 0x04
    int64  min_operation_index
    int64  max_operation_index
```

这条可选记录用于把已刷出的数据批次与存储引擎的操作日志关联起来。两个值分别是该批次
涉及的最小和最大操作序号，可用于协调检查点、快照、备份和恢复，与时间戳或文件偏移量
无关。

通用写入器可以省略这条记录。读取器即使不使用操作索引，也要完整读过两个 `int64`，
再读取下一个标记。一个文件可以先后刷入多个批次，因此也可以出现多条这样的记录。
