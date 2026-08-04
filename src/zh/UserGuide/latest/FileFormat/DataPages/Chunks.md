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

TsFile 的数据区从偏移量 7 开始，由 marker 引导的 Chunk Group、Chunk 和 Operation
Index Range 组成。

顶层语法为：

```text
DataSection := (ChunkGroup | OperationIndexRange)* Separator
ChunkGroup  := ChunkGroupHeader Chunk*
Separator   := byte(0x02)
```

`Chunk*` 组成的分组自身没有长度前缀。读取器必须根据每个 Chunk 的 `data_size` 完整消费
该 Chunk，随后读取下一个顶层标记，才能确定 Chunk Group 已结束。

```text
File
└── Chunk Group                 一个设备的一批数据
    ├── Non-aligned Chunk       自带时间流的一个测量列
    │   └── Page(s)
    └── Aligned Chunk Set       多个测量列共享时间列
        ├── Time Chunk
        │   └── Time Page(s)
        └── Value Chunk(s)
            └── Value Page(s)
```

## Markers

| 值 | 名称 | 后续结构 |
| ---: | --- | --- |
| `0x00` | `CHUNK_GROUP_HEADER` | `DeviceID` |
| `0x01` | `CHUNK_HEADER` | 多页、非对齐 Chunk |
| `0x02` | `SEPARATOR` | 数据区结束，元数据区开始 |
| `0x03` | `VERSION` | v3 起不再使用，编号保留 |
| `0x04` | `OPERATION_INDEX_RANGE` | 两个 `int64` |
| `0x05` | `ONLY_ONE_PAGE_CHUNK_HEADER` | 单页、非对齐 Chunk |
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

只有位于已知结构边界的字节才能解释为标记。编码或压缩后的 Page 载荷内部也可能出现相同
字节值，但在载荷内部不具有结构含义。

## Chunk Group Header

```text
ChunkGroupHeader :=
    byte       marker = 0x00
    DeviceID   device_id

DeviceID :=
    UVarInt    segment_count
    VarString  segment[segment_count]
```

设备标识是可空字符串片段组成的有序元组。序列化时删除末尾的 null 片段，设备标识不能
全部由 null 片段组成。`segment_count = 0` 在读取时表示只包含一个空字符串片段的设备。

Chunk Group Header 之后的 Chunk 属于该设备，直到下一个 Chunk Group Header、Operation
Index Range 或 Separator。

`segment_count` 统计序列化元组元素数量，而不是 UTF-8 字节数。每个 segment 都有独立的
长度。设备身份是有序元组本身；使用显示分隔符拼接 segment 不属于二进制格式，而且当
segment 内含该分隔符时可能产生歧义。

## Chunk Header

marker 是 Chunk Header 的第一个字段：

```text
ChunkHeader :=
    byte       chunk_type
    VarString  measurement_id
    UVarInt    data_size
    byte       data_type
    byte       compression_type
    byte       encoding_type
```

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

对齐 Chunk 集满足以下不变量：

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

1. 从偏移 7 开始读取一个顶层标记。
2. 遇到 `0x00` 时读取 DeviceID，并进入对应 Chunk Group。
3. 遇到 Chunk 标记时读取 Header；可以解析 Page，也可以直接向后移动 `data_size` 字节。
4. 遇到 `0x04` 时消费后续 16 字节 Operation Index Range。
5. 遇到 `0x02` 时停止；下一个字节属于索引元数据区。
6. 顶层标记无法识别，或声明边界超出可用字节时，判定文件不完整或损坏。

## Operation Index Range

```text
OperationIndexRange :=
    byte   marker = 0x04
    int64  min_operation_index
    int64  max_operation_index
```

该结构记录最近一次写入批次涉及的最小和最大操作索引。Operation Index 是存储引擎的
逻辑操作序号，不是时间戳、测量索引或文件偏移。它可用于 checkpoint、snapshot、backup
和恢复逻辑，以判断哪些逻辑写入已经进入文件。

对于通用写入器，该记录是可选的。没有 Operation Index 语义的写入器可以省略它，也可以
写入占位范围。不使用该信息的读取器仍必须识别 `0x04` 并跳过完整的 16 字节载荷，保证在
正确边界读取下一标记。一个文件可以包含多个此类记录，因为多个写入批次可以先后刷入同一
文件。
