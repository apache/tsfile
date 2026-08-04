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

# 数据页

Chunk Header 之后连续保存一个或多个 Page。Page 之间没有填充字节；Chunk Header 的
`data_size` 给出全部 Page Header 和 Page 数据的总长度。

## Page Header

非空 Page 的格式为：

```text
Page :=
    UVarInt    uncompressed_size
    UVarInt    compressed_size
    Statistics statistics?       # 由 Chunk marker 决定
    byte       stored_data[compressed_size]
```

`uncompressed_size` 是解密、解压后的 Page 负载大小，`compressed_size` 是文件中保存的
Page 负载大小。当 `compression_type = UNCOMPRESSED` 时，两个值相等。

Chunk marker 的低 6 位决定 Page Header 是否包含 Statistics：

- `0x01`：Chunk 有多页，每个非空 Page Header 包含 Statistics。
- `0x05`：Chunk 只有一页或没有 Page，Page Header 不包含 Statistics。

Statistics 的二进制格式见 [Metadata](../Metadata.md#statistics)。

因此，无需检查 Page 载荷即可根据 Chunk marker 选择 Header 结构：

| Chunk marker 低位 | Page 数量 | Page Statistics |
| ---: | --- | --- |
| `0x01` | 多于一个 | 每个非空 Page Header 中存在 |
| `0x05` | 零个或一个 | 不存在 |

若 Page 起始偏移为 `P`，两个长度字段和可选 Statistics 的序列化总长度为 `h`，则磁盘
载荷占用半开区间 `[P + h, P + h + compressed_size)`；如果还有下一个 Page，它从
`P + h + compressed_size` 开始。该结束位置不得超过所属 Chunk 的边界。

解密并解压后，载荷必须恰好产生 `uncompressed_size` 字节。即使压缩库报告成功，实际输出
长度不一致时读取器也必须拒绝该 Page。

## Page Encoding

写入非空 Page 时，数据依次经过：

```text
logical values
  -> value encoding
  -> assemble uncompressed page payload
  -> compression
  -> optional encryption
  -> stored_data
```

读取顺序相反：先解密，再解压，最后执行值解码。

编码与压缩相互独立。编码把有类型的值转换成未压缩字节流，压缩把该结果视为普通字节。
Page 边界会重置值编码器/解码器和压缩器/解压器，状态不会跨 Page 延续。

## Non-aligned Page

非对齐 Page 的解压后负载为：

```text
NonAlignedPagePayload :=
    UVarInt  time_stream_size
    byte     encoded_time_stream[time_stream_size]
    byte     encoded_value_stream[remaining bytes]
```

一个非对齐 Page 同时保存时间和值。它没有独立的 Time Page 和 Value Page：Page 先保存
完整时间流，再保存完整值流，而不是按 `timestamp, value` 逐点交替写入。两个流包含
相同数量的元素，并按位置组成 `(timestamp, value)`。

设未压缩载荷大小为 `U`，序列化 `time_stream_size` 字段本身占 `v` 字节，则值流长度为：

```text
value_stream_size = U - v - time_stream_size
```

结果必须非负。解码后，两个流的解码器都必须恰好到达各自边界，并产生相同的数据点数量。
存在尾随字节、提前结束或数量不一致时，该 Page 格式错误。

非对齐 Chunk Header 只记录值流的 `encoding_type`。时间流编码见
[Configurations](../Configurations.md#non-aligned-time-encoding)。

## Aligned Time Page

对齐 Time Page 的解压后负载完全由时间编码流组成：

```text
AlignedTimePagePayload := byte encoded_time_stream[remaining bytes]
```

时间编码由 Time Chunk Header 的 `encoding_type` 指定。

解码得到的时间戳数量就是该 Page 的逻辑位置数。所有对应对齐 Value Page 的 bitmap 宽度
和 null 重建都由该数量决定。

## Aligned Value Page

非空对齐 Value Page 的解压后负载为：

```text
AlignedValuePagePayload :=
    int32  position_count
    byte   present_bitmap[ceil(position_count / 8)]
    byte   encoded_non_null_values[remaining bytes]
```

同一对齐集合中，第 `i` 个 Value Page 对应第 `i` 个 Time Page。null 位图和全 null
空 Page 的格式见 [Nulls](../Nulls.md)。

`position_count` 必须等于对应 Time Page 解码得到的时间戳数量。bitmap 必须恰好具有
`ceil(position_count / 8)` 字节；最后一个字节中未使用的低位不表示逻辑位置。值解码器只
对置位位置解码，并且必须恰好产生 bitmap 置位数个值。

## Page 选择与跳过

Page Statistics 允许读取器在读取磁盘 Page 数据前判断时间和值谓词。只有当 Statistics
能够证明没有数据点满足谓词时，才能跳过 Page。Statistics 是保守摘要：区间重叠只表示
“可能匹配”，并不表示“一定匹配”。

单 Page Chunk 的 Page Header 不包含 Statistics。读取器可以在跳转到 Chunk 前使用文件尾
中的 Chunk 或 Timeseries Statistics。某种数据类型的 Statistics 不包含查询所需摘要时，
读取器必须解码 Page 或依赖其他谓词，不能据此排除数据。

## 全 null 对齐 Value Page

一个对齐值列在某个 Time Page 的全部位置上都可以为 null。该情况只写入一个无符号变长
整数零：

```text
EmptyAlignedValuePage := UVarInt(0)  # uncompressed_size
```

该零之后没有 `compressed_size`、Page Statistics、bitmap 或磁盘载荷。位置数继承自对应
Time Page。只有在对齐 Page 序列能够提供外部位置数时，才允许使用这种表示。

## Data Page Details

- [Chunks](Chunks.md)
- [Compression](Compression.md)
- [Encodings](Encodings.md)
- [Encryption](Encryption.md)
- [Checksumming](Checksumming.md)
- [Error Recovery](Error-Recovery.md)
