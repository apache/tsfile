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

# 空值

非对齐时间序列只保存实际存在的 `(timestamp, value)` 点，Page 内没有 null 标记。

对齐值列通过 Value Page 的位图保存 null：

```text
AlignedValuePagePayload :=
    int32  position_count
    byte   present_bitmap[ceil(position_count / 8)]
    byte   encoded_non_null_values[remaining bytes]
```

位置 `i` 对应 `present_bitmap[i / 8]` 的位 `7 - (i mod 8)`：

- 位为 1：该位置有值。
- 位为 0：该位置为 null。

值编码流只保存位图中为 1 的值，顺序与位置顺序一致。`position_count` 等于对应
Time Page 的时间戳数量。

例如，`position_count = 10` 且位图为 `10110000 01000000` 时，位置 `0`、`2`、`3` 和
`9` 有值。值解码器因此产生 4 个值并放到这些位置，其余 6 个位置为 null。最后一个
字节中未使用的低位不对应逻辑位置，应当忽略。

非空 Value Page 满足以下约束：

```text
bitmap_bytes        = ceil(position_count / 8)
decoded_value_count = popcount(present_bitmap 的 position_count 个有效位)
position_count      = 对应 Time Page 的 decoded_timestamp_count
```

任何不相等都表示结构损坏；读取器不能根据位图尾部的填充位或多余编码值推导新位置。

如果整个 Value Page 都是 null，Page 只写一个值为 0 的 `uncompressed_size`：

```text
EmptyPage := UVarInt(0)
```

空 Page 不保存 `compressed_size`、Statistics、位图或值流。其位置数量由对应的
Time Page 确定。

缺少 Value Chunk 与空 Value Page 的含义不同。缺少 Chunk 表示该 Chunk Group 中没有
该测点的物理列；空 Page 表示该值列存在，并维持 Page 之间的位置对应关系，但在这个
区间的所有位置均为 null。
