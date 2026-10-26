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

# 二进制表示

所有偏移量都从文件首字节开始计算，所有长度均以字节为单位。除非单独说明，固定宽度
整数和 IEEE 754 浮点数使用大端序，字符串使用 UTF-8。

<div id="fixed-width-values"></div>

## 定长数值

| 记号 | 字节数 | 含义 |
| --- | ---: | --- |
| `byte` | 1 | 8 位原始值 |
| `bool` | 1 | `0x00` 为 false，`0x01` 为 true |
| `int32` | 4 | 大端、有符号 32 位整数 |
| `int64` | 8 | 大端、有符号 64 位整数 |
| `float32` | 4 | 大端 IEEE 754 binary32 |
| `float64` | 8 | 大端 IEEE 754 binary64 |

<div id="unsigned-variable-length-integer"></div>

## 无符号变长整数

`UVarInt` 是无符号 LEB128 风格的 32 位变长整数。每个字节的低 7 位保存数据，最高位
为 1 表示后面仍有字节，低位组先写。

```text
value = Σ ((byte[i] & 0x7f) << (7 * i))
```

编码使用最短表示，占 1 至 5 字节。例如，十进制 `300` 可以拆成从低到高的七位组
`0x2c` 和 `0x02`，因此写成 `ac 02`。32 位解码器最多读取五个字节，同时检查数值是否
溢出；五个字节后仍未结束的序列无效。

<div id="signed-variable-length-integer"></div>

## 有符号变长整数

`VarInt` 先对有符号 32 位整数做 ZigZag 映射，再按 `UVarInt` 写入：

```text
zigzag(n) = (n << 1) XOR (n >> 31)
```

| 原值 | 编码前的无符号值 | 字节 |
| ---: | ---: | --- |
| 0 | 0 | `00` |
| -1 | 1 | `01` |
| 1 | 2 | `02` |
| -2 | 3 | `03` |

<div id="strings-and-byte-arrays"></div>

## 字符串与字节数组

| 记号 | 格式 |
| --- | --- |
| `VarString` | `VarInt byte_length` + `byte[byte_length]` |
| `String32` | `int32 byte_length` + `byte[byte_length]` |
| `Binary32` | `int32 byte_length` + `byte[byte_length]` |
| `VarBinary` | `VarInt byte_length` + `byte[byte_length]` |

对于 `VarString`、`String32` 和 `VarBinary`，长度 `-1` 表示 null，长度 `0` 表示空字符串
或空字节串；`Binary32` 不接受负长度。`VarString` 和 `String32`
保存 UTF-8；`Binary32` 和 `VarBinary` 保存原始字节。`VarBinary` 使用有符号 ZigZag
长度，不同于 [FileEncryptionHeader](DataPages/Encryption.md#file-encryption-header)
中二进制组件使用的无符号长度。

字符串长度按 UTF-8 编码后的字节数计算。例如，“温”占三个字节，其 `VarString` 长度字段
为 `VarInt(3)`，编码为 `06`。允许 null 的字段还需要区分 null 和空值，不能在读取时将
两者合并。

## 边界与防御性解码

每个带长度字段都受更高层边界约束：字符串受所属记录约束，Page 受 Chunk 约束，Footer
对象受元数据 offset 或文件尾元数据长度约束。为长度 `N` 分配内存或创建切片前，读取器
需要验证：

```text
N >= 0                         # 文档规定的 null 哨兵除外
current_offset + N >= current_offset
current_offset + N <= enclosing_end
```

使用定长整数计算偏移量时，第二个比较用于检测溢出回绕。应先检查这些边界，再分配内存，
并根据运行环境限制最大分配量：长度即使没有超出文件，也可能超过读取器能承受的范围。

固定宽度值必须具有完整字节数。截断的 `int64`、没有终止字节的 VarInt、非 null 字段的
负长度、要求字符串位置的无效 UTF-8，以及带长度记录结束后的尾随字节均属于格式错误。

## 偏移约定

元数据中的 offset 以有符号 64 位整数保存，但表示非负绝对字节位置。offset 指向所命名
结构的第一个字节，例如 `offset_of_chunk_header` 指向 Chunk marker，`meta_offset` 指向
`0x02` separator。计算结构末尾时，将起始 offset 与长度相加，并按上文检查溢出和所属结构边界。

## 如何阅读布局示例

结构示意中的 `?` 表示字段或结构可选，`*` 表示可以重复零次或多次，`[n]` 表示 n 个
元素。具体是否出现、重复多少次，由旁边的说明或已读取的长度字段决定。`remaining bytes`
表示当前载荷边界内尚未使用的字节；对于 Page，这个边界指解密、解压后的载荷末尾。
