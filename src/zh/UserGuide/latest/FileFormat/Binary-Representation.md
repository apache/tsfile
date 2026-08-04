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

## Fixed-width Values

| 记号 | 字节数 | 含义 |
| --- | ---: | --- |
| `byte` | 1 | 8 位原始值 |
| `bool` | 1 | `0x00` 为 false，`0x01` 为 true |
| `int32` | 4 | 大端、有符号 32 位整数 |
| `int64` | 8 | 大端、有符号 64 位整数 |
| `float32` | 4 | 大端 IEEE 754 binary32 |
| `float64` | 8 | 大端 IEEE 754 binary64 |

## Unsigned Variable-length Integer

`UVarInt` 是无符号 LEB128 风格的 32 位变长整数。每个字节的低 7 位保存数据，最高位
为 1 表示后面仍有字节，低位组先写。

```text
value = Σ ((byte[i] & 0x7f) << (7 * i))
```

编码长度为 1 至 5 字节，并使用最短形式。

例如，十进制 `300` 拆分成从低到高的七位组 `0x2c` 和 `0x02`，线上字节为 `ac 02`。
32 位字段的读取器最多读取五个字节，并检查移位或数值溢出。写入器只产生规范的最短表示，
不会写入冗长编码。

## Signed Variable-length Integer

`VarInt` 先对有符号 32 位整数做 ZigZag 映射，再按 `UVarInt` 写入：

```text
zigzag(n) = (n << 1) XOR (n >> 31)
```

| 原值 | 线上的无符号值 | 字节 |
| ---: | ---: | --- |
| 0 | 0 | `00` |
| -1 | 1 | `01` |
| 1 | 2 | `02` |
| -2 | 3 | `03` |

## Strings and Byte Arrays

| 记号 | 格式 |
| --- | --- |
| `VarString` | `VarInt byte_length` + `byte[byte_length]` |
| `String32` | `int32 byte_length` + `byte[byte_length]` |
| `Binary32` | `int32 byte_length` + `byte[byte_length]` |

长度 `-1` 表示 null，长度 `0` 表示空字符串或空字节串。`VarString` 和 `String32`
保存 UTF-8；`Binary32` 保存原始字节。

长度统计编码后的字节数，而不是 Unicode code point 或字符数。例如，一个非 ASCII 字符的
UTF-8 字节长度可能大于 1。对于允许 null 的字段，null 和空值具有不同线上表示，读取后
必须保持区别。

## 边界与防御性解码

每个带长度字段都受更高层边界约束：字符串受所属记录约束，Page 受 Chunk 约束，Footer
对象受元数据 offset 或文件尾元数据长度约束。为长度 `N` 分配内存或创建切片前，读取器
需要验证：

```text
N >= 0                         # 文档规定的 null 哨兵除外
current_offset + N >= current_offset
current_offset + N <= enclosing_end
```

第二个比较用于检测整数回绕。读取器还应施加本地资源上限；结构上有效的长度不要求实现
分配无限内存。

固定宽度值必须具有完整字节数。截断的 `int64`、没有终止字节的 VarInt、非 null 字段的
负长度、要求字符串位置的无效 UTF-8，以及带长度记录结束后的尾随字节均属于格式错误。

## 偏移约定

元数据中的 offset 以有符号 64 位整数保存，但表示非负绝对字节位置。offset 指向所命名
结构的第一个字节，例如 `offset_of_chunk_header` 指向 Chunk marker，`meta_offset` 指向
`0x02` separator。长度不是隐式 offset；将 offset 与长度相加时必须检查溢出，并验证结果
不超过文件或所属记录边界。
