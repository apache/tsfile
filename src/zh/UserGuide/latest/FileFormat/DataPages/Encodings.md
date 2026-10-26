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

# 编码

编码先把一串有类型的值转换为字节流，再交给压缩器处理。Chunk Header 的
`encoding_type` 保存所选编码：在 Value Chunk 和非对齐 Chunk 中，它描述值流；
在对齐 Time Chunk 中，它描述时间戳流。

| 值 | 名称 |
| ---: | --- |
| 0 | `PLAIN` |
| 1 | `DICTIONARY` |
| 2 | `RLE` |
| 3 | `DIFF` |
| 4 | `TS_2DIFF` |
| 5 | `BITMAP` |
| 6 | `GORILLA_V1` |
| 7 | `REGULAR` |
| 8 | `GORILLA` |
| 9 | `ZIGZAG` |
| 10 | `FREQ`（已弃用） |
| 11 | `CHIMP` |
| 12 | `SPRINTZ` |
| 13 | `RLBE` |
| 14 | `CAMEL` |

参考值编码器支持下列组合。一个编码已经分配了编号，并不表示所有数据类型都能使用它：

| 数据类型 | 可用编码 |
| --- | --- |
| `BOOLEAN` | `PLAIN`, `RLE` |
| `INT32`, `INT64`, `TIMESTAMP`, `DATE` | `PLAIN`, `RLE`, `TS_2DIFF`, `GORILLA`, `ZIGZAG`, `CHIMP`, `SPRINTZ`, `RLBE` |
| `FLOAT` | `PLAIN`, `RLE`, `TS_2DIFF`, `GORILLA_V1`, `GORILLA`, `CHIMP`, `SPRINTZ`, `RLBE` |
| `DOUBLE` | `PLAIN`, `RLE`, `TS_2DIFF`, `GORILLA_V1`, `GORILLA`, `CHIMP`, `SPRINTZ`, `RLBE`, `CAMEL` |
| `TEXT`, `STRING` | `PLAIN`, `DICTIONARY` |
| `BLOB`, `OBJECT` | `PLAIN` |

## 编码类别

这些编号描述把逻辑值序列转换为字节的不同方式。它们是文件格式中的固定编号，不是
某个实现内部枚举的序号。

| 类别 | 编码 | 通常利用的数据特征 |
| --- | --- | --- |
| 直接表示 | `PLAIN` | 不预测，各值独立序列化 |
| 字典 | `DICTIONARY` | 用字典引用替代重复的二进制值 |
| 游程与位打包 | `RLE`, `RLBE` | 重复值或较小的有效位宽 |
| 差分与预测 | `DIFF`, `TS_2DIFF`, `REGULAR`, `SPRINTZ` | 相邻值之间较小或较规律的变化 |
| 整数重映射 | `ZIGZAG` | 把绝对值较小的有符号整数映射为较小的无符号整数 |
| 基于异或的编码 | `GORILLA_V1`, `GORILLA`, `CHIMP`, `CAMEL` | 相邻浮点数具有稳定的前导位和尾随位 |
| 历史或专用编码 | `BITMAP`, `FREQ` | 保留的历史布局；`FREQ` 已弃用 |

不同的数据分布适合不同的编码。读取时，无论写入器为什么选择某种编码，都需要根据
`(data_type, encoding_type)` 这一组合选择解码器。

<div id="plain-encoding"></div>

## PLAIN 编码

`PLAIN` 流不保存元素个数，元素数量由 Page 上下文、时间流或 null 位图确定。各元素
连续写入：

| 数据类型 | 单个值的编码 |
| --- | --- |
| `BOOLEAN` | `0x00` 或 `0x01` |
| `INT32`, `DATE` | `VarInt` |
| `INT64`, `TIMESTAMP` | 大端序 `int64` |
| `FLOAT` | 大端序 `float32` |
| `DOUBLE` | 大端序 `float64` |
| `TEXT`, `STRING`, `BLOB`, `OBJECT` | `VarInt byte_length` + 原始字节 |

`PLAIN` 二进制值的长度非负。非对齐值流不包含 null；对齐值流的 null 由 Value Page
位图表示。

其他编码编号决定各自的值流位布局，读取器不能把未知编码按 `PLAIN` 解码。

## 解码约束

解码状态只在一个 Page 流内有效。解密和解压之后，选择对应的解码器，在流的边界内读取，
并检查值的数量是否符合 Page 上下文。处理下一个 Page 时，重新开始解码状态。

对齐数据的时间编码来自 Time Chunk Header，Value Page 每遇到一个置位的位图位置就
解码一个值。非对齐 Chunk Header 只说明值编码，时间编码来自共享配置，默认
`TS_2DIFF`；Footer 中没有序列化的 `time_encoder` 字段。详见
[配置](../Configurations.md#non-aligned-time-encoding)。

编码不受支持时，无法解码具体值。扫描器如果直接解析原始 Header 字段，仍能通过
`data_size` 找到下一个 Chunk，即使上层编解码器注册表拒绝该编号。此时不能擅自改用
`PLAIN` 或其他解码器。
