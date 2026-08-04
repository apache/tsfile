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

Chunk Header 使用单字节 `encoding_type` 指定值流编码。对齐 Time Chunk 的该字段指定
时间流编码；非对齐 Chunk 的该字段只指定值流编码。

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

类型与编码组合如下：

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
| 基于异或的浮点编码 | `GORILLA_V1`, `GORILLA`, `CHIMP`, `CAMEL` | 相邻浮点数具有稳定的前导位和尾随位 |
| 历史或专用编码 | `BITMAP`, `FREQ` | 保留的历史布局；`FREQ` 已弃用 |

最合适的编码取决于数据分布。文件不记录写入器选择某种编码的原因；读取器必须同时
根据 `data_type` 和 `encoding_type` 选择解码器。

## Plain Encoding

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

编码状态只在一个解压后的 Page 流内有效。Page 解压完成后，读取器需要：

1. 根据 Chunk 的 `data_type` 和 `encoding_type` 创建解码器；
2. 解码出 Page 上下文所要求的准确逻辑元素数量；
3. 验证解码过程既没有越过 Page 边界，也没有留下不完整的编码块；
4. 在处理下一个 Page 前丢弃当前解码状态。

非对齐 Page 的时间流使用文件元数据中 `time_encoder` 指定的独立编码，
`encoding_type` 只作用于值流。对齐 Time Page 中，`encoding_type` 作用于时间流；对齐
Value Page 会先移除 null 位置再编码，因此值解码器必须恰好产生位图中置位的元素数。

读取器不支持某种编码时，不能解码对应 Chunk，但仍可借助 `data_size` 跳过其字节范围；
退回到另一种解码器并不合法。
