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

# 类型

Chunk Header、Timeseries Metadata 和 Table Schema 使用单字节 `data_type` 编号：

| 值 | 名称 | 物理/逻辑含义 |
| ---: | --- | --- |
| 0 | `BOOLEAN` | 布尔值 |
| 1 | `INT32` | 有符号 32 位整数 |
| 2 | `INT64` | 有符号 64 位整数 |
| 3 | `FLOAT` | IEEE 754 binary32 |
| 4 | `DOUBLE` | IEEE 754 binary64 |
| 5 | `TEXT` | 长度前缀字节串 |
| 6 | `VECTOR` | 对齐 Time Chunk 使用的物理类型标记 |
| 7 | `UNKNOWN` | 未知类型标记 |
| 8 | `TIMESTAMP` | 有符号 64 位整数；时间单位由上层约定 |
| 9 | `DATE` | `YYYYMMDD` 十进制形式的有符号 32 位整数 |
| 10 | `BLOB` | 任意字节串 |
| 11 | `STRING` | UTF-8 字符串 |
| 12 | `OBJECT` | 对象值的字节表示 |

`DATE` 的物理值示例：2026-07-27 写为十进制整数 `20260727`，不是 Unix epoch day。

数据类型决定 Statistics 的附加字段和值编码器的输入类型。具体线上值格式见
[Encodings](DataPages/Encodings.md)。

## 物理类型与逻辑类型

类型编号决定物理解码器；`TIMESTAMP` 和 `DATE` 还具有额外的逻辑解释：

- `TIMESTAMP` 与 `INT64` 具有相同的八字节物理值域。时间单位由使用该文件的协议或数据集
  提供，TsFile v4 不序列化该单位。
- `DATE` 与 `INT32` 使用相同的整数编码接口，但有效逻辑值采用十进制 `YYYYMMDD` 形式。
- `TEXT` 是带长度的字节串。`STRING` 使用相同的物理二进制容器，但具有 UTF-8 字符串
  语义；`BLOB` 是不解释的二进制数据。
- `OBJECT` 是不透明的二进制表示，其对象契约必须在文件格式之外约定。

`VECTOR` 是对齐数据共享时间列使用的物理标记，不是标量值类型，也不定义独立值流编码。
`UNKNOWN` 是哨兵值，不能作为可解码测量 Page 的有效类型。

## 类型一致性

同一序列的类型会在多个层级重复，使读取器既能从 Footer 进入，也能顺序扫描文件。这些副本
必须一致：

- 非对齐 Chunk 或对齐 Value Chunk Header 使用测量的标量 `data_type`。
- 对应 Timeseries Metadata 使用相同的 `data_type`。
- Statistics 根据该类型解析，自身不包含独立类型字节。
- 对齐 Time Chunk 使用 `VECTOR`，解码值为有符号 64 位时间戳。

改变已经分配的类型编号含义属于不兼容变更。不支持某个类型编号的读取器仍可利用
`data_size` 跳过完整 Chunk，但无法解码该序列的值或类型相关 Statistics。

## 排序与比较

整数和浮点值按数值顺序生成最小/最大 Statistics。`STRING` 的最小/最大值按序列化字节的
二进制字典序比较。`TEXT` 只保存 first 和 last，`BLOB` 与 `OBJECT` 不保存类型专有的值
Statistics。

类型提升和应用层转换不编码在 TsFile 中。读取器可以对外提供转换，但这不会改变磁盘
`data_type`，也不会改变计算 Statistics 时使用的字节和值域。
