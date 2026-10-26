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

# 格式版本

格式版本是头部魔数之后的一个字节：

```text
6-byte magic string "TsFile"
1-byte format version
```

本文介绍 `0x04`。格式版本和项目发布版本含义不同：2.x 系列的一个发布版本可以使用
v4 文件格式。文件头不保存项目的发布版本标签。

v4 使用分段 DeviceID，并在 `TsFileMetadata` 中保存按表划分的设备索引根和 Table Schema。
v3 使用单字符串设备标识和不同的 Footer。读取这两种文件时，需要分别使用对应版本的
结构定义。

## 读取器分派

先读取版本，再解释数据标记并确定加密模式：

```text
校验头部魔数 -> 读取版本 -> 检查可选加密头
  -> 确定加密模式/profile -> 按对应布局解析数据区和 Footer
```

版本不受支持时，应在解析数据之前报告错误。某个标记或尾部魔数看起来熟悉，并不能说明
文件其他部分也采用读取器预期的布局。

## 兼容性约束

支持格式版本之后，还要支持目标数据使用的类型、编解码器和加密能力。例如，v4 的
Page AEAD 文件在偏移 `7` 处有 `0x06` 加密头，用它选择扩展的 Chunk Header 和 Page
载荷 profile。版本字节仍为 `0x04`；不具备该能力的 v4 读取器应明确报告不支持。

已分配编号的含义保持不变。未知 Footer 属性的键和值都有长度，读取器可以读过相应
字节区间；未知数据标记没有统一的载荷长度，通常不能安全跳过。这些边界见
[扩展机制](Extensibility.md)，AEAD 布局见[加密](DataPages/Encryption.md)。
