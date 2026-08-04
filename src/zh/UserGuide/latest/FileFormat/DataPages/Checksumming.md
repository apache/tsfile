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

# 校验和

TsFile v4 不保存通用的 Page 校验和、Chunk 校验和或文件校验和。

头尾魔数、长度字段、marker、offset 和结构边界可以发现部分截断或结构损坏，但不提供
内容校验或密码学完整性保证。

## 结构校验能够发现什么

严格的读取器可以通过以下约束发现大量非法布局：

- 头尾魔数和版本字节完整存在；
- Footer 长度与 `meta_offset` 能解析到合法边界；
- 每个 `data_size`、`compressed_size`、字符串长度和元数据列表长度都位于所属结构内；
- 解压结果恰好包含 `uncompressed_size` 字节；
- 解码元素数量、对齐位图、Statistics 和索引范围内部一致。

即使文件另有外部校验和，这些检查仍然必要。它们能够发现截断和许多意外改动，但某些
字节替换不会改变长度与边界，因而仍可能不被发现。

## 静态存储与传输完整性

需要检测数据损坏的系统应为完整文件维护外部校验和或对象存储完整性字段。需要防止
恶意修改时，应使用带认证的摘要或签名，并把密钥或可信摘要保存在 TsFile 之外。外部
校验值必须覆盖完整文件的精确字节序列，包括 Footer 长度和尾魔数。

Page 负载加密是另一项独立能力。除非所选加密算法明确认证密文，否则仅加密既不会为
TsFile 添加校验和，也不会验证未加密的 Header 与元数据。
