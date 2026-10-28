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

# 扩展机制

TsFile 主要通过三种方式增加能力：分配新的类型或编解码器编号、增加 Footer 属性，或者
定义新的文件布局。选择哪种方式，取决于读取器在不理解新能力时，是否仍能找到下一个
结构的边界。

Footer 属性容易确定边界：每个属性由 `VarString` 键和可空的 `VarBinary` 值组成，
读取器无需解释值的内容，就能保留未知属性。Chunk 也有明确的字节长度，扫描器即使不支持
其中的编解码器，仍可以跳过。数据区标记没有统一的长度字段，遇到未知标记时就不能套用
同样的做法。

## 安全演进规则

兼容的扩展应保留已有字段的含义和边界：

- 新类型和编解码器使用未分配的编号，已分配编号的含义保持不变。
- 新编解码器说明支持的数据类型、字节布局、输出长度及错误处理方式。
- 沿用现有的字节序和长度约定。
- 解码目标数据必须使用、但读取器不支持的能力，应明确报错。

如果直接在 Header 中增加字节，却没有规则选择新布局，旧读取器预期的后续字段位置就会
改变。这类修改需要新的版本号，或明确的扩展机制。

Page AEAD 使用了后一种方式：`0x06` 文件加密头决定额外的 Chunk Header 字段和
profile 指定的 Page 帧格式，文件版本字节仍为 `0x04`。旧 v4 读取器可能拒绝这些文件。
[加密](DataPages/Encryption.md)说明了其布局和能力要求；仅支持基础格式版本还不够。

## 属性与语义

一个属性可以被跳过，不代表它的含义总能被忽略。描述性属性可以是可选的；如果属性会
改变 Page 字节的解码方式，就需要明确规定读取器应如何支持。读取器无法执行必要的加密
处理或其他解释规则时，应报告错误，不能按默认方式继续解码。
