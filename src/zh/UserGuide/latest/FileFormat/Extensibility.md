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

TsFile v4 在以下位置使用已编号或自描述的扩展点：

- 数据类型、编码和压缩方式使用单字节编号。
- `TsFileMetadata` 末尾保存长度前缀的字符串属性 Map。
- 文件版本字节用于选择完整的文件布局。

新数据类型、编码或压缩方式需要使用新的编号；已有编号的线上含义保持不变。

Footer 属性由键和值两个 `VarString` 组成。读取器可以在不改变其他 Footer 字段边界的
情况下读取未识别属性。

数据区 marker 没有通用长度字段。读取器无法安全跳过未知 marker，因此新增无法由现有
marker 表达的数据区结构需要新的文件版本或能够由现有结构完整界定的编码方式。

## 安全演进规则

只有旧读取器无需解释新负载就能确定其完整字节范围时，扩展才可以安全跳过。例如新的
Footer 属性值，以及位于已知 `data_size` 的 Chunk 内的新编解码器。向没有长度边界的
Header 直接追加字节并不可跳过，因为其后的所有字段都会发生位移。

在 v4 内，扩展需要：

- 分配尚未使用的编号，且不复用任何已有值；
- 保持现有结构的字节序、长度解释和包含边界；
- 为新编码规定可用的物理数据类型；
- 为新编解码器规定输出长度、元素数量以及非法输入行为；
- 读取器不能解码该扩展时，必须明确报告能力缺失。

保留或未知编号不是默认值的别名。特别是，未知编码不等于 `PLAIN`，未知压缩方式不等于
`UNCOMPRESSED`，未知 marker 也不表示填充字节。

## 属性与语义

长度前缀让未知 Footer 属性在结构上可读取，但不表示该属性的语义一定可忽略。若某个
属性会改变现有字节的解释方式，就必须定义读取器如何声明和强制其支持能力。安全相关
属性的语义无法执行时，必须按失败处理。

改变顶层结构语法、重新解释已分配编号，或让旧结构边界产生歧义的变更需要新的文件版本。
