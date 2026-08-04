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

TsFile 的文件版本位于头魔数之后：

```text
6-byte magic string "TsFile"
1-byte format version
```

本文档描述版本字节 `0x04`。读取器在检查头魔数后，根据版本字节选择对应的文件布局。

v4 使用分片的 `DeviceID`，并在 `TsFileMetadata` 中保存按表划分的设备索引根和
Table Schema。v3 使用单字符串设备标识和不同的 Footer 布局，不能使用 v4 的
`DeviceID` 或 `TsFileMetadata` 结构解析。

## 读取器分派

必须在解释任何数据区 marker 之前处理版本字节。读取顺序为：

```text
校验头魔数 -> 读取版本 -> 选择该版本的完整语法 -> 解析数据区与 Footer
```

只支持 v4、但读到其他版本字节的实现必须报告“不支持的版本”。即使某个 marker 或
Footer 后缀看起来熟悉，也不能试探性地按 v4 解析，因为不同版本中结构的含义和宽度
可能不同。

## 兼容性约束

兼容性在字节格式层面定义：

- v4 读取器在支持目标数据所需全部编号的前提下，可以读取 v4 文件。
- v4 写入器输出本文定义的 v4 语法，与其编程语言或内存类无关。
- Footer 属性的键和值都有长度，可以读取并跳过未知属性。
- 数据区 marker 没有统一的负载长度，因此通常无法跳过未知 marker。
- v4 已分配的数据类型、编码、压缩方式、marker 和索引节点类型编号保持原有含义。

支持某个文件版本不代表支持所有可选编解码器或加密算法。缺少相应能力时，所属 Chunk
在结构上可能仍可跳过，但其中样本值不可解码。

项目发布系列与磁盘格式字节是两套独立版本。2.x 发布系列的产品可以读写格式字节
`0x04`；TsFile Header 不保存项目发布版本标签。
