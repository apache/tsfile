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

TsFile v4 没有通用的 Page、Chunk 或完整文件校验和。一个看起来已经写完的文件，仍可能
包含损坏的值，因此不能把头尾魔数当作内容校验和。

## 结构校验能够发现什么

长度、偏移量和计数可以帮助发现不完整或格式错误的结构。例如，Page 超出 Chunk 边界、
Footer 长度超出文件，或者解压结果长度不符，都可以判为错误。具体检查见
[二进制表示](../Binary-Representation.md)和[数据页](README.md)。

这些检查无法发现所有改动。一个值被修改后，字节数可能保持不变，周围的结构边界也可能
仍然有效。即使另有校验和，读取时仍需要进行结构检查。

## 静态存储与传输完整性

如果需要检测文件任意位置的变化，可以维护外部校验和，或使用存储系统提供的完整性检查。
如果还需要防止恶意修改，则应使用带密钥的消息认证码或数字签名，并在文件之外保存可信
密钥或参考值。校验范围应覆盖完整字节序列，包括 Footer 长度和尾部魔数。

Page AEAD 的保护范围更具体：它认证非空 Page 载荷及上下文，并将 Chunk 序号绑定到文件
位置，但不覆盖全部可见 Header、Statistics、索引或 Footer 元数据。空的对齐 Value Page
没有 Page 认证标签，旧版加密也不一定认证密文。具体范围见[加密](Encryption.md)。
