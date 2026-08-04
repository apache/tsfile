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

# 错误恢复

完成写入的 TsFile 包含头魔数、版本字节、`0x02` separator、Footer 元数据、4 字节
`TsFileMetadata` 长度和尾魔数。没有 Footer 长度或尾魔数的文件是未完成文件。

恢复读取器可以从偏移量 7 开始按 marker 扫描数据区，并利用 Chunk Header 的
`data_size` 跳过完整 Chunk。在 Chunk 内部，所有 Page 的序列化长度之和等于
`data_size`。

扫描器不能把 Page 数据中出现的任意 `0x02` 当作 separator；只有在完整解析或跳过
前一个 marker 所界定的结构后，才可以读取下一个 marker。

## 完成性检查

设文件大小为 `F`，区间 `[F - 10, F - 6)` 中保存的大端 Footer 长度为 `L`。普通的
元数据优先读取开始前，完整文件必须满足：

```text
bytes[F - 6, F)         = "TsFile"
TsFileMetadata 区间     = [F - 10 - L, F - 10)
meta_offset             = 顶层 0x02 separator 的偏移量
meta_offset < F - 10 - L
```

头魔数和版本也必须有效，并且 `TsFileMetadata` 解码器必须恰好消费 `L` 字节。仅有合法的
尾魔数并不能证明文件完整。

## 安全正向扫描

恢复扫描器可以识别数据区中最长的结构完整前缀：

1. 校验头魔数和版本，把游标置于偏移量 `7`；
2. 仅在已知顶层边界读取一个 marker；
3. 遇到 Chunk Group Header（`0x00`）时，解码完整 `DeviceID` 并保存组上下文；
4. 遇到 Chunk marker 时，解码完整 Chunk Header，校验 `cursor + data_size`，再跳过该
   区间或校验其中所有 Page 边界；
5. 遇到 Operation Index Range（`0x04`）时，准确消费两个大端 `int64`；
6. 遇到顶层 separator（`0x02`）时正常结束；字段或负载不完整时，在最后一个已知安全
   边界停止。

最后安全边界是一个完整定界结构的末尾，不是“最后一个碰巧可读的字节”。恢复工具可以
使用该边界之前的完整 Chunk 重建索引和新 Footer，但不能臆造或部分解码被截断的 Chunk。

检测到以下情况时，文件结构不完整或已损坏：

- Chunk 的 `data_size` 超出文件或下一个结构边界。
- Page 的长度字段超出 Chunk 边界。
- Footer 长度无法定位到完整的 `TsFileMetadata`。
- `meta_offset` 没有指向 `0x02` separator。
- 索引 offset 位于文件之外或指入结构中间。

其他失败还包括：未知顶层 marker、未终止的 VarInt 或 VarString、计算结束偏移量时整数
溢出、Page 解码结果数量错误，以及对齐 Time/Value Page 不匹配。

## 恢复能力边界

恢复只能重建结构，不能补回缺失信息。完整的压缩或加密 Page 仍需要其声明的编解码器，
必要时还需要外部密钥。由于没有文件级校验和，结构仍合法的字节替换可能无法与原始数据
区分。Operation Index Range 可以帮助存储引擎把保留下来的前缀与外部操作日志关联，
但不能替代对 Chunk 和 Page 边界的校验。

TsFile v4 没有通用校验和；相关限制见 [Checksumming](Checksumming.md)。
