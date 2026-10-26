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

Footer 在数据区之后写入。如果写入提前中断，文件虽然无法通过索引正常打开，前面仍可能
保留一些完整的 Chunk。恢复时从文件头向后扫描，找到可以保留的结构，再据此重建缺失的
索引和 Footer。

## 完成性检查

设文件大小为 `F`，从 `[F - 10, F - 6)` 读取大端元数据长度 `L`。完成写入的文件具有
以下关系：

```text
bytes[F - 6, F)            = "TsFile"
TsFileMetadata interval    = [F - 10 - L, F - 10)
meta_offset                = offset of the top-level 0x02 separator
meta_offset < F - 10 - L
```

还要校验头部魔数和版本，并恰好解析 `L` 字节的 `TsFileMetadata`。仅有尾部魔数不够，
Footer 长度和其中的偏移量也必须能正确使用。

## 安全正向扫描

正向扫描使用与普通读取器相同的 Chunk 布局，但不依赖 Footer：

1. 校验魔数和版本，检查偏移 `7`，读取可选的 `0x06` 加密头。数据区从 `7 + E` 开始，
   `E` 为该头的总长度；不存在时为零。
2. 在结构边界读取标记。遇到 `0x00` 时读取 DeviceID，并记住当前分组。
3. 遇到 Chunk 标记时读取完整 Header，包括需要的 AEAD 后缀。在使用 Chunk 身份之前
   验证位置证明，再检查剩余文件是否容得下 `data_size` 字节。随后检查 Page，或按该
   长度完整跳过。
4. 遇到 `0x04` 时，读过两个操作索引 `int64`。
5. 遇到顶层 `0x02` 分隔符时，停止扫描数据区。

字段或载荷被截断时，在最后一个完整结构处停止。未知标记、重复或位置错误的加密头、
无效长度和认证失败都应作为错误处理，不能通过搜索下一个熟悉的字节来绕过。
Page 载荷中的 `0x02` 是数据，不能据此确定数据区结束。

只保留已经完整读取并检查过的结构。被截断 Chunk 中尚能读取的部分，不会自动成为有效
Chunk。通用边界检查见[二进制表示](../Binary-Representation.md)，Page 边界见[数据页](README.md)。

## 恢复能力边界

重建索引不能补回缺失的值，也无法识别所有结构仍有效的字节改动。保留的 Page 仍需要
相应的编解码器；已加密的数据还需要密钥。操作索引记录有助于把保留数据与外部日志关联，
但不能替代结构检查。

Page AEAD 恢复要保留原加密头、文件加密 ID、Chunk 序号和 Page 索引。仅截断文件尾并
重写 Footer 时，可以保留 Chunk 的原始偏移。移动 Chunk 需要新的位置证明；改变文件
身份、Chunk 序号或 Page 上下文时，还要按变化内容对相关数据重新认证和加密。追加写入
应从已保留的最大 Chunk 序号之后继续分配。

认证恢复需要相应的 provider 和外部密钥。AEAD 文件的旧版 Footer 字段故意设为未加密，
因此不能用它代替损坏的加密头。模式判断见[加密](Encryption.md)，格式完整性检查仍有哪些
限制，见[校验和](Checksumming.md)。
