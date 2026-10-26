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

# 压缩

每个 Chunk Header 使用单字节 `compression_type` 指定该 Chunk 中所有 Page 的压缩格式。

| 值 | 名称 | Page 数据格式 |
| ---: | --- | --- |
| 0 | `UNCOMPRESSED` | 原始字节 |
| 1 | `SNAPPY` | Snappy raw block |
| 2 | `GZIP` | 单个 gzip member |
| 3 | `LZO` | LZO block（C++ 实现，编译期可选启用） |
| 4 | `SDT` | 保留编号，无编解码器 |
| 5 | `PAA` | 保留编号，无编解码器 |
| 6 | `PLA` | 保留编号，无编解码器 |
| 7 | `LZ4` | LZ4 raw block |
| 8 | `ZSTD` | Zstandard frame |
| 9 | `LZMA2` | XZ container 中的 LZMA2 流 |

每个 Page 独立压缩，不跨 Page 共享压缩状态。Page Header 的 `uncompressed_size` 和
`compressed_size` 分别保存未压缩负载长度及压缩、加密后的磁盘载荷长度。Page AEAD 的
磁盘长度还包含密文帧和认证标签。

这些数值是稳定的文件格式编号，不是某个压缩库枚举中的位置。编号 `3` 到 `6` 是早期
TsFile 版本留下的历史标识符。Java 实现不定义也不实现它们，解析 Chunk Header 时
会直接拒绝这些字节。C++ 实现保留 `LZO`、`SDT`、`PAA`、`PLA` 四个标识；其中只有
`LZO` 有编解码器（编译期可选启用），`SDT`、`PAA`、`PLA` 仍是无编解码器的保留值。
读取器未提供该编解码器时，不能解码相应 Chunk。

## Page 级帧边界

设 Page 从偏移 `P` 开始，Header 占 `H` 字节，载荷就取后续恰好 `compressed_size` 字节：

```text
[P + H, P + H + compressed_size)
```

这里使用的是磁盘长度，包含加密帧信息。解密后，`UNCOMPRESSED` 已经是编码后的 Page
载荷；其他方式还需要解压。两种路径最终都应得到恰好 `uncompressed_size` 字节。
不加密或加密保持长度时，`UNCOMPRESSED` 的这两个长度相等；Page AEAD 会增加帧开销，
因而不再相等。

Page 之间不共享压缩历史。读取器可以直接跳转到目标 Page，也可以按磁盘长度跳过它，
无需先解压前面的 Page。

## 读取顺序与校验

读取器先读 Page Header，因而能在分配内存或解压前获得输入、输出边界：

```text
读取 Page Header
  -> 准确读取 compressed_size 字节
  -> 按文件加密头或旧版属性进行认证/解密
  -> 按 compression_type 解压
  -> 要求结果恰好为 uncompressed_size 字节
  -> 解码 Page 内各数据流
```

分配缓冲区之前，按[二进制表示](../Binary-Representation.md)检查长度。编解码器不可用时，
无法解压该 Chunk；扫描器直接解析原始 Header 字段后，仍可利用 `data_size` 跳过它，
即使编解码器注册表拒绝这个编号。不能把该编号当作 `UNCOMPRESSED`。
