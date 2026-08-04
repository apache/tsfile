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
| 3 | `LZO` | LZO block |
| 7 | `LZ4` | LZ4 raw block |
| 8 | `ZSTD` | Zstandard frame |
| 9 | `LZMA2` | XZ container 中的 LZMA2 流 |

每个 Page 独立压缩，不跨 Page 共享压缩状态。Page Header 的 `uncompressed_size` 和
`compressed_size` 分别保存压缩前和压缩后的负载长度。

这些数值是稳定的文件格式编号，不是某个压缩库枚举中的位置。v4 未分配编号 `4` 到
`6`。`LZO` 作为历史编号保留；读取器未提供该编解码器时，不能解码相应 Chunk。

## Page 级帧边界

若 Page 起始偏移量为 `P`，其序列化 Page Header 占 `H` 字节，则磁盘负载恰好位于以下
左闭右开区间：

```text
[P + H, P + H + compressed_size)
```

使用 `UNCOMPRESSED` 时，`compressed_size` 等于 `uncompressed_size`，这些字节已经是编码
后的 Page 负载。使用其他方式时，解压结果必须恰好包含 `uncompressed_size` 字节。结果
更短、更长，或编解码器要求读取上述区间外的字节，都表示 Page 无效。

每个 Page 都是独立的压缩单元。因此，读取器可以先用 Page Statistics 排除不需要的
Page，再直接前进 `compressed_size` 字节，而不必初始化解压器。随机访问也不依赖前一个
Page 的字典或历史状态。

## 读取顺序与校验

读取器先读 Page Header，因而能在分配内存或解压前获得输入、输出边界：

```text
读取 Page Header
  -> 准确读取 compressed_size 字节
  -> 文件属性要求时解密
  -> 按 compression_type 解压
  -> 要求结果恰好为 uncompressed_size 字节
  -> 解码 Page 内各数据流
```

实现应在分配内存前拒绝会导致偏移量运算溢出或超过资源限制的长度。不支持某个
`compression_type` 时，读取器不能解码相应 Chunk，但仍可通过 `data_size` 跳过该
Chunk；不能把未知方式当作 `UNCOMPRESSED`。
