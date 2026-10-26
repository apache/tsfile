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

# 加密

写入时先压缩 Page，再加密；读取时先解密，再解压。Header、统计信息、Schema 和索引
仍然可见。TsFile v4 支持两种加密机制，格式版本字节都为 `0x04`：

| 模式 | 判断依据 | Chunk 和 Page 布局 |
| --- | --- | --- |
| 明文 | 没有文件加密头，旧版级别为 `0` 或不存在 | 基础 Chunk Header，Page 载荷不加密 |
| 旧版加密 | 没有文件加密头，旧版级别为 `1` 或 `2` | 基础 Chunk Header，按指定算法加密载荷 |
| Page AEAD | 偏移 `7` 处有 `0x06` 文件加密头 | 扩展 Chunk Header，按 profile 定义认证和加密 Page |

解析 Chunk 之前，先检查文件加密头。AEAD 文件的旧版 Footer 属性仍写为未加密状态，
实际模式以加密头为准。

写入和读取的顺序为：

```text
编码后的 Page 载荷 -> 压缩 -> 加密 -> stored_data
stored_data -> 按需认证解密 -> 解压 -> 值解码
```

<div id="file-encryption-header"></div>

## 文件加密头

AEAD 文件在七字节魔数/版本前缀之后、任何 Chunk Group 或操作索引记录之前，保存一个
`FileEncryptionHeader`：

```text
FileEncryptionHeader :=
    byte     marker = 0x06
    UVarInt  payload_size
    byte     header_version = 0x01
    VarString provider_id
    VarString profile_id
    VarString key_id
    VarString key_version
    UVarInt  file_crypto_id_size = 16
    byte     file_crypto_id[16]
    UVarInt  wrapped_data_key_size
    byte     wrapped_data_key[wrapped_data_key_size]
```

`payload_size` 从 `header_version` 计到封装密钥结束，不包含标记和长度字段自身，取值范围
为 `1..1048576`。解析必须恰好在该边界结束。四个字符串标识符均不得为 null 或空白，
文件加密 ID 恰好为 16 字节，封装数据密钥不得为空。这些二进制长度使用 `UVarInt`，
与 Footer 属性使用的可空 `VarBinary` 不同。

**Provider** 标识加密实现或服务，**profile** 则为一套固定的字节格式命名，其中包括
Page 帧、nonce 和认证标签的表示、长度映射以及 Chunk 证明长度。一个 provider 可以
提供多个 profile。文件保存这些公开标识和封装密钥，不依赖 Java 实现类名，也不保存
AEAD 的明文密钥。读取端需要相应的 provider/profile，并能取得外部密钥材料。

当前支持的头版本为 `0x01`。未知版本、字段截断、尾随字节、重复的加密头，或加密头
出现在偏移 `7` 以外的位置，均属于错误。

<div id="aead-chunk-header"></div>

## AEAD Chunk 头部

文件加密头还决定了每个 Chunk Header 的解析方式。Chunk marker 保持不变，但在
`encoding_type` 之后增加三个字段：

```text
    int64  chunk_ordinal
    int32  chunk_authentication_size
    byte   chunk_authentication[chunk_authentication_size]
```

`chunk_ordinal` 是文件内唯一分配的非负标识，包括 Time Chunk 和 Value Chunk 在内。
证明长度必须与 profile 声明一致，并位于 `1..1024`。这些字段属于 Header，均不计入
`data_size`。

认证证明把序号绑定到 Chunk 的绝对位置，使用该身份之前需要验证证明。移动 Chunk 会
改变被认证的位置，因此必须重新生成证明。它使用的关联数据按大端序排列如下：

```text
int32 domain_size = 20
byte  domain[20] = UTF8("TsFile/ChunkPosition")
int32 associated_data_version = 1
byte  file_crypto_id[16]
int64 absolute_chunk_marker_offset
int64 chunk_ordinal
```

<div id="aead-page-body-and-context"></div>

## AEAD Page 载荷与关联数据

非空 Page 的 `compressed_size` 记录整个加密载荷的长度，包括帧信息、nonce、认证标签
和填充。它大于压缩明文长度，使用 `UNCOMPRESSED` 时也是如此。Profile 可以采用固定
开销，也可以采用随输入大小变化的布局，因此需要通过 profile 的长度映射取得候选明文
长度。

Page 认证还把载荷绑定到对应的文件、Chunk 和 Page 位置。关联数据是下列字段按大端序
精确拼接的结果。`SizedBytes` 表示 `int32` 字节长度加上相应字节，文本使用 UTF-8。

```text
int32      associated_data_version = 1
SizedBytes provider_id
SizedBytes profile_id
SizedBytes key_id
SizedBytes key_version
SizedBytes file_crypto_id
int64      chunk_ordinal
int32      page_index
int32      uncompressed_size
int32      compressed_plaintext_size
```

`page_index` 在每个物理 Chunk 内从零开始，空的对齐 Value Page 也占一个序号。
全 null Value Page 仍采用[空值](../Nulls.md)章节中的单个 `UVarInt(0)` 表示，
不保存密文或 Page 认证标签。

认证成功后，候选明文长度才可信。解密输出必须等于该长度，解压后还必须恰好得到
`uncompressed_size` 字节。认证标签无效、密钥错误、profile 不受支持或长度不符时，
应报错，不能按明文重试。

<div id="legacy-file-properties"></div>

## 旧版加密属性

没有文件加密头时，读取 `TsFileMetadata` 中的以下属性：

| 属性 | 含义 |
| --- | --- |
| `encryptLevel` | `0` 不加密；`1` 直接保存数据密钥；`2` 保存被外部主密钥加密的数据密钥 |
| `encryptType` | 旧版加解密算法标识符 |
| `encryptKey` | 数据密钥或密钥封装结果的文本表示 |

这几个属性的值保存文本。级别 `1` 直接保存数据密钥；级别 `2` 先使用外部主密钥解密器
解封密钥，再解密 Page。缺少 `encryptLevel` 时按 `0` 处理；级别 `1` 和 `2` 要求存在
`encryptType` 和非空 `encryptKey`。未知级别或不可用算法均为错误。

旧版加密使用算法自身的加解密操作，没有 AEAD Chunk 后缀或 Page 上下文。
`compressed_size` 仍表示实际磁盘输出长度，仅在算法保持长度时才等于压缩明文长度。

AEAD 写入器把旧版属性设为级别 `0`、类型 `org.apache.tsfile.encrypt.UNENCRYPTED`
和空密钥，实际模式和封装密钥由文件加密头提供。这些旧版值不能用作回退到明文的依据。

## 安全边界

Page AEAD 认证非空 Page 载荷及其上下文，Chunk 证明将序号绑定到文件位置。它不认证
全部可见字段，也不提供完整文件摘要。旧版加密不一定具有认证能力。完整文件的完整性
检查见[校验和](Checksumming.md)，重建 Footer 时如何保留加密上下文见[错误恢复](Error-Recovery.md)。
