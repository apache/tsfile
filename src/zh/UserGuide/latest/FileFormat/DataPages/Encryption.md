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

加密只作用于压缩后的 Page `stored_data`。Page Header、Chunk Header、Statistics、
Metadata Index 和 File Metadata 不加密。

写入顺序为：

```text
encoded page payload
  -> compression
  -> encryption
  -> stored_data
```

读取顺序为先解密，再解压和解码。

## File Properties

`TsFileMetadata` 的属性 Map 使用以下键描述加密：

| 属性 | 含义 |
| --- | --- |
| `encryptLevel` | `0` 不加密；`1` 直接保存数据密钥；`2` 保存被外部主密钥加密的数据密钥 |
| `encryptType` | 加解密算法标识符 |
| `encryptKey` | 数据密钥或密钥封装结果的文本表示 |

Page Header 没有单独的密文长度字段。加密结果长度与输入的压缩数据长度相同，文件中的
`stored_data` 长度仍由 `compressed_size` 给出。

为保持兼容，缺少 `encryptLevel` 时按 `0` 处理。级别为 `1` 或 `2` 时，必须同时存在
`encryptType` 和非空 `encryptKey`。级别未知或算法不可用时，加密的 Page 数据不可读；
读取器不能静默地把它当作未加密数据。

## 密钥解释

级别 `1` 的 `encryptKey` 表示供 `encryptType` 使用的数据密钥。级别 `2` 的该属性表示
封装后的数据密钥：读取器先取得外部主密钥解密器并解封数据密钥，再使用
`encryptType` 解密 Page 负载。密钥派生、主密钥保存、轮换和访问控制不属于文件格式。

## 安全边界

Page 加密保护编码后样本数据的机密性，但不会隐藏文件形状、设备和测点标识、类型、
时间范围、值 Statistics、Schema、索引以及加密属性本身。应用需要把这些元数据视为
可见信息。

TsFile v4 没有为 Page 或整个文件定义通用认证标签。密文被修改后能否检测出来取决于
所选算法；[Checksumming](Checksumming.md) 中的结构检查和外部完整性建议仍然适用。

读取器应先解析加密属性和密钥材料，再分配 Page 缓冲区，并严格执行
`读取 -> 解密 -> 解压 -> 解码`。解密失败、输出长度错误、算法未知或密钥缺失都属于
对应数据的读取错误，不能退回明文处理。
