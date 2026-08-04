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

# 布隆过滤器

TsFile v4 在 `TsFileMetadata` 中保存文件级布隆过滤器，用于判断时间序列路径是否可能
存在。布隆过滤器可以返回“肯定不存在”或“可能存在”，不能用于肯定某路径存在。

## File Format

```text
BloomFilter :=
    UVarInt byte_length
    byte    bits[byte_length]
    UVarInt bit_count?           # byte_length > 0 时存在
    UVarInt hash_function_count? # byte_length > 0 时存在
```

`byte_length = 0` 表示文件没有布隆过滤器，后两个字段不存在。

位编号 `i` 存储在 `bits[i / 8]` 的位 `i mod 8`，即每个字节从低位方向编号。尾部全零
字节不写入文件，逻辑位数由 `bit_count` 保存。

## Filter Input

过滤器输入是时间序列完整路径的 UTF-8 字节：

```text
UTF8(join(DeviceID segments, ".") + "." + measurement_id)
```

null 设备片段在路径文本中表示为 ASCII `null`。

## Hashing

哈希算法使用 MurmurHash3 x64 128 位变体。最终的两个 64 位结果相加，取低 32 位；
若结果为最小 32 位整数则使用 0，否则取绝对值并对 `bit_count` 取模。

哈希 seed 依次为：

```text
5, 7, 11, 19, 31, 37, 43, 59
```

实际使用的 seed 数量由 `hash_function_count` 指定，最大为 8。

## 查询过程

对一个精确路径进行查询时，读取器：

1. 按相同的 `DeviceID` 分片和测点规则构造路径字节；
2. 使用前 `hash_function_count` 个 seed 计算位置；
3. 检查逻辑 `bit_count` 位向量中的对应位置；
4. 任意一位为 0 时返回“肯定不存在”，否则返回“可能存在”。

“可能存在”仍需访问元数据索引确认。只有查询端使用与写入器完全相同的路径规范化方式
和 UTF-8 字节时，“肯定不存在”才可靠。

## 空间与概率

令插入路径数为 `n`、逻辑位数 `bit_count` 为 `m`、哈希函数数为 `k`，标准布隆过滤器的
近似关系为：

```text
false_positive_probability ≈ (1 - exp(-k * n / m))^k
optimal_k                  ≈ (m / n) * ln(2)
```

这些公式解释写入器在空间和准确率之间的权衡，不是额外的文件字段。读取时应以序列化的
`bit_count` 和 `hash_function_count` 为准。

## 结构校验

当 `byte_length > 0` 时，`bit_count` 必须为正数，`hash_function_count` 必须在 `1..8`
范围内，且 `byte_length` 不得超过 `ceil(bit_count / 8)`。被省略的尾部零字节在逻辑上
补零；如果位向量长度越过 `TsFileMetadata` 边界，读取器必须拒绝该结构。
