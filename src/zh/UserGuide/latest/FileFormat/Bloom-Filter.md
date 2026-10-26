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

`TsFileMetadata` 中可选的布隆过滤器用于省去不必要的索引查找。检查到某一位为零时，
可以排除该路径；检查的位全为一时，只能判断“可能存在”，仍需查索引。这一结论依赖
查询端采用与写入端一致的路径编码和哈希方式，下面分别说明。

<div id="file-format"></div>

## 存储布局

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

<div id="filter-input"></div>

## 路径如何转为字节

写入端使用 UTF-8 时，过滤器输入是时间序列完整路径的 UTF-8 字节：

```text
UTF8(join(DeviceID segments, ".") + "." + measurement_id)
```

null 设备片段在路径文本中表示为 ASCII `null`。构建路径字节时使用的字符集不保存在
文件中。下列示例使用 UTF-8。读取器必须匹配写入端的字符集和哈希行为；无法确定时应
绕过布隆过滤器，直接查询元数据索引。

<div id="hashing"></div>

## 哈希计算

TsFile 使用 `Murmur128Hash` 所实现的 MurmurHash3 x64 128 位变体。完整的 16 字节块
使用标准的小端无符号字节组装与混合步骤；不足 16 字节的尾块与标准算法不同：**每个尾部
字节先从有符号 8 位符号扩展到 64 位，再移位；各项使用 XOR 而非 OR 合并**。

设尾块长度 `n = byte_length mod 16`，尾部字节为 `t`，按下式组装两个字。全部移位和运算
限制在 64 位；`signed64(b)` 将字节 `0x80..0xff` 映射为 `-128..-1`。

```text
k1 = XOR(signed64(t[j]) << (8 * j))       for j = 0 .. min(n, 8) - 1
k2 = XOR(signed64(t[j]) << (8 * (j - 8))) for j = 8 .. n - 1
```

空 XOR 的结果为 0。仅当 `n >= 9` 时对 `k2` 做标准尾部混合，仅当 `n >= 1` 时对 `k1`
做标准尾部混合，再执行标准 MurmurHash3 的长度 XOR 与 finalization。将最终的 `h1`
和 `h2` 按 64 位相加，取低 32 位并解释为有符号整数 `hash32`。读取现有文件时必须保持
这种有符号尾块行为；直接替换为标准 MurmurHash3 库可能让非 ASCII 路径出现假阴性。

```text
bit_index = (0 if hash32 == -2147483648 else abs(hash32)) mod bit_count
```

哈希 seed 依次为：

```text
5, 7, 11, 19, 31, 37, 43, 59
```

实际使用的 seed 数量由 `hash_function_count` 指定，最大为 8。

### UTF-8 测试向量

以下向量使用 `seed = 5` 和 `bit_count = 256`。最后一行还覆盖有符号尾块之前存在完整
16 字节块的情况。

| 路径 | 有符号 `hash32` | 位编号 |
| --- | ---: | ---: |
| `root.sg.d.s1` | -1267668613 | 133 |
| `root.设备.s1` | 1697017247 | 159 |
| `root.abcdefghijklmnop.设备.s1` | 866805081 | 89 |

对于 `root.设备.s1`，标准 MurmurHash3 在最终同样求和后得到 `1978455175`，对应位编号
`135`。读取器若检查该位，可能把文件中实际存在的路径判为不存在。

## 查询过程

对一个精确路径进行查询时，读取器：

1. 按相同的 `DeviceID` 分片和测点规则构造路径字节；
2. 使用前 `hash_function_count` 个 seed 计算位置；
3. 检查逻辑 `bit_count` 位向量中的对应位置；
4. 任意一位为 0 时返回“肯定不存在”，否则返回“可能存在”。

“可能存在”仍需访问元数据索引确认。只有查询端使用与写入器完全相同的路径规范化方式、
字符集及有符号尾块哈希时，“肯定不存在”才可靠。否则应跳过该优化并查询元数据索引。

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
