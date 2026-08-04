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

# 元数据

TsFile 中的元数据分为两类：

- 与数据一起保存的 Chunk Header、Page Header 和 Page Statistics。
- 数据区之后的 Timeseries Metadata、Metadata Index 和 File Metadata。

Footer 元数据从 `SEPARATOR (0x02)` 之后开始。`TimeseriesMetadata` 范围与测量索引
节点可以交错出现；索引中的 offset 和 `end_offset` 决定每个范围的实际边界。所有
offset 都是从文件首字节开始计算的绝对偏移量。

元数据层级支持逐步细化的数据裁剪：

| 层级 | 标识对象 | 主要用途 |
| --- | --- | --- |
| 文件元数据 | 表及设备索引根 | 进入正确的索引树 |
| 设备索引 | 设备 | 定位一个设备的测量索引 |
| 测量索引 | 测量名称区间 | 定位 Timeseries Metadata |
| Timeseries Metadata | 一个逻辑序列 | 选择 Chunk 偏移及序列 Statistics |
| Chunk Metadata | 一个物理 Chunk | 跳转到数据并利用 Chunk Statistics 裁剪 |
| Page Header | 一个 Page | 利用 Page Statistics 裁剪并读取载荷 |

## Statistics

Page、Chunk 和时间序列级 Statistics 使用相同的类型相关结构：

```text
Statistics :=
    UVarInt  count
    int64    start_time
    int64    end_time
    ...      type-specific fields
```

`count` 是该结构覆盖的值数。非对齐序列的值数等于点数；对齐值列的 `count` 是非 null
值数；对齐时间列的 `count` 是时间戳数。

类型专有字段按以下顺序紧随公共前缀：

| 数据类型 | 类型专有字段 |
| --- | --- |
| `BOOLEAN` | `bool first`, `bool last`, `int64 true_count` |
| `INT32`, `DATE` | `int32 min`, `int32 max`, `int32 first`, `int32 last`, `int64 sum` |
| `INT64`, `TIMESTAMP` | `int64 min`, `int64 max`, `int64 first`, `int64 last`, `float64 sum` |
| `FLOAT` | `float32 min`, `float32 max`, `float32 first`, `float32 last`, `float64 sum` |
| `DOUBLE` | `float64 min`, `float64 max`, `float64 first`, `float64 last`, `float64 sum` |
| `TEXT` | `Binary32 first`, `Binary32 last` |
| `STRING` | `Binary32 first`, `Binary32 last`, `Binary32 min`, `Binary32 max` |
| `BLOB`, `OBJECT`, `VECTOR` | 无类型专有字段 |

Statistics 自身不携带数据类型编号。解析 Statistics 时，数据类型来自 Chunk Header 或
Timeseries Metadata。

`first` 和 `last` 表示值的先后顺序，不等同于最小值和最大值。对于按时间有序的序列，
它们对应覆盖区间首尾时间戳处的值。`start_time` 和 `end_time` 均包含在统计区间内。数值
`sum` 用于聚合；其中 `INT64` 的 sum 以 `float64` 保存，数值较大时可能无法精确表示每个
整数位。

Statistics 是保守的数据裁剪信息。查询时间范围与统计时间范围不相交，或者受支持的值摘要
能够证明不存在匹配值时，读取器可以跳过该结构；区间重叠本身不能证明存在匹配值。
`BLOB`、`OBJECT` 等没有最小/最大值字段的类型不能通过 Statistics 做值范围裁剪。

父层 Statistics 汇总该父层表示的全部值。序列包含多个 Chunk 时，series Statistics
覆盖各 Chunk Statistics 的并集；Chunk 包含多个 Page 时，Chunk Statistics 覆盖各 Page
Statistics 的并集。计数和时间边界必须与低层记录保持一致。

## Timeseries Metadata

每条时间序列对应一个 `TimeseriesMetadata`：

```text
TimeseriesMetadata :=
    byte        timeseries_type
    VarString   measurement_id
    byte        data_type
    UVarInt     chunk_metadata_list_size
    Statistics  series_statistics
    byte        chunk_metadata_list[chunk_metadata_list_size]
```

`timeseries_type` 的高位表示列角色，低 6 位表示 Chunk Metadata 的布局：

```text
bit 7 = 1: aligned time column
bit 6 = 1: aligned value column
low 6 bits = 0: exactly one Chunk; ChunkMetadata omits Statistics
low 6 bits = 1: more than one Chunk; every ChunkMetadata contains Statistics
```

## Chunk Metadata

当 `timeseries_type & 0x3f == 0` 时，时间序列只有一个 Chunk：

```text
ChunkMetadata := int64 offset_of_chunk_header
```

该 Chunk 的 Statistics 直接使用 `series_statistics`。

当 `timeseries_type & 0x3f != 0` 时：

```text
ChunkMetadata :=
    int64      offset_of_chunk_header
    Statistics chunk_statistics
```

`offset_of_chunk_header` 指向 Chunk marker，即 Chunk Header 的第一个字节。

`chunk_metadata_list_size` 是字节边界，不是 Chunk 数量。读取器应先计算列表结束位置，再
持续反序列化 Chunk Metadata，直到恰好到达该位置。提前结束、跨越边界或留下尾随字节均为
格式错误。

## Metadata Index

```text
MetadataIndexNode :=
    UVarInt          child_count
    MetadataEntry    child[child_count]
    int64            end_offset
    byte             node_type
```

`node_type` 决定节点层级、Entry 的编码和 Entry 指向的数据：

| 值 | 名称 | Entry 编码 | Entry 指向 |
| ---: | --- | --- | --- |
| 0 | `INTERNAL_DEVICE` | `DeviceIndexEntry` | 子设备索引节点 |
| 1 | `LEAF_DEVICE` | `DeviceIndexEntry` | 设备的测量索引根节点 |
| 2 | `INTERNAL_MEASUREMENT` | `MeasurementIndexEntry` | 子测量索引节点 |
| 3 | `LEAF_MEASUREMENT` | `MeasurementIndexEntry` | 一段 Timeseries Metadata |

Entry 格式为：

```text
DeviceIndexEntry :=
    DeviceID  first_device_id
    int64     offset

MeasurementIndexEntry :=
    VarString first_measurement_id
    int64     offset
```

每个 Entry 的 `offset` 是对应范围的起点。终点是下一个 Entry 的 `offset`；最后一个
Entry 的终点是节点的 `end_offset`。

```text
entry[0].offset <= entry[1].offset <= ... <= end_offset
```

Entry 的 key 是其覆盖范围内的第一个 key。设备索引按 `DeviceID` 片段逐段排序，null
小于非 null；短元组在公共前缀相同时排在长元组之前。测量索引按测量名词典序排序。

### 索引查找

在索引节点中查找键 `K` 时，选择 first key 小于或等于 `K` 的最后一个 Entry。所选字节
区间从该 Entry 的 `offset` 开始，在下一 Entry 的 offset 处结束；最后一个 Entry 则在节点
`end_offset` 处结束。内部节点区间包含下一级索引节点，叶子区间包含上表中对应的数据对象。

沿索引继续查找前，读取器需要验证节点：

- `child_count` 与节点内完整 Entry 数量一致。
- Entry key 按该节点的比较规则有序。
- Entry offset 单调不减，每个引用区间长度非负。
- 最后一个 Entry offset 不超过 `end_offset`。
- 子节点层级与父节点的 node type 相符。

first-key 表示允许实现在节点内部对 Entry 使用二分查找，但文件格式不规定具体内存搜索
算法。

## File Metadata

`TsFileMetadata` 位于文件尾部长度字段之前：

```text
TsFileMetadata :=
    UVarInt          table_index_count
    TableIndexRoot   table_index[table_index_count]
    UVarInt          table_schema_count
    NamedTableSchema table_schema[table_schema_count]
    int64            meta_offset
    BloomFilter      bloom_filter
    VarInt           property_count
    Property         property[property_count]

TableIndexRoot :=
    VarString         table_name
    MetadataIndexNode device_index_root

NamedTableSchema :=
    VarString    table_name
    TableSchema  schema

Property :=
    VarString key
    VarString value
```

`meta_offset` 指向数据区末尾的 `0x02` separator。`TsFileMetadata` 前没有 marker，
其起始位置由文件尾保存的 4 字节长度确定。

Map 项的物理顺序不参与文件语义。`table_index` 保存树模型和表模型的设备索引根；
`table_schema` 保存显式表模型的 schema。

文件尾四字节元数据长度位于 `TsFileMetadata` 之外。设文件长度为 `F`、元数据长度为 `L`，
读取器使用下式得到精确字节区间：

```text
file_metadata_start = F - 6 - 4 - L
file_metadata_end   = F - 6 - 4       # 半开区间结束位置
```

读取器必须恰好消费 `L` 字节。该检查可以在接受尾部魔数之前发现 Schema 不匹配或变长字段
损坏。

## Table Schema

```text
TableSchema :=
    UVarInt      column_count
    TableColumn  column[column_count]

TableColumn :=
    MeasurementSchema schema
    int32             column_category

MeasurementSchema :=
    String32       column_name
    byte           data_type
    byte           encoding_type
    byte           compression_type
    int32          property_count
    SchemaProperty property[property_count]

SchemaProperty :=
    String32 key
    String32 value
```

`MeasurementSchema` 使用 `String32`，而不是 Footer 其他位置常用的 `VarString`。

列类别编号为：

| 值 | 类别 |
| ---: | --- |
| 0 | `TAG` |
| 1 | `FIELD` |
| 2 | `ATTRIBUTE` |
| 3 | `TIME` |

列顺序具有语义，因为表记录和对齐值列按位置引用 Schema。`TAG` 列参与设备身份，`FIELD`
列保存测量值，`ATTRIBUTE` 列描述实体，`TIME` 标识时间列。列类别不能替代 `data_type`；
解释一个列时必须同时使用这两个字段。
