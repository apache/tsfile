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

# TsFile Document
<pre>
___________    ___________.__.__          
\__    ___/____\_   _____/|__|  |   ____  
  |    | /  ___/|    __)  |  |  | _/ __ \ 
  |    | \___ \ |     \   |  |  |_\  ___/ 
  |____|/____  >\___  /   |__|____/\___  >  version 1.0.0
             \/     \/                 \/  
</pre>
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.tsfile/tsfile-parent/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.tsfile")

## 简介

TsFile是一种为时间序列数据设计的列式存储文件格式，它支持高效压缩、高读写吞吐量，并且兼容多种框架，如Spark和Flink。TsFile很容易集成到物联网大数据处理框架中。

时序数据即时间序列数据，是指带时间标签（按照时间的顺序变化，即时间序列化）的数据，其来源多元、数据量庞大，可广泛应用于物联网、智能制造、金融分析等领域。在数据驱动的当下，时序数据的重要性不言而喻。

尽管时序数据如此普遍且重要，但长期以来，时序数据的管理都缺乏标准化的文件格式。TsFile 的出现为用户管理时序数据提供了统一的文件格式。

[点击查看更多](https://www.timecho.com/archives/tian-bu-shi-chang-kong-bai-apache-tsfile-ru-he-chong-xin-ding-yi-shi-xu-shu-ju-guan-li)


## TsFile 特性

TsFile 通过自研实现了时序数据高效率管理、高灵活传输，并支持多类软件深度集成。其特性包括：

- 时序模型：专门为物联网设计的数据模型，每个时间序列与特定设备相关联，所有设备通过分层结构相互连接；

- 跨语言独立使用：可以使用多种语言的 SDK 直接读写 TsFile，使得一些轻量级的数据读写场景成为可能。

- 高效写入和压缩：为时间序列量身定制的列式存储格式，将数据按设备进行组织，并保证每个序列的数据连续存储，最小化存储空间。相比 CSV，压缩比可提升 90% 以上。

- 高查询性能：通过设备、物理量和时间维度索引，TsFile 实现了基于特定时间范围的时序数据快速过滤和查询。相比通用文件格式，查询吞吐可提升 2-10 倍。

- 开放集成：TsFile 是时序数据库 IoTDB 的底层存储文件格式，可与 IoTDB 形成可插拔的存算分离架构。TsFile 支持与 Spark、Flink 等大数据软件建立无缝生态集成，从而确保跨不同数据处理环境的兼容性和互操作性，实现时序数据跨生态深度分析。

## TsFile 基本概念

TsFile 可管理多个设备的时序数据。每个设备可具有不同的物理量。

每个设备的每个物理量对应一条时间序列。

TsFile 数据模型（Schema）定义了所有设备物理量的集合，如下表所示（m1 ~ m5）

| Time | deviceId | m1 | m2 | m3 | m4 | m5 |
|------|----------|----|----|----|----|----|
| 1    | device1  | 1  | 2  | 3  |    |    |
| 2    | device1  | 1  | 2  | 3  |    |    |
| 3    | device2  | 1  |    | 3  | 4  | 5  |
| 4    | device2  | 1  |    | 3  | 4  | 5  |
| 5    | device3  | 1  | 2  | 3  | 4  | 5  |

其中 Time 和 deviceId 为内置字段，无需定义，可直接写入。

## TsFile 设计原理

### 文件结构

下为 Apache TsFile 的文件结构。

- Page：一段连续的时序数据，存储的基本单元，按时间升序排序，时间戳和值各有单独的列进行存储。

- Chunk：由同一序列的多个连续的 Page 组成，一个文件同一个序列可以存储多个 Chunk。

- ChunkGroup：由一个设备的一至多个 Chunk 组成，多个 Chunk 可共享一列时间存储（多值模型）。

- Index：TsFile 末尾的元数据文件包含序列内部时间维度的索引和序列间的索引信息。

![TsFile 文件结构](https://alioss.timecho.com/upload/Apache%20TsFile%20%E5%8F%91%E5%B8%83%E5%9B%BE3-20240315.png)

### 编码和压缩

TsFile 通过采用二阶差分编码、游程编码（RLE）、位压缩和 Snappy 等先进的编码和压缩技术，优化时序数据的存储和访问，并支持对时间戳列和数据值列进行单独编码，以实现更好的数据处理效能。

其独特之处在于编码算法专为时序数据特性设计，聚焦在时间属性和数据之间的相关性。

TsFile、CSV 和 Parquet 三种文件格式的比较

| Dimension       | TsFile       | CSV   | Parquet |
|-----------------|--------------|-------|---------|
| Data Model      | IoT          | Plain | Nested  |
| Write Mode      | Tablet, Line | Line  | Line    |
| Compression     | Yes          | No    | Yes     |
| Read Mode       | Query, Scan  | Scan  | Query   |
| Index on Series | Yes          | No    | No      |
| Index on Time   | Yes          | No    | No      |

基于对时序数据应用需求的深刻理解，TsFile 有助于实现时序数据高压缩比和实时访问速度，并为企业进一步构建高效、可扩展、灵活的数据分析平台提供底层文件技术支撑。

| 数据类型    | 推荐编码       | 推荐压缩算法 |
|---------|------------|--------|
| INT32   | TS_2DIFF   | LZ4    |
| INT64   | TS_2DIFF   | LZ4    |
| FLOAT   | GORILLA    | LZ4    |
| DOUBLE  | GORILLA    | LZ4    |
| BOOLEAN | RLE        | LZ4    |
| TEXT    | DICTIONARY | LZ4    |

更多类型的编码和压缩方式参见[文档](https://iotdb.apache.org/zh/UserGuide/latest/Basic-Concept/Encoding-and-Compression.html)

## 开发 TsFile

[Java](./java/tsfile/README-zh.md#开发)

[C++](./cpp/tsfile/README-zh.md#开发)


## 使用 TsFile

[Java](./java/tsfile/README-zh.md#使用)

[C++](./cpp/tsfile/README-zh.md#使用)