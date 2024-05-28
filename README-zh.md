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
  |____|/____  >\___  /   |__|____/\___  >  version 1.0.1-SNAPSHOT
             \/     \/                 \/  
</pre>
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.tsfile/tsfile-parent/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.tsfile")

## 摘要

TsFile是一种为时间序列数据设计的列式存储文件格式，它支持高效压缩、高读写吞吐量，并且兼容多种框架，如Spark和Flink。TsFile很容易集成到物联网大数据处理框架中。

[点击查看更多](https://www.timecho.com/archives/tian-bu-shi-chang-kong-bai-apache-tsfile-ru-he-chong-xin-ding-yi-shi-xu-shu-ju-guan-li)

## 时序数据管理统一格式

时序数据即时间序列数据，是指带时间标签（按照时间的顺序变化，即时间序列化）的数据，其来源多元、数据量庞大，可广泛应用于物联网、智能制造、金融分析等领域。在数据驱动的当下，时序数据的重要性不言而喻。

尽管时序数据如此普遍且重要，但长期以来，时序数据的管理都缺乏标准化的文件格式。

当前企业会面临着多种时序数据的存储格式，如自定义格式的 CSV、自定义的二进制格式，或者使用 Parquet、ORC 等通用文件格式，这导致时序数据源的统一管理和汇聚十分复杂。

同时，通用文件格式没有针对时间、设备、测点等时序数据特有的数据概念，可能导致主键信息存储冗余，并缺乏时序数据场景常用索引，使得快速定位与查询数据性能受限。

这便是 TsFile 针对这一市场空白，希望实现的价值：为时序数据提供统一和标准化的格式。IoTDB 团队在构思 TsFile 结构时，便考虑了几个关键因素：

- 时序模型：专门为物联网设计的数据模型，每个时间序列与特定设备相关联，所有设备通过分层结构相互连接；

- 高压缩比：为时间序列量身定制的列式存储格式，将数据按设备进行组织，并保证每个序列的数据连续存储，最小化存储空间；

- 高效写入：数据可以按块写入，能够达到最大吞吐；

- 高效访问：为时间、设备、物理量构建了相关索引结构，实现快速数据检索。




## TsFile 的几大特性

TsFile 通过自研实现了时序数据高效率管理、高灵活传输，并支持多类软件深度集成。其特性包括：

- 可独立使用：可以使用 SDK 直接读写 TsFile，使得一些轻量级的数据读写场景成为可能。

- 高效存储和压缩：TsFile 采用先进的压缩技术，可最大限度地减少存储需求磁盘空间消耗并提高系统效率，从而减少磁盘空间消耗和优化数据管理。相比通用文件格式，压缩比可提升 20% 以上。

- 灵活的元数据管理架构：与传统写入方式不同，TsFile 支持灵活的元数据管理，无需预定义元数据即可实现数据写入。这种适应性结合时序数据的动态特性，简化了数据写入和管理过程。结合列式数据写入模式，相比通用文件格式，写入吞吐可提升 2-3 倍。

- 高查询性能：通过设备、传感器和时间维度索引，TsFile 实现了基于特定时间范围的时序数据快速过滤和查询。相比通用文件格式，查询吞吐可提升 2-10 倍。

- 协同同步：TsFile 是时序数据库 IoTDB 的底层存储文件格式，可与 IoTDB 形成可插拔的存算分离架构。通过 TsFile，用户可对 IoTDB 中的数据进行便捷的加载与导出。同一个 TsFile 可以在嵌入式设备、边缘服务器和云节点中灵活部署和同步。

- 开放集成：TsFile 支持与 Spark、Flink 等大数据软件建立无缝生态集成，从而确保跨不同数据处理环境的兼容性和互操作性，实现时序数据跨生态深度分析。


## 基于时序数据特性的内核创新

### 列式存储文件结构

下图为 Apache TsFile 的文件结构。

- Page：一段连续的时序数据，存储的基本单元，按时间升序排序，时间戳和值各有单独的列进行存储。

- Chunk：由同一序列的多个连续的 Page 组成，一个文件同一个序列可以存储多个 Chunk。

- ChunkGroup：由一个设备的一至多个 Chunk 组成，多个 Chunk 可共享一列时间存储（多值模型）。

- Index：TsFile 末尾的元数据文件包含序列内部时间维度的索引和序列间的索引信息。

![TsFile 文件结构](https://alioss.timecho.com/upload/Apache%20TsFile%20%E5%8F%91%E5%B8%83%E5%9B%BE3-20240315.png)

由于每列数据的同质性，TsFile 可实现更好的压缩比；通过仅将必要的数据列加载到内存中，TsFile 可加快查询速度；通过将数据组织成可管理的单元进行处理和检索，TsFile 可提高可扩展性。


### 编码和压缩技术

TsFile 通过采用二阶差分编码、游程编码（RLE）、位压缩和 Snappy 等先进的编码和压缩技术，优化时序数据的存储和访问，并支持对时间戳列和数据值列进行单独编码，以实现更好的数据处理效能。

其独特之处在于编码算法专为时序数据特性设计，聚焦在时间属性和数据之间的相关性。此外，TsFile 结合了频域编码，利用量化和位宽缩减来高效存储频域数据，在不会影响数据准确性的情况下节省空间占用。

(![TsFile、Parquet 和 ORC 三种文件格式的比较](https://alioss.timecho.com/upload/Apache%20TsFile%20%E5%8F%91%E5%B8%83%E5%9B%BE4-20240315.png))


基于对时序数据应用需求的深刻理解，TsFile 有助于实现时序数据高压缩比和实时访问速度，并为企业进一步构建高效、可扩展、灵活的数据分析平台提供底层文件技术支撑。


## 开发 TsFile

[Java](./java/tsfile/README-zh.md#开发)

[C++(开发中)](./cpp/tsfile/README-zh.md#开发)


## 使用 TsFile

[Java](./java/tsfile/README-zh.md#使用)

[C++(开发中)](./cpp/tsfile/README-zh.md#使用)