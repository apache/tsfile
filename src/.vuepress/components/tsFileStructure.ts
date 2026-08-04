/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export type TsFileStructureLocale = 'en' | 'zh';

export type TsFileStructureKind =
  | 'file'
  | 'region'
  | 'repeated'
  | 'record'
  | 'field'
  | 'choice';

export interface TsFileStructureText {
  title: string;
  description: string;
  offset?: string;
  size?: string;
  layout?: string;
  note?: string;
}

export interface TsFileStructureNode {
  id: string;
  kind: TsFileStructureKind;
  text: Record<TsFileStructureLocale, TsFileStructureText>;
  link?: string;
  children?: TsFileStructureNode[];
}

export const tsFileStructure: TsFileStructureNode = {
  id: 'tsfile',
  kind: 'file',
  text: {
    en: {
      title: 'TsFile v4',
      description:
        'The complete physical file. All offsets are absolute byte offsets measured from the first byte. Readers normally locate the footer from the tail, traverse metadata, and then read only selected Chunks and Pages.',
      offset: '0 … F - 1',
      size: 'F bytes',
      layout: 'File Header | Data Section | Separator | Metadata | Footer',
    },
    zh: {
      title: 'TsFile v4',
      description: '完整的物理文件。所有偏移均为从文件第一个字节开始计算的绝对字节偏移。读取器通常先从文件尾定位 Footer、遍历元数据，再按需读取选中的 Chunk 和 Page。',
      offset: '0 … F - 1',
      size: 'F 字节',
      layout: '文件头 | 数据区 | 分隔符 | 元数据 | 文件尾',
    },
  },
  children: [
    {
      id: 'file-header',
      kind: 'region',
      link: 'Binary-Representation.html',
      text: {
        en: {
          title: 'File Header',
          description: 'A fixed seven-byte prefix identifying TsFile and format version 4. The version byte selects the complete grammar before any data marker is interpreted.',
          offset: '0 … 6',
          size: '7 bytes',
          layout: 'magic[6] | version[1]',
        },
        zh: {
          title: '文件头',
          description: '固定的 7 字节前缀，用于标识 TsFile 以及第 4 版文件格式。解释任何数据区标记之前，必须先用版本字节选择完整语法。',
          offset: '0 … 6',
          size: '7 字节',
          layout: 'magic[6] | version[1]',
        },
      },
      children: [
        {
          id: 'head-magic',
          kind: 'field',
          text: {
            en: {
              title: 'Head Magic',
              description: 'The UTF-8 bytes of the literal string "TsFile". They identify the container but are not a checksum or a content-integrity proof.',
              offset: '0 … 5',
              size: '6 bytes',
              layout: '54 73 46 69 6c 65',
            },
            zh: {
              title: '头部魔数',
              description: '字符串 “TsFile” 的 UTF-8 字节。它用于识别容器，但不是校验和，也不能证明内容完整。',
              offset: '0 … 5',
              size: '6 字节',
              layout: '54 73 46 69 6c 65',
            },
          },
        },
        {
          id: 'version',
          kind: 'field',
          link: 'Format-Versions.html',
          text: {
            en: {
              title: 'Format Version',
              description: 'The one-byte on-disk format identifier. Value 0x04 selects this layout; it is independent of the project 2.x release label.',
              offset: '6',
              size: '1 byte',
              layout: '04',
            },
            zh: {
              title: '格式版本',
              description: '单字节磁盘格式标识。数值 0x04 选择本页面所述布局，它与项目的 2.x 发布版本标签相互独立。',
              offset: '6',
              size: '1 字节',
              layout: '04',
            },
          },
        },
      ],
    },
    {
      id: 'data-section',
      kind: 'region',
      link: 'DataPages/Chunks.html',
      text: {
        en: {
          title: 'Data Section',
          description:
            'Marker-delimited Chunk Groups and optional Operation Index Ranges. Structures are written sequentially, and every marker is interpreted only at a known top-level boundary.',
          offset: '7 … meta_offset - 1',
          size: 'variable',
          layout: 'Chunk Group* | Operation Index Range*',
        },
        zh: {
          title: '数据区',
          description: '由标记分隔的 Chunk Group 和可选 Operation Index Range。各结构顺序写入，只有位于已知顶层边界的字节才可解释为 marker。',
          offset: '7 … meta_offset - 1',
          size: '可变',
          layout: 'Chunk Group* | Operation Index Range*',
        },
      },
      children: [
        {
          id: 'chunk-group',
          kind: 'repeated',
          link: 'DataPages/Chunks.html#chunk-group-header',
          text: {
            en: {
              title: 'Chunk Group',
              description:
                'A repeated device-scoped container. All following Chunks belong to its device until another top-level marker is encountered. The group has no total-length field of its own.',
              offset: 'chunk_group_offset',
              size: 'variable · repeated',
              layout: 'Chunk Group Header | Chunk*',
            },
            zh: {
              title: 'Chunk Group',
              description: '按设备组织的重复容器。后续 Chunk 均属于该设备，直到遇到下一个顶层标记。Chunk Group 自身没有总长度字段。',
              offset: 'chunk_group_offset',
              size: '可变 · 重复',
              layout: 'Chunk Group Header | Chunk*',
            },
          },
          children: [
            {
              id: 'chunk-group-header',
              kind: 'record',
              link: 'DataPages/Chunks.html#chunk-group-header',
              text: {
                en: {
                  title: 'Chunk Group Header',
                  description: 'Starts one device group and serializes the segmented DeviceID. The segment count is an element count, and every segment is a separate VarString.',
                  offset: 'chunk_group_offset',
                  size: 'variable',
                  layout: 'marker 0x00 | DeviceID',
                },
                zh: {
                  title: 'Chunk Group Header',
                  description: '开始一个设备分组，并序列化分片 DeviceID。片段数表示元素数量，每个片段都是独立 VarString。',
                  offset: 'chunk_group_offset',
                  size: '可变',
                  layout: 'marker 0x00 | DeviceID',
                },
              },
            },
            {
              id: 'chunk-layout',
              kind: 'choice',
              link: 'DataPages/Chunks.html#chunk-header',
              text: {
                en: {
                  title: 'Chunk Layout',
                  description:
                    'A device contains non-aligned Chunks, aligned Chunk sets, or both. Each Chunk stores one physical column.',
                  offset: 'after Chunk Group Header',
                  size: 'variable · repeated',
                  layout: 'Non-aligned Chunk | Aligned Chunk Set',
                },
                zh: {
                  title: 'Chunk 布局',
                  description: '一个设备可包含非对齐 Chunk、对齐 Chunk 集，或同时包含两者。每个 Chunk 保存一个物理列。',
                  offset: 'Chunk Group Header 之后',
                  size: '可变 · 重复',
                  layout: '非对齐 Chunk | 对齐 Chunk 集',
                },
              },
              children: [
                {
                  id: 'non-aligned-chunk',
                  kind: 'repeated',
                  link: 'DataPages/Chunks.html#chunk-header',
                  text: {
                    en: {
                      title: 'Non-aligned Chunk',
                      description:
                        'Stores one measurement and its independent timestamp stream. Every Page contains both streams, and the decoded timestamp and value counts must match.',
                      offset: 'chunk_offset',
                      size: 'header + data_size',
                      layout: 'Chunk Header | Non-aligned Page*',
                    },
                    zh: {
                      title: '非对齐 Chunk',
                      description: '保存一个测量列及其独立时间戳流。每个 Page 同时包含时间流和值流，解码后的时间戳数和值数必须相等。',
                      offset: 'chunk_offset',
                      size: 'header + data_size',
                      layout: 'Chunk Header | 非对齐 Page*',
                    },
                  },
                  children: [
                    {
                      id: 'non-aligned-chunk-header',
                      kind: 'record',
                      link: 'DataPages/Chunks.html#chunk-header',
                      text: {
                        en: {
                          title: 'Chunk Header',
                          description:
                            'Describes the value column and gives the exact total byte length of all Page Headers and stored Page data. This boundary lets readers skip the whole Chunk without decoding its codec.',
                          offset: 'chunk_offset',
                          size: 'variable',
                          layout:
                            'chunk_type | measurement_id | data_size | data_type | compression_type | encoding_type',
                          note: 'For a non-aligned Chunk, encoding_type describes the value stream.',
                        },
                        zh: {
                          title: 'Chunk Header',
                          description: '描述值列，并给出全部 Page Header 与磁盘 Page 数据的准确总字节数。读取器可以利用该边界跳过整个 Chunk，而无需调用其编解码器。',
                          offset: 'chunk_offset',
                          size: '可变',
                          layout:
                            'chunk_type | measurement_id | data_size | data_type | compression_type | encoding_type',
                          note: '对于非对齐 Chunk，encoding_type 描述的是值流。',
                        },
                      },
                    },
                    {
                      id: 'non-aligned-page',
                      kind: 'repeated',
                      link: 'DataPages/index.html#non-aligned-page',
                      text: {
                        en: {
                          title: 'Non-aligned Page',
                          description:
                            'The Page Header is followed by one stored Page payload. After decryption and decompression, a length-delimited time stream comes first and the value stream occupies the remaining bytes.',
                          offset: 'inside Chunk data_size',
                          size: 'variable · repeated',
                          layout:
                            'Page Header | time_stream_size | encoded_time_stream | encoded_value_stream',
                          note: 'Timestamps and values pair by position; they are not interleaved point by point.',
                        },
                        zh: {
                          title: '非对齐 Page',
                          description: 'Page Header 后是一个磁盘 Page 载荷。解密并解压后，先存带长度的完整时间流，值流占据剩余全部字节。',
                          offset: 'Chunk data_size 内部',
                          size: '可变 · 重复',
                          layout:
                            'Page Header | time_stream_size | encoded_time_stream | encoded_value_stream',
                          note: '时间戳和值按位置配对，并不是逐点交错存储。',
                        },
                      },
                    },
                  ],
                },
                {
                  id: 'aligned-chunk-set',
                  kind: 'repeated',
                  link: 'DataPages/Chunks.html#chunk-header',
                  text: {
                    en: {
                      title: 'Aligned Chunk Set',
                      description:
                        'Stores one shared time column followed by aligned measurement value columns. Chunks are contiguous rather than interleaved Page by Page, while equal Page ordinals define correspondence.',
                      offset: 'first aligned Chunk offset',
                      size: 'variable · repeated',
                      layout: 'Time Chunk | Value Chunk*',
                    },
                    zh: {
                      title: '对齐 Chunk 集',
                      description: '先保存一个共享时间列，再保存各个对齐测量值列。不同 Chunk 连续存放而不按 Page 交错；相同 Page 序号建立对应关系。',
                      offset: '第一个对齐 Chunk 的偏移',
                      size: '可变 · 重复',
                      layout: 'Time Chunk | Value Chunk*',
                    },
                  },
                  children: [
                    {
                      id: 'time-chunk',
                      kind: 'record',
                      link: 'DataPages/index.html#aligned-time-page',
                      text: {
                        en: {
                          title: 'Time Chunk',
                          description: 'Contains all Time Pages for the aligned set. Its Chunk Header records the time encoding, and each decoded Page establishes the logical positions shared by matching Value Pages.',
                          offset: 'time_chunk_offset',
                          size: 'header + data_size',
                          layout: 'Time Chunk Header | Time Page*',
                        },
                        zh: {
                          title: 'Time Chunk',
                          description: '包含该对齐集合的全部 Time Page；其 Chunk Header 记录时间编码。每个解码后的 Page 建立对应 Value Page 共用的逻辑位置。',
                          offset: 'time_chunk_offset',
                          size: 'header + data_size',
                          layout: 'Time Chunk Header | Time Page*',
                        },
                      },
                    },
                    {
                      id: 'value-chunk',
                      kind: 'repeated',
                      link: 'DataPages/index.html#aligned-value-page',
                      text: {
                        en: {
                          title: 'Value Chunk',
                          description:
                            'Contains one aligned measurement column. Value Page number i corresponds to Time Page number i; its bitmap maps present values and nulls onto those shared positions.',
                          offset: 'value_chunk_offset',
                          size: 'header + data_size · repeated',
                          layout: 'Value Chunk Header | Value Page*',
                        },
                        zh: {
                          title: 'Value Chunk',
                          description: '包含一个对齐测量值列。编号为 i 的 Value Page 对应编号为 i 的 Time Page，并通过位图把实际值和 null 映射到共享位置。',
                          offset: 'value_chunk_offset',
                          size: 'header + data_size · 重复',
                          layout: 'Value Chunk Header | Value Page*',
                        },
                      },
                    },
                  ],
                },
              ],
            },
          ],
        },
        {
          id: 'operation-index-range',
          kind: 'record',
          link: 'DataPages/Chunks.html#operation-index-range',
          text: {
            en: {
              title: 'Operation Index Range',
              description: 'An optional storage-engine record containing two operation sequence numbers for checkpoint, snapshot, backup, or recovery coordination. They are not timestamps or file offsets.',
              offset: 'between Chunk Groups',
              size: '17 bytes',
              layout: 'marker 0x04 | min_operation_index[8] | max_operation_index[8]',
            },
            zh: {
              title: 'Operation Index Range',
              description: '可选的存储引擎顶层记录，包含两个操作序列号，用于协调检查点、快照、备份或恢复。它们不是时间戳，也不是文件偏移量。',
              offset: 'Chunk Group 之间',
              size: '17 字节',
              layout: 'marker 0x04 | min_operation_index[8] | max_operation_index[8]',
            },
          },
        },
      ],
    },
    {
      id: 'separator',
      kind: 'field',
      link: 'DataPages/Chunks.html#markers',
      text: {
        en: {
          title: 'Data/Metadata Separator',
          description: 'The single top-level separator ending the data section. TsFileMetadata.meta_offset points to this byte; a matching 0x02 inside payload data has no marker meaning.',
          offset: 'meta_offset',
          size: '1 byte',
          layout: '02',
        },
        zh: {
          title: '数据/元数据分隔符',
          description: '结束数据区的唯一顶层分隔符，TsFileMetadata.meta_offset 指向这个字节。负载内部出现相同的 0x02 不具有 marker 含义。',
          offset: 'meta_offset',
          size: '1 字节',
          layout: '02',
        },
      },
    },
    {
      id: 'metadata-section',
      kind: 'region',
      link: 'Metadata.html',
      text: {
        en: {
          title: 'Indexed Metadata Section',
          description:
            'Timeseries Metadata and metadata index nodes. Their physical ranges can be interleaved and are delimited by absolute offsets; this is a graph of referenced byte ranges, not one self-delimiting list.',
          offset: 'meta_offset + 1 … file_metadata_pos - 1',
          size: 'variable',
          layout: 'TimeseriesMetadata* | MetadataIndexNode*',
        },
        zh: {
          title: '索引元数据区',
          description: 'Timeseries Metadata 与元数据索引节点。两类记录的物理区间可以交错，并由绝对偏移划分；这里是由字节范围引用形成的图，而不是单一自定界列表。',
          offset: 'meta_offset + 1 … file_metadata_pos - 1',
          size: '可变',
          layout: 'TimeseriesMetadata* | MetadataIndexNode*',
        },
      },
      children: [
        {
          id: 'timeseries-metadata',
          kind: 'repeated',
          link: 'Metadata.html#timeseries-metadata',
          text: {
            en: {
              title: 'Timeseries Metadata',
              description: 'One logical-series record containing aggregate Statistics and a byte-length-delimited Chunk Metadata list. Entries point back to physical Chunk Headers in the data section.',
              offset: 'referenced by a measurement index entry',
              size: 'variable · repeated',
              layout:
                'timeseries_type | measurement_id | data_type | chunk_metadata_list_size | Statistics | ChunkMetadata*',
            },
            zh: {
              title: 'Timeseries Metadata',
              description: '每个逻辑序列对应一条记录，包含聚合 Statistics 和按字节长度定界的 Chunk Metadata 列表。各条目反向指向数据区的物理 Chunk Header。',
              offset: '由测量索引条目引用',
              size: '可变 · 重复',
              layout:
                'timeseries_type | measurement_id | data_type | chunk_metadata_list_size | Statistics | ChunkMetadata*',
            },
          },
        },
        {
          id: 'metadata-index',
          kind: 'repeated',
          link: 'Metadata.html#metadata-index',
          text: {
            en: {
              title: 'Metadata Index Node',
              description: 'An ordered internal or leaf node used to locate devices, measurements, and metadata ranges. Each child offset starts a range whose end is the next child offset or end_offset.',
              offset: 'referenced by a parent node or TsFileMetadata',
              size: 'variable · repeated',
              layout: 'child_count | MetadataEntry* | end_offset | node_type',
            },
            zh: {
              title: 'Metadata Index Node',
              description: '用于定位设备、测量列和元数据区间的有序内部节点或叶子节点。每个子项的范围从自身 offset 开始，到下一子项 offset 或 end_offset 结束。',
              offset: '由父节点或 TsFileMetadata 引用',
              size: '可变 · 重复',
              layout: 'child_count | MetadataEntry* | end_offset | node_type',
            },
          },
        },
      ],
    },
    {
      id: 'file-metadata',
      kind: 'record',
      link: 'Metadata.html#file-metadata',
      text: {
        en: {
          title: 'TsFileMetadata',
          description:
            'The file-level index roots, table schemas, data-section offset, Bloom filter, and properties. Its start is derived from the trailing fixed-width length, so no preceding marker is needed.',
          offset: 'file_metadata_pos = F - 6 - 4 - L',
          size: 'L bytes',
          layout:
            'table indexes | table schemas | meta_offset | BloomFilter | properties',
          note: 'No separator or marker immediately precedes TsFileMetadata.',
        },
        zh: {
          title: 'TsFileMetadata',
          description: '包含文件级索引根、表 Schema、数据区偏移、布隆过滤器以及文件属性。其起点由尾部定宽长度反向计算，因此前面无需 marker。',
          offset: 'file_metadata_pos = F - 6 - 4 - L',
          size: 'L 字节',
          layout: 'table indexes | table schemas | meta_offset | BloomFilter | properties',
          note: 'TsFileMetadata 前面没有独立分隔符或标记。',
        },
      },
      children: [
        {
          id: 'table-index-roots',
          kind: 'repeated',
          text: {
            en: {
              title: 'Table Index Roots',
              description: 'Named device-index roots for tree-model and table-model data. A root stores the entry range used to descend toward one device and then one measurement.',
              offset: 'inside TsFileMetadata',
              size: 'variable · repeated',
            },
            zh: {
              title: '表索引根',
              description: '树模型与表模型数据使用的具名设备索引根。根节点保存向下定位某个设备、再定位某个测点所需的条目范围。',
              offset: 'TsFileMetadata 内部',
              size: '可变 · 重复',
            },
          },
        },
        {
          id: 'table-schemas',
          kind: 'repeated',
          link: 'Metadata.html#table-schema',
          text: {
            en: {
              title: 'Table Schemas',
              description: 'Named schemas for explicit table-model data. Column names, physical types, and categories preserve a common column order.',
              offset: 'inside TsFileMetadata',
              size: 'variable · repeated',
            },
            zh: {
              title: '表 Schema',
              description: '显式表模型数据使用的具名 Schema。列名、物理类型和列类别共同保持同一个列顺序。',
              offset: 'TsFileMetadata 内部',
              size: '可变 · 重复',
            },
          },
        },
        {
          id: 'meta-offset',
          kind: 'field',
          text: {
            en: {
              title: 'meta_offset',
              description: 'Absolute offset of the top-level 0x02 separator at the end of the data section. The indexed metadata section begins at the following byte.',
              offset: 'inside TsFileMetadata',
              size: '8 bytes',
              layout: 'int64, big-endian',
            },
            zh: {
              title: 'meta_offset',
              description: '数据区末尾顶层 0x02 分隔符的绝对偏移；索引元数据区从它的下一个字节开始。',
              offset: 'TsFileMetadata 内部',
              size: '8 字节',
              layout: 'int64，大端序',
            },
          },
        },
        {
          id: 'bloom-filter',
          kind: 'record',
          link: 'Bloom-Filter.html',
          text: {
            en: {
              title: 'Bloom Filter',
              description: 'Optional membership filter used to reject missing full paths without index traversal. A negative answer is definitive; a positive answer still requires metadata lookup.',
              offset: 'inside TsFileMetadata',
              size: 'variable',
              layout: 'filter bytes | bit count | hash-function count',
            },
            zh: {
              title: '布隆过滤器',
              description: '可选的成员过滤器，无需遍历索引即可排除不存在的完整路径。否定结果是确定的，肯定结果仍需查询元数据确认。',
              offset: 'TsFileMetadata 内部',
              size: '可变',
              layout: 'filter bytes | bit count | hash-function count',
            },
          },
        },
        {
          id: 'file-properties',
          kind: 'repeated',
          text: {
            en: {
              title: 'File Properties',
              description: 'A length-delimited map of string keys and values, including encryption properties when present. Unknown properties remain structurally consumable.',
              offset: 'end of TsFileMetadata',
              size: 'variable · repeated',
              layout: 'property_count | (key | value)*',
            },
            zh: {
              title: '文件属性',
              description: '按长度定界的字符串键值映射；启用加密时也包含加密属性。未知属性在结构上仍可读取和跳过。',
              offset: 'TsFileMetadata 尾部',
              size: '可变 · 重复',
              layout: 'property_count | (key | value)*',
            },
          },
        },
      ],
    },
    {
      id: 'metadata-size',
      kind: 'field',
      text: {
        en: {
          title: 'TsFileMetadata Size',
          description: 'The big-endian serialized byte length L of TsFileMetadata. With file size F, metadata occupies exactly [F - 10 - L, F - 10).',
          offset: 'F - 10 … F - 7',
          size: '4 bytes',
          layout: 'int32 L, big-endian',
        },
        zh: {
          title: 'TsFileMetadata 长度',
          description: 'TsFileMetadata 的大端序列化字节长度 L。文件大小为 F 时，元数据恰好位于 [F - 10 - L, F - 10)。',
          offset: 'F - 10 … F - 7',
          size: '4 字节',
          layout: 'int32 L，大端序',
        },
      },
    },
    {
      id: 'tail-magic',
      kind: 'field',
      text: {
        en: {
          title: 'Tail Magic',
          description: 'The same six-byte magic string as the file header. It supports completion checks but is not sufficient without validating the footer length and structure.',
          offset: 'F - 6 … F - 1',
          size: '6 bytes',
          layout: '54 73 46 69 6c 65',
        },
        zh: {
          title: '尾部魔数',
          description: '与文件头相同的 6 字节魔数字符串。它可用于完成性检查，但还必须同时校验 Footer 长度与结构。',
          offset: 'F - 6 … F - 1',
          size: '6 字节',
          layout: '54 73 46 69 6c 65',
        },
      },
    },
  ],
};
