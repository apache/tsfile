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
        'Data is written in device groups, with indexes at the end. A reader usually opens the footer first, then seeks to the Chunks and Pages it needs.',
      offset: '0 … F - 1',
      size: 'F bytes',
      layout: 'File Header | FileEncryptionHeader? | Data | Separator | Indexed Metadata | TsFileMetadata | Length | Tail Magic',
      note: 'F is the file length, L is the TsFileMetadata length, and E is the total encryption-header length (0 when absent).',
    },
    zh: {
      title: 'TsFile v4',
      description: '数据按设备分组写入，索引放在文件尾部。读取器通常先查 Footer，再跳转到需要的 Chunk 和 Page。',
      offset: '0 … F - 1',
      size: 'F 字节',
      layout: '文件头 | FileEncryptionHeader? | 数据区 | 分隔符 | 索引元数据 | TsFileMetadata | 长度 | 尾魔数',
      note: 'F 为文件长度，L 为 TsFileMetadata 长度，E 为可选加密头的总长度；无加密头时 E = 0。',
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
          description: 'The first seven bytes contain the TsFile magic string and format version. Read these before choosing how to parse the rest of the file.',
          offset: '0 … 6',
          size: '7 bytes',
          layout: 'magic[6] | version[1]',
        },
        zh: {
          title: '文件头',
          description: '文件最前面的七个字节保存 TsFile 魔数和格式版本。先读取这部分，再确定后续结构的解析方式。',
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
              description: 'The six bytes spelling "TsFile" identify the file format.',
              offset: '0 … 5',
              size: '6 bytes',
              layout: '54 73 46 69 6c 65',
            },
            zh: {
              title: '头部魔数',
              description: '由字符串“TsFile”对应的六个字节组成，用于识别文件格式。',
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
              description: 'Value 0x04 identifies format v4. This byte is separate from the project release number.',
              offset: '6',
              size: '1 byte',
              layout: '04',
            },
            zh: {
              title: '格式版本',
              description: '数值 0x04 表示 v4 文件格式，与项目的发布版本号分开定义。',
              offset: '6',
              size: '1 字节',
              layout: '04',
            },
          },
        },
      ],
    },
    {
      id: 'file-encryption-header',
      kind: 'record',
      link: 'DataPages/Encryption.html#file-encryption-header',
      text: {
        en: {
          title: 'File Encryption Header (optional)',
          description: 'An optional header at offset 7 selects Page AEAD. It identifies the provider, profile, file crypto ID, and wrapped data key.',
          offset: '7 … 6 + E; absent when E = 0',
          size: 'E = 1 + UVarInt-size(payload_size) + payload_size',
          layout: '06 | payload_size | 01 | provider_id | profile_id | key_id | key_version | file_crypto_id_size | file_crypto_id | wrapped_data_key_size | wrapped_data_key',
          note: 'The header selects the AEAD layout even though legacy footer encryptLevel is 0. E is its total serialized size; E = 0 when absent.',
        },
        zh: {
          title: '文件加密头（可选）',
          description: '偏移 7 处的可选加密头用于选择 Page AEAD，记录 provider、profile、文件加密 ID 和封装数据密钥。',
          offset: '7 … 6 + E；E = 0 时不存在',
          size: 'E = 1 + UVarInt-size(payload_size) + payload_size',
          layout: '06 | payload_size | 01 | provider_id | profile_id | key_id | key_version | file_crypto_id_size | file_crypto_id | wrapped_data_key_size | wrapped_data_key',
          note: '即使旧版 Footer 的 encryptLevel 为 0，也以该头选择 AEAD 布局。E 是该头的总序列化长度，不存在时为 0。',
        },
      },
    },
    {
      id: 'data-section',
      kind: 'region',
      link: 'DataPages/Chunks.html',
      text: {
        en: {
          title: 'Data Section',
          description:
            'Chunk Groups and operation-index records appear here in write order. The section begins after any file encryption header and ends before the separator.',
          offset: '7 + E … meta_offset - 1',
          size: 'variable',
          layout: '(ChunkGroup | OperationIndexRange)*',
        },
        zh: {
          title: '数据区',
          description: '这里按写入顺序保存 Chunk Group 和操作索引记录，从可选文件加密头之后开始，到分隔符之前结束。',
          offset: '7 + E … meta_offset - 1',
          size: '可变',
          layout: '(ChunkGroup | OperationIndexRange)*',
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
                'A batch of Chunks for one device. Another Chunk continues the group; a new group, operation-index record, or separator ends it.',
              offset: 'chunk_group_offset',
              size: 'variable · repeated',
              layout: 'Chunk Group Header | Chunk*',
            },
            zh: {
              title: '设备分组（Chunk Group）',
              description: '同一设备的一批 Chunk。后续 Chunk 标记表示分组继续，新的分组、操作索引记录或分隔符表示当前分组结束。',
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
                  description: 'Begins a device group with its DeviceID. The identifier stores a segment count followed by separately length-prefixed string segments.',
                  offset: 'chunk_group_offset',
                  size: 'variable',
                  layout: 'marker 0x00 | DeviceID',
                },
                zh: {
                  title: 'Chunk Group 头部',
                  description: '开始一个设备分组，并保存 DeviceID：先记录片段数量，再依次记录各个带长度的字符串片段。',
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
                    'Non-aligned Chunks store time and values together. Aligned Chunk sets put shared timestamps in a Time Chunk and measurements in Value Chunks.',
                  offset: 'after Chunk Group Header',
                  size: 'variable · repeated',
                  layout: 'Non-aligned Chunk | Aligned Chunk Set',
                },
                zh: {
                  title: 'Chunk 布局',
                  description: '非对齐 Chunk 把时间和值放在一起；对齐 Chunk 集用一个 Time Chunk 保存共享时间戳，再用各个 Value Chunk 保存测量值。',
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
                        'Stores one measurement with its own timestamps. Each Page contains a time stream and a value stream with matching point counts.',
                      offset: 'chunk_offset',
                      size: 'header + data_size',
                      layout: 'Chunk Header | Non-aligned Page*',
                    },
                    zh: {
                      title: '非对齐 Chunk',
                      description: '保存一个测量及其独立的时间戳。每个 Page 都有时间流和值流，两者解码后的数据点数量相同。',
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
                            'Names the measurement and its codecs. data_size counts all Page Headers and stored bodies after this Header, so a reader can locate the next Chunk without decoding values.',
                          offset: 'chunk_offset',
                          size: 'variable',
                          layout:
                            'chunk_type | measurement_id | data_size | data_type | compression_type | encoding_type | AEAD suffix?',
                          note: 'encoding_type describes values; time encoding is external configuration (default TS_2DIFF). The AEAD suffix is int64 ordinal | int32 proof_size | proof[proof_size], outside data_size.',
                        },
                        zh: {
                          title: 'Chunk 头部',
                          description: '记录测量名及编解码方式。data_size 统计头部之后全部 Page Header 和载荷的长度，读取器无需解码值就能定位下一个 Chunk。',
                          offset: 'chunk_offset',
                          size: '可变',
                          layout:
                            'chunk_type | measurement_id | data_size | data_type | compression_type | encoding_type | AEAD suffix?',
                          note: 'encoding_type 描述值流；时间编码来自外部配置，默认 TS_2DIFF。AEAD 后缀为 int64 序号 | int32 证明长度 | 证明字节，不计入 data_size。',
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
                            'After decryption and decompression, the payload starts with the time-stream length and time bytes. The value stream fills the rest of the payload.',
                          offset: 'inside Chunk data_size',
                          size: 'variable · repeated',
                          layout:
                            'Page Header | time_stream_size | encoded_time_stream | encoded_value_stream',
                          note: 'Time and values pair by position. Their codecs can differ; the time codec comes from shared configuration.',
                        },
                        zh: {
                          title: '非对齐 Page',
                          description: '解密、解压后的载荷先保存时间流长度和时间流字节，剩余部分保存值流。',
                          offset: 'Chunk data_size 内部',
                          size: '可变 · 重复',
                          layout:
                            'Page Header | time_stream_size | encoded_time_stream | encoded_value_stream',
                          note: '时间戳和值按位置配对，可以使用不同编码；时间编码由共享配置提供。',
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
                        'One Time Chunk is followed by its Value Chunks. Page i in each Value Chunk corresponds to Page i in the Time Chunk; the set has no separate header.',
                      offset: 'first aligned Chunk offset',
                      size: 'variable · repeated',
                      layout: 'Time Chunk | Value Chunk*',
                    },
                    zh: {
                      title: '对齐 Chunk 集',
                      description: '一个 Time Chunk 后跟若干 Value Chunk。每个 Value Chunk 的第 i 个 Page 对应 Time Chunk 的第 i 个 Page，集合本身没有额外头部。',
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
                          description: 'Stores the timestamps shared by the aligned measurements. Its Header names the time codec, and each Time Page defines positions for the corresponding Value Pages.',
                          offset: 'time_chunk_offset',
                          size: 'header + data_size',
                          layout: 'Time Chunk Header | Time Page*',
                        },
                        zh: {
                          title: '时间列（Time Chunk）',
                          description: '保存各对齐测量共享的时间戳。头部说明时间编码，每个 Time Page 确定对应 Value Page 使用的那组位置。',
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
                            'Stores one aligned measurement. Its Page bitmaps place the stored values at the matching timestamps and leave other positions null.',
                          offset: 'value_chunk_offset',
                          size: 'header + data_size · repeated',
                          layout: 'Value Chunk Header | Value Page*',
                        },
                        zh: {
                          title: '测量列（Value Chunk）',
                          description: '保存一个对齐测量列，通过各 Page 的位图把值对应到时间戳，其余位置为 null。',
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
              description: 'Records the minimum and maximum operation sequence numbers for a flushed batch. A storage engine can use them to relate file data to checkpoints or its operation log.',
              offset: 'top-level boundary',
              size: '17 bytes',
              layout: 'marker 0x04 | min_operation_index[8] | max_operation_index[8]',
            },
            zh: {
              title: '操作索引范围',
              description: '记录已刷出批次的最小和最大操作序号，供存储引擎把文件数据与检查点或操作日志关联。',
              offset: '顶层结构边界',
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
          description: 'Byte 0x02 ends the data section. meta_offset points here, and indexed metadata starts at the next byte.',
          offset: 'meta_offset',
          size: '1 byte',
          layout: '02',
        },
        zh: {
          title: '数据/元数据分隔符',
          description: '字节 0x02 结束数据区，meta_offset 指向此处，索引元数据从下一个字节开始。',
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
            'Holds series metadata and index nodes. They can be interleaved, so follow their absolute offsets rather than assuming a fixed order.',
          offset: 'meta_offset + 1 … file_metadata_pos - 1',
          size: 'variable',
          layout: '(TimeseriesMetadata | MetadataIndexNode)*',
        },
        zh: {
          title: '索引元数据区',
          description: '保存时间序列元数据和索引节点。两者可以交错存放，读取时应跟随绝对偏移量，不依赖固定顺序。',
          offset: 'meta_offset + 1 … file_metadata_pos - 1',
          size: '可变',
          layout: '(TimeseriesMetadata | MetadataIndexNode)*',
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
              description: 'Describes one series, with summary Statistics and a list of its Chunks. Each Chunk entry points to a Header in the data section.',
              offset: 'referenced by a measurement index entry',
              size: 'variable · repeated',
              layout:
                'timeseries_type | measurement_id | data_type | chunk_metadata_list_size | Statistics | ChunkMetadata*',
            },
            zh: {
              title: '时间序列元数据',
              description: '描述一条时间序列，包含汇总统计信息和 Chunk 列表。列表中的每个条目指向数据区的一个 Chunk Header。',
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
              description: 'Locates devices, measurements, or further index nodes. An entry covers bytes from its offset up to, but not including, the next offset or end_offset.',
              offset: 'referenced by a parent node or TsFileMetadata',
              size: 'variable · repeated',
              layout: 'child_count | MetadataEntry* | end_offset | node_type',
            },
            zh: {
              title: '元数据索引节点',
              description: '用于查找设备、测量或下一级索引节点。每个条目的字节范围从自身偏移开始，到下一偏移或 end_offset 之前结束。',
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
            'The entry point for metadata lookup: table index roots, schemas, separator offset, Bloom filter, and properties. Its start is calculated from the trailing length.',
          offset: 'file_metadata_pos = F - 6 - 4 - L',
          size: 'L bytes',
          layout:
            'table indexes | table schemas | meta_offset | BloomFilter | properties',
          note: 'No separator or marker immediately precedes TsFileMetadata.',
        },
        zh: {
          title: 'TsFileMetadata',
          description: '元数据查找的入口，保存表索引根、Schema、分隔符偏移、布隆过滤器和属性。通过文件尾的长度反向定位其起点。',
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
              description: 'Stores each table\'s device-index root. Follow a root to find a device, then its measurements.',
              offset: 'inside TsFileMetadata',
              size: 'variable · repeated',
            },
            zh: {
              title: '表索引根',
              description: '保存各表的设备索引根。从根节点查找设备，再查找设备下的测量。',
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
              description: 'Describes table columns in their declared order, including names, types, and categories. FIELD Chunks are matched by measurement name.',
              offset: 'inside TsFileMetadata',
              size: 'variable · repeated',
            },
            zh: {
              title: '表 Schema',
              description: '按声明顺序描述表的列，记录列名、类型和类别；FIELD Chunk 通过测量名与列对应。',
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
              description: 'The absolute offset of the separator. Add one to find the start of indexed metadata.',
              offset: 'inside TsFileMetadata',
              size: '8 bytes',
              layout: 'int64, big-endian',
            },
            zh: {
              title: 'meta_offset',
              description: '分隔符的绝对偏移量，加一就是索引元数据区的起点。',
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
              description: 'Can rule out a missing path before an index lookup. Trust a negative result only when path encoding and hashing match the writer.',
              offset: 'inside TsFileMetadata',
              size: 'variable',
              layout: 'byte_length | filter bytes | bit count | hash-function count',
            },
            zh: {
              title: '布隆过滤器',
              description: '可以在查索引之前排除不存在的路径，但要先确认路径编码和哈希方式与写入端一致。',
              offset: 'TsFileMetadata 内部',
              size: '可变',
              layout: 'byte_length | filter bytes | bit count | hash-function count',
            },
          },
        },
        {
          id: 'file-properties',
          kind: 'repeated',
          link: 'Metadata.html#file-metadata',
          text: {
            en: {
              title: 'File Properties',
              description: 'Maps UTF-8 keys to nullable binary values. Keep unknown values as raw bytes; only a property\'s own definition can say whether its value is text.',
              offset: 'end of TsFileMetadata',
              size: 'variable · repeated',
              layout: 'VarInt property_count | (VarString key | VarBinary value)*',
            },
            zh: {
              title: '文件属性',
              description: '把 UTF-8 键映射到可空的二进制值。未知值按原始字节保留，只有具体属性定义才能决定它是否应按文本解释。',
              offset: 'TsFileMetadata 尾部',
              size: '可变 · 重复',
              layout: 'VarInt property_count | (VarString key | VarBinary value)*',
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
          description: 'Stores L, the byte length of TsFileMetadata. Subtract L and the ten trailing bytes from file size F to locate that metadata.',
          offset: 'F - 10 … F - 7',
          size: '4 bytes',
          layout: 'int32 L, big-endian',
        },
        zh: {
          title: 'TsFileMetadata 长度',
          description: '保存 TsFileMetadata 的字节长度 L。用文件大小 F 减去 L 和尾部十个字节，即可找到元数据起点。',
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
          description: 'The same six magic bytes as the header. Check this suffix together with the footer length and metadata structure before opening the file.',
          offset: 'F - 6 … F - 1',
          size: '6 bytes',
          layout: '54 73 46 69 6c 65',
        },
        zh: {
          title: '尾部魔数',
          description: '与文件头相同的六个魔数字节。打开文件时，还要同时检查 Footer 长度和元数据结构。',
          offset: 'F - 6 … F - 1',
          size: '6 字节',
          layout: '54 73 46 69 6c 65',
        },
      },
    },
  ],
};
