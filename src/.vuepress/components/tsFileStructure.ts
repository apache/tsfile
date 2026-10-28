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

export type TsFileStructureLayoutRule =
  | 'optional'
  | 'repeated'
  | 'one-or-more'
  | 'one-of';

export interface TsFileStructureLayoutRow {
  text: string;
  rule?: TsFileStructureLayoutRule;
  note?: string;
  alternatives?: boolean;
  children?: TsFileStructureLayoutRow[];
}

export interface TsFileStructureText {
  title: string;
  description: string;
  offset?: string;
  size?: string;
  layout?: string;
  layoutNotes?: string[];
  layoutRows?: TsFileStructureLayoutRow[];
  note?: string;
}

export interface TsFileStructureNode {
  id: string;
  kind: TsFileStructureKind;
  text: Record<TsFileStructureLocale, TsFileStructureText>;
  link?: string;
  children?: TsFileStructureNode[];
}

type PageDataText = Pick<
  TsFileStructureText,
  'description' | 'layout' | 'layoutNotes' | 'note'
>;

function pageHeaderNode(id: string): TsFileStructureNode {
  return {
    id,
    kind: 'record',
    link: 'DataPages/index.html#page-header',
    text: {
      en: {
        title: 'Page Header',
        description:
          'Sizes and optional Statistics for one Page. An empty aligned Value Page has no Header.',
        offset: 'start of the Page',
        size: 'header bytes',
        layout: 'UVarInt uncompressed_size | UVarInt compressed_size | Statistics?',
        layoutNotes: [
          'Page payload length after decryption and decompression',
          'Stored payload length, including compression and encryption overhead',
          'Present for every non-empty Page in a multi-Page Chunk; absent for a single-Page Chunk',
        ],
        note: 'An empty aligned Value Page stores only UVarInt(0); it has no compressed_size, Statistics, or payload.',
      },
      zh: {
        title: '页头（Page Header）',
        description: '一个 Page 的两个长度字段和可选 Statistics；空的对齐 Value Page 没有 Header。',
        offset: 'Page 起始处',
        size: 'Header 字节数',
        layout: 'UVarInt uncompressed_size | UVarInt compressed_size | Statistics?',
        layoutNotes: [
          '解密、解压后的 Page 载荷长度',
          '磁盘载荷长度，包含压缩和加密开销',
          '多 Page Chunk 的每个非空 Page 都有；单 Page Chunk 没有',
        ],
        note: '空的对齐 Value Page 只保存 UVarInt(0)，没有 compressed_size、Statistics 或载荷。',
      },
    },
  };
}

function pageDataNode(
  id: string,
  link: string,
  en: PageDataText,
  zh: PageDataText,
): TsFileStructureNode {
  return {
    id,
    kind: 'record',
    link,
    text: {
      en: {
        title: 'Page Data',
        offset: 'after the Page Header',
        size: 'compressed_size on disk / uncompressed_size decoded',
        ...en,
      },
      zh: {
        title: '页数据（Page Data）',
        offset: 'Page Header 之后',
        size: '磁盘 compressed_size / 解码后 uncompressed_size',
        ...zh,
      },
    },
  };
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
      link: 'index.html#physical-regions',
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
          offset: '7 (when present)',
          size: 'E = 1 + UVarInt-size(payload_size) + payload_size',
          layout: '06 | payload_size | 01 | provider_id | profile_id | key_id | key_version | file_crypto_id_size | file_crypto_id | wrapped_data_key_size | wrapped_data_key',
          layoutNotes: [
            'Encryption header marker (ENCRYPTION_HEADER)',
            'UVarInt length of the following payload',
            'Header version (0x01)',
            'Encryption provider identifier',
            'Encryption profile identifier',
            'Key identifier',
            'Key version',
            'UVarInt file crypto ID length (must be 16)',
            '16-byte file crypto ID',
            'UVarInt wrapped data key length',
            'Wrapped data key bytes',
          ],
          note: 'The header selects the AEAD layout even though legacy footer encryptLevel is 0. E is its total serialized size; E = 0 when absent.',
        },
        zh: {
          title: '文件加密头（可选）',
          description: '偏移 7 处的可选加密头用于选择 Page AEAD，记录 provider、profile、文件加密 ID 和封装数据密钥。',
          offset: '7（存在时）',
          size: 'E = 1 + UVarInt-size(payload_size) + payload_size',
          layout: '06 | payload_size | 01 | provider_id | profile_id | key_id | key_version | file_crypto_id_size | file_crypto_id | wrapped_data_key_size | wrapped_data_key',
          layoutNotes: [
            '加密头标记（ENCRYPTION_HEADER）',
            '后续载荷的 UVarInt 长度',
            '头部版本（0x01）',
            '加密 provider 标识',
            '加密 profile 标识',
            '密钥标识',
            '密钥版本',
            '文件加密 ID 的 UVarInt 长度（必须为 16）',
            '16 字节文件加密 ID',
            '封装数据密钥的 UVarInt 长度',
            '封装数据密钥字节',
          ],
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
            'Chunk Groups and operation-index records appear here in write order, grouped by flush batch. The section begins after any file encryption header and ends before the separator.',
          offset: '7 + E … meta_offset - 1',
          size: 'variable',
          layout: '(ChunkGroup+ OperationIndexRange?)*',
          layoutRows: [
            {
              text: 'Batch',
              rule: 'repeated',
              note: 'Zero or more flush batches.',
              children: [
                {
                  text: 'ChunkGroup',
                  rule: 'one-or-more',
                  note: 'Each batch starts with one or more ChunkGroups.',
                },
                {
                  text: 'OperationIndexRange',
                  rule: 'optional',
                  note: 'At most one record after each batch.',
                },
              ],
            },
          ],
        },
        zh: {
          title: '数据区',
          description: '这里按写入顺序保存 Chunk Group 和操作索引记录，并按刷出批次分组；从可选文件加密头之后开始，到分隔符之前结束。',
          offset: '7 + E … meta_offset - 1',
          size: '可变',
          layout: '(ChunkGroup+ OperationIndexRange?)*',
          layoutRows: [
            {
              text: '批次',
              rule: 'repeated',
              note: '零个或多个刷出批次。',
              children: [
                {
                  text: 'ChunkGroup',
                  rule: 'one-or-more',
                  note: '每批以至少一个 ChunkGroup 开始。',
                },
                {
                  text: 'OperationIndexRange',
                  rule: 'optional',
                  note: '每批之后最多一条。',
                },
              ],
            },
          ],
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
                  title: '设备分组头部（Chunk Group Header）',
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
                  layoutRows: [
                    {
                      text: 'Non-aligned Chunk or Aligned Chunk Set',
                      rule: 'one-of',
                      alternatives: true,
                      children: [
                        {
                          text: 'Non-aligned Chunk',
                          note: 'Timestamps and values live in the same Chunk; every measurement carries its own time stream.',
                        },
                        {
                          text: 'Aligned Chunk Set',
                          note: 'One Time Chunk holds the shared timestamps, and each Value Chunk holds one measurement.',
                        },
                      ],
                    },
                  ],
                },
                zh: {
                  title: '数据块布局（Chunk Layout）',
                  description: '非对齐 Chunk 把时间和值放在一起；对齐 Chunk 集用一个 Time Chunk 保存共享时间戳，再用各个 Value Chunk 保存测量值。',
                  offset: 'Chunk Group Header 之后',
                  size: '可变 · 重复',
                  layout: '非对齐 Chunk | 对齐 Chunk 集',
                  layoutRows: [
                    {
                      text: '非对齐 Chunk 或 对齐 Chunk 集',
                      rule: 'one-of',
                      alternatives: true,
                      children: [
                        {
                          text: '非对齐 Chunk',
                          note: '时间和值在同一个 Chunk 内，每个测点各自带时间流。',
                        },
                        {
                          text: '对齐 Chunk 集',
                          note: '一个 Time Chunk 保存共享时间戳，每个 Value Chunk 保存一个测点。',
                        },
                      ],
                    },
                  ],
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
                      size: 'Chunk Header length + data_size',
                      layout: 'Chunk Header | Non-aligned Page*',
                          note: 'data_size counts Page Headers and stored Page data; it excludes the Chunk Header. Add the full Header length (including an AEAD suffix) for the total span.',
                    },
                    zh: {
                      title: '非对齐数据块（Non-aligned Chunk）',
                      description: '保存一个测量及其独立的时间戳。每个 Page 都有时间流和值流，两者解码后的数据点数量相同。',
                      offset: 'chunk_offset',
                      size: 'Chunk Header 长度 + data_size',
                      layout: 'Chunk Header | 非对齐 Page*',
                          note: 'data_size 统计 Page Header 和已保存 Page 数据的长度，不包含 Chunk Header；总长度需加上完整 Header 长度（含 AEAD 后缀）。',
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
                          layoutNotes: [
                            'Chunk role and Page-count marker',
                            'Measurement identifier',
                            'Byte length of Page Headers and stored Page data',
                            'Value type',
                            'Compression method',
                            'Value encoding (time encoding for a Time Chunk)',
                            'AEAD suffix: int64 ordinal, int32 proof length, and proof bytes',
                          ],
                          note: 'encoding_type describes values; time encoding is external configuration (default TS_2DIFF). The AEAD suffix is int64 ordinal | int32 proof_size | proof[proof_size], outside data_size.',
                        },
                        zh: {
                          title: '数据块头部（Chunk Header）',
                          description: '记录测量名及编解码方式。data_size 统计头部之后全部 Page Header 和载荷的长度，读取器无需解码值就能定位下一个 Chunk。',
                          offset: 'chunk_offset',
                          size: '可变',
                          layout:
                            'chunk_type | measurement_id | data_size | data_type | compression_type | encoding_type | AEAD suffix?',
                          layoutNotes: [
                            'Chunk 角色与 Page 数量标记',
                            '测量标识',
                            'Page Header 与已保存 Page 数据的字节长度',
                            '值类型',
                            '压缩方式',
                            '值编码（Time Chunk 为时间编码）',
                            'AEAD 后缀：int64 序号、int32 证明长度、证明字节',
                          ],
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
                            'One Page of a non-aligned Chunk: a Header followed by the encoded time and value streams.',
                          offset: 'inside Chunk data_size',
                          size: 'variable · repeated',
                          layout: 'Page Header | Page Data',
                          note: 'Time and values pair by position. Their codecs can differ; the time codec comes from shared configuration.',
                        },
                        zh: {
                          title: '非对齐数据页（Non-aligned Page）',
                          description: '非对齐 Chunk 的一个 Page：Header 之后是编码后的时间流和值流。',
                          offset: 'Chunk data_size 内部',
                          size: '可变 · 重复',
                          layout: 'Page Header | Page Data',
                          note: '时间戳和值按位置配对，可以使用不同编码；时间编码由共享配置提供。',
                        },
                      },
                      children: [
                        pageHeaderNode('non-aligned-page-header'),
                        pageDataNode(
                          'non-aligned-page-data',
                          'DataPages/index.html#non-aligned-page',
                          {
                            description:
                              'Logical payload after decryption and decompression: the time stream comes first, then the value stream.',
                            layout: 'time_stream_size | encoded_time_stream | encoded_value_stream',
                            layoutNotes: [
                              'UVarInt length of the time stream',
                              'Encoded time stream',
                              'Encoded value stream; fills the remaining bytes',
                            ],
                            note: 'On disk the whole payload is stored_data[compressed_size], compressed and possibly encrypted.',
                          },
                          {
                            description: '解密、解压后的逻辑载荷：先时间流，后值流。',
                            layout: 'time_stream_size | encoded_time_stream | encoded_value_stream',
                            layoutNotes: [
                              '时间流的 UVarInt 长度',
                              '编码后的时间流',
                              '编码后的值流；占据剩余字节',
                            ],
                            note: '磁盘上整个载荷是 stored_data[compressed_size]，经过压缩并可能加密。',
                          },
                        ),
                      ],
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
                      title: '对齐数据块集（Aligned Chunk Set）',
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
                          size: 'Chunk Header length + data_size',
                          layout: 'Time Chunk Header | Time Page*',
                          note: 'data_size counts Page Headers and stored Page data; it excludes the Chunk Header. Add the full Header length (including an AEAD suffix) for the total span.',
                        },
                        zh: {
                          title: '时间列（Time Chunk）',
                          description: '保存各对齐测量共享的时间戳。头部说明时间编码，每个 Time Page 确定对应 Value Page 使用的那组位置。',
                          offset: 'time_chunk_offset',
                          size: 'Chunk Header 长度 + data_size',
                          layout: 'Time Chunk Header | Time Page*',
                          note: 'data_size 统计 Page Header 和已保存 Page 数据的长度，不包含 Chunk Header；总长度需加上完整 Header 长度（含 AEAD 后缀）。',
                        },
                      },
                      children: [
                        {
                          id: 'time-page',
                          kind: 'repeated',
                          link: 'DataPages/index.html#aligned-time-page',
                          text: {
                            en: {
                              title: 'Time Page',
                              description: 'One Page of encoded timestamps shared by the matching Value Pages.',
                              offset: 'inside Time Chunk data_size',
                              size: 'variable · repeated',
                              layout: 'Page Header | Page Data',
                              note: 'A Time Page has no bitmap; every position has a timestamp.',
                            },
                            zh: {
                              title: '时间页（Time Page）',
                              description: '一个编码时间戳 Page，对应 Value Page 使用它的位置序列。',
                              offset: 'Time Chunk data_size 内部',
                              size: '可变 · 重复',
                              layout: 'Page Header | Page Data',
                              note: 'Time Page 没有位图；每个位置都有时间戳。',
                            },
                          },
                          children: [
                            pageHeaderNode('time-page-header'),
                            pageDataNode(
                              'time-page-data',
                              'DataPages/index.html#aligned-time-page',
                              {
                                description:
                                  'Logical payload after decryption and decompression: the encoded time stream.',
                                layout: 'encoded_time_stream',
                                layoutNotes: [
                                  'Encoded timestamps for the positions shared by the matching Value Pages',
                                ],
                                note: 'On disk this is stored_data[compressed_size].',
                              },
                              {
                                description: '解密、解压后的逻辑载荷：编码后的时间流。',
                                layout: 'encoded_time_stream',
                                layoutNotes: [
                                  '对应 Value Page 共享位置的编码时间戳',
                                ],
                                note: '磁盘上是 stored_data[compressed_size]。',
                              },
                            ),
                          ],
                        },
                      ],
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
                          size: 'Chunk Header length + data_size · repeated',
                          layout: 'Value Chunk Header | Value Page*',
                          note: 'data_size counts Page Headers and stored Page data; it excludes the Chunk Header. Add the full Header length (including an AEAD suffix) for the total span.',
                        },
                        zh: {
                          title: '测点列（Value Chunk）',
                          description: '保存一个对齐测点列，通过各 Page 的位图把值对应到时间戳，其余位置为 null。',
                          offset: 'value_chunk_offset',
                          size: 'Chunk Header 长度 + data_size · 重复',
                          layout: 'Value Chunk Header | Value Page*',
                          note: 'data_size 统计 Page Header 和已保存 Page 数据的长度，不包含 Chunk Header；总长度需加上完整 Header 长度（含 AEAD 后缀）。',
                        },
                      },
                      children: [
                        {
                          id: 'value-page',
                          kind: 'repeated',
                          link: 'DataPages/index.html#aligned-value-page',
                          text: {
                            en: {
                              title: 'Value Page',
                              description: 'One Page of values for an aligned measurement; its bitmap maps stored values onto positions from the matching Time Page.',
                              offset: 'inside Value Chunk data_size',
                              size: 'variable · repeated',
                              layout: 'Page Header | Page Data',
                              note: 'An all-null Value Page is only UVarInt(0); it has no Header or Data.',
                            },
                            zh: {
                              title: '测点页（Value Page）',
                              description: '单个对齐测点的一个 Page；位图把值映射到对应 Time Page 的位置。',
                              offset: 'Value Chunk data_size 内部',
                              size: '可变 · 重复',
                              layout: 'Page Header | Page Data',
                              note: '全 null 的 Value Page 只保存 UVarInt(0)，没有 Header 或 Data。',
                            },
                          },
                          children: [
                            pageHeaderNode('value-page-header'),
                            pageDataNode(
                              'value-page-data',
                              'DataPages/index.html#aligned-value-page',
                              {
                                description:
                                  'Logical payload after decryption and decompression: the position count, present bitmap, and non-null values.',
                                layout:
                                  'position_count | present_bitmap | encoded_non_null_values',
                                layoutNotes: [
                                  'int32 position count, equal to the matching Time Page',
                                  'Bitmap with ceil(position_count / 8) bytes; a set bit means a value is present',
                                  'Encoded non-null values, in position order',
                                ],
                                note: 'On disk this is stored_data[compressed_size]; an all-null Value Page is only UVarInt(0).',
                              },
                              {
                                description:
                                  '解密、解压后的逻辑载荷：位置数、存在位图和非 null 值。',
                                layout:
                                  'position_count | present_bitmap | encoded_non_null_values',
                                layoutNotes: [
                                  'int32 位置数，等于对应 Time Page 的位置数',
                                  '位图长度为 ceil(position_count / 8)；置位表示该位置有值',
                                  '按位置顺序编码的非 null 值',
                                ],
                                note: '磁盘上是 stored_data[compressed_size]；全 null 的 Value Page 只保存 UVarInt(0)。',
                              },
                            ),
                          ],
                        },
                      ],
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
          layoutNotes: [
            'Zero or more entries; each entry is either a TimeseriesMetadata or a MetadataIndexNode. The two kinds may be interleaved.',
          ],
        },
        zh: {
          title: '索引元数据区',
          description: '保存时间序列元数据和索引节点。两者可以交错存放，读取时应跟随绝对偏移量，不依赖固定顺序。',
          offset: 'meta_offset + 1 … file_metadata_pos - 1',
          size: '可变',
          layout: '(TimeseriesMetadata | MetadataIndexNode)*',
          layoutNotes: [
            '零个或多个条目；每条是 TimeseriesMetadata 或 MetadataIndexNode，两者可以交错出现。',
          ],
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
              layoutNotes: [
                'Column role and Chunk Metadata layout flags',
                'Measurement identifier',
                'Physical value type',
                'Byte length of the Chunk Metadata list',
                'Series-level Statistics',
                'Chunk Metadata records; one record per Chunk',
              ],
            },
            zh: {
              title: '时间序列元数据',
              description: '描述一条时间序列，包含汇总统计信息和 Chunk 列表。列表中的每个条目指向数据区的一个 Chunk Header。',
              offset: '由测量索引条目引用',
              size: '可变 · 重复',
              layout:
                'timeseries_type | measurement_id | data_type | chunk_metadata_list_size | Statistics | ChunkMetadata*',
              layoutNotes: [
                '列角色与 Chunk Metadata 布局标志',
                '测量标识',
                '物理值类型',
                'Chunk Metadata 列表的字节长度',
                '序列级 Statistics',
                'Chunk Metadata 记录；每个 Chunk 一条',
              ],
            },
          },
          children: [
            {
              id: 'series-statistics',
              kind: 'record',
              link: 'Metadata.html#statistics',
              text: {
                en: {
                  title: 'Series Statistics',
                  description: 'Aggregated Statistics for the whole series. The common prefix is followed by fields selected by data_type.',
                  offset: 'inside Timeseries Metadata',
                  size: 'variable',
                  layout: 'UVarInt count | int64 start_time | int64 end_time | type-specific fields',
                  layoutNotes: [
                    'Number of values represented by this record',
                    'Inclusive start time',
                    'Inclusive end time',
                    'Fields selected by data_type (min, max, first, last, sum, or none)',
                  ],
                },
                zh: {
                  title: '统计信息（Statistics）',
                  description: '整个序列的汇总 Statistics；公共前缀之后是按 data_type 选择的类型字段。',
                  offset: '时间序列元数据内部',
                  size: '可变',
                  layout: 'UVarInt count | int64 start_time | int64 end_time | type-specific fields',
                  layoutNotes: [
                    '该记录覆盖的值数量',
                    '包含首部的开始时间',
                    '包含尾部的结束时间',
                    '按 data_type 选择的字段（min / max / first / last / sum 或无）',
                  ],
                },
              },
            },
            {
              id: 'chunk-metadata',
              kind: 'repeated',
              link: 'Metadata.html#chunk-metadata',
              text: {
                en: {
                  title: 'Chunk Metadata',
                  description: 'One record per Chunk. It points to the Chunk Header in the data section; its Statistics are omitted when the series has only one Chunk.',
                  offset: 'referenced by a Chunk Metadata list entry',
                  size: 'variable · repeated',
                  layout: 'int64 offset_of_chunk_header | Statistics?',
                  layoutNotes: [
                    'Absolute offset of the Chunk marker',
                    'Chunk-level Statistics; present only when the series has multiple Chunks',
                  ],
                },
                zh: {
                  title: '数据块元数据（Chunk Metadata）',
                  description: '每个 Chunk 一条记录，指向数据区的 Chunk Header；序列只有一个 Chunk 时省略这里的 Statistics。',
                  offset: '由 Chunk Metadata 列表条目引用',
                  size: '可变 · 重复',
                  layout: 'int64 offset_of_chunk_header | Statistics?',
                  layoutNotes: [
                    'Chunk marker 的绝对偏移',
                    'Chunk 级 Statistics；仅当序列包含多个 Chunk 时存在',
                  ],
                },
              },
            },
          ],
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
          link: 'Metadata.html#file-metadata',
          text: {
            en: {
              title: 'Table Index Roots',
              description: 'Entry point for each table\'s device-index tree. Look up the table root, descend to a device, then follow that device\'s measurement index to the Timeseries Metadata.',
              offset: 'inside TsFileMetadata',
              size: 'variable · repeated',
              layout: 'VarString table_name | MetadataIndexNode device_index_root',
              layoutNotes: [
                'Table name: the actual table name, or the first DeviceID segment for tree-model data',
                'Root node of the table\'s device-level metadata index',
              ],
              note: 'Lookup path: table root -> device index -> measurement index -> Timeseries Metadata -> Chunk Metadata.',
            },
            zh: {
              title: '表索引根（Table Index Root）',
              description: '每张表的设备索引树入口：先按表名找到根，再向下定位设备，最后通过设备的测量索引找到 Timeseries Metadata。',
              offset: 'TsFileMetadata 内部',
              size: '可变 · 重复',
              layout: 'VarString table_name | MetadataIndexNode device_index_root',
              layoutNotes: [
                '表名：表模型为实际表名；树模型为 DeviceID 的第一段',
                '该表设备级元数据索引的根节点',
              ],
              note: '查找路径：表根 → 设备索引 → 测量索引 → Timeseries Metadata → Chunk Metadata。',
            },
          },
          children: [
            {
              id: 'device-index-root',
              kind: 'record',
              link: 'Metadata.html#metadata-index',
              text: {
                en: {
                  title: 'Device Index Root',
                  description: 'Root of the device-level index tree. Its entries locate a device and, through the entry offset, that device\'s measurement index.',
                  offset: 'device_index_root',
                  size: 'variable',
                  layout: 'child_count | DeviceIndexEntry* | end_offset | node_type',
                  layoutNotes: [
                    'Number of child entries',
                    'Device entries: DeviceID plus an absolute offset',
                    'End offset of this node\'s byte range',
                    'LEAF_DEVICE points directly to measurement indexes; INTERNAL_DEVICE points to another device-index node',
                  ],
                },
                zh: {
                  title: '设备索引根（Device Index Root）',
                  description: '设备级索引树的根；条目定位设备，并通过条目 offset 找到该设备的测量索引。',
                  offset: 'device_index_root',
                  size: '可变',
                  layout: 'child_count | DeviceIndexEntry* | end_offset | node_type',
                  layoutNotes: [
                    '子条目数量',
                    '设备条目：DeviceID 加绝对偏移',
                    '该节点字节范围的结束偏移',
                    'LEAF_DEVICE 直接指向测量索引；INTERNAL_DEVICE 指向下一级设备索引节点',
                  ],
                },
              },
              children: [
                {
                  id: 'device-index-entry',
                  kind: 'repeated',
                  link: 'Metadata.html#metadata-index',
                  text: {
                    en: {
                      title: 'Device Index Entry',
                      description: 'One device entry in a device-index node. The entry key is the first DeviceID in its range.',
                      offset: 'inside a device-index node',
                      size: 'variable · repeated',
                      layout: 'DeviceID first_device_id | int64 offset',
                      layoutNotes: [
                        'First DeviceID covered by this entry',
                        'Absolute offset to the measurement index root (LEAF_DEVICE) or to a child device-index node (INTERNAL_DEVICE)',
                      ],
                    },
                    zh: {
                      title: '设备索引条目（Device Index Entry）',
                      description: '设备索引节点中的一个设备条目；条目 key 是它覆盖范围里的第一个 DeviceID。',
                      offset: '设备索引节点内部',
                      size: '可变 · 重复',
                      layout: 'DeviceID first_device_id | int64 offset',
                      layoutNotes: [
                        '该条目覆盖范围里的第一个 DeviceID',
                        '绝对偏移：LEAF_DEVICE 指向测量索引根，INTERNAL_DEVICE 指向下一级设备索引节点',
                      ],
                    },
                  },
                },
              ],
            },
          ],
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
              title: '表结构（Schema）',
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
