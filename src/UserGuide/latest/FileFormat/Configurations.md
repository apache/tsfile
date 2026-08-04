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

# Configurations

Most TsFile configurations are writer policies. They control buffering, Page boundaries,
Chunk Group boundaries, and the shape of metadata indexes, but they are not additional fields in
the file. A reader determines the actual layout from markers, length fields, Chunk Headers, and
serialized metadata.

The names below identify format-level concepts. Individual APIs may expose different setting names,
but the effect on bytes and the reader requirement are the same.

| Conceptual setting | Observable file effect | Setting serialized directly? | Reader needs original setting? |
| --- | --- | --- | --- |
| Chunk Group size | Where a device group is flushed | No | No |
| Page size target | Page boundaries and Statistics granularity | No | No |
| Maximum points per Page | Page boundaries | No | No |
| Metadata index fanout | Index-tree depth and node widths | No | No |
| Bloom false-positive rate | Generated bit count and hash-function count | No; generated values are serialized | No |
| Value encoding | Encoded Page stream | Yes, as `encoding_type` in each Chunk | No |
| Compression | Stored Page payload | Yes, as `compression_type` in each Chunk | No |
| Time precision and zone | Interpretation of `int64` timestamps | No | Yes, from the surrounding protocol |
| Non-aligned time encoding | Encoded time stream in each non-aligned Page | No | Yes |
| Encryption | Stored Page payload and footer properties | Properties are serialized; key context may be external | Yes, when encrypted |

“Not serialized directly” means that the configured threshold or rate is absent as a named field.
Its result can still be visible in the physical layout.

## Chunk Group Size

The Chunk Group size is the amount of data that a writer buffers before it flushes the Chunks of
the current device. Larger Chunk Groups reduce marker and metadata overhead and favor sequential
I/O, but require more writer memory. Smaller Chunk Groups reduce peak memory and make data
available to the output sooner.

The configured size is a flush threshold, not an exact on-disk size. The format does not prescribe
a default. Chunk Group boundaries are identified by the `0x00` Chunk Group Header marker and the
next top-level marker.

## Data Page Size

The Data Page size is a target for the in-memory data accumulated by a series writer. Smaller
Pages provide finer-grained Statistics and skipping. Larger Pages reduce Page Header overhead and
can improve compression, at the cost of more buffering and coarser skipping.

The threshold is not an exact serialized Page size. A writer can finish a Page earlier because of
the maximum point count, an explicit flush, or aligned-page boundary requirements. The actual
Page boundary is determined by the Page Header lengths. The format does not prescribe a default
Page size.

## Maximum Points per Page

A writer may limit the number of logical positions in one Page independently of the Page size
target. This prevents a Page containing many compact values from growing to an excessive number
of points. The reference writer configuration uses `10,000` points.

For aligned series, the Time Page and its corresponding Value Pages must cover the same logical
position range. A writer therefore applies a common boundary to the aligned columns.

The configured limit is not stored as a separate field. Readers use Page boundaries and, for an
aligned Value Page, `position_count`.

## Metadata Index Fanout

The metadata index fanout limits the number of children in one internal metadata index node.
A larger fanout produces a shallower tree with larger nodes; a smaller fanout produces a deeper
tree with smaller nodes. The value must be at least `2`; the reference writer configuration uses
`256`.

The configured limit is not serialized. Each metadata index node stores its node type, children,
and end offset, so a reader traverses the resulting tree without knowing the writer setting.

## Bloom Filter False-positive Rate

The Bloom filter false-positive rate controls the space/accuracy tradeoff for path lookup. A lower
rate uses more bits and hash functions. Standard writers constrain the value to the range `0.01`
through `0.10` and use `0.05` by default.

The rate itself is not serialized. The resulting filter bytes, bit count, and hash-function count
are stored in `TsFileMetadata`, so a reader does not need the writer setting. See
[Bloom Filter](Bloom-Filter.md).

## Value Encoding and Compression

A writer may select a default value encoding by physical type and a default compression method.
These defaults are schema and writer policies rather than shared reader configuration:

- `encoding_type` in the Chunk Header identifies the value encoding, or the time encoding for an
  aligned Time Chunk.
- `compression_type` in the Chunk Header identifies how every Page in the Chunk is compressed.

Consequently, a reader uses the Chunk Header and does not need the writer's defaults. Supported
identifiers and their payload formats are defined in [Encodings](DataPages/Encodings.md) and
[Compression](DataPages/Compression.md).

## Time Precision and Time Zone

A physical timestamp is a signed 64-bit integer. TsFile v4 does not record a time unit or time
zone in the file. The protocol that uses the file defines whether the precision is milliseconds,
microseconds, nanoseconds, or another unit, as well as any time-zone semantics.

## Non-aligned Time Encoding

The `encoding_type` in a non-aligned Chunk Header describes the value stream, not the time stream
in the same Page. Readers and writers determine the non-aligned time-stream encoding from shared
configuration. The default is `TS_2DIFF`.

This restriction does not apply to an aligned Time Chunk. Its Chunk Header records the time
encoding in `encoding_type`.

## Encryption

The `encryptLevel`, `encryptType`, and `encryptKey` properties in `TsFileMetadata` describe whether
Page data is encrypted and which algorithm is used. The key material required to open an encrypted
file remains external to the file. See [Encryption](DataPages/Encryption.md).

## Implementation Settings

Settings for memory allocation, memory-check cadence, batch size, parallel execution, thread
count, storage backend, and close-time synchronization do not define TsFile bytes. They are
implementation and deployment concerns and are outside this file-format specification.
