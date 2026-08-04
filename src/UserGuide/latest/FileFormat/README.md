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

# File Format

This document describes the language-independent binary format of TsFile version 4. The file
version byte is `0x04`.

<TsFileStructureTree locale="en" />

## Scope and Terminology

This specification defines the bytes required to exchange a completed TsFile v4 between
independent implementations. It distinguishes four levels of organization:

- A **Page** is the smallest independently compressed and decoded data unit.
- A **Chunk** contains the Pages of one physical column and fixes their data type, encoding, and
  compression method.
- A **Chunk Group** associates a sequence of Chunks with one device identifier.
- A **file** contains the sequential data section followed by indexes and file-level metadata.

“Aligned” describes measurements that share one physical time column. “Non-aligned” describes a
measurement that carries its own time stream. Alignment changes the Page payload layout; it does
not change the meaning of a timestamp or value.

All byte intervals in this specification are inclusive unless expressed as a half-open range.
An absolute offset is measured from the first byte of the file. A serialized length excludes the
field that stores that length unless stated otherwise.

## Physical Regions

For a file of length `F`, let `L` be the four-byte length of `TsFileMetadata` and let
`meta_offset` be the value stored inside `TsFileMetadata`:

| Region | Inclusive byte interval | How it is located |
| --- | --- | --- |
| Header magic | `0 … 5` | Fixed position |
| Version | `6` | Fixed position |
| Data section | `7 … meta_offset - 1` | Ends immediately before the separator |
| Separator | `meta_offset` | `0x02`, referenced by `TsFileMetadata` |
| Indexed metadata | `meta_offset + 1 … F - 11 - L` | Delimited by metadata offsets |
| `TsFileMetadata` | `F - 10 - L … F - 11` | Located from the trailing length |
| Metadata length | `F - 10 … F - 7` | Fixed position relative to the end |
| Tail magic | `F - 6 … F - 1` | Fixed position relative to the end |

The indexed metadata interval can contain Timeseries Metadata and Metadata Index Nodes in an
implementation-dependent physical order. Their absolute offsets, rather than adjacency, define
their relationships.

## Write Model

The data is divided into Chunk Groups by device. A Chunk stores one measurement column and
contains one or more Pages. A non-aligned Chunk stores its time stream and value stream in the
same Page. Aligned data stores one shared time column in a Time Chunk and the measurement columns
in Value Chunks.

File metadata follows the data, so a writer can write all Chunk Groups sequentially before
writing the indexes and footer. A reader normally starts at the end of the file, uses the
`TsFile Metadata size` to locate `TsFileMetadata`, and follows its indexes to the required Chunks.

Writers may choose Page sizes, Chunk Group flush thresholds, metadata-index fanout, encodings,
and compression methods. Those choices affect performance and physical boundaries, but readers
derive the resulting layout entirely from the bytes in the file.

## Metadata-first Read Algorithm

A random-access reader can open a completed file without scanning the data section:

1. Verify the six-byte tail magic.
2. Read `L` at `F - 10` and compute `file_metadata_pos = F - 10 - L`.
3. Deserialize `TsFileMetadata`, then verify that `meta_offset` points to `0x02`.
4. Optionally test the complete path with the Bloom filter. A negative result proves absence; a
   positive result requires index lookup.
5. Traverse the table/device index and then the measurement index to locate the requested
   Timeseries Metadata range.
6. Read the Chunk Metadata offsets selected by the time and value predicates.
7. Seek to each Chunk Header, use Page Statistics when present, and decode only the required
   Pages.

This path makes metadata size proportional to the selected devices, measurements, Chunks, and
Pages rather than to the size of the complete file.

## Structural Invariants

A conforming reader validates at least the following relationships before allocating or reading
an externally supplied length:

- The head and tail magic strings are identical and the version byte is supported.
- `0 <= L <= F - 10` and the computed `file_metadata_pos` is after `meta_offset`.
- `meta_offset` is an in-file absolute offset whose byte is `0x02`.
- Every Chunk ends exactly `data_size` bytes after its Chunk Header.
- Every non-empty Page fits inside its Chunk; its stored payload contains exactly
  `compressed_size` bytes.
- Metadata entry offsets are monotonic within a node and do not exceed `end_offset`.
- An aligned Value Page corresponds to the Time Page at the same ordinal position; its
  `position_count` equals the number of positions in that Time Page.

Length or offset violations are format errors. A reader must not use a marker byte found inside a
length-delimited Page payload as a top-level marker.

The file contains exactly one top-level `0x02` separator that terminates the data section. It precedes all
metadata and is referenced by `meta_offset`. There is no separate separator or marker immediately
before `TsFileMetadata`; its position is determined by the length field at the end of the file.

For a file of length `F` whose serialized `TsFileMetadata` has length `L`:

```text
metadata_size_pos = F - 6 - 4
file_metadata_pos = F - 6 - 4 - L
```

The magic bytes at both the beginning and the end of the file are:

```text
54 73 46 69 6c 65    # "TsFile"
```

## Format Details

- [Binary Representation](Binary-Representation.md)
- [Configurations](Configurations.md)
- [Extensibility](Extensibility.md)
- [Metadata](Metadata.md)
- [Types](Types.md)
- [Bloom Filter](Bloom-Filter.md)
- [Data Pages](DataPages/README.md)
  - [Chunks](DataPages/Chunks.md)
  - [Compression](DataPages/Compression.md)
  - [Encodings](DataPages/Encodings.md)
  - [Encryption](DataPages/Encryption.md)
  - [Checksumming](DataPages/Checksumming.md)
  - [Error Recovery](DataPages/Error-Recovery.md)
- [Nulls](Nulls.md)
- [Format Versions](Format-Versions.md)
