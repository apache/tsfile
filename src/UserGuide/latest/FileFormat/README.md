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

A TsFile stores time series data in columns. Writers append data in device groups, then write
the indexes at the end of the file. Readers usually work in the opposite direction: they open
the footer, look up a series, and seek to the Chunks that contain its values.

This guide describes the v4 file layout, identified by version byte `0x04`, using the Java
reference implementation as of 2026-10-26. It covers the file structures and how they fit together.
The encoding chapter gives codec identifiers and the `PLAIN` layout; the bit layouts of the other
value codecs are outside its scope.

## Scope and Terminology

Data has three levels of organization within a file:

- A **Page** holds a batch of encoded values. Each Page is compressed independently.
- A **Chunk** holds the Pages of one physical column. Its header identifies the data type,
  encoding, and compression method. An aligned Value Chunk can have `data_size = 0`; in that
  case the Header exists but contains no Page.
- A **Chunk Group** holds a batch of Chunks for one device. The same device can appear in several
  Chunk Groups as the writer flushes more data.

In **non-aligned** data, each measurement stores its own timestamps alongside its values. In
**aligned** data, a Time Chunk stores the timestamps shared by several Value Chunks. A bitmap in
each Value Page identifies the positions that have values. [Data Pages](DataPages/README.md)
explains both layouts.

<TsFileStructureTree locale="en" />

## Physical Regions

The file begins with the six-byte magic string `TsFile` and a one-byte version. An optional
file encryption header follows for Page AEAD files. The data section ends at a single `0x02`
separator; indexes and file metadata follow it. The last ten bytes hold the metadata length and
the trailing magic string.

The table below uses these names:

- `F`: total file length.
- `L`: serialized length of `TsFileMetadata`, stored in the four-byte trailing length field.
- `E`: total serialized length of the optional encryption header, or `0` when absent.
- `data_start = 7 + E`: start of the data section.
- `meta_offset`: separator offset, stored inside `TsFileMetadata`.

| Region | Inclusive byte interval | How it is located |
| --- | --- | --- |
| Header magic | `0 … 5` | Fixed position |
| Version | `6` | Fixed position |
| Optional file encryption header | `7 … 6 + E` | Present only for Page AEAD; marker `0x06` |
| Data section | `data_start … meta_offset - 1` | Ends immediately before the separator |
| Separator | `meta_offset` | `0x02`, referenced by `TsFileMetadata` |
| Indexed metadata | `meta_offset + 1 … F - 11 - L` | Delimited by metadata offsets |
| `TsFileMetadata` | `F - 10 - L … F - 11` | Located from the trailing length |
| Metadata length | `F - 10 … F - 7` | Fixed position relative to the end |
| Tail magic | `F - 6 … F - 1` | Fixed position relative to the end |

All offsets are absolute, starting at the first byte of the file. The table uses inclusive
intervals; `[start, end)` elsewhere denotes a half-open interval. A length excludes its own
length field unless stated otherwise. The optional-header interval is absent when `E = 0`.

Timeseries Metadata and index nodes can be interleaved in the indexed metadata section. Readers
follow their offsets rather than assuming a fixed physical order. `TsFileMetadata` has no marker
of its own: its length locates it from the end of the file.

## Write Model

A writer appends Chunk Groups, then writes the indexes and footer when it finishes the file.
Page sizes, flush thresholds, and index fanout affect where structures begin and end, but readers
can recover those boundaries from headers and offsets. Timestamp units and non-aligned time
encoding need the shared context described in [Configurations](Configurations.md).

Both magic strings use the same bytes:

```text
54 73 46 69 6c 65    # "TsFile"
```

## Metadata-first Read Algorithm

A reader can locate the requested series without scanning the data section:

1. Check the leading magic and version. Inspect offset `7` for an optional
   [file encryption header](DataPages/Encryption.md#file-encryption-header), which determines
   how Chunk Headers and Page bodies are parsed.
2. Check the tail magic, read `L` at `F - 10`, and calculate `file_metadata_pos = F - 10 - L`.
3. Read `TsFileMetadata` and check that `meta_offset` points to the separator.
4. Optionally check the complete path in the [Bloom filter](Bloom-Filter.md). Use a negative
   result only when the writer's charset, path construction, and hash behavior are known to
   match.
5. Follow the table/device index, then the measurement index, to find the series metadata.
6. Use the series and Chunk Statistics to select Chunk offsets. Seek to those Chunks and use
   any Page Statistics to narrow the selection further.
7. Read the selected Page bodies, decrypt when required, decompress, and decode their streams.

This avoids reading the payloads of unrelated Pages. A single-Page Chunk omits Page Statistics,
so pruning at that level relies on the Chunk or series Statistics instead.

## Structural Invariants

The main relationships to check when reading are:

- Both magic strings and the version are valid.
- A file encryption header, when present, appears once at offset `7`; its length and version
  are valid.
- `0 <= L <= F - 10`, and `file_metadata_pos` lies after the separator.
- A Chunk's Pages occupy exactly `data_size` bytes after its complete Header.
- Each non-empty Page body occupies exactly `compressed_size` bytes within that Chunk.
- Index offsets are monotonic (non-decreasing) within a node and do not exceed `end_offset`.
- An aligned Value Page covers the same positions as the Time Page with the same ordinal; its
  `position_count` equals the Time Page's position count.

A violation is a format error.

[Binary Representation](Binary-Representation.md) describes the common length and offset checks.
[Encryption](DataPages/Encryption.md) adds the authentication checks for Page AEAD. Marker bytes
have meaning only at structure boundaries; the same bytes can occur inside a Page body.

## Format Details

For a first reading, start with [Binary Representation](Binary-Representation.md),
[Chunks](DataPages/Chunks.md), and [Data Pages](DataPages/README.md), then follow the metadata
lookup in [Metadata](Metadata.md).

- [Types](Types.md) and [Encodings](DataPages/Encodings.md)
- [Nulls](Nulls.md) and [Compression](DataPages/Compression.md)
- [Bloom Filter](Bloom-Filter.md)
- [Encryption](DataPages/Encryption.md) and [Checksumming](DataPages/Checksumming.md)
- [Error Recovery](DataPages/Error-Recovery.md)
- [Configurations](Configurations.md), [Format Versions](Format-Versions.md), and
  [Extensibility](Extensibility.md)
