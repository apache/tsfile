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

# Chunks

The TsFile data section consists of marker-delimited Chunk Groups, Chunks, and Operation Index
Ranges. It starts at offset `7` without a file encryption header, or at `7 + E` when a Page AEAD
[FileEncryptionHeader](Encryption.md#file-encryption-header) of total serialized size `E` occupies
offset `7`. That optional header occurs exactly once, before the data section.

The top-level grammar is:

```text
FilePrefix  := magic[6] version[1] FileEncryptionHeader?
DataSection := (ChunkGroup | OperationIndexRange)*
ChunkGroup  := ChunkGroupHeader Chunk*
Separator   := byte(0x02)
```

The `0x02` separator follows the data section; it is not part of a Chunk Group. A Chunk Group
has no total-length field. After reading each Chunk, the reader examines the next marker: another
Chunk continues the group, while a new group, operation-index record, or separator ends it.

```text
File
└── Chunk Group                 a batch of data for one device
    ├── Non-aligned Chunk       one measurement column with its own time stream
    │   └── Page(s)
    └── Aligned Chunk Set       measurement columns sharing a time column
        ├── Time Chunk
        │   └── Time Page(s)
        └── Value Chunk(s)
            └── Value Page(s)
```

## Markers

| Value | Name | Following structure |
| ---: | --- | --- |
| `0x00` | `CHUNK_GROUP_HEADER` | `DeviceID` |
| `0x01` | `CHUNK_HEADER` | Multi-page non-aligned Chunk |
| `0x02` | `SEPARATOR` | End of data section and start of metadata section |
| `0x03` | `VERSION` | Unused since v3; identifier reserved |
| `0x04` | `OPERATION_INDEX_RANGE` | Two `int64` values |
| `0x05` | `ONLY_ONE_PAGE_CHUNK_HEADER` | Single-page non-aligned Chunk |
| `0x06` | `ENCRYPTION_HEADER` | File encryption header at offset `7` only; not a Chunk |
| `0x41` | `VALUE_CHUNK_HEADER` | Multi-page aligned Value Chunk |
| `0x45` | `ONLY_ONE_PAGE_VALUE_CHUNK_HEADER` | Single-page aligned Value Chunk |
| `0x81` | `TIME_CHUNK_HEADER` | Multi-page aligned Time Chunk |
| `0x85` | `ONLY_ONE_PAGE_TIME_CHUNK_HEADER` | Single-page aligned Time Chunk |

The high two bits of a Chunk marker identify the column role. The low six bits identify the Page
count category:

```text
bit 7 = 1: aligned time column
bit 6 = 1: aligned value column
low 6 bits = 0x01: more than one Page
low 6 bits = 0x05: one Page or no Page
```

An aligned Value Chunk with `data_size = 0` can contain no Page.

Read markers only after consuming the preceding structure. A Page body can contain any byte
value, including bytes that look like markers.

## Chunk Group Header

```text
ChunkGroupHeader :=
    byte       marker = 0x00
    DeviceID   device_id

DeviceID :=
    UVarInt    segment_count
    VarString  segment[segment_count]
```

A DeviceID is an ordered list of string segments; individual segments can be null. Serialization
removes trailing nulls, but at least one segment must be non-null. The special `segment_count = 0`
form is read as one empty-string segment.

Chunks following a Chunk Group Header belong to that device until the next Chunk Group Header,
Operation Index Range, or Separator.

`segment_count` counts segments. Each segment has its own byte length. Keep the segments separate
when comparing device identities: joining them with dots for display can be ambiguous if a segment
itself contains a dot.

## Chunk Header

The marker is the first field of the Chunk Header:

```text
ChunkHeader :=
    byte       chunk_type
    VarString  measurement_id
    UVarInt    data_size
    byte       data_type
    byte       compression_type
    byte       encoding_type
    int64      chunk_ordinal?
    int32      chunk_authentication_size?
    byte       chunk_authentication[chunk_authentication_size]?
```

The final three fields appear only in Page AEAD files. The file encryption header selects this
suffix; the Chunk marker remains the same. The ordinal, proof format, and validation rules are
described in [AEAD Chunk Header](Encryption.md#aead-chunk-header). Like the rest of the Header,
the suffix is excluded from `data_size`.

`data_size` is the total size of all Page Headers and stored Page data in the Chunk. It does not
include the Chunk Header. A reader consumes exactly `data_size` bytes after the Chunk Header.

If `H` is the offset immediately after the Chunk Header, then the next structure begins at:

```text
next_structure_offset = H + data_size
```

The sum of every serialized Page Header and its `compressed_size` bytes must equal `data_size`.
The value is therefore both a skip length and a containment boundary for validating Pages.

For an aligned Time Chunk, `measurement_id` is an empty string and `data_type` is `VECTOR (0x06)`.
For an aligned Value Chunk or non-aligned Chunk, `data_type` is the measurement value type.

Aligned data is stored Chunk by Chunk, not as interleaved Time Pages and Value Pages. The Time
Chunk stores the complete sequence of Time Pages, and each Value Chunk stores its own sequence of
Value Pages. Pages correspond by ordinal position.

An aligned Chunk set is one Time Chunk followed by its Value Chunks. It has no separate set
header. The relationships within a set are:

- The Time Chunk appears before the Value Chunks that use it.
- A Value Chunk identifies an individual measurement; the Time Chunk uses an empty
  `measurement_id` and `VECTOR (0x06)` as its physical type marker.
- Value Page `i` covers the logical positions of Time Page `i`.
- A Value Chunk may be absent when the measurement has no data in the set. An existing aligned
  Value Chunk can also have `data_size = 0`.

For a non-aligned Chunk, every logical point carries a timestamp and a value. Consequently, the
decoded time stream and decoded value stream of each Page must contain the same number of
elements.

## Chunk Scan State Machine

A streaming or recovery reader can process the data section with this state machine:

1. Validate the prefix, inspect offset `7`, and consume the optional `0x06` encryption header.
   Resolve its provider/profile before parsing Chunks; continue at `data_start = 7 + E` (`E = 0`
   when absent). A later or repeated `0x06` is invalid. Read the first data-section marker there.
2. For `0x00`, read the DeviceID and enter that Chunk Group.
3. For a Chunk marker, read its complete Header, including the AEAD suffix when selected, and
   verify the Chunk-position proof before using its identity. Advance by exactly `data_size` bytes
   after optionally parsing its Pages.
4. For `0x04`, consume the following 16 bytes as an operation-index range.
5. For `0x02`, stop. The following byte belongs to the indexed metadata region.
6. Reject an unrecognized top-level marker or a structure whose declared boundary exceeds the
   available bytes.

## Operation Index Range

```text
OperationIndexRange :=
    byte   marker = 0x04
    int64  min_operation_index
    int64  max_operation_index
```

This optional record lets a storage engine associate a flushed batch with its operation log.
The two values are the minimum and maximum operation sequence numbers in that batch. They help
coordinate checkpoints, snapshots, backups, and recovery; they are unrelated to timestamps or
file offsets.

A general-purpose writer can omit this record. A reader that does not use operation indexes
still consumes both `int64` fields before reading the next marker. Several records can appear
when several batches are flushed into the same file.
