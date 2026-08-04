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

The TsFile data section starts at offset 7 and consists of marker-delimited Chunk Groups, Chunks,
and Operation Index Ranges.

The top-level grammar is:

```text
DataSection := (ChunkGroup | OperationIndexRange)* Separator
ChunkGroup  := ChunkGroupHeader Chunk*
Separator   := byte(0x02)
```

`Chunk*` is not length-prefixed as a group. A reader recognizes the end of a Chunk Group only
after consuming each Chunk by its `data_size` and then reading the next top-level marker.

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

A marker is interpreted only at a known structure boundary. Marker values can occur inside an
encoded or compressed Page payload and have no structural meaning there.

## Chunk Group Header

```text
ChunkGroupHeader :=
    byte       marker = 0x00
    DeviceID   device_id

DeviceID :=
    UVarInt    segment_count
    VarString  segment[segment_count]
```

A device identifier is an ordered tuple of nullable string segments. Trailing null segments are
removed during serialization, and a device identifier cannot consist entirely of null segments.
On read, `segment_count = 0` represents a device containing one empty-string segment.

Chunks following a Chunk Group Header belong to that device until the next Chunk Group Header,
Operation Index Range, or Separator.

`segment_count` counts serialized tuple elements, not UTF-8 bytes. Each segment is independently
length-delimited. Device identity is the ordered tuple itself; joining segments with a display
separator is not part of the binary format and can be ambiguous when a segment contains that
separator.

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
```

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

An aligned set has the following invariants:

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

1. At offset 7, read one top-level marker.
2. For `0x00`, read the DeviceID and enter that Chunk Group.
3. For a Chunk marker, read its Header and advance by exactly `data_size` bytes after optionally
   parsing its Pages.
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

This structure records the minimum and maximum operation indexes involved in the most recent
write batch. Operation indexes are storage-engine sequence numbers; they are not timestamps,
measurement indexes, or file offsets. They can support checkpoint, snapshot, backup, and recovery
logic by describing which logical operations have reached the file.

The record is optional for a general-purpose writer. A writer without operation-index semantics
may omit it or write a placeholder range. A reader that does not use this information still has
to recognize marker `0x04` and skip the complete 16-byte payload so that the next marker is read
at the correct boundary. Multiple records can occur because multiple write batches can be
flushed into one file.
