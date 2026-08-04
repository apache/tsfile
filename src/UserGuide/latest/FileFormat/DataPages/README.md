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

# Data Pages

One or more Pages follow a Chunk Header without padding between Pages. The Chunk Header's
`data_size` is the total byte length of all Page Headers and stored Page data in that Chunk.

## Page Header

A non-empty Page has the following format:

```text
Page :=
    UVarInt    uncompressed_size
    UVarInt    compressed_size
    Statistics statistics?       # determined by the Chunk marker
    byte       stored_data[compressed_size]
```

`uncompressed_size` is the size of the Page payload after decryption and decompression.
`compressed_size` is the size of the Page payload stored in the file. The two values are equal
when `compression_type = UNCOMPRESSED`.

The low six bits of the Chunk marker determine whether the Page Header contains Statistics:

- `0x01`: the Chunk has multiple Pages; every non-empty Page Header contains Statistics.
- `0x05`: the Chunk has one Page or no Page; the Page Header does not contain Statistics.

See [Metadata](../Metadata.md#statistics) for the binary format of Statistics.

The Header shape can therefore be selected without inspecting the Page payload:

| Chunk marker low bits | Page count | Page Statistics |
| ---: | --- | --- |
| `0x01` | More than one | Present in every non-empty Page Header |
| `0x05` | Zero or one | Absent |

If a Page starts at `P`, let `h` be the serialized size of its two length fields and optional
Statistics. The stored payload occupies the half-open interval `[P + h, P + h + compressed_size)`
and the next Page, if any, starts at `P + h + compressed_size`. That end position must not exceed
the enclosing Chunk boundary.

After decryption and decompression, the payload must contain exactly `uncompressed_size` bytes.
A decoder rejects a payload whose produced size differs, even if the compression library reports
successful termination.

## Page Encoding

When writing a non-empty Page, the data passes through these stages:

```text
logical values
  -> value encoding
  -> assemble uncompressed page payload
  -> compression
  -> optional encryption
  -> stored_data
```

Reading applies the inverse order: decrypt, decompress, then decode values.

Encoding and compression are independent. The encoding converts typed values into an
uncompressed byte stream; compression treats that stream as bytes. Page boundaries reset both
the value encoder/decoder and the compressor/decompressor, so no state is carried from one Page
to the next.

## Non-aligned Page

The uncompressed payload of a non-aligned Page is:

```text
NonAlignedPagePayload :=
    UVarInt  time_stream_size
    byte     encoded_time_stream[time_stream_size]
    byte     encoded_value_stream[remaining bytes]
```

A non-aligned Page stores both time and values. It does not use a separate Time Page followed by
a Value Page. The complete time stream is stored first, followed by the complete value stream;
`timestamp, value` pairs are not interleaved point by point. The two streams contain the same
number of elements and pair by position to form `(timestamp, value)` points.

For an uncompressed payload of size `U` and a serialized `time_stream_size` field of `v` bytes,
the value stream length is:

```text
value_stream_size = U - v - time_stream_size
```

The result must be non-negative. After decoding, both stream decoders must reach their boundaries
and produce the same point count. A trailing byte, an early end, or unequal counts is a malformed
Page.

A non-aligned Chunk Header records only the value stream's `encoding_type`. See
[Configurations](../Configurations.md#non-aligned-time-encoding) for the time-stream encoding.

## Aligned Time Page

The uncompressed payload of an aligned Time Page consists entirely of the encoded time stream:

```text
AlignedTimePagePayload := byte encoded_time_stream[remaining bytes]
```

The `encoding_type` in the Time Chunk Header identifies the time encoding.

The number of decoded timestamps is the logical position count of this Page. That count controls
the bitmap width and null reconstruction of every corresponding aligned Value Page.

## Aligned Value Page

The uncompressed payload of a non-empty aligned Value Page is:

```text
AlignedValuePagePayload :=
    int32  position_count
    byte   present_bitmap[ceil(position_count / 8)]
    byte   encoded_non_null_values[remaining bytes]
```

Within one aligned set, Value Page number `i` corresponds to Time Page number `i`. See
[Nulls](../Nulls.md) for the null bitmap and the all-null empty Page representation.

`position_count` must equal the decoded timestamp count of the corresponding Time Page. The
bitmap has exactly `ceil(position_count / 8)` bytes; unused low-order bits in its last byte do not
describe positions. The value decoder is invoked only for set bits and must produce exactly the
number of set bits.

## Page Selection and Skipping

Page Statistics allow a reader to evaluate time and value predicates before reading stored Page
data. A Page can be skipped only when its Statistics prove that no point can satisfy the
predicate. Statistics are conservative summaries: overlap means “possibly matches,” not
“definitely matches.”

For a one-Page Chunk, the Page Header omits Statistics. The equivalent Chunk or Timeseries
Statistics in the footer can be used before seeking to the Chunk. If Statistics for a data type do
not contain the requested summary, the reader must decode the Page or rely on another predicate.

## Empty Aligned Value Page

An aligned value column can be null for every position in a Time Page. This case is encoded as a
single unsigned-varint zero:

```text
EmptyAlignedValuePage := UVarInt(0)  # uncompressed_size
```

There is no `compressed_size`, Page Statistics, bitmap, or stored payload after that zero. The
position count is inherited from the corresponding Time Page. This representation is valid only
where the aligned Page sequence supplies that external position count.

## Data Page Details

- [Chunks](Chunks.md)
- [Compression](Compression.md)
- [Encodings](Encodings.md)
- [Encryption](Encryption.md)
- [Checksumming](Checksumming.md)
- [Error Recovery](Error-Recovery.md)
