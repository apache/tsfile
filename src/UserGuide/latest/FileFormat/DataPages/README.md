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

A Chunk stores its Pages consecutively after the Chunk Header, without padding between them.
`data_size` counts all Page Headers and stored bodies together. An empty aligned Value Chunk
has `data_size = 0` and no Pages.

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
when `compression_type = UNCOMPRESSED` and no length-expanding encryption is used. For Page AEAD,
`compressed_size` includes the encrypted body framing and authentication overhead, even when
compression is disabled. See [Encryption](Encryption.md#aead-page-body-and-context).

The low six bits of the Chunk marker determine whether Page Statistics are present:

| Chunk marker low bits | Page count | Page Statistics |
| ---: | --- | --- |
| `0x01` | More than one | Present in every non-empty Page Header |
| `0x05` | Zero or one | Absent |

The Statistics layout is defined in [Metadata](../Metadata.md#statistics).

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
  -> encode time and/or values
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

The length must be non-negative. Both streams are bounded by the Page payload and must decode
to the same number of points, with valid endings for their respective codecs.

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

Page Statistics can rule out a Page before its body is read. For example, a Page whose time
range misses the query range can be skipped. See [Statistics](../Metadata.md#statistics) for
which value summaries are available.

A one-Page Chunk omits Page Statistics, so use its Chunk or series Statistics from the footer.
If the available summary cannot rule out a match, read the Page and check its values.

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
