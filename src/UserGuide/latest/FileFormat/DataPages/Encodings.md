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

# Encodings

The single-byte `encoding_type` in a Chunk Header selects the value-stream encoding. In an
aligned Time Chunk, it selects the time-stream encoding. In a non-aligned Chunk, it selects only
the value-stream encoding.

| Value | Name |
| ---: | --- |
| 0 | `PLAIN` |
| 1 | `DICTIONARY` |
| 2 | `RLE` |
| 3 | `DIFF` |
| 4 | `TS_2DIFF` |
| 5 | `BITMAP` |
| 6 | `GORILLA_V1` |
| 7 | `REGULAR` |
| 8 | `GORILLA` |
| 9 | `ZIGZAG` |
| 10 | `FREQ` (deprecated) |
| 11 | `CHIMP` |
| 12 | `SPRINTZ` |
| 13 | `RLBE` |
| 14 | `CAMEL` |

The following data-type and encoding combinations are defined:

| Data type | Available encodings |
| --- | --- |
| `BOOLEAN` | `PLAIN`, `RLE` |
| `INT32`, `INT64`, `TIMESTAMP`, `DATE` | `PLAIN`, `RLE`, `TS_2DIFF`, `GORILLA`, `ZIGZAG`, `CHIMP`, `SPRINTZ`, `RLBE` |
| `FLOAT` | `PLAIN`, `RLE`, `TS_2DIFF`, `GORILLA_V1`, `GORILLA`, `CHIMP`, `SPRINTZ`, `RLBE` |
| `DOUBLE` | `PLAIN`, `RLE`, `TS_2DIFF`, `GORILLA_V1`, `GORILLA`, `CHIMP`, `SPRINTZ`, `RLBE`, `CAMEL` |
| `TEXT`, `STRING` | `PLAIN`, `DICTIONARY` |
| `BLOB`, `OBJECT` | `PLAIN` |

## Encoding Families

The identifiers describe different ways to transform a logical sequence into bytes. They are wire
format identifiers, not implementation-specific enumeration ordinals.

| Family | Encodings | Data property typically exploited |
| --- | --- | --- |
| Literal | `PLAIN` | No prediction; values are serialized independently |
| Dictionary | `DICTIONARY` | Repeated binary values are replaced by dictionary references |
| Run length / bit packing | `RLE`, `RLBE` | Repeated values or small bit widths |
| Delta and prediction | `DIFF`, `TS_2DIFF`, `REGULAR`, `SPRINTZ` | Small or regular changes between adjacent values |
| Integer remapping | `ZIGZAG` | Small signed integers represented as small unsigned integers |
| XOR-based floating-point | `GORILLA_V1`, `GORILLA`, `CHIMP`, `CAMEL` | Stable leading and trailing bits between adjacent floating-point values |
| Legacy or specialized | `BITMAP`, `FREQ` | Reserved historical layouts; `FREQ` is deprecated |

The best encoding depends on the data distribution. The file does not record why a writer selected
an encoding, and a reader must select its decoder from both `data_type` and `encoding_type`.

## Plain Encoding

A `PLAIN` stream does not store an element count. The Page context, time stream, or null bitmap
determines the number of elements. Values are written consecutively:

| Data type | Encoding of one value |
| --- | --- |
| `BOOLEAN` | `0x00` or `0x01` |
| `INT32`, `DATE` | `VarInt` |
| `INT64`, `TIMESTAMP` | Big-endian `int64` |
| `FLOAT` | Big-endian `float32` |
| `DOUBLE` | Big-endian `float64` |
| `TEXT`, `STRING`, `BLOB`, `OBJECT` | `VarInt byte_length` + raw bytes |

The length of a `PLAIN` binary value is non-negative. A non-aligned value stream does not contain
nulls; the Value Page bitmap represents nulls in an aligned value stream.

The other encoding identifiers select their respective value-stream bit layouts. A reader cannot
decode an unknown encoding as `PLAIN`.

## Decoder Contract

Encoding state is scoped to one uncompressed Page stream. After Page decompression, a reader:

1. creates a decoder for the Chunk's `data_type` and `encoding_type`;
2. decodes exactly the number of logical elements required by the Page context;
3. verifies that decoding neither reads past the Page boundary nor leaves an invalid partial block;
4. discards the decoder state before processing the next Page.

For a non-aligned Page, the time stream has its own encoding declared by `time_encoder` in the file
metadata; `encoding_type` applies to the value stream only. For an aligned Time Page,
`encoding_type` applies to the Page's time stream. For an aligned Value Page, null positions are
removed before value encoding, so the value decoder must produce exactly the number of set bits in
the bitmap.

An unsupported encoding prevents decoding the affected Chunk, but its byte range can still be
skipped using `data_size`. Falling back to another decoder is not valid.
