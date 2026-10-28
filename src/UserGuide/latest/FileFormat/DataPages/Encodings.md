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

Encoding turns a sequence of typed values into a byte stream before compression. The Chunk
Header records the selected codec in `encoding_type`. This field describes values in a Value or
non-aligned Chunk, and timestamps in an aligned Time Chunk.

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

The following combinations are supported by the reference value encoders. An assigned codec
number does not mean that every data type can use it:

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
| XOR-based | `GORILLA_V1`, `GORILLA`, `CHIMP`, `CAMEL` | Stable leading and trailing bits between adjacent floating-point values |
| Legacy or specialized | `BITMAP`, `FREQ` | Reserved historical layouts; `FREQ` is deprecated |

Different distributions favor different encodings. Whatever the writer chooses, the reader
selects a decoder from the pair `(data_type, encoding_type)`.

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

A decoder's state lasts for one Page stream. After decryption and decompression, select the codec,
decode within the stream boundary, and check that the number of values matches the Page context.
Start with fresh decoding state for the next Page.

For aligned data, the Time Chunk Header names the time codec; an aligned Value Page decodes one
value for each set bitmap bit. For non-aligned data, the Chunk Header names only the value codec.
Its time codec comes from shared configuration, defaulting to `TS_2DIFF`; there is no serialized
`time_encoder` footer field. See [Configurations](../Configurations.md#non-aligned-time-encoding).

An unsupported encoding prevents value decoding. A scanner that parses the raw Header fields
can still locate the next Chunk using `data_size`, even if a higher-level codec registry rejects
the identifier. It must not substitute `PLAIN` or another decoder.
