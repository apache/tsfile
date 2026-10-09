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

# Current Encoding and Compression Factories

Prefer schema constructors for normal file writes. Use these low-level
factories only for codec, page, or format work. An enum member does not prove
that a factory can construct the requested codec.

## Java

Validate with `TSEncoding.isSupported(dataType, encoding)`, create encoders with
`TSEncodingBuilder.getEncodingBuilder(encoding).getEncoder(dataType)`, and
create decoders with `Decoder.getDecoderByType(encoding, dataType)`.

| data type | current supported encodings |
|---|---|
| `BOOLEAN` | `PLAIN`, `RLE` |
| `INT32`, `INT64`, `DATE`, `TIMESTAMP` | `PLAIN`, `RLE`, `TS_2DIFF`, `GORILLA`, `ZIGZAG`, `CHIMP`, `SPRINTZ`, `RLBE` |
| `FLOAT` | `PLAIN`, `RLE`, `TS_2DIFF`, `GORILLA_V1`, `GORILLA`, `CHIMP`, `SPRINTZ`, `RLBE` |
| `DOUBLE` | the FLOAT set plus `CAMEL` |
| `TEXT`, `STRING` | `PLAIN`, `DICTIONARY` |
| `BLOB`, `OBJECT` | `PLAIN` |

Create compressors with `ICompressor.getCompressor(CompressionType)` and
decompressors with `IUnCompressor.getUnCompressor(CompressionType)`. The
current Java factory set is `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZ4`, `ZSTD`,
and `LZMA2`.

## C++

Use `EncoderFactory::alloc_time_encoder`,
`EncoderFactory::alloc_value_encoder`, `DecoderFactory::alloc_time_decoder`,
`DecoderFactory::alloc_value_decoder`, and each factory's `free` function.
Current time factories accept `PLAIN` or `TS_2DIFF`.

| value encoding | current accepted data types |
|---|---|
| `PLAIN` | all types handled by `PlainEncoder`/`PlainDecoder` |
| `DICTIONARY` | `STRING`, `TEXT` |
| `RLE` | `INT32`, `DATE`, `INT64`, `TIMESTAMP` |
| `TS_2DIFF`, `GORILLA` | `INT32`, `DATE`, `INT64`, `TIMESTAMP`, `FLOAT`, `DOUBLE` |
| `ZIGZAG` | `INT32`, `INT64` |
| `SPRINTZ` | `INT32`, `INT64`, `FLOAT`, `DOUBLE` |

`CompressorFactory::alloc_compressor` always supports `UNCOMPRESSED`; `SNAPPY`,
`GZIP`, `LZO`, and `LZ4` require their corresponding compile definitions.
`SDT`, `PAA`, and `PLA` are enum values but the current compressor factory
returns no implementation for them. Check for `nullptr` and release successful
allocations with `CompressorFactory::free`.

Python exposes schema encoding/compression enums through the native binding but
does not expose these Java/C++ low-level factories as an equivalent Python API.

## Source Anchors

- Java: `TSEncoding.java`, `TSEncodingBuilder.java`, `Decoder.java`,
  `CompressionType.java`, `ICompressor.java`, and `IUnCompressor.java`
- C++: `cpp/src/encoding/encoder_factory.h`,
  `cpp/src/encoding/decoder_factory.h`,
  `cpp/src/compress/compressor_factory.h`, and `cpp/src/common/db_common.h`
