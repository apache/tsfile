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

# Compression

The single-byte `compression_type` in a Chunk Header selects the compression format for every
Page in that Chunk.

| Value | Name | Page data format |
| ---: | --- | --- |
| 0 | `UNCOMPRESSED` | Raw bytes |
| 1 | `SNAPPY` | Snappy raw block |
| 2 | `GZIP` | One gzip member |
| 3 | `LZO` | LZO block |
| 7 | `LZ4` | LZ4 raw block |
| 8 | `ZSTD` | Zstandard frame |
| 9 | `LZMA2` | LZMA2 stream in an XZ container |

Each Page is compressed independently; compression state is not shared across Pages. The Page
Header's `uncompressed_size` and `compressed_size` record the payload lengths before and after
compression, respectively.

The numeric values are stable wire identifiers rather than the ordinal positions of a compressor
library. Values `4` through `6` are not assigned by this version. `LZO` is retained as a historical
identifier; a reader that does not provide the codec cannot decode that Chunk.

## Page-local Framing

If a Page begins at offset `P` and its serialized Page Header occupies `H` bytes, the stored payload
is exactly the half-open interval:

```text
[P + H, P + H + compressed_size)
```

For `UNCOMPRESSED`, `compressed_size` equals `uncompressed_size` and the bytes are already the
encoded Page payload. For every other method, decompression must produce exactly
`uncompressed_size` bytes. A shorter result, a longer result, or a codec request for bytes outside
the interval makes the Page invalid.

Because each Page is an independent compression unit, a reader can use Page Statistics to reject a
Page and advance by `compressed_size` without initializing the codec. Random access does not need
the preceding Page's dictionary or history.

## Read Pipeline and Validation

The Page Header is read before the payload, so the reader knows both bounds before allocating or
decompressing:

```text
read Page Header
  -> read exactly compressed_size bytes
  -> decrypt when file properties require it
  -> decompress according to compression_type
  -> require exactly uncompressed_size bytes
  -> decode the Page streams
```

Implementations should reject sizes that overflow offset arithmetic or exceed configured resource
limits before allocating memory. An unsupported `compression_type` prevents decoding the affected
Chunk, but the Chunk remains skippable through its `data_size`; it must not be interpreted as
`UNCOMPRESSED`.
