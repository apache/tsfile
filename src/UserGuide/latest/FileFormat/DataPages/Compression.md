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
| 3 | `LZO` | LZO block (C++ implementation, enabled at build time) |
| 4 | `SDT` | Reserved; no codec |
| 5 | `PAA` | Reserved; no codec |
| 6 | `PLA` | Reserved; no codec |
| 7 | `LZ4` | LZ4 raw block |
| 8 | `ZSTD` | Zstandard frame |
| 9 | `LZMA2` | LZMA2 stream in an XZ container |

Each Page is compressed independently; compression state is not shared across Pages. The Page
Header's `uncompressed_size` records the uncompressed payload length. `compressed_size` records
the stored body length after compression and any encryption, including Page AEAD framing and tags.

The numeric values are stable wire identifiers rather than the ordinal positions of a compressor
library. Values `3` through `6` are legacy identifiers from earlier TsFile versions. The reference
Java implementation does not define or implement them and rejects these bytes while parsing a
Chunk Header. The C++ implementation retains them as `LZO`, `SDT`, `PAA`, and `PLA`; only `LZO`
has a codec (enabled at build time), while `SDT`, `PAA`, and `PLA` remain reserved without a codec.
A reader that does not provide the codec cannot decode the affected Chunk.

## Page-local Framing

Let a Page start at `P` and its Header occupy `H` bytes. Read exactly `compressed_size` bytes
for its body:

```text
[P + H, P + H + compressed_size)
```

This is the stored length, including any encryption framing. After decryption, `UNCOMPRESSED`
already contains the encoded Page payload; other methods need decompression. Either path must
produce exactly `uncompressed_size` bytes. With length-preserving encryption or no encryption,
`UNCOMPRESSED` has equal stored and uncompressed lengths. Page AEAD adds framing, so that equality
no longer holds.

Pages do not share compression history. A reader can seek directly to a selected Page or skip
its body using the stored length, without decompressing earlier Pages.

## Read Pipeline and Validation

The Page Header is read before the payload, so the reader knows both bounds before allocating or
decompressing:

```text
read Page Header
  -> read exactly compressed_size bytes
  -> authenticate/decrypt according to the file encryption header or legacy properties
  -> decompress according to compression_type
  -> require exactly uncompressed_size bytes
  -> decode the Page streams
```

Apply the length checks in [Binary Representation](../Binary-Representation.md) before allocating
buffers. An unavailable codec prevents decompression. A scanner can still use `data_size` to skip
the Chunk if it parses the raw Header fields, even when the codec registry rejects the identifier;
it must not treat that identifier as `UNCOMPRESSED`.
