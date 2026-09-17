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

# ALP Float/Double Codec Design

Status: draft for the C++-first implementation on `colin/alp-codec`.

## Scope

- This work is C++-only. The Java implementation is intentionally untouched.
- ALP is an opt-in encoding for `FLOAT` and `DOUBLE` only.
- The existing Gorilla default is preserved. ALP is enabled through an
  experimental build option until the format is accepted by the community.
- The format is language-neutral so a Java decoder can be added later without
  changing encoded bytes.
- Python will reuse the C++ implementation through the C wrapper.

## Relationship to TS_2DIFF

ALP must not reuse the TS_2DIFF float-adaptation path. In particular it must not
use TS_2DIFF's page precision (`maxPointNumber`), page overflow bitmap,
`page_blocks_` buffering, or `PageMeta` parsing.

ALP has its own block metadata because the algorithm needs it: exponent/factor
indices, frame-of-reference base, bit width, and exceptions. This metadata is
block-local and is part of the ALP payload. A TsFile page is only a container;
the ALP block boundary is a flush boundary.

## Wire Format (version 1)

All integers are little-endian. `value_size` is 4 for `FLOAT` and 8 for
`DOUBLE`.

### Page header

| field | size | description |
|---|---:|---|
| version | 1 | format version, currently 1 |
| data_type | 1 | 0 = float, 1 = double |
| reserved | 2 | zero |
| value_count | 4 | total values in the page |
| block_count | 4 | number of ALP blocks |
| reserved | 4 | zero |

### Block header

| field | size | description |
|---|---:|---|
| scheme | 1 | 0 = ALP, 1 = PLAIN fallback |
| factor | 1 | ALP factor index |
| exponent | 1 | ALP exponent index |
| flags | 1 | bit 0 = exceptions present |
| value_count | 4 | values in this block |
| exception_count | 4 | number of exceptions |
| body_bytes | 4 | padded body size in bytes |
| for_base | 8 | signed frame-of-reference base, int32 for float and int64 for double |
| bit_width | 1 | 0..32 for float, 0..64 for double |
| reserved | 3 | zero |

The header is followed by:

- for `scheme == ALP` with exceptions: an exception bitmap of
  `ceil(value_count / 8)` bytes, followed by `exception_count` raw
  `value_size` values in increasing position order;
- for `scheme == ALP`: the bit-packed body;
- for `scheme == PLAIN`: `value_count * value_size` raw values.

The packed body is padded with zero bytes to a multiple of 32 bytes after
adding 16 bytes of guard space, so SIMD/word loads may safely read past the
logical end of the stream. `body_bytes` records the padded size.

### Bit packing

The body stores adjusted integers using an LSB-first word stream. For float
each word is 32 bits; for double each word is 64 bits. Value `i` occupies
`bit_width` bits starting at bit `i * bit_width`. A zero bit width means every
non-exception value equals `for_base`.

This layout is deliberately different from TsFile's existing MSB-first pack8
layout. It is defined by the scalar reference implementation, and SIMD kernels
must produce and consume the same bytes.

## Encoding

1. Buffer values per page.
2. Split the buffered values into blocks of at most 1024 values.
3. For each block, sample values and choose the best `(factor, exponent)`
   combination using the estimated compressed size.
4. Encode each value with the chosen pair. A value is an exception when it is
   non-finite, `-0.0`, outside the safe integer range, or when the reference
   decode does not reproduce the original bit pattern.
5. Compute the frame-of-reference base and bit width over non-exception
   integers.
6. Estimate whether ALP is smaller than PLAIN. If not, emit the block as
   `PLAIN`.
7. Emit the block header, exception data, and packed body.

The encoder's exception check must use the same decode routine as the decoder.
This is required for correctness: a value classified as a non-exception must
be bit-exactly reproducible by the decoder.

## Decoding

1. Parse and validate the page header.
2. For each block, parse and validate the block header.
3. For `PLAIN`, copy raw values.
4. For `ALP`, unpack adjusted integers, add `for_base`, apply the reference
   decode, and overwrite exception positions from the exception array.
5. Validate that the decoded value count matches the page header.
6. Implement `read_batch_float` and `read_batch_double` so the SIMD path can
   work on whole blocks rather than one value at a time.

## SIMD Plan

SIMDe is already a TsFile C++ dependency. The ALP kernels will use the existing
`ENABLE_SIMD` switch and `TsFile::SIMDe` target.

- Write the kernels with `simde__m256i` and `simde_mm256_*`, matching the
  existing encoding code style.
- Compile an AVX2 kernel translation unit with `-mavx2` or `/arch:AVX2` on
  x86. Do not apply AVX2 flags to baseline TsFile translation units.
- On AArch64 the same SIMDe source maps to NEON without the AVX2 flag.
- Add a scalar fallback used when SIMD is disabled or unavailable.
- Dispatch once per block through a function table. Do not branch per value.
- Add an optional AVX-512 translation unit later. The wire format must not
  depend on the availability of AVX-512.

## Testing

- Scalar round-trip tests for float and double.
- Boundary values: NaN, +Inf, -Inf, -0.0, subnormals, smallest/largest
  magnitudes, exact zero, and values forcing exceptions.
- Constant blocks, single-value blocks, partial final blocks, and multi-block
  pages.
- Scalar and SIMD must encode to identical bytes and decode to identical
  values.
- Corrupt/truncated payload tests must return errors without out-of-bounds
  access.
- Benchmarks compare ALP against Gorilla for decimal, normalized, smooth,
  repeated, and high-entropy data.
