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

# ALP Profiling Analysis

This note explains why the ALP codec still loses to Gorilla in some cases
even though the value kernels are SIMD-accelerated. The profiles were collected
with gperftools on the FLOAT decimal benchmark case.

## Profile Setup

```bash
CPUPROFILE=/tmp/alp_float.prof \
CPUPROFILE_FREQUENCY=1000 \
DYLD_INSERT_LIBRARIES=/opt/homebrew/lib/libprofiler.dylib \
./cpp/build/alp/alp_vs_gorilla_benchmark 500 ALP FLOAT decimal

/opt/homebrew/bin/pprof --svg \
  ./cpp/build/alp/alp_vs_gorilla_benchmark \
  /tmp/alp_float.prof > /tmp/alp_float.svg
```

The same command was repeated with `GORILLA` for comparison.

## ALP FLOAT Decimal

![ALP FLOAT decimal pprof](images/alp-float-decimal-encode-pprof.png)

Flat profile:

| Function | Flat | Cumulative |
| --- | ---: | ---: |
| `storage::AlpEncoderBase::flush` | 73.5% | 86.3% |
| `storage::alp::AlpAppendU32` | 7.8% | 7.8% |
| `storage::AlpDecoderBase::ensure_initialized` | 3.0% | 11.3% |
| `storage::alp::AlpDecodeValues` | 2.7% | 2.7% |
| `storage::alp::AlpUnpackBits` | 2.3% | 2.3% |
| `storage::AlpDecoderBase::read_batch_float` | 0.5% | 11.8% |

The encode path dominates. Decode is already a small fraction of total time.

## Gorilla FLOAT Decimal

![Gorilla FLOAT decimal pprof](images/gorilla-float-decimal-pprof.png)

Flat profile:

| Function | Flat | Cumulative |
| --- | ---: | ---: |
| `storage::GorillaEncoder::compress_value` | 29.0% | 72.3% |
| `common::ByteStream::write_buf` | 27.2% | 43.3% |
| `storage::FloatGorillaDecoder::read_batch_float` | 22.0% | 22.0% |
| `storage::FloatGorillaEncoder::encode` | 2.2% | 74.5% |
| `storage::Encoder::encode_batch` | 1.9% | 76.4% |

Gorilla spends most of its time in its scalar bit writer and in the ByteStream
write path. Its encode is not SIMD either; it is simply a very efficient
scalar bit stream for the FLOAT decimal pattern.

## Per-Block Breakdown

A separate microbenchmark measured one 1024-value block with the same decimal
pattern:

| Stage | FLOAT (ms/block) | DOUBLE (ms/block) |
| --- | ---: | ---: |
| `AlpChooseFactorExponent` (two-stage) | 0.002 | 0.003 |
| `AlpEncodeValues` (SIMD) | 0.001 | 0.001 |
| `AlpPackBits` (word-based) | 0.002 | 0.002 |
| `AlpEncodePage` total | 0.005 | 0.007 |

After replacing the per-bit packing loop with word-based packing and adding a
two-stage exponent/factor search, the block encode cost dropped to 0.005 ms
(FLOAT) and 0.007 ms (DOUBLE). The two-stage search is still the largest single
component (about 40% FLOAT and 43% DOUBLE), followed by word-based packing.
The SIMD value kernel is about 14-20% of the block encode time.

## Why ALP Still Loses

1. **The e/f search is still scalar.**
   The two-stage search cut the cost substantially, but it remains the largest
   single encode component. Stage 1 evaluates every candidate on an 8-value
   sample; stage 2 runs the exact round-trip check on the shortlist. Further
   gains require SIMD candidate evaluation or a cheaper estimator.

2. **The SIMD value kernel is still a minority of encode time.**
   `AlpEncodeValues` is about 14-20% of the block encode time. End-to-end
   encode speed also depends on the search and packing decisions.

3. **Non-decimal data falls back to PLAIN.**
   Smooth/random/constant data often has too many exceptions for ALP to win.
   The encoder then emits a PLAIN block. In that case SIMD cannot help because
   the data is not ALP-encoded at all; Gorilla is usually the better codec for
   those columns.

4. **Gorilla has a dedicated constant path.**
   Gorilla compresses constant blocks extremely well and decodes them very
   quickly. ALP intentionally does not special-case that pattern, so Gorilla
   wins the constant rows of the benchmark.

5. **The benchmark is on Apple Silicon.**
   SIMDe maps the AVX2-style kernels to NEON here. On x86, a native `-mavx2`
   translation unit and runtime dispatch are still required to get the AVX2
   peak; without those flags SIMDe may emulate AVX2 with SSE2.

## Next Optimizations

1. **SIMD or cheaper exponent/factor selection.**
   Evaluate the candidate pairs on the 32-value sample with SIMD, or reduce the
   candidate set with a two-stage/early-exit estimate. This is now the largest
   remaining encode cost for both FLOAT and DOUBLE.

2. **Remove per-block allocations and copies.**
   Reuse scratch vectors for `encoded`, `adjusted`, `bitmap`, `body`, and the
   exception list instead of constructing them per block. Decode directly into
   the caller's output buffer rather than an intermediate `values_` vector.

3. **Adaptive codec selection.**
   Choose Gorilla for constant/high-entropy columns and ALP for decimal-like
   columns. This avoids the PLAIN fallback cases where Gorilla is better.

4. **Native x86 dispatch.**
   Add an AVX2 object library and CPU detection so x86 users get native AVX2
   without `-march=native`, plus an optional AVX-512 path for DOUBLE.
