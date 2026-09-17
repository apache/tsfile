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

# ALP vs Gorilla Codec Benchmark

This document records the first C++ codec-level comparison between ALP and
Gorilla on the `colin/alp-codec` branch. It is not an end-to-end TsFile I/O
benchmark; it measures the codec path through the same
`EncoderFactory` / `DecoderFactory` interfaces used by TsFile.

## Environment

| Item | Value |
| --- | --- |
| Machine | Apple Silicon (`arm64`) |
| Compiler | AppleClang 17.0.0 |
| Build | Release, `-O3` |
| SIMDe | `0.8.4-rc3` |
| Values per case | 1,048,576 |
| Repetitions | best of 5 |
| Compression | `UNCOMPRESSED` |

On this machine SIMDe maps the AVX2-style kernels to NEON. The x86 native
AVX2 multi-version object library and runtime dispatch are a follow-up step;
the benchmark therefore reflects the portable SIMDe path, not the final
AVX2/AVX-512 peak.

## Data Patterns

| Pattern | Description |
| --- | --- |
| decimal | repeated fixed-scale decimals (`0.01` float steps, `0.001` double steps) |
| smooth | smooth `sin`/`cos` values |
| random | pseudo-random raw IEEE-754 bit patterns, finite values |
| constant | one repeated constant value |

## Results

### FLOAT

| Codec | Pattern | Encoded bytes | Ratio | Encode MB/s | Decode MB/s | Correct |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| ALP | decimal | 2,365,932 | 1.773 | 539.3 | 2646.9 | yes |
| Gorilla | decimal | 2,878,091 | 1.457 | 342.0 | 1389.3 | yes |
| ALP | smooth | 4,124,940 | 1.017 | 528.5 | 2399.5 | yes |
| Gorilla | smooth | 4,453,445 | 0.942 | 248.1 | 1384.7 | yes |
| ALP | random | 4,222,992 | 0.993 | 506.2 | 2329.7 | yes |
| Gorilla | random | 4,456,456 | 0.941 | 239.6 | 1378.9 | yes |
| ALP | constant | 28,688 | 146.204 | 1335.4 | 7393.7 | yes |
| Gorilla | constant | 131,082 | 31.998 | 1900.8 | 69214.9 | yes |

### DOUBLE

| Codec | Pattern | Encoded bytes | Ratio | Encode MB/s | Decode MB/s | Correct |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| ALP | decimal | 1,426,248 | 5.882 | 1055.3 | 6470.7 | yes |
| Gorilla | decimal | 8,650,641 | 0.970 | 276.7 | 2546.8 | yes |
| ALP | smooth | 8,348,616 | 1.005 | 780.8 | 2228.6 | yes |
| Gorilla | smooth | 8,629,855 | 0.972 | 276.8 | 2600.1 | yes |
| ALP | random | 8,417,296 | 0.997 | 753.6 | 2118.6 | yes |
| Gorilla | random | 8,650,764 | 0.970 | 277.1 | 2644.3 | yes |
| ALP | constant | 8,417,296 | 0.997 | 861.9 | 2094.3 | yes |
| Gorilla | constant | 131,090 | 63.991 | 3824.2 | 76646.7 | yes |

## Interpretation

- ALP is designed for decimal-like floating-point columns. On the decimal
  cases it improves both compression and decode throughput:
  - FLOAT decimal: 1.22x better ratio, 1.58x faster encode, and 1.91x faster
    decode than Gorilla.
  - DOUBLE decimal: 6.06x better ratio, 3.81x faster encode, and 2.54x faster
    decode than Gorilla.
- ALP now wins both encode and decode on the decimal cases. Word-based packing
  removed the largest FLOAT encode cost, and the two-stage exponent/factor
  search cut the remaining scalar search cost substantially.
- ALP falls back to PLAIN for non-decimal smooth/random data. In those cases
  Gorilla is the better choice, so ALP must remain opt-in.
- Gorilla is extremely fast on constant blocks. ALP does not try to beat that
  case; the adaptive selection should leave constant columns on Gorilla.
- The benchmark includes the SIMDe value kernels, SIMD 4-lane unpack, and
  word-based packing. Native x86 AVX2 multi-versioning and AVX-512 kernels are
  expected to increase ALP throughput further on x86.

## Reproduce

```bash
cmake -S cpp -B cpp/build/alp \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_BENCHMARK=ON \
  -DBUILD_TEST=ON \
  -DENABLE_SIMD=ON \
  -DTSFILE_DEPENDENCY_SOURCE=BUNDLED

cmake --build cpp/build/alp --target alp_vs_gorilla_benchmark -j

./cpp/build/alp/alp_vs_gorilla_benchmark 5
```
