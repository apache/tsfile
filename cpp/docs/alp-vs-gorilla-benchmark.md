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
| ALP | decimal | 2,365,932 | 1.773 | 221.7 | 2622.2 | yes |
| Gorilla | decimal | 2,878,091 | 1.457 | 340.6 | 1218.5 | yes |
| ALP | smooth | 4,117,400 | 1.019 | 142.8 | 2205.6 | yes |
| Gorilla | smooth | 4,453,445 | 0.942 | 238.4 | 1322.9 | yes |
| ALP | random | 4,222,992 | 0.993 | 324.5 | 2248.3 | yes |
| Gorilla | random | 4,456,456 | 0.941 | 242.8 | 1325.3 | yes |
| ALP | constant | 28,688 | 146.204 | 895.9 | 7439.0 | yes |
| Gorilla | constant | 131,082 | 31.998 | 1916.0 | 69213.7 | yes |

### DOUBLE

| Codec | Pattern | Encoded bytes | Ratio | Encode MB/s | Decode MB/s | Correct |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| ALP | decimal | 1,423,880 | 5.891 | 395.4 | 4593.3 | yes |
| Gorilla | decimal | 8,650,641 | 0.970 | 281.6 | 2546.4 | yes |
| ALP | smooth | 8,343,184 | 1.005 | 194.6 | 2117.8 | yes |
| Gorilla | smooth | 8,629,855 | 0.972 | 276.8 | 2571.0 | yes |
| ALP | random | 8,417,296 | 0.997 | 287.8 | 1970.9 | yes |
| Gorilla | random | 8,650,764 | 0.970 | 279.9 | 2617.4 | yes |
| ALP | constant | 8,417,296 | 0.997 | 546.3 | 2026.0 | yes |
| Gorilla | constant | 131,090 | 63.991 | 3764.5 | 81321.5 | yes |

## Interpretation

- ALP is designed for decimal-like floating-point columns. On the decimal
  cases it improves both compression and decode throughput:
  - FLOAT decimal: 1.22x better ratio and 2.15x faster decode than Gorilla.
  - DOUBLE decimal: 6.07x better ratio and 1.80x faster decode than Gorilla.
- ALP FLOAT encode is slower than Gorilla on this data (222 MB/s versus
  341 MB/s). DOUBLE encode is faster (395 MB/s versus 282 MB/s). The
  exponent/factor search is currently scalar and is the main FLOAT encode
  cost; it is a candidate for SIMD acceleration.
- ALP falls back to PLAIN for non-decimal smooth/random data. In those cases
  Gorilla is the better choice, so ALP must remain opt-in.
- Gorilla is extremely fast on constant blocks. ALP does not try to beat that
  case; the adaptive selection should leave constant columns on Gorilla.
- The current benchmark covers the portable SIMDe path. Native AVX2
  multi-versioning and AVX-512 kernels are expected to increase ALP decode
  throughput further on x86.

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
