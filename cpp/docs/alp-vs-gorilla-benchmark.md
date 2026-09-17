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
| ALP | decimal | 2,365,932 | 1.773 | 424.8 | 2719.7 | yes |
| Gorilla | decimal | 2,878,091 | 1.457 | 334.8 | 1275.9 | yes |
| ALP | smooth | 4,117,400 | 1.019 | 387.9 | 2281.3 | yes |
| Gorilla | smooth | 4,453,445 | 0.942 | 233.8 | 1324.8 | yes |
| ALP | random | 4,222,992 | 0.993 | 377.4 | 2249.6 | yes |
| Gorilla | random | 4,456,456 | 0.941 | 229.4 | 1361.4 | yes |
| ALP | constant | 28,688 | 146.204 | 853.0 | 7202.3 | yes |
| Gorilla | constant | 131,082 | 31.998 | 1840.8 | 68965.5 | yes |

### DOUBLE

| Codec | Pattern | Encoded bytes | Ratio | Encode MB/s | Decode MB/s | Correct |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| ALP | decimal | 1,423,880 | 5.891 | 562.6 | 6297.2 | yes |
| Gorilla | decimal | 8,650,641 | 0.970 | 270.4 | 2534.7 | yes |
| ALP | smooth | 8,343,184 | 1.005 | 479.5 | 2077.6 | yes |
| Gorilla | smooth | 8,629,855 | 0.972 | 270.8 | 2615.2 | yes |
| ALP | random | 8,417,296 | 0.997 | 511.7 | 2022.4 | yes |
| Gorilla | random | 8,650,764 | 0.970 | 270.5 | 2607.3 | yes |
| ALP | constant | 8,417,296 | 0.997 | 543.5 | 2012.7 | yes |
| Gorilla | constant | 131,090 | 63.991 | 3689.6 | 80000.0 | yes |

## Interpretation

- ALP is designed for decimal-like floating-point columns. On the decimal
  cases it improves both compression and decode throughput:
  - FLOAT decimal: 1.22x better ratio, 1.27x faster encode, and 2.13x faster
    decode than Gorilla.
  - DOUBLE decimal: 6.07x better ratio, 2.08x faster encode, and 2.48x faster
    decode than Gorilla.
- ALP now wins both encode and decode on the decimal cases. The word-based
  bit-packing change removed the largest FLOAT encode cost; the remaining
  encode cost is the scalar exponent/factor search.
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
