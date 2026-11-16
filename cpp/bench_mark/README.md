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

# C++ Benchmarks

`read_backend_benchmark` compares `PREAD` and `MMAP` without enforcing a
performance threshold. It reports sequential 64 KiB reads, deterministic
random 4 KiB reads, repeated parsing of real TsFile metadata, random bounded
`queryByRow` queries, and one concurrent random-read worker per input file. The
query-planning scan is performed before the timed random-query interval.
Files with no queryable rows still participate in the byte-read and metadata
workloads, but are skipped for the random-query workload.

Build it through the main CMake project so it links the exact SDK under test:

```bash
cmake -S cpp -B cpp/target/read-backend-benchmark \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_BENCHMARK=ON -DBUILD_TEST=OFF -DBUILD_TOOLS=OFF \
  -DTSFILE_BUILD_SHARED=OFF
cmake --build cpp/target/read-backend-benchmark \
  --target read_backend_benchmark --config Release
```

With a single-config generator such as Ninja, the executable is
`cpp/target/read-backend-benchmark/read_backend_benchmark` (plus `.exe` on
Windows). Run it with one or, preferably, several representative TsFiles:

```bash
./read_backend_benchmark data-1.tsfile data-2.tsfile data-3.tsfile
./read_backend_benchmark --mmap-first data-1.tsfile data-2.tsfile data-3.tsfile
```

The second form reverses backend order to expose warm page-cache bias. For
meaningful results, repeat both forms and record filesystem, storage device,
file sizes, compiler flags, and whether the OS page cache was warm. The checksum
makes accidental short reads, metadata-load failures, or optimizer removal
visible; it is not a TsFile-content checksum.

`alp_avx2_benchmark` measures the ALP SIMD kernels: the SIMDe portable
implementation against the runtime-dispatched native AVX2 copies for
`EncodeValues`, `DecodeValues`, and bit-unpacking. It is meant to be built with
the default flags (no `-mavx2`), which is what a portable build ships:

```bash
cmake -S cpp -B cpp/build/Release \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARK=ON -DBUILD_TEST=OFF
cmake --build cpp/build/Release --target alp_avx2_benchmark
./cpp/build/Release/alp_avx2_benchmark
```

On non-x86-64 targets, and on x86-64 CPUs without AVX2, only the SIMDe column
is measured. The `ok` column checks that both kernels produce identical values.

`gorilla_v1_batch_decode_benchmark` compares the per-value GORILLA_V1 decode
loop with the raw-pointer batch decoder on the same encoded stream for FLOAT
and DOUBLE columns. It reports elapsed time, values per second and the speedup
for a smooth (time-series-like) series, a high-entropy series and a constant
series, and verifies that both paths decode identical bit patterns. Build it
through the main CMake project:

```bash
cmake -S cpp -B cpp/target/gv1-benchmark \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_BENCHMARK=ON -DBUILD_TEST=OFF -DBUILD_TOOLS=OFF
cmake --build cpp/target/gv1-benchmark \
  --target gorilla_v1_batch_decode_benchmark --config Release
./cpp/target/gv1-benchmark/gorilla_v1_batch_decode_benchmark [values] [runs]
```

The defaults are 4 Mi values and a best-of-5 timing per case.
