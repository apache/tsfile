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

# C++ Third-Party Dependencies

Keeping third-party source code in the repository is an exception. Such source
must be small and stable, have a license that permits redistribution, and be
accepted through community review. Build-managed dependencies should instead
use pinned upstream source archives with cryptographic digest verification. When a
dependency is added or updated, this file and the root `LICENSE` file must be
updated together.

The build provides explicit `SYSTEM`, `BUNDLED`, and `AUTO` dependency source
modes. LZ4 has migrated to these modes; the other dependencies in the bundled
inventory continue to use their bundled copies until they are migrated
incrementally.

## Dependency Inventory

| Dependency | Source management | Upstream version | License |
| --- | --- | --- | --- |
| ANTLR4 C++ Runtime | `antlr4-cpp-runtime-4` | [`4.9.3`](https://github.com/antlr/antlr4/tree/4.9.3/runtime/Cpp) | BSD-3-Clause, with MIT notices; see `antlr4-cpp-runtime-4/LICENSE.txt` |
| Snappy | `google_snappy` | [`1.2.1`](https://github.com/google/snappy/tree/1.2.1) | BSD-3-Clause; see `google_snappy/COPYING` |
| LZ4 | Verified tag archive downloaded during configuration | [`v1.9.4`](https://github.com/lz4/lz4/tree/v1.9.4/lib) | BSD-2-Clause; included in the downloaded archive and reproduced in the root `LICENSE` |
| lzokay | `lzokay` | [`5cb18da`](https://github.com/AxioDL/lzokay/commit/5cb18da508cc4d3ec41bc04dccdeef9c5ffedfb2) | MIT; see `lzokay/LICENSE` |
| SIMDe | `simde-0.8.4-rc3` | [`v0.8.4-rc3`](https://github.com/simd-everywhere/simde/tree/v0.8.4-rc3) | MIT; see `simde-0.8.4-rc3/COPYING` |
| zlib | `zlib-1.3.1` | [`v1.3.1`](https://github.com/madler/zlib/tree/v1.3.1) | zlib License; see `zlib-1.3.1/LICENSE` |

## Bundling Details

### ANTLR4 C++ Runtime

- Origin and scope: the `runtime/Cpp` subtree from upstream tag `4.9.3`.
- Trimming: `CMakeSettings.json` is omitted. The runtime, CMake support, and
  demos are retained.
- Local modifications: CMake integration was adapted for position-independent
  code and local demo/UTF-8 handling. Portability and compiler fixes modify
  `RuleContext.h`, `Token.h`, `Vocabulary.cpp`, `ATN.cpp`, `LL1Analyzer.cpp`,
  `LL1Analyzer.h`, `LexerATNSimulator.cpp`, `LexerATNSimulator.h`,
  `IntervalSet.cpp`, `Any.h`, and `CPPUtils.cpp`.

### Snappy

- Origin and scope: selected library sources and CMake files from upstream tag
  `1.2.1`.
- Trimming: command-line, C API, tests, benchmarks, fuzzers, Bazel files,
  documentation, test data, and upstream submodules are omitted.
- Local modifications: CMake defaults enable position-independent code and
  disable upstream tests and benchmarks; the NEON probe was adjusted; and
  `snappy.cc` adds the headers needed for local compiler configurations.

### LZ4

- Origin and scope: the upstream `v1.9.4` tag archive. The build
  compiles `lib/lz4.c` and exposes `lib/lz4.h` from the extracted archive.
- Archive URL:
  `https://github.com/lz4/lz4/archive/refs/tags/v1.9.4.tar.gz`.
- Archive SHA-256:
  `0b0e3aa07c8c063ddf40b082bdf7e37a1562bda40a0ff5272957f3e987e0e54b`.
- Repository scope and local modifications: no LZ4 source is committed and no
  upstream source is modified. The TsFile-owned CMake integration selects and
  compiles the required files after verification.
- Resolution: `SYSTEM` accepts LZ4 1.9.4 or newer in the 1.x release series;
  `BUNDLED` downloads or reuses the verified archive; and `AUTO` prefers a
  compatible system LZ4 before falling back to the verified archive.

### lzokay

- Origin and scope: `lzokay.cpp`, `lzokay.hpp`, and `LICENSE` from upstream
  commit `5cb18da508cc4d3ec41bc04dccdeef9c5ffedfb2`.
- Trimming: upstream build, example, and repository metadata files are omitted.
- Local modifications: `lzokay.cpp` comments out unused constants and
  initializes `lb_pos` to zero. The header and license are unchanged.

### SIMDe

- Origin and scope: the upstream tree at tag `v0.8.4-rc3`.
- Trimming: the `test/munit` Git submodule content is omitted.
- Local modifications: no source modifications are recorded.

### zlib

- Origin and scope: the upstream tree at tag `v1.3.1`.
- Trimming: generated `zconf.h` and `treebuild.xml` are omitted.
- Local modifications: `CMakeLists.txt` compiles `zlibstatic` with `-fPIC`.
  Some text files have normalized line endings; no source behavior changes are
  recorded.

## Non-Bundled Dependencies

| Dependency | Purpose | Upstream version | License | Current resolution |
| --- | --- | --- | --- | --- |
| utf8cpp | ANTLR4 UTF-8 support | [`v3.1.1`](https://github.com/nemtrif/utfcpp/tree/v3.1.1) | Boost Software License 1.0 | Use a CMake package or installed headers when available; otherwise clone the tag during the build |
| GoogleTest | C++ tests | [`release-1.12.1`](https://github.com/google/googletest/tree/release-1.12.1) | BSD-3-Clause | Download the release archive during test configuration |

Neither dependency is committed to this repository, and TsFile does not carry
local source modifications for either dependency. The GoogleTest archive is
verified with SHA-256 digest
`24564e3b712d3eb30ac9a85d92f7d720f60cc0173730ac166f27dda7fed76cb2`
before extraction. A previously downloaded local archive is subject to the same
verification.
