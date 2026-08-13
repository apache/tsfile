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
use pinned upstream source archives with cryptographic digest verification.
When a dependency is added or updated, this file and the root `LICENSE` file
must be updated together.

The build provides explicit `SYSTEM`, `BUNDLED`, and `AUTO` dependency source
modes. Snappy, LZ4, lzokay, and zlib have migrated to these modes; the other
dependencies in the inventory continue to use their repository copies until
they are migrated incrementally.

## Dependency Inventory

| Dependency | Source management | Upstream version | License |
| --- | --- | --- | --- |
| ANTLR4 C++ Runtime | `antlr4-cpp-runtime-4` | [`4.9.3`](https://github.com/antlr/antlr4/tree/4.9.3/runtime/Cpp) | BSD-3-Clause, with MIT notices; see `antlr4-cpp-runtime-4/LICENSE.txt` |
| Snappy | Verified tag archive downloaded during configuration | [`1.2.1`](https://github.com/google/snappy/tree/1.2.1) | BSD-3-Clause; included in the downloaded archive and reproduced in the root `LICENSE` |
| LZ4 | Verified tag archive downloaded during configuration | [`v1.9.4`](https://github.com/lz4/lz4/tree/v1.9.4/lib) | BSD-2-Clause; included in the downloaded archive and reproduced in the root `LICENSE` |
| lzokay | Verified commit archive downloaded during configuration | [`5cb18da`](https://github.com/AxioDL/lzokay/commit/5cb18da508cc4d3ec41bc04dccdeef9c5ffedfb2) | MIT; included in the downloaded archive and reproduced in the root `LICENSE` |
| SIMDe | `simde-0.8.4-rc3` | [`v0.8.4-rc3`](https://github.com/simd-everywhere/simde/tree/v0.8.4-rc3) | MIT; see `simde-0.8.4-rc3/COPYING` |
| zlib | Verified tag archive downloaded during configuration | [`v1.3.1`](https://github.com/madler/zlib/tree/v1.3.1) | zlib License; included in the downloaded archive and reproduced in the root `LICENSE` |

## Dependency Details

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

- Origin and scope: the upstream `1.2.1` tag archive. The build uses the
  upstream CMake target with tests, benchmarks, and installation disabled.
- Archive URL:
  `https://github.com/google/snappy/archive/refs/tags/1.2.1.tar.gz`.
- Archive SHA-256:
  `736aeb64d86566d2236ddffa2865ee5d7a82d26c9016b36218fcc27ea4f09f86`.
- Repository scope and local modifications: no Snappy source is committed and
  no upstream source is modified. TsFile's CMake integration enables PIC and
  supplies a stricter ARM NEON capability probe before configuring upstream.
- Resolution: `SYSTEM` accepts Snappy 1.2.1 or newer in the 1.x release series
  through the `Snappy::snappy` target; `BUNDLED` downloads or reuses the
  verified archive; and `AUTO` prefers a compatible system package before
  falling back to the verified archive.

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

- Origin and scope: the upstream archive at commit
  `5cb18da508cc4d3ec41bc04dccdeef9c5ffedfb2`. The build compiles
  `lzokay.cpp` and exposes `lzokay.hpp` from the extracted archive.
- Archive URL:
  `https://github.com/AxioDL/lzokay/archive/5cb18da508cc4d3ec41bc04dccdeef9c5ffedfb2.tar.gz`.
- Archive SHA-256:
  `eb518bf793da0b4420a3ffdf1511851575bc62ef350b303f14ff7355f370da6a`.
- Repository scope and local modifications: no lzokay source is committed and
  no upstream source is modified. The earlier repository copy's warning-only
  changes and defensive initialization are no longer carried.
- Resolution: `SYSTEM` accepts an upstream-compatible CMake package version
  0.1 or newer and earlier than 1.0; `BUNDLED` downloads or reuses the verified
  archive; and `AUTO` prefers a compatible system package before falling back
  to the verified archive.

### SIMDe

- Origin and scope: the upstream tree at tag `v0.8.4-rc3`.
- Trimming: the `test/munit` Git submodule content is omitted.
- Local modifications: no source modifications are recorded.

### zlib

- Origin and scope: the upstream `v1.3.1` tag archive. The upstream CMake build
  produces the static library used by TsFile.
- Archive URL:
  `https://github.com/madler/zlib/archive/refs/tags/v1.3.1.tar.gz`.
- Archive SHA-256:
  `17e88863f3600672ab49182f217281b6fc4d3c762bde361935e436a95214d05c`.
- Repository scope and local modifications: no zlib source is committed and no
  upstream source is modified. The TsFile-owned CMake integration disables
  upstream examples and enables position-independent code on `zlibstatic`.
- Resolution: `SYSTEM` accepts zlib 1.3.1 or newer and earlier than 2.0.0;
  `BUNDLED` downloads or reuses the verified archive; and `AUTO` prefers a
  compatible system zlib before falling back to the verified archive.

## Other Build Dependencies

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
