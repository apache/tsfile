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
modes. ANTLR4, Snappy, LZ4, lzokay, SIMDe, zlib, Zstandard, and liblzma have
migrated to these modes. In `BUNDLED` mode, source is downloaded into the build
cache rather than kept in this repository.

## Dependency Inventory

| Dependency | Source management | Upstream version | License |
| --- | --- | --- | --- |
| ANTLR4 C++ Runtime | Verified tag archive downloaded during configuration | [`4.9.3`](https://github.com/antlr/antlr4/tree/4.9.3/runtime/Cpp) | BSD-3-Clause, with MIT notices; included in the downloaded archive and reproduced in the root `LICENSE` |
| Snappy | Verified tag archive downloaded during configuration | [`1.2.1`](https://github.com/google/snappy/tree/1.2.1) | BSD-3-Clause; included in the downloaded archive and reproduced in the root `LICENSE` |
| LZ4 | Verified tag archive downloaded during configuration | [`v1.9.4`](https://github.com/lz4/lz4/tree/v1.9.4/lib) | BSD-2-Clause; included in the downloaded archive and reproduced in the root `LICENSE` |
| lzokay | Verified commit archive downloaded during configuration | [`5cb18da`](https://github.com/AxioDL/lzokay/commit/5cb18da508cc4d3ec41bc04dccdeef9c5ffedfb2) | MIT; included in the downloaded archive and reproduced in the root `LICENSE` |
| SIMDe | Verified tag archive downloaded during configuration | [`v0.8.4-rc3`](https://github.com/simd-everywhere/simde/tree/v0.8.4-rc3) | MIT; included in the downloaded archive and reproduced in the root `LICENSE` |
| zlib | Verified tag archive downloaded during configuration | [`v1.3.1`](https://github.com/madler/zlib/tree/v1.3.1) | zlib License; included in the downloaded archive and reproduced in the root `LICENSE` |
| Zstandard | Verified tag archive downloaded during configuration | [`v1.5.7`](https://github.com/facebook/zstd/tree/v1.5.7) | BSD-3-Clause option selected from the upstream dual license; included in the downloaded archive and reproduced in the root `LICENSE` |
| liblzma (XZ Utils) | Verified release archive downloaded during configuration | [`v5.8.3`](https://github.com/tukaani-project/xz/tree/v5.8.3/src/liblzma) | 0BSD for `liblzma`; the mixed-license archive's GPL/LGPL tools and scripts are excluded from the build and distribution |

## Dependency Details

### ANTLR4 C++ Runtime

- Origin and scope: the upstream `4.9.3` tag archive. The build compiles the
  C++ runtime sources under `runtime/Cpp/runtime/src` into a PIC static library;
  upstream demos, tests, packaging, and installation rules are not configured.
- Archive URL:
  `https://github.com/antlr/antlr4/archive/refs/tags/4.9.3.tar.gz`.
- Archive SHA-256:
  `efe4057d75ab48145d4683100fec7f77d7f87fa258707330cadd1f8e6f7eecae`.
- UTF-8 support: bundled ANTLR4 uses the utf8cpp `v3.1.1` tag archive from
  `https://github.com/nemtrif/utfcpp/archive/refs/tags/v3.1.1.tar.gz`, with
  SHA-256
  `33496a4c3cc2de80e9809c4997052331af5fb32079f43ab4d667cd48c3a36e88`.
- Repository scope and local modifications: no ANTLR4 or utf8cpp source is
  committed. During extraction, `ANTLR4Patch.cmake` applies and verifies the
  existing TsFile portability and compiler fixes to `RuleContext.h`, `Token.h`,
  `Vocabulary.cpp`, `ATN.cpp`, `LL1Analyzer.cpp`, `LL1Analyzer.h`,
  `LexerATNSimulator.cpp`, `LexerATNSimulator.h`, `IntervalSet.cpp`, `Any.h`,
  and `CPPUtils.cpp`. utf8cpp is not modified.
- Resolution: `SYSTEM` accepts ANTLR4 4.9.3 or newer and earlier than 5.0.0
  through an `antlr4_static` or `antlr4_shared` target; `BUNDLED` downloads or
  reuses both verified archives; and `AUTO` prefers a compatible system
  package before falling back to the verified archives.

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

- Origin and scope: the upstream `v0.8.4-rc3` tag archive. SIMDe is
  header-only; the build exposes the extracted header root without configuring
  its upstream tests or installation rules.
- Archive URL:
  `https://github.com/simd-everywhere/simde/archive/refs/tags/v0.8.4-rc3.tar.gz`.
- Archive SHA-256:
  `a5407985439fef1435ac1f091a4d2e6c71981faed213e1be156aca575ce7052c`.
- Repository scope and local modifications: no SIMDe source is committed and
  no upstream source is modified.
- Resolution: `SYSTEM` accepts SIMDe 0.8.4 or newer and earlier than 1.0.0,
  either through the `simde::simde` CMake target or installed headers;
  `BUNDLED` downloads or reuses the verified archive; and `AUTO` prefers a
  compatible system installation before falling back to the verified archive.

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

### Zstandard

- Origin and scope: the upstream `v1.5.7` tag archive. The upstream CMake build
  produces a PIC static compression/decompression library; programs, tests,
  contrib code, dictionary-builder code, deprecated APIs, legacy frame support,
  and Zstandard-internal threading are disabled.
- Archive URL:
  `https://github.com/facebook/zstd/archive/refs/tags/v1.5.7.tar.gz`.
- Archive SHA-256:
  `37d7284556b20954e56e1ca85b80226768902e2edabd3b649e9e72c0c9012ee3`.
- License choice: upstream offers BSD-3-Clause or GPLv2. TsFile selects only
  the BSD-3-Clause option. BSD-3-Clause is an ASF Category A license.
- Repository scope and local modifications: no Zstandard source is committed
  and no upstream source is modified. The TsFile Zstandard compressor consumes
  only the `TsFile::ZSTD` target.
- Resolution: `SYSTEM` accepts Zstandard 1.5.7 or newer and earlier than 2.0.0
  through an upstream CMake target; `BUNDLED` downloads or reuses the verified
  archive; and `AUTO` prefers a compatible system package before falling back
  to the verified archive.

### liblzma (XZ Utils)

- Origin and scope: the `liblzma` component of the upstream XZ Utils `v5.8.3`
  release. The build enables the LZMA1/LZMA2 encoders and decoders with CRC32
  and CRC64 checks in a PIC static library. It disables threading, MicroLZMA,
  lzip, optional Delta/BCJ filters, tools, scripts, tests, translations,
  examples, and documentation.
- Archive URL:
  `https://github.com/tukaani-project/xz/releases/download/v5.8.3/xz-5.8.3.tar.gz`.
- Archive SHA-256:
  `3d3a1b973af218114f4f889bbaa2f4c037deaae0c8e815eec381c3d546b974a0`.
- License boundary: XZ Utils is a mixed-license source distribution. The
  `liblzma` source and the CMake files used to build it are 0BSD, an ASF
  Category A license. GPL-licensed scripts and LGPL-licensed GNU getopt code
  exist elsewhere in the downloaded upstream archive but are not compiled,
  linked, installed, committed, or redistributed by TsFile. The integration
  verifies that every source in the configured `liblzma` target is marked
  0BSD and rejects any configuration that links `libgnu` into `liblzma`.
- Repository scope and local modifications: no XZ or liblzma source is
  committed and no upstream source is modified. The TsFile LZMA2 compressor
  consumes only the `TsFile::LibLZMA` target. The upstream bundled build
  requires CMake 3.20 or
  newer; SYSTEM mode remains available with TsFile's CMake 3.11 baseline.
- Resolution: `SYSTEM` accepts liblzma 5.8.3 or newer and earlier than 6.0.0;
  `BUNDLED` downloads or reuses the verified XZ archive; and `AUTO` prefers a
  compatible system package before falling back to the verified archive.

## Other Build Dependencies

| Dependency | Purpose | Upstream version | License | Current resolution |
| --- | --- | --- | --- | --- |
| GoogleTest | C++ tests | [`release-1.12.1`](https://github.com/google/googletest/tree/release-1.12.1) | BSD-3-Clause | Download the release archive during test configuration |

GoogleTest is not committed to this repository and TsFile does not carry local
source modifications for it. Its archive is verified with SHA-256 digest
`24564e3b712d3eb30ac9a85d92f7d720f60cc0173730ac166f27dda7fed76cb2`
before extraction. A previously downloaded local archive is subject to the same
verification.
