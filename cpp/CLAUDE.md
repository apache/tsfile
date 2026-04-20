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

# CLAUDE.md — C++ Module

This file provides guidance to Claude Code (claude.ai/code) when working with the C++ module.

## Build Commands

```bash
# Build (Release, default)
bash build.sh

# Build variants
bash build.sh -t=Debug
bash build.sh -t=RelWithDebInfo
bash build.sh -a=ON              # Enable AddressSanitizer
bash build.sh -c=ON              # Enable code coverage

# Disable optional compression libraries
bash build.sh --disable-snappy --disable-lz4 --disable-lzokay --disable-zlib

# Or use CMake directly
mkdir -p build/Release && cd build/Release
cmake ../.. -DCMAKE_BUILD_TYPE=Release -DBUILD_TEST=ON
make -j$(nproc)

# Via Maven from repo root
./mvnw clean verify -P with-cpp

# Run all tests
./build/Release/lib/TsFile_Test

# Run specific test suite
./build/Release/lib/TsFile_Test --gtest_filter=SnappyCompressorTest.*
```

## Build Options (CMake)

All `ON` by default unless noted:

| Option | Purpose |
|--------|---------|
| `BUILD_TEST` | Compile tests (GTest 1.12.1, auto-downloaded) |
| `ENABLE_ANTLR4` | ANTLR4 parser runtime |
| `ENABLE_SNAPPY` / `ENABLE_LZ4` / `ENABLE_LZOKAY` / `ENABLE_ZLIB` | Compression libraries |
| `ENABLE_THREADS` | Multi-threaded read/write via pthreads |
| `ENABLE_ASAN` | AddressSanitizer (`OFF` by default) |
| `ENABLE_SIMDE` | SIMD Everywhere (`OFF` by default) |

## Source Structure

```
cpp/src/
├── common/        # Core types: Schema, Tablet, DeviceId, TsBlock, allocators, config
├── compress/      # Compression: Snappy, LZ4, LZOKAY, Zlib (factory pattern)
├── encoding/      # Encoding: Plain, TS2Diff, Gorilla, Dictionary, RLE, Zigzag, SPRINTZ
├── file/          # File I/O: TsFileIOReader/Writer, RestorableTsFileIOWriter
├── reader/        # Read path: TsFileReader, QueryExecutor, filters, result sets
├── writer/        # Write path: TsFileWriter, TsFileTableWriter, ChunkWriter, PageWriter
├── parser/        # ANTLR4 path parser (grammars + generated code)
├── cwrapper/      # C language bindings (used by Python module)
└── utils/         # Utilities: error codes, date handling, fault injection
```

## Architecture Notes

- **C++11** standard, targets CMake 3.11+
- Dual data model: **tree-view** (`TsFileTreeWriter/Reader`) and **table-view** (`TsFileTableWriter`, `TableQueryExecutor`)
- Parallel column encoding in table write path, controlled by `ENABLE_THREADS`
- Third-party libraries are bundled under `third_party/` (ANTLR4, Snappy, LZ4, LZOKAY, Zlib, SIMDe)
- `cwrapper/` provides the C API that the Python module binds to via Cython

## Code Style

- **Formatter**: clang-format (Google style), configured in `.clang-format`

## Testing

- **Framework**: Google Test 1.12.1 (auto-downloaded during build, or supply `third_party/googletest-release-1.12.1.zip`)
- Tests in `cpp/test/`, mirroring `src/` structure
- Test discovery via `gtest_discover_tests()`

## License Header

Every new file must include the Apache License 2.0 header at the top. For C/C++ files, use the `/* */` block comment style. See any existing `.h` or `.cc` file for the exact wording.

## Git Commit

- Do NOT add `Co-Authored-By` trailer to commit messages.
