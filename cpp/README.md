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

# TsFile C++ Document

<p align="center">
  <img src="https://www.apache.org/logos/originals/tsfile.svg"
       alt="TsFile Logo"
       width="400"/>
</p>

## Introduction


This directory contains the C++ implementation of TsFile. The C++ version currently supports the query and write functions of TsFile, including time filtering queries.

The source code can be found in the `./src` directory. C/C++ examples are located in the `./examples` directory, and a benchmark for TsFile_cpp can be found in the `./bench_mark` directory. Additionally, a C function wrapper is available in the `./src/cwrapper` directory, which the Python tool relies on.

## How to make contributions

We use `clang-format` to ensure that our C++ code adheres to a consistent set of rules defined in `.clang-format`. This is similar to the Google style.

`mvn spotless:apply` uses `clang-format v17.0.6` for C++ code formatting. Please make sure the `clang-format` in your `PATH` matches this version before submitting code.

How to install `clang-format v17.0.6`:

- macOS

```bash
brew install llvm@17
ln -sf /opt/homebrew/opt/llvm@17/bin/clang-format /opt/homebrew/bin/clang-format
```

- Windows

```bash
choco install llvm --version 17.0.6 --force
```

You can verify the installed version with:

```bash
clang-format --version
```

To format the C++ code, run:

```bash
mvn spotless:apply -P with-cpp
```

If you need to skip code formatting temporarily, you can add `-Dspotless.skip=true`, for example:

```bash
mvn clean verify -P with-cpp -Dspotless.skip=true
```

### Platform Support

TsFile C++ now supports:
- **Linux**: GCC/Clang
- **macOS**: Clang
- **Windows**: MSVC 2017+ and MinGW

All code must compile without errors on all supported platforms before submission.

We welcome any bug reports. You can open an issue with a title starting with [CPP] to describe the bug, like: https://github.com/apache/tsfile/issues/94

## Build

### Requirements

TsFile C++ supports three toolchains:

**Linux (GCC/Clang):**
```bash
sudo apt-get update
sudo apt-get install -y cmake make g++ clang-format libuuid-dev
```

**Windows (MSVC):**
- Visual Studio 2017 or later
- CMake 3.11+

**Windows (MinGW):**
If you compile using MinGW on windows and encounter an error, you can try replacing MinGW with the following version that we have tried without problems:
* GCC 14.2.0 (with **POSIX** threads) + LLVM/Clang/LLD/LLDB 18.1.8 + MinGW-w64 12.0.0 UCRT - release 1
* GCC 12.2.0 + LLVM/Clang/LLD/LLDB 16.0.0 + MinGW-w64 10.0.0 (UCRT) - release 5
* GCC 12.2.0 + LLVM/Clang/LLD/LLDB 16.0.0 + MinGW-w64 10.0.0 (MSVCRT) - release 5
* GCC 11.2.0 + MinGW-w64 10.0.0 (MSVCRT) - release 1

### Build Instructions

To build tsfile, use Maven which automatically detects and uses the appropriate toolchain:

```bash
mvn clean verify -P with-cpp
```

**Toolchain Selection:**

Maven will automatically select the compiler based on your platform:
- **Linux**: GCC/Clang
- **macOS**: Clang  
- **Windows**: MinGW (default) or MSVC

To explicitly specify a toolchain on Windows:

```bash
# Use MinGW (default on Windows)
mvn clean verify -P with-cpp -Dcpp.toolchain=mingw

# Use MSVC
mvn clean verify -P with-cpp -Dcpp.toolchain=msvc
```

By default, the shared library is written to `./cpp/target/build/lib`.

To build `libtsfile` as a static library instead, disable
`TSFILE_BUILD_SHARED` through Maven:

```bash
mvn clean verify -P with-cpp -Dtsfile.build.shared=OFF
```

The static library is written to the same directory (`libtsfile.a` on
Linux/macOS and `tsfile.lib` on Windows). When consuming the installed archive
directly on MSVC rather than linking the CMake `tsfile` target, define
`TSFILE_STATIC` for the consumer so public headers do not use DLL import
decorations.

For a direct CMake build, use:

```bash
cmake -S cpp -B cpp/build/static \
  -DTSFILE_BUILD_SHARED=OFF \
  -DBUILD_TEST=OFF
cmake --build cpp/build/static --target tsfile
```

### Dependency Source Selection

The global `TSFILE_DEPENDENCY_SOURCE` CMake option defines how migrated C++
dependencies are resolved:

- `AUTO` (default): prefer a compatible system package and fall back to the
  verified source archive managed by the build.
- `SYSTEM`: require compatible system packages and fail configuration with a
  clear error when one is unavailable.
- `BUNDLED`: download and build pinned dependency source archives managed by
  the TsFile build.

LZ4 and zlib are currently resolved through this policy. A compatible system
LZ4 must be version 1.9.4 or newer in the 1.x release series. A compatible
system zlib must be version 1.3.1 or newer and earlier than 2.0.0. Other
dependencies retain their existing resolution behavior until they are migrated
incrementally.

For a direct CMake build, select the policy with:

```bash
cmake -S cpp -B cpp/build/system \
  -DTSFILE_DEPENDENCY_SOURCE=SYSTEM
```

If LZ4 or zlib is installed in a non-standard prefix, set `LZ4_ROOT` or
`ZLIB_ROOT`, respectively:

```bash
cmake -S cpp -B cpp/build/system \
  -DTSFILE_DEPENDENCY_SOURCE=SYSTEM \
  -DLZ4_ROOT=/path/to/lz4 \
  -DZLIB_ROOT=/path/to/zlib
```

For a Maven build, use the corresponding Maven property:

```bash
mvn clean verify -P with-cpp \
  -Dtsfile.dependency.source=SYSTEM
```

In `BUNDLED` mode, LZ4 v1.9.4 and zlib v1.3.1 are downloaded from their
upstream GitHub tag archives and verified with SHA-256 before extraction.
Third-party source is placed in the build directory and is not committed to
this repository.

For an offline build with both dependencies enabled, first place
`lz4-v1.9.4.tar.gz` and `zlib-v1.3.1.tar.gz` in a persistent cache, then
configure with network access disabled:

```bash
cmake -S cpp -B cpp/build/offline \
  -DTSFILE_DEPENDENCY_SOURCE=BUNDLED \
  -DTSFILE_DEPENDENCY_OFFLINE=ON \
  -DTSFILE_DEPENDENCY_CACHE=/path/to/dependency-cache
```

The archives can also be supplied explicitly with
`-DTSFILE_LZ4_ARCHIVE=/path/to/lz4-v1.9.4.tar.gz` and
`-DTSFILE_ZLIB_ARCHIVE=/path/to/zlib-v1.3.1.tar.gz`. Cached and explicitly
supplied archives must match their pinned SHA-256 digests. The equivalent Maven
properties are `tsfile.dependency.offline` and `tsfile.dependency.cache`.

Dependencies are being migrated to this framework incrementally. Until an
individual dependency is migrated, it continues to use its existing resolution
behavior.

Before you submit your code to GitHub, please ensure that the compilation is correct.

### configure the cross-compilation toolchain

Modify the Toolchain File `cmake/ToolChain.cmake`, define the following variables:

- `CMAKE_C_COMPILER`: Specify the path to the C compiler.
- `CMAKE_CXX_COMPILER`: Specify the path to the C++ compiler.
- `CMAKE_FIND_ROOT_PATH`: Set the root path for the cross-compilation environment (e.g., the directory of the cross-compilation toolchain).

In the `cpp/` directory, run the following commands to create the build directory and start the compilation:
```
mkdir build && cd build
cmake .. -DToolChain=ON
make
```

## Parallel Write

TsFile C++ supports thread pool-based parallel column encoding for the table write path (`write_table`). When enabled, each column (time and value columns) is written in parallel using precomputed page boundaries, while maintaining aligned page sealing across columns.

### Build Options

Parallel write is controlled by the `ENABLE_THREADS` CMake option (ON by default):

```bash
cmake .. -DENABLE_THREADS=ON   # enable (default)
cmake .. -DENABLE_THREADS=OFF  # disable — all thread code is stripped at compile time
```

### Runtime Configuration

```cpp
#include "common/global.h"

// Enable or disable parallel write at runtime (auto-disabled on single-core machines)
storage::set_parallel_write_enabled(true);

// Set the number of worker threads (must be called before creating TsFileWriter)
storage::set_write_thread_count(4);
```

By default, parallel write is enabled when the machine has more than one CPU core, and the thread count is set to the number of hardware cores (capped at 64).

## Use TsFile

You can find examples on how to read and write data in `demo_read.cpp` and `demo_write.cpp` located under `./examples/cpp_examples`. There are also examples under `./examples/c_examples` on how to use a C-style API to read and write data in a C environment. The examples will be built automatically when you run the main build command.

### File-level properties

`TsFileWriter` and `TsFileTableWriter` can add or replace binary properties
while the writer is open. Values are copied immediately and may still be
changed after `flush()`; a closed file cannot be modified.

```cpp
std::vector<uint8_t> value = {0x01, 0x00, 0xFF};
writer.add_tsfile_property("binary-property", value);

// nullptr with length 0 is null; an empty vector is a non-null empty value.
writer.add_tsfile_property("null-property", nullptr, 0);
writer.add_tsfile_property("empty-property", std::vector<uint8_t>());

storage::TsFileProperties properties = reader.get_tsfile_properties();
```

Property values do not store a data type. Applications should define their own
portable byte encoding for integers, floating-point values, or structures.
