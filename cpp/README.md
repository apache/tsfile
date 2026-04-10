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

We use `clang-format` to ensure that our C++ code adheres to a consistent set of rules defined in `./clang-format`. This is similar to the Google style.

We welcome any bug reports. You can open an issue with a title starting with [CPP] to describe the bug, like: https://github.com/apache/tsfile/issues/94

## Build

### Requirements

```bash
sudo apt-get update
sudo apt-get install -y cmake make g++ clang-format libuuid-dev
```

To build tsfile, you can run: `bash build.sh`. If you have Maven tools, you can run: `mvn package -P with-cpp clean verify`. Then, you can find the shared object at `./build`.

Before you submit your code to GitHub, please ensure that the `mvn` compilation is correct.

If you compile using MinGW on windows and encounter an error, you can try replacing MinGW with the following version that we have tried without problems:

* GCC 14.2.0 (with **POSIX** threads) + LLVM/Clang/LLD/LLDB 18.1.8 + MinGW-w64 12.0.0 UCRT - release 1
* GCC 12.2.0 + LLVM/Clang/LLD/LLDB 16.0.0 + MinGW-w64 10.0.0 (UCRT) - release 5
* GCC 12.2.0 + LLVM/Clang/LLD/LLDB 16.0.0 + MinGW-w64 10.0.0 (MSVCRT) - release 5
* GCC 11.2.0 + MinGW-w64 10.0.0 (MSVCRT) - release 1

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

You can find examples on how to read and write data in `demo_read.cpp` and `demo_write.cpp` located under `./examples/cpp_examples`. There are also examples under `./examples/c_examples`on how to use a C-style API to read and write data in a C environment. You can run `bash build.sh` under `./examples` to generate an executable output under `./examples/build`.