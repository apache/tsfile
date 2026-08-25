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

# C++ Build and Parallel Write

## Shared or Static libtsfile

`TSFILE_BUILD_SHARED` selects the library kind and defaults to `ON`.

```sh
# Maven profile: shared default or static override
./mvnw clean verify -P with-cpp
./mvnw clean verify -P with-cpp -Dtsfile.build.shared=OFF

# Direct CMake static build
cmake -S cpp -B cpp/build/static \
  -DTSFILE_BUILD_SHARED=OFF -DBUILD_TEST=OFF
cmake --build cpp/build/static --target tsfile

# Repository wrapper
cd cpp && bash build.sh --build-static
```

Use a separate build directory when switching library kind. A direct MSVC
consumer of the static archive must define `TSFILE_STATIC`; consumers of the
exported CMake target inherit the required definition. Verify the artifact in
`cpp/target/build/lib` for Maven or the selected CMake build tree.

`ENABLE_SIMD` also defaults to `ON` in the current CMake and Maven builds. Set
it explicitly only for portability diagnosis or controlled benchmarking; it
is independent of static/shared selection and thread support.

## Parallel Write

Compile thread support with `ENABLE_THREADS=ON` (the default). `OFF` strips the
threaded paths. Configure the current global worker pool through
`common/global.h`:

```cpp
#include "common/global.h"

common::set_parallel_write_enabled(true);
int status = common::set_thread_count(4);  // valid range: 1..64
```

The current baseline uses `common::set_parallel_write_enabled` and
`common::set_thread_count`; use those exact names and namespace. The pool is
shared by parallel read and write paths. Configure it before creating active
readers/writers. Changing the count after initialization rebuilds the pool and
must never race with an operation using it.

Current source initializes parallel writing as enabled and the pool size as 6.
Do not substitute a hardware-concurrency default from older prose. Parallel
work is used only when the compiled path and runtime conditions qualify; keep
the tablet/batch write contract correct when it falls back to serial work.

Always check write return codes. If a multi-column batch fails partway, stop
using that writer rather than attempting to flush a potentially misaligned
partial batch.

## Source Anchors

- Link selection: `cpp/CMakeLists.txt`, `cpp/src/CMakeLists.txt`,
  `cpp/build.sh`, `cpp/pom.xml`, and `cpp/README.md`
- Runtime API and defaults: `cpp/src/common/global.h`,
  `cpp/src/common/global.cc`, and `cpp/src/common/config/config.h`
- Write behavior: `cpp/src/writer/tsfile_writer.cc` and current writer tests
