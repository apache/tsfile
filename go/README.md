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

# TsFile Go API

This module provides a cgo binding to the Apache TsFile C++ implementation.
It supports Linux and macOS with cgo enabled. Windows MinGW builds are covered
by CI; MSVC CI verifies the native DLL/import-library artifacts. The native
library must be built locally before running Go code; this module does not
download or install native dependencies.

Prerequisites are Go 1.22 or newer, a C/C++ toolchain, CMake, Maven, a JDK,
and `CGO_ENABLED=1`.

## Build the native library

From the repository root:

```bash
./mvnw -P with-cpp -DskipTests package
```

The Go bridge expects headers under `cpp/target/build/include` and the shared
library under `cpp/target/build/lib`.

## Test

```bash
cd go
make test
make race
make vet
```

The public package is `github.com/apache/tsfile/go/tsfile` and currently exposes
the table model. Create a writer with its single table schema, then write
targetless `Tablet` values or Arrow record batches. Use `Reader.Query` with
optional time-range, tag-filter, pagination, and batch-size options.

`Tablet` is a Go API backed by the native C++ `storage::Tablet`: Go validates
arguments and manages the handle lifetime, while its setters populate the C++
batch through cgo. Arrow batches are created with Arrow Go and passed through
the Arrow C Data Interface; the C++ bridge converts them to a native Tablet
before writing. Writer methods serialize access to the native writer. A Tablet
is not safe for concurrent use; callers must not modify or close one while
`WriteTableTablet` is using it.

Result-set columns are one-based: column 1 is `time`, followed by the selected
data columns. Tablet rows and columns retain Go's zero-based indexing. A query
with a positive batch size returns Arrow batches; other queries use `Next` and
the scalar getters. Readers own their active result sets, so closing a reader
also closes every result set created from it. Returned Arrow objects have their
own lifetime and must be released by the caller.

The runnable table example is under `examples/table_read_write`:

```bash
go run ./examples/table_read_write
```

The example writes one batch with a Tablet and another with an Arrow record
batch, then queries and prints all rows.
