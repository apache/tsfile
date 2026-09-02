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

The public package is `github.com/apache/tsfile/go/tsfile`. Result-set columns
use idiomatic Go zero-based indexes; the underlying C ABI remains one-based.
Readers own their active result sets, so closing a reader also closes every
result set created from it. All native `Close` methods are idempotent.
`Writer.AddProperty` rejects empty values because the native ABI reserves a nil
value for a NULL property.

Runnable tree and table examples are under `examples/tree_read_write` and
`examples/table_read_write`:

```bash
go run ./examples/tree_read_write
go run ./examples/table_read_write
```
