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

# Go Table API Implementation Plan

**Goal:** Implement the table-only Go API described in
`docs/superpowers/specs/2026-09-08-go-table-api-design.md`, backed exclusively
by supported public C ABI entry points.

**Architecture:** Go owns validation, immutable option values, wrapper
lifecycle, and Arrow Go objects. The C wrapper owns translation to native
TsFile table writers, query filters, result batches, and Arrow C Data
Interface structures. Existing C APIs remain available to other bindings.

**Toolchain:** Go 1.22, cgo, Arrow Go v18, C++11, CMake, Maven, GoogleTest.

---

## Task 1: Lock the table-only Go surface

**Files:**
- Modify: `go/tsfile/schema_test.go`
- Modify: `go/tsfile/writer_test.go`
- Modify: `go/tsfile/tablet_test.go`
- Modify: `go/tsfile/reader_test.go`
- Modify: `go/tsfile/result_set_test.go`
- Modify: `go/tsfile/schema.go`
- Modify: `go/tsfile/writer.go`
- Modify: `go/tsfile/tablet.go`
- Modify: `go/tsfile/reader.go`
- Modify: `go/tsfile/result_set.go`

Add failing tests for schema deep-copy and validation, constructor-bound
writers, targetless tablets, BLOB setters, reset, query defaults/options,
one-based result indexes, mode mismatch, and close behavior. Implement only
the Go-level validation and state needed to pass tests that do not require a
native library.

Run: `cd go && go test ./tsfile -run 'Test(TableSchema|WriterOptions|TabletValidation|QueryOptions|ResultSetValidation)'`

## Task 2: Add supported table writer and Tablet C ABI

**Files:**
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.h`
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.cc`
- Modify: `cpp/src/cwrapper/arrow_c.cc`
- Modify: `cpp/test/cwrapper/tsfile_cwrapper_test.cc`
- Modify: `go/tsfile/cgo_bridge.go`
- Modify: `go/tsfile/writer.go`
- Modify: `go/tsfile/tablet.go`

Add native tests for atomic table-writer creation, targetless Tablet writes,
BLOB values, and Arrow schema rejection. Promote Arrow table writing to a
documented public C entry point. Wire the constructor-bound Go writer and
targetless Tablet to these APIs.

Run: `./mvnw -P with-cpp -DskipTests package`

## Task 3: Add the unified query and safe schema ABI

**Files:**
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.h`
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.cc`
- Modify: `cpp/test/cwrapper/tsfile_cwrapper_test.cc`
- Modify: `go/tsfile/cgo_bridge.go`
- Modify: `go/tsfile/reader.go`
- Add: `go/tsfile/tag_filter.go`

Test and add one public C query entry point that composes time range, tag
filter, offset, limit, and batch size. Make missing-table schema lookup return
an error instead of dereferencing an empty schema. Implement immutable Go
query options and TagFilter builders, then use the unified entry point from
`Reader.Query`.

Run: `./mvnw -P with-cpp -DskipTests package && cd go && go test ./tsfile -run 'Test(Query|TagFilter|Schema)'`

## Task 4: Implement scalar result behavior and BLOB access

**Files:**
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.h`
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.cc`
- Modify: `go/tsfile/cgo_bridge.go`
- Modify: `go/tsfile/result_set.go`
- Modify: `go/tsfile/result_set_test.go`

Add a length-aware binary getter and wire `ResultSet.Bytes`. Convert all Go
scalar access to one-based indexes without a second translation at the C
boundary. Enforce current-row and row-mode checks before native calls.

Run: `cd go && go test ./tsfile -run 'TestResultSet'`

## Task 5: Integrate Arrow Go read and write

**Files:**
- Modify: `go/go.mod`
- Modify: `go/go.sum`
- Add: `go/tsfile/arrow.go`
- Add: `go/tsfile/arrow_test.go`
- Modify: `go/tsfile/cgo_bridge.go`
- Modify: `go/tsfile/writer.go`
- Modify: `go/tsfile/result_set.go`

Use Arrow Go v18 C Data export/import helpers. Validate RecordBatch and Table
schemas before export. Import each native query batch with independent release
ownership and return `io.EOF` at completion. Test slices, NULL values, BLOB,
empty results, and use-after-close boundaries.

Run: `cd go && go test ./tsfile -run 'TestArrow'`

## Task 6: Replace tree-model examples and complete integration coverage

**Files:**
- Modify: `go/README.md`
- Modify: `go/examples/**`
- Modify: `go/tsfile/integration_test.go`
- Modify: `.github/workflows/unit-test-go.yml`
- Add or modify: `python/tests/**` for interop fixtures

Remove tree-model usage from the Go-facing documentation and tests. Cover all
supported types, Tablet and Arrow round trips, query option composition,
schema enumeration, Go/Python interop, and platform build commands.

Run: `./mvnw -P with-cpp -DskipTests package && cd go && go test ./...`

## Task 7: Final validation

Run C++ cwrapper tests, all Go tests, focused Python interop tests, and
formatting checks. Review the final diff for accidental tree-model public API
references and for files unrelated to this work.

Run:

```bash
./mvnw -P with-cpp test
cd go && go test ./...
./mvnw spotless:check
```
