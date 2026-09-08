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

# Go Table API Design

## Scope

The Go SDK exposes the TsFile table model only. The module path remains
`github.com/apache/tsfile/go`. Tree-model constructors, registration methods,
tablets, records, and query entry points are removed from the public Go API.

The implementation uses the supported public C ABI. It does not call C++ APIs
or Python-only compatibility functions directly.

## Public Go API

`NewWriter(path, schema, options...)` validates and deep-copies one
`TableSchema`, opens the native writer, and registers that schema atomically.
The writer retains the table name and column definitions until close. The
caller does not put a table name on a `Tablet`; `WriteTableTablet` and Arrow
writes always target the writer's bound table.

`NewTablet(columns, maxRows)` creates a reusable table batch. Row and column
indexes on a Tablet are zero-based. It supports setters for BOOLEAN, INT32,
DATE, INT64, TIMESTAMP, FLOAT, DOUBLE, STRING, TEXT, and BLOB. Cells that are
not assigned remain NULL. A Tablet is not reset or reused after writing.

`NewReader(path)` opens a file. `Reader.Query(table, columns, options...)` is
the only table query entry point. The options are `WithTimeRange`,
`WithTagFilter`, `WithOffset`, `WithLimit`, and `WithBatchSize`. Defaults are
the full time range, no tag filter, offset 0, limit -1, and row mode. A
positive batch size selects Arrow batch mode.

Tag predicates are immutable Go values. Equality, inequality, ordering,
between, and/or/not builders validate their shape in Go. Column category and
type validation is completed against the selected table schema when the query
is opened.

Result-set scalar access uses one-based column indexes: column 1 is time and
data columns begin at 2. Row methods reject batch-mode result sets, and Arrow
methods reject row-mode result sets. `ReadArrowRecordBatch` and
`ReadArrowBatch` return `io.EOF` after the last batch.

Arrow writes use `github.com/apache/arrow-go/v18`. `WriteArrowBatch` accepts
an `arrow.Record` or `arrow.Table` and validates the time column, field names,
field types, row counts, and nullability against the writer schema before
crossing the C boundary.

## C ABI

The public C wrapper provides table-bound writer creation, targetless Tablet
creation, Arrow batch writing, combined table querying, length-aware BLOB
access, and safe table-schema retrieval.

The combined query accepts an optional time filter, optional tag filter,
offset, limit, and batch size in one call. It maps directly to the existing
native table query executor so option composition has one implementation.

Arrow data crosses the boundary through the Arrow C Data Interface. Importing
a Go record does not transfer ownership to the writer. Exporting a native
query batch transfers release callbacks to Arrow Go; closing a result set does
not invalidate a batch already returned to the caller.

All C entry points validate null handles and input ranges, translate C++
exceptions to error codes, and leave output values in a releasable empty state
when they fail.

## Data and validation rules

Table schemas require a non-empty table name, unique non-empty column names,
supported data types, and TAG or FIELD column categories. Names containing a
NUL byte are rejected before cgo conversion. Public table fields exclude
VECTOR and NULL_TYPE. Real table and column identifiers are normalized to
lower case, matching the native table model and Python binding.

BLOB values preserve their explicit length, embedded NUL bytes, empty values,
and NULL distinction. STRING and TEXT remain UTF-8 strings. Time values are
signed 64-bit integers and may not be NULL in Tablet or Arrow input.

The writer serializes operations. If native close reports an error, the writer
retains the native handle so the caller may retry. A reader owns the result
sets it creates; closing the reader closes those result sets first. Successful
close is idempotent for all Go wrappers.

## Compatibility and migration

This is a deliberate breaking change to the current experimental Go package.
Existing tree-model Go examples and tests are replaced with table-model
coverage. The Go module path is unchanged.

The C ABI remains source-compatible for existing consumers: new table APIs are
added and existing entry points remain available for C++ and Python bindings.

## Verification

Unit tests cover schema copying and validation, option defaults and conflicts,
one-based result indexes, mode errors, lifecycle behavior, and Arrow schema
validation. Native and Go integration tests cover Tablet and Arrow round
trips, all ten public data types, NULL versus zero values, binary data with
embedded NUL, tag predicates, time filters, pagination across batches, schema
enumeration, empty results, and repeated close.

Interop tests write with Go and read with Python, then write with Python and
read with Go. Platform CI continues to build on Linux, macOS, and MinGW; MSVC
continues to validate native artifacts separately.
