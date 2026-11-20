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

# Go SDK

Use this reference only for Go binding work. The package is a cgo binding over
the C++ library in this source line, covers the table model only, and does not
expose the tree model. Module `github.com/apache/tsfile/go`; package
`github.com/apache/tsfile/go/tsfile`; Go 1.22 or newer with `CGO_ENABLED=1`.

Read `go/README.md` and `go/tsfile/` before generating exact signatures.

## Build and Test

- Build the C++ library first: `./mvnw -P with-cpp -DskipTests package`. cgo
  compiles against `cpp/target/build/include` and links `libtsfile` from
  `cpp/target/build/lib`; Linux and macOS builds also embed that directory as an
  rpath, while Windows needs `tsfile.dll` on `PATH`.
- From `go/`, run `make vet`, `make test`, `make race`. `make fmt` applies
  `gofmt`. The maintained example is `go/examples/table_read_write/`:
  `go run ./examples/table_read_write`.
- CI runs the Go tests on Linux, macOS, and Windows MinGW; an MSVC job only
  verifies that the native DLL/import library is produced. Do not claim MSVC Go
  test coverage.

## Table-Model Write

Imports and surrounding function scaffolding are elided; the compile-checked
sources are `go/examples/table_read_write/main.go` and `go/tsfile/*_test.go`.

```go
schema := tsfile.TableSchema{Table: "metrics", Columns: []tsfile.ColumnSchema{
    {Name: "device", DataType: tsfile.DataTypeString, Category: tsfile.ColumnCategoryTag},
    {Name: "value", DataType: tsfile.DataTypeInt64, Category: tsfile.ColumnCategoryField},
}}
writer, err := tsfile.NewWriter("data.tsfile", schema)
if err != nil {
    return err
}
defer writer.Close()

tablet, err := tsfile.NewTablet([]tsfile.TabletColumn{
    {Name: "device", DataType: tsfile.DataTypeString},
    {Name: "value", DataType: tsfile.DataTypeInt64},
}, 2)
if err != nil {
    return err
}
defer tablet.Close()

if err := tablet.AddTimestamp(0, 1); err != nil {
    return err
}
if err := tablet.SetString(0, 0, "d1"); err != nil {
    return err
}
if err := tablet.SetInt64(0, 1, 10); err != nil {
    return err
}
return writer.WriteTableTablet(tablet)
```

`NewWriter` creates or truncates the target file and binds it to one table
schema. `Close` flushes and is idempotent; `Flush` writes buffered data without
closing. `WriteTableTablet` does not consume the tablet, and writer methods
serialize access to the native writer.

Arrow batches use the same bound table. `WriteArrowBatch` accepts `arrow.Record`
or `arrow.Table` whose schema is exactly the table columns plus a `time` field
(case-insensitive) typed `int64` or `arrow.TimestampType{Unit: arrow.Nanosecond}`
without a time zone. The time column must not contain nulls, each record written
from the batch must hold fewer than 2^30 rows, and an empty batch is a no-op.
The caller owns the batch and may release it once the call returns.

`AddProperty` writes a binary file property; both the key and the value must be
non-empty. `WithMemoryThreshold` overrides the 128 MiB default, but the native
ABI stores it in process-global `chunk_group_size_threshold_`, which is read
during writes; constructing another writer replaces it for the whole process, so
keep thresholds consistent across concurrently used writers.

## Table-Model Read

```go
reader, err := tsfile.NewReader("data.tsfile")
if err != nil {
    return err
}
defer reader.Close()

result, err := reader.Query("metrics", []string{"device", "value"},
    tsfile.WithTimeRange(0, 100), tsfile.WithLimit(10))
if err != nil {
    return err
}
defer result.Close()

for {
    ok, err := result.Next()
    if err != nil {
        return err
    }
    if !ok {
        break
    }
    timestamp, err := result.Int64(1) // one-based; column 1 is time
    if err != nil {
        return err
    }
    device, err := result.String(2)
    if err != nil {
        return err
    }
    value, err := result.Int64(3)
    if err != nil {
        return err
    }
    fmt.Printf("%d %s %d\n", timestamp, device, value)
}
```

Query options:

- `Query` requires a non-empty column list; table and column names are matched
  case-insensitively.
- `WithTimeRange(start, end)` is inclusive; `start > end` yields an empty result.
- `WithOffset` must be nonnegative; `WithLimit` with a negative value or no limit
  scans to the end.
- `WithBatchSize(n)` with `n > 0` switches to Arrow batch mode; `n <= 0` keeps
  row mode. Row and batch operations are not interchangeable and report
  `ErrWrongResultMode` when mixed.
- `WithTagFilter` takes `Eq`, `Neq`, `Lt`, `Lte`, `Gt`, `Gte`, `Between`,
  `And`, `Or`, or `Not`. Operands are strings, `Between` rejects `lower > upper`,
  and tag columns must exist in the table.
- `GetTableSchema` and `GetAllTableSchemas` return independent copies; the latter
  yields a slice, not a map keyed by table name.

Row result sets expose `Next`, `Metadata`, `IsNull`, and typed getters. Columns
are one-based with `time` at column 1, unlike the zero-based `Tablet`. A getter
on a null value returns `ErrNullValue`, and calling one before `Next` or after
the last row returns `ErrNoCurrentRow`. In batch mode, `ReadArrowRecordBatch`
returns the next `arrow.Record` and `ReadArrowBatch` wraps one batch in an
`arrow.Table`; both return `io.EOF` after the last batch, and the caller must
`Release` every returned object.

Readers own their active result sets: closing a reader closes all of them, while
closing an individual result set earlier releases its native memory. Both closes
are idempotent. A `Tablet` is not safe for concurrent use and must not be
modified or closed while `WriteTableTablet` is using it.

## Schema and Errors

Table and column names are normalized to lowercase, so validation and lookups are
case-insensitive. `NewWriter` rejects an empty schema, duplicate column names,
categories other than Tag/Field, and unsupported data types; TAG columns must use
`DataTypeString`. Supported types are `DataTypeBoolean`, `DataTypeInt32`,
`DataTypeInt64`, `DataTypeFloat`, `DataTypeDouble`, `DataTypeText`,
`DataTypeString`, `DataTypeTimestamp`, `DataTypeDate`, and `DataTypeBlob`.

`Tablet.SetInt32` also accepts DATE columns and `SetInt64` also accepts TIMESTAMP
columns; `SetString` rejects embedded NUL bytes, and BLOB values use `SetBytes`.
Tablet row capacity must satisfy `1 <= maxRows < 2^30`.

Errors are `*tsfile.Error` values carrying the C error code. Compare them with
`errors.Is`, which matches by code, for example `errors.Is(err, tsfile.ErrTypeMismatch)`.
Sentinels include `ErrClosed`, `ErrNullValue`, `ErrNoCurrentRow`, and
`ErrWrongResultMode`; the remaining exported errors mirror C wrapper codes such
as `ErrInvalidArgument`, `ErrInvalidSchema`, `ErrTableNotExist`, and
`ErrColumnNotExist`.

Do not invent tree-model APIs for Go, and do not claim Arrow batch reads or
writes beyond the interfaces above without checking `go/tsfile/` in the target
checkout.
