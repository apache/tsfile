# TsFile Go API Design

## Status

Proposed design for an initial Go binding over the TsFile C++ implementation.
This document covers the API and implementation boundary only; it is not an
implementation plan.

## Goals and scope

Add a Go module at `go/` with module path `github.com/apache/tsfile/go` and a
public package imported as `github.com/apache/tsfile/go/tsfile`.

The first release supports:

- Linux and macOS with cgo enabled;
- single-file tree-model and table-model read, write, and query operations;
- schemas, tablets, readers, writers, and result sets;
- the same C++ core and C wrapper used by the Python binding; and
- deterministic ownership, error handling, and cross-language tests.

The first release does not include Arrow, pandas/dataframe-style APIs, dataset
or multi-file merging, Windows, automatic native dependency installation, or a
pure-Go TsFile implementation.

## Architecture

```text
Go public API (go/tsfile)
        |
internal cgo bridge and ownership helpers
        |
stable, public cpp/src/cwrapper C ABI
        |
TsFile C++ core
```

Only bridge files import `C`. Public Go types do not expose C pointers or C
types. Every value crossing the boundary is validated and either copied or
owned by one explicitly documented handle.

Suggested layout:

```text
go/
  go.mod
  tsfile/
    constants.go
    errors.go
    schema.go
    tablet.go
    writer.go
    reader.go
    result_set.go
    cgo_bridge.go
  examples/
    tree_read_write/
    table_read_write/
  testdata/
  README.md
```

The cgo bridge remains in the public package because Go's `internal` import
rules do not improve the C boundary by themselves. It is kept in narrowly
scoped files with unexported functions, so all C allocation and release sites
remain easy to audit.

## Stable C ABI prerequisite

The public C ABI already provides reader construction and close, table and
tree queries, result-set iteration and typed accessors, schema structs, tablet
allocation and typed insertion, and resource release functions.

Several writer operations needed for Python parity are currently available
only as underscore-prefixed functions under a section explicitly marked as a
temporary Python API. The Go binding must not depend on those private symbols.
Before implementing the corresponding Go methods, add public, non-underscore
thin delegates for the existing behavior:

- pathname-based writer construction;
- target-named tablet construction;
- table, timeseries, and device registration;
- tree-tablet and table-tablet writes; and
- writer flush.

The wrappers are additive and mirror existing arguments and `ERRNO` return
semantics. Existing public functions remain unchanged. Python may migrate to
the public delegates, but its private symbols need not be removed as part of
this change. Public writer close and property functions are reused when their
existing lifetime semantics match the pathname writer; otherwise an additive
delegate is supplied rather than changing an existing signature.

No generic configuration ABI, error-string ABI, or Go-specific C ABI is
introduced.

## Public Go API

Enums use distinct Go integer types whose constant values match
`TSDataType`, `TSEncoding`, `CompressionType`, and `ColumnCategory`. Tests pin
every value to the C header.

Representative schema and writer API:

```go
type TimeseriesSchema struct {
    Name        string
    DataType    DataType
    Encoding    Encoding
    Compression Compression
}

type DeviceSchema struct {
    Device     string
    TimeSeries []TimeseriesSchema
}

type ColumnSchema struct {
    Name     string
    DataType DataType
    Category ColumnCategory
}

type TableSchema struct {
    Table   string
    Columns []ColumnSchema
}

func NewWriter(path string, options ...WriterOption) (*Writer, error)
func (w *Writer) RegisterTable(TableSchema) error
func (w *Writer) RegisterTimeseries(device string, schema TimeseriesSchema) error
func (w *Writer) RegisterDevice(DeviceSchema) error
func (w *Writer) WriteTreeTablet(*Tablet) error
func (w *Writer) WriteTableTablet(*Tablet) error
func (w *Writer) AddProperty(key string, value []byte) error
func (w *Writer) Flush() error
func (w *Writer) Close() error
```

A tablet represents one target device for tree data or one target table for
table data. Its column definitions are fixed at construction:

```go
type TabletColumn struct {
    Name     string
    DataType DataType
}

func NewTablet(target string, columns []TabletColumn, maxRows int) (*Tablet, error)
func (t *Tablet) AddTimestamp(row int, timestamp int64) error
func (t *Tablet) SetBool(row, column int, value bool) error
func (t *Tablet) SetInt32(row, column int, value int32) error
func (t *Tablet) SetInt64(row, column int, value int64) error
func (t *Tablet) SetFloat32(row, column int, value float32) error
func (t *Tablet) SetFloat64(row, column int, value float64) error
func (t *Tablet) SetString(row, column int, value string) error
func (t *Tablet) Rows() int
func (t *Tablet) Close() error
```

All constructors and mutations return errors. The API does not use reflection
or `any` in the write path. A successful write does not consume the tablet;
the caller remains responsible for closing it.

The reader exposes the query shapes already present in the C ABI, not SQL:

```go
type TableQuery struct {
    Table      string
    Columns    []string
    Start, End int64
}

type TreeQuery struct {
    Paths      []string
    Start, End int64
}

type TreeRowsQuery struct {
    Devices, Measurements []string
    Offset, Limit         int
}

type TableRowsQuery struct {
    Table         string
    Columns       []string
    Offset, Limit int
    BatchSize     int
}

func NewReader(path string) (*Reader, error)
func (r *Reader) QueryTable(TableQuery) (*ResultSet, error)
func (r *Reader) QueryTree(TreeQuery) (*ResultSet, error)
func (r *Reader) QueryTreeRows(TreeRowsQuery) (*ResultSet, error)
func (r *Reader) QueryTableRows(TableRowsQuery) (*ResultSet, error)
func (r *Reader) Close() error
```

The MVP passes a null tag-filter handle for table-row queries. A Go tag-filter
DSL can be added separately after the core reader/writer API is stable.

Result sets retain typed access and make end-of-stream distinct from failure:

```go
func (rs *ResultSet) Next() (bool, error)
func (rs *ResultSet) Metadata() []ColumnMetadata
func (rs *ResultSet) IsNull(column int) (bool, error)
func (rs *ResultSet) Bool(column int) (bool, error)
func (rs *ResultSet) Int32(column int) (int32, error)
func (rs *ResultSet) Int64(column int) (int64, error)
func (rs *ResultSet) Float32(column int) (float32, error)
func (rs *ResultSet) Float64(column int) (float64, error)
func (rs *ResultSet) String(column int) (string, error)
func (rs *ResultSet) Close() error
```

`Next` returns `(false, nil)` at a clean end and `(false, err)` on failure.

## Errors

C calls report numeric values defined in `errno_define_c.h`. Go preserves the
raw value:

```go
type Error struct {
    Code int32
    Op   string
}
```

`Error.Error` maps known codes to stable messages and includes the operation.
Unknown codes remain inspectable and render as `unknown error code N`.
Exported sentinel errors cover common conditions such as invalid arguments,
out-of-range access, type mismatch, missing devices/measurements/tables, and
file I/O. `errors.Is` compares TsFile error codes.

`ErrClosed` and `ErrNullValue` describe Go binding state rather than C error
codes. No error-string allocation or free function is invented.

## Ownership, lifetime, and concurrency

Each `Reader`, `Writer`, `Tablet`, and `ResultSet` owns exactly one C handle.
Its `Close` method is the only release site, clears the pointer, and is
idempotent. Other methods return `ErrClosed` after release. Finalizers are not
used as the primary ownership mechanism.

Go strings and byte slices are copied into temporary C allocations and freed
immediately after the call. C strings and metadata returned to Go are copied
before their documented release function is called. Heap strings returned by
result-set string accessors are copied and released with the matching C
allocator (`free`). Go pointers are never retained by C.

A reader owns its result sets because the C API states that closing a reader
invalidates them. `Reader.Close` first closes registered result sets, then the
reader. Explicit result-set close unregisters it. Lock ordering is always
reader before result set.

Handle methods are internally serialized with a small mutex. This prevents a
method from racing with `Close` and makes concurrent calls memory-safe, but it
does not promise parallel execution inside one handle. Independent handles may
run concurrently.

## Build and runtime linking

The repository build is the MVP packaging path. From `go/tsfile`, cgo uses:

```text
-I${SRCDIR}/../../cpp/target/build/include
-L${SRCDIR}/../../cpp/target/build/lib -ltsfile
```

The C++ library is built first with `TSFILE_BUILD_SHARED=ON`, which is already
the default. Runtime instructions document `LD_LIBRARY_PATH` on Linux and
`DYLD_LIBRARY_PATH` or a corrected install name on macOS. Static linking is
deferred until all transitive native dependencies are proven. A future native
package may expose `libtsfile.pc`; pkg-config is not required for the MVP.

## Testing and acceptance criteria

Unit tests cover validation, enum pinning, error mapping, typed tablet access,
idempotent close, use-after-close, null values, and clean-end versus iteration
failure.

Integration tests cover all supported data types and:

- tree and table write/read round trips;
- multiple devices and columns;
- empty and bounded time-range queries;
- offset and limit behavior;
- flush followed by close; and
- reader/result-set parent-child lifetime behavior.

Cross-language tests read Python or Java fixtures from Go and read Go-written
files with an existing Python or C++ reader. C wrapper lifetime tests run under
ASan on supported CI jobs, with valgrind as an optional Linux supplement.
`go test -race` exercises concurrent method and close calls to verify internal
serialization.

Acceptance requires:

- clean builds and tests on Linux amd64 and macOS arm64;
- no references from Go to underscore-prefixed C symbols;
- no C leaks or use-after-free in the exercised wrapper paths;
- value-identical tree and table cross-language round trips;
- clean `gofmt`, `go vet`, and `go test -race`; and
- runnable tree and table examples plus reproducible build instructions.

## Implementation sequence

1. Promote and test the minimal stable C writer/tablet delegates.
2. Add the Go module, enum/error mapping, and cgo bridge.
3. Implement Reader and ResultSet with fixture-based tests.
4. Implement schemas, Tablet, and Writer with round-trip tests.
5. Add cross-language, ASan, race, examples, and documentation gates.
