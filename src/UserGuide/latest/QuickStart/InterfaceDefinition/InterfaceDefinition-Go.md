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
# Interface Definitions - Go

The Go package `github.com/apache/tsfile/go/tsfile` exposes the table model. Tablet rows and columns are
zero-based; result-set columns are one-based, with the timestamp at column 1. Table and column identifiers are
case-insensitive and stored normalized to lower case.

## Data Types and Schema

```go
// DataType identifies the data type of a table column.
type DataType int32

const (
	DataTypeBoolean   DataType = 0
	DataTypeInt32     DataType = 1
	DataTypeInt64     DataType = 2
	DataTypeFloat     DataType = 3
	DataTypeDouble    DataType = 4
	DataTypeText      DataType = 5
	DataTypeTimestamp DataType = 8
	DataTypeDate      DataType = 9
	DataTypeBlob      DataType = 10
	DataTypeString    DataType = 11
)

// ColumnCategory identifies the category of a schema entry.
type ColumnCategory int32

const (
	ColumnCategoryTag   ColumnCategory = 0
	ColumnCategoryField ColumnCategory = 1
)
```

Only TAG and FIELD columns are accepted: a TAG column is part of the device identifier and must use
`DataTypeString`; a FIELD column holds the measured value and may use any type above.

```go
// ColumnSchema describes one table-model column.
type ColumnSchema struct {
	Name     string
	DataType DataType
	Category ColumnCategory
}

// TableSchema describes a table-model table.
type TableSchema struct {
	Table   string
	Columns []ColumnSchema
}

// TabletColumn fixes the name and type of a Tablet column.
type TabletColumn struct {
	Name     string
	DataType DataType
}

// ColumnMetadata describes one column in a query result. The timestamp is the
// first column.
type ColumnMetadata struct {
	Name     string
	DataType DataType
}
```

`NewWriter` copies and validates the schema, so later changes to the caller's `Columns` slice do not affect the
writer. Validation rejects an empty table or column name, duplicate column names, an unsupported data type, and a
TAG column that is not `DataTypeString`.

## Write Interface

### Writer

```go
// WriterOption customizes a writer constructor.
type WriterOption func(*writerOptions) error

// WithMemoryThreshold sets the native writer's in-memory threshold in bytes.
// The default is 128 MB.
func WithMemoryThreshold(bytes uint64) WriterOption

// NewWriter creates or truncates a TsFile and binds it to schema.
func NewWriter(path string, schema TableSchema, options ...WriterOption) (*Writer, error)

// WriteTableTablet writes a table-model tablet without consuming it.
func (w *Writer) WriteTableTablet(tablet *Tablet) error

// WriteArrowBatch writes an Arrow record batch or table to the bound table.
// The input remains owned by the caller and may be released after this method
// returns.
func (w *Writer) WriteArrowBatch(data any) error

// AddProperty adds a binary TsFile property. Empty keys and empty values are
// rejected.
func (w *Writer) AddProperty(key string, value []byte) error

// Flush writes buffered data without closing the writer.
func (w *Writer) Flush() error

// Close flushes and releases the writer. It is idempotent.
func (w *Writer) Close() error
```

A `Writer` serializes access to the native writer, so its methods may be called from multiple goroutines.

`WriteTableTablet` requires the tablet columns to match the writer schema by name and type; a missing column
returns `ErrColumnNotExist` and a type difference returns `ErrTypeMismatch`.

### Tablet

```go
// NewTablet allocates a tablet with fixed columns and row capacity.
// maxRows must be in [1, 1<<30).
func NewTablet(columns []TabletColumn, maxRows int) (*Tablet, error)

// AddTimestamp assigns the timestamp for a row.
func (t *Tablet) AddTimestamp(row int, timestamp int64) error

// SetBool assigns a BOOLEAN value by zero-based row and column.
func (t *Tablet) SetBool(row, column int, value bool) error
// SetInt32 assigns an INT32 or DATE value.
func (t *Tablet) SetInt32(row, column int, value int32) error
// SetInt64 assigns an INT64 or TIMESTAMP value.
func (t *Tablet) SetInt64(row, column int, value int64) error
// SetFloat32 assigns a FLOAT value.
func (t *Tablet) SetFloat32(row, column int, value float32) error
// SetFloat64 assigns a DOUBLE value.
func (t *Tablet) SetFloat64(row, column int, value float64) error
// SetString assigns a TEXT or STRING value.
func (t *Tablet) SetString(row, column int, value string) error
// SetBytes assigns a BLOB value without interpreting its contents.
func (t *Tablet) SetBytes(row, column int, value []byte) error

// Rows returns the number of rows currently present in the tablet.
func (t *Tablet) Rows() int

// Close releases the native tablet. It is safe to call more than once.
func (t *Tablet) Close() error
```

| Setter | Accepted column types |
|---|---|
| `SetBool` | `BOOLEAN` |
| `SetInt32` | `INT32`, `DATE` |
| `SetInt64` | `INT64`, `TIMESTAMP` |
| `SetFloat32` | `FLOAT` |
| `SetFloat64` | `DOUBLE` |
| `SetString` | `TEXT`, `STRING` |
| `SetBytes` | `BLOB` |

Each setter returns `ErrOutOfRange` for a row or column outside the tablet, `ErrTypeMismatch` for a value that does
not match the column type, and `ErrClosed` after the tablet has been closed. Strings must not contain an embedded
NUL byte. A tablet is not safe for concurrent use and must not be modified or closed while
`Writer.WriteTableTablet` is using it.

### Arrow batch schema

`WriteArrowBatch` accepts an `arrow.Record` or an `arrow.Table`. The schema must have exactly one more field than
the writer schema: a `time` column plus every table column. The `time` column may be `int64` or `timestamp[ns]`
without a time zone and must not contain nulls; any other null column is written as a null value.

| `DataType` | Arrow type |
|---|---|
| `DataTypeBoolean` | `arrow.FixedWidthTypes.Boolean` |
| `DataTypeInt32` | `arrow.PrimitiveTypes.Int32` |
| `DataTypeInt64` | `arrow.PrimitiveTypes.Int64` |
| `DataTypeFloat` | `arrow.PrimitiveTypes.Float32` |
| `DataTypeDouble` | `arrow.PrimitiveTypes.Float64` |
| `DataTypeText`, `DataTypeString` | `arrow.BinaryTypes.String` |
| `DataTypeTimestamp` | `arrow.TimestampType{Unit: arrow.Nanosecond}` |
| `DataTypeDate` | `arrow.FixedWidthTypes.Date32` |
| `DataTypeBlob` | `arrow.BinaryTypes.Binary` |

A batch whose column count differs from the writer schema plus the time column, that repeats or omits a column, or
that uses a type other than the ones listed returns `ErrInvalidSchema`, `ErrColumnNotExist`, or
`ErrTypeMismatch`. A batch must have fewer than `1<<30` rows. The input
remains owned by the caller and may be released as soon as the call returns.

## Read Interface

### Reader

```go
// NewReader opens an existing TsFile for queries.
func NewReader(path string) (*Reader, error)

// Query executes a table query. Omitting options scans the full time range in
// row mode.
func (r *Reader) Query(table string, columns []string, options ...QueryOption) (*ResultSet, error)

// GetTableSchema returns an independent copy of one table schema.
func (r *Reader) GetTableSchema(table string) (TableSchema, error)

// GetAllTableSchemas returns independent copies of every table schema.
func (r *Reader) GetAllTableSchemas() ([]TableSchema, error)

// Close releases all active result sets and then the reader. It is idempotent.
func (r *Reader) Close() error
```

`columns` selects the data columns to return and must not be empty; the timestamp column is always added by the
reader. An unknown table returns `ErrTableNotExist` and an unknown column returns `ErrColumnNotExist`.

### Query options

```go
// WithTimeRange limits the query to the inclusive [start, end] range.
func WithTimeRange(start, end int64) QueryOption
// WithTagFilter restricts the query with a TAG predicate.
func WithTagFilter(filter TagFilter) QueryOption
// WithOffset skips the leading rows.
func WithOffset(offset int) QueryOption
// WithLimit caps the returned rows; a negative value means unlimited.
func WithLimit(limit int) QueryOption
// WithBatchSize selects batch mode; a positive value returns Arrow batches
// of at most that many rows, a value <= 0 returns rows one by one.
func WithBatchSize(batchSize int) QueryOption
```

Without `WithTimeRange` the query covers the full time range. When `start` is greater than `end` the range is
empty and the query returns no rows. Offset, limit, time range, and tag filter are pushed down to the native
reader.

### TagFilter

```go
// TagFilter is an immutable predicate over table TAG columns.
type TagFilter interface { tagFilter() }

func Eq(column, value string) TagFilter
func Neq(column, value string) TagFilter
func Lt(column, value string) TagFilter
func Lte(column, value string) TagFilter
func Gt(column, value string) TagFilter
func Gte(column, value string) TagFilter
func Between(column, lower, upper string) TagFilter
func And(left, right TagFilter) TagFilter
func Or(left, right TagFilter) TagFilter
func Not(filter TagFilter) TagFilter
```

Tag values are compared as strings, matching the `DataTypeString` requirement for TAG columns. `Between` requires
`lower <= upper`. Passing an invalid filter to `WithTagFilter` returns `ErrInvalidArgument`.

### ResultSet

```go
// Next advances to the next row. It returns false with a nil error at EOF.
func (rs *ResultSet) Next() (bool, error)

// Metadata returns a copy of the result columns, including timestamp first.
func (rs *ResultSet) Metadata() []ColumnMetadata

// IsNull reports whether the one-based column is null in the current row.
func (rs *ResultSet) IsNull(column int) (bool, error)

// Typed getters for the current row; column is one-based.
func (rs *ResultSet) Bool(column int) (bool, error)
func (rs *ResultSet) Int32(column int) (int32, error)
func (rs *ResultSet) Int64(column int) (int64, error)
func (rs *ResultSet) Float32(column int) (float32, error)
func (rs *ResultSet) Float64(column int) (float64, error)
func (rs *ResultSet) String(column int) (string, error)
func (rs *ResultSet) Bytes(column int) ([]byte, error)

// ReadArrowRecordBatch returns the next query batch. The caller owns the
// returned record and must call Release. It returns io.EOF after the last
// batch.
func (rs *ResultSet) ReadArrowRecordBatch() (arrow.Record, error)

// ReadArrowBatch returns the next query batch as a one-batch Arrow table.
func (rs *ResultSet) ReadArrowBatch() (arrow.Table, error)

// Close releases the native result. It is idempotent.
func (rs *ResultSet) Close() error
```

A query built with `WithBatchSize(n)` where `n > 0` runs in batch mode, and only `ReadArrowRecordBatch`,
`ReadArrowBatch`, `Metadata`, and `Close` are usable; `Next` and the scalar getters return
`ErrWrongResultMode`. Every other query runs in row mode, where only `Next`, the scalar getters, `Metadata`, and
`Close` are usable and the Arrow readers return `ErrWrongResultMode`.

In row mode, `Next` must return `true` before a value can be read; otherwise the getters return
`ErrNoCurrentRow`. Reading a null column with a typed getter returns `ErrNullValue`, so call `IsNull` first. A
getter whose type does not match the column returns `ErrTypeMismatch`.

The typed getters accept the same column types as their Tablet counterparts (`Int32` also reads `DATE`, `Int64`
also reads `TIMESTAMP`, `String` also reads `TEXT`).

A result set is owned by its reader: closing the reader closes every result set created from it. A result set is
forward-only; call `Close` when done to release the native resources before the reader is closed.

## Error Handling

Native error codes are returned as `*tsfile.Error`, which compares by code, so `errors.Is` works with the exported
sentinels:

```go
if err := writer.WriteTableTablet(tablet); errors.Is(err, tsfile.ErrTypeMismatch) {
	// The tablet column type does not match the writer schema.
}
```

| Sentinel | Native code |
|---|---|
| `ErrInvalidArgument` | invalid argument |
| `ErrOutOfRange` | out of range |
| `ErrOverflow` | overflow |
| `ErrInvalidSchema` | invalid schema |
| `ErrTypeNotSupported` | type not supported |
| `ErrTypeMismatch` | type mismatch |
| `ErrFileOpen` | file open failed |
| `ErrFileClose` | file close failed |
| `ErrFileWrite` | file write failed |
| `ErrFileRead` | file read failed |
| `ErrInvalidPath` | invalid path |
| `ErrTableNotExist` | table does not exist |
| `ErrColumnNotExist` | column does not exist |

The package also returns `ErrClosed`, `ErrNullValue`, `ErrWrongResultMode`, and `ErrNoCurrentRow`, which are
plain Go errors rather than native codes. The `*tsfile.Error` value carries the numeric `Code` and the operation
`Op` that produced it.
