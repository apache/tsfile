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
# 接口定义 - Go

Go 包 `github.com/apache/tsfile/go/tsfile` 提供表模型接口。Tablet 的行和列下标从 0 开始；结果集的列从 1 开始，
第 1 列是时间戳。表和列标识符大小写不敏感，写入时统一规范化为小写。

## 数据类型与模式

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

仅支持 TAG 和 FIELD 两类列：TAG 列是设备标识的一部分，必须使用 `DataTypeString`；FIELD 列存放测量值，
可以使用上述任意类型。

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

`NewWriter` 会复制并校验 schema，因此之后再修改调用方的 `Columns` 切片不会影响 writer。校验会拒绝空表名或空列名、
重复列名、不支持的数据类型，以及不是 `DataTypeString` 的 TAG 列。

## 写入接口

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

`Writer` 会串行化对原生 writer 的访问，因此可以在多个 goroutine 中调用其方法。

`WriteTableTablet` 要求 tablet 的列与 writer schema 按名称和类型一一匹配：缺少列返回 `ErrColumnNotExist`，
类型不一致返回 `ErrTypeMismatch`。

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

| Setter | 可接受的列类型 |
|---|---|
| `SetBool` | `BOOLEAN` |
| `SetInt32` | `INT32`、`DATE` |
| `SetInt64` | `INT64`、`TIMESTAMP` |
| `SetFloat32` | `FLOAT` |
| `SetFloat64` | `DOUBLE` |
| `SetString` | `TEXT`、`STRING` |
| `SetBytes` | `BLOB` |

每个 setter 在下标超出 tablet 范围时返回 `ErrOutOfRange`，在与列类型不符时返回 `ErrTypeMismatch`，
在 tablet 已关闭后返回 `ErrClosed`。字符串中不能包含 NUL 字节。tablet 不是并发安全的，在
`Writer.WriteTableTablet` 使用期间不得修改或关闭它。

### Arrow 批次 schema

`WriteArrowBatch` 接受 `arrow.Record` 或 `arrow.Table`。其 schema 的字段数必须比 writer schema 多一列：
`time` 列加上全部表列。`time` 列可以是 `int64` 或 `timestamp[ns]`（不带时区），且不能包含空值；其他列的空值会
作为 NULL 写入。

| `DataType` | Arrow 类型 |
|---|---|
| `DataTypeBoolean` | `arrow.FixedWidthTypes.Boolean` |
| `DataTypeInt32` | `arrow.PrimitiveTypes.Int32` |
| `DataTypeInt64` | `arrow.PrimitiveTypes.Int64` |
| `DataTypeFloat` | `arrow.PrimitiveTypes.Float32` |
| `DataTypeDouble` | `arrow.PrimitiveTypes.Float64` |
| `DataTypeText`、`DataTypeString` | `arrow.BinaryTypes.String` |
| `DataTypeTimestamp` | `arrow.TimestampType{Unit: arrow.Nanosecond}` |
| `DataTypeDate` | `arrow.FixedWidthTypes.Date32` |
| `DataTypeBlob` | `arrow.BinaryTypes.Binary` |

当批次的列数与 writer schema 加 time 列不一致、重复或缺少某列、或者使用了上表以外的类型时，分别返回
`ErrInvalidSchema`、`ErrColumnNotExist` 或 `ErrTypeMismatch`。批次行数必须小于 `1<<30`。输入数据仍归调用方
所有，调用返回后即可释放。

## 读取接口

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

`columns` 指定要返回的数据列，不能为空；时间戳列由读取器自动添加。表不存在时返回 `ErrTableNotExist`，
列不存在时返回 `ErrColumnNotExist`。

### 查询选项

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

不使用 `WithTimeRange` 时查询覆盖全部时间范围。当 `start` 大于 `end` 时范围为空，查询不返回任何行。offset、limit、
时间范围和 Tag 过滤都会下推到原生读取器。

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

Tag 值按字符串比较，这与 TAG 列必须为 `DataTypeString` 的要求一致。`Between` 要求 `lower <= upper`。
向 `WithTagFilter` 传入非法过滤条件会返回 `ErrInvalidArgument`。

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

使用 `WithBatchSize(n)`（`n > 0`）构造的查询运行在批次模式下，此时只有 `ReadArrowRecordBatch`、`ReadArrowBatch`、
`Metadata` 和 `Close` 可用；`Next` 和标量 getter 会返回 `ErrWrongResultMode`。其他查询运行在行模式下，此时只有
`Next`、标量 getter、`Metadata` 和 `Close` 可用，Arrow 读取方法返回 `ErrWrongResultMode`。

在行模式下，必须先由 `Next` 返回 `true` 才能读取值，否则 getter 返回 `ErrNoCurrentRow`。对 NULL 列调用类型化
getter 会返回 `ErrNullValue`，因此应先调用 `IsNull`。getter 类型与列类型不匹配时返回 `ErrTypeMismatch`。

类型化 getter 接受与 Tablet 对应 setter 相同的列类型（`Int32` 也可读取 `DATE`，`Int64` 也可读取 `TIMESTAMP`，
`String` 也可读取 `TEXT`）。

结果集由所属 reader 管理：关闭 reader 会关闭由它创建的所有结果集。结果集只能向前遍历；用完后应调用 `Close`
在关闭 reader 之前释放原生资源。

## 错误处理

原生错误码以 `*tsfile.Error` 形式返回，该类型按错误码比较，因此 `errors.Is` 可以和导出的哨兵错误一起使用：

```go
if err := writer.WriteTableTablet(tablet); errors.Is(err, tsfile.ErrTypeMismatch) {
	// The tablet column type does not match the writer schema.
}
```

| 哨兵错误 | 原生错误码含义 |
|---|---|
| `ErrInvalidArgument` | 非法参数 |
| `ErrOutOfRange` | 越界 |
| `ErrOverflow` | 溢出 |
| `ErrInvalidSchema` | 非法 schema |
| `ErrTypeNotSupported` | 不支持的类型 |
| `ErrTypeMismatch` | 类型不匹配 |
| `ErrFileOpen` | 文件打开失败 |
| `ErrFileClose` | 文件关闭失败 |
| `ErrFileWrite` | 文件写入失败 |
| `ErrFileRead` | 文件读取失败 |
| `ErrInvalidPath` | 非法路径 |
| `ErrTableNotExist` | 表不存在 |
| `ErrColumnNotExist` | 列不存在 |

此外，包还会返回 `ErrClosed`、`ErrNullValue`、`ErrWrongResultMode` 和 `ErrNoCurrentRow`，它们是普通的 Go error，
不对应原生错误码。`*tsfile.Error` 携带数字错误码 `Code` 以及产生错误的操作 `Op`。
