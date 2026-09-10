// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tsfile

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

const defaultMemoryThreshold = 128 * 1024 * 1024

// nativeWriterConfigMu serializes construction because the current native
// writer ABI applies the memory threshold to process-global configuration.
var nativeWriterConfigMu sync.Mutex

type writerOptions struct{ memoryThreshold uint64 }

// WriterOption customizes a writer constructor.
type WriterOption func(*writerOptions) error

// WithMemoryThreshold sets the native writer's in-memory threshold in bytes.
func WithMemoryThreshold(bytes uint64) WriterOption {
	return func(options *writerOptions) error {
		if bytes == 0 {
			return fmt.Errorf("%w: memory threshold must be positive", ErrInvalidArgument)
		}
		options.memoryThreshold = bytes
		return nil
	}
}

// Writer writes batches for one table to a TsFile. Its methods serialize
// access to the underlying native writer.
type Writer struct {
	mu     sync.Mutex
	handle *writerHandle
	schema TableSchema
}

// NewWriter creates or truncates a TsFile and binds it to schema.
func NewWriter(path string, schema TableSchema, options ...WriterOption) (*Writer, error) {
	boundSchema, err := validateAndCopyTableSchema(schema)
	if err != nil {
		return nil, err
	}
	settings := writerOptions{memoryThreshold: defaultMemoryThreshold}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil writer option", ErrInvalidArgument)
		}
		if err := option(&settings); err != nil {
			return nil, err
		}
	}
	nativeWriterConfigMu.Lock()
	defer nativeWriterConfigMu.Unlock()
	handle, err := newWriterHandle(path, boundSchema, settings.memoryThreshold)
	if err != nil {
		return nil, err
	}
	return &Writer{handle: handle, schema: boundSchema}, nil
}

func (w *Writer) withHandle(fn func(*writerHandle) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil || w.handle.ptr == nil {
		return ErrClosed
	}
	return fn(w.handle)
}

func (w *Writer) writeTablet(tablet *Tablet) error {
	if tablet == nil {
		return fmt.Errorf("%w: nil tablet", ErrInvalidArgument)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil || w.handle.ptr == nil {
		return ErrClosed
	}
	if tablet.handle == nil || tablet.handle.ptr == nil {
		return ErrClosed
	}
	if len(tablet.columns) != len(w.schema.Columns) {
		return fmt.Errorf("%w: Tablet has %d columns; writer schema has %d",
			ErrInvalidSchema, len(tablet.columns), len(w.schema.Columns))
	}
	expected := make(map[string]DataType, len(w.schema.Columns))
	for _, column := range w.schema.Columns {
		expected[column.Name] = column.DataType
	}
	for _, column := range tablet.columns {
		dataType, ok := expected[column.Name]
		if !ok {
			return fmt.Errorf("%w: Tablet column %q is not present in the writer schema", ErrColumnNotExist, column.Name)
		}
		if dataType != column.DataType {
			return fmt.Errorf("%w: Tablet column %q has data type %d", ErrTypeMismatch, column.Name, column.DataType)
		}
		delete(expected, column.Name)
	}
	rows := tablet.rows
	for row := 0; row < rows; row++ {
		if !tablet.timeSet[row] {
			return fmt.Errorf("%w: Tablet row %d has no timestamp", ErrInvalidArgument, row)
		}
	}
	return w.handle.writeTableTablet(tablet.handle)
}

// WriteTableTablet writes a table-model tablet without consuming it.
func (w *Writer) WriteTableTablet(tablet *Tablet) error { return w.writeTablet(tablet) }

func arrowDataType(dataType DataType) arrow.DataType {
	switch dataType {
	case DataTypeBoolean:
		return arrow.FixedWidthTypes.Boolean
	case DataTypeInt32:
		return arrow.PrimitiveTypes.Int32
	case DataTypeInt64:
		return arrow.PrimitiveTypes.Int64
	case DataTypeFloat:
		return arrow.PrimitiveTypes.Float32
	case DataTypeDouble:
		return arrow.PrimitiveTypes.Float64
	case DataTypeText, DataTypeString:
		return arrow.BinaryTypes.String
	case DataTypeTimestamp:
		return &arrow.TimestampType{Unit: arrow.Nanosecond}
	case DataTypeDate:
		return arrow.FixedWidthTypes.Date32
	case DataTypeBlob:
		return arrow.BinaryTypes.Binary
	default:
		return nil
	}
}

func isArrowTimeType(dataType arrow.DataType) bool {
	if arrow.TypeEqual(dataType, arrow.PrimitiveTypes.Int64) {
		return true
	}
	timestamp, ok := dataType.(*arrow.TimestampType)
	return ok && timestamp.Unit == arrow.Nanosecond && timestamp.TimeZone == ""
}

func validateArrowSchema(schema *arrow.Schema, bound TableSchema) (int, error) {
	if schema == nil {
		return -1, fmt.Errorf("%w: nil Arrow schema", ErrInvalidArgument)
	}
	if schema.NumFields() != len(bound.Columns)+1 {
		return -1, fmt.Errorf("%w: Arrow batch has %d columns; expected time plus %d table columns",
			ErrInvalidSchema, schema.NumFields(), len(bound.Columns))
	}
	expected := make(map[string]DataType, len(bound.Columns))
	for _, column := range bound.Columns {
		expected[column.Name] = column.DataType
	}
	seen := make(map[string]struct{}, schema.NumFields())
	timeColumn := -1
	for i, field := range schema.Fields() {
		name := normalizeIdentifier(field.Name)
		if _, duplicate := seen[name]; duplicate {
			return -1, fmt.Errorf("%w: duplicate Arrow column %q", ErrInvalidSchema, field.Name)
		}
		seen[name] = struct{}{}
		if name == "time" {
			if !isArrowTimeType(field.Type) {
				return -1, fmt.Errorf("%w: Arrow time column must be int64 or timestamp[ns] without timezone", ErrTypeMismatch)
			}
			timeColumn = i
			continue
		}
		dataType, ok := expected[name]
		if !ok {
			return -1, fmt.Errorf("%w: Arrow column %q is not present in the writer schema", ErrColumnNotExist, field.Name)
		}
		if !arrow.TypeEqual(field.Type, arrowDataType(dataType)) {
			return -1, fmt.Errorf("%w: Arrow column %q has type %s", ErrTypeMismatch, field.Name, field.Type)
		}
	}
	if timeColumn < 0 {
		return -1, fmt.Errorf("%w: Arrow batch has no time column", ErrInvalidSchema)
	}
	return timeColumn, nil
}

func (w *Writer) writeArrowRecordLocked(record arrow.Record) error {
	if record == nil {
		return fmt.Errorf("%w: nil Arrow record", ErrInvalidArgument)
	}
	timeColumn, err := validateArrowSchema(record.Schema(), w.schema)
	if err != nil {
		return err
	}
	if record.Column(timeColumn).NullN() != 0 {
		return fmt.Errorf("%w: Arrow time column must not contain null values", ErrInvalidArgument)
	}
	if record.NumRows() == 0 {
		return nil
	}
	if record.NumRows() >= cTabletMaxRowsMax {
		return fmt.Errorf("%w: Arrow batch has %d rows; native limit is %d",
			ErrOverflow, record.NumRows(), cTabletMaxRowsMax-1)
	}
	var nativeArray cdata.CArrowArray
	var nativeSchema cdata.CArrowSchema
	cdata.ExportArrowRecordBatch(record, &nativeArray, &nativeSchema)
	defer cdata.ReleaseCArrowArray(&nativeArray)
	defer cdata.ReleaseCArrowSchema(&nativeSchema)
	return w.handle.writeArrow(unsafe.Pointer(&nativeArray), unsafe.Pointer(&nativeSchema), timeColumn)
}

// WriteArrowBatch writes an Arrow record batch or table to the bound table.
// The input remains owned by the caller and may be released after this method
// returns.
func (w *Writer) WriteArrowBatch(data any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil || w.handle.ptr == nil {
		return ErrClosed
	}
	switch value := data.(type) {
	case arrow.Record:
		if value == nil {
			return fmt.Errorf("%w: nil Arrow record", ErrInvalidArgument)
		}
		return w.writeArrowRecordLocked(value)
	case arrow.Table:
		if value == nil {
			return fmt.Errorf("%w: nil Arrow table", ErrInvalidArgument)
		}
		timeColumn, err := validateArrowSchema(value.Schema(), w.schema)
		if err != nil {
			return err
		}
		if value.Column(timeColumn).NullN() != 0 {
			return fmt.Errorf("%w: Arrow time column must not contain null values", ErrInvalidArgument)
		}
		reader := array.NewTableReader(value, 0)
		defer reader.Release()
		for reader.Next() {
			if err := w.writeArrowRecordLocked(reader.Record()); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: expected arrow.Record or arrow.Table", ErrInvalidArgument)
	}
}

// AddProperty adds a binary TsFile property. Empty values are rejected because
// the native ABI represents a nil pointer as a NULL property, not an empty
// byte string.
func (w *Writer) AddProperty(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("%w: property key must not be empty", ErrInvalidArgument)
	}
	if len(value) == 0 {
		return fmt.Errorf("%w: property value must not be empty", ErrInvalidArgument)
	}
	return w.withHandle(func(h *writerHandle) error { return h.addProperty(key, value) })
}

// Flush writes buffered data without closing the writer.
func (w *Writer) Flush() error {
	return w.withHandle(func(h *writerHandle) error { return h.flush() })
}

// Close flushes and releases the writer. It is idempotent.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handle == nil {
		return nil
	}
	return w.handle.close()
}
