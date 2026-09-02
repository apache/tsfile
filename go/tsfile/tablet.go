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
	"strings"
	"sync"
)

// Tablet is a fixed-capacity batch of rows for one device or table.
type Tablet struct {
	mu      sync.Mutex
	handle  *tabletHandle
	columns []TabletColumn
	maxRows int
}

// NewTablet allocates a tablet with fixed columns and row capacity.
func NewTablet(target string, columns []TabletColumn, maxRows int) (*Tablet, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("%w: at least one column is required", ErrInvalidArgument)
	}
	names := make([]string, len(columns))
	types := make([]DataType, len(columns))
	for i, column := range columns {
		if err := validateCString("new tablet", "column name", column.Name); err != nil {
			return nil, fmt.Errorf("column %d: %w", i, err)
		}
		if !validDataType(column.DataType) {
			return nil, fmt.Errorf("%w: unsupported data type %d for column %q", ErrInvalidSchema, column.DataType, column.Name)
		}
		names[i], types[i] = column.Name, column.DataType
	}
	handle, err := newTabletHandle(target, names, types, maxRows)
	if err != nil {
		return nil, err
	}
	return &Tablet{handle: handle, columns: append([]TabletColumn(nil), columns...), maxRows: maxRows}, nil
}

func (t *Tablet) validateLocked(row, column int, allowed ...DataType) error {
	if t.handle == nil || t.handle.ptr == nil {
		return ErrClosed
	}
	if row < 0 || row >= t.maxRows || column < 0 || column >= len(t.columns) {
		return ErrOutOfRange
	}
	actual := t.columns[column].DataType
	for _, expected := range allowed {
		if actual == expected {
			return nil
		}
	}
	return fmt.Errorf("%w: column %d has data type %d", ErrTypeMismatch, column, actual)
}

// AddTimestamp assigns the timestamp for a row.
func (t *Tablet) AddTimestamp(row int, timestamp int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.handle == nil || t.handle.ptr == nil {
		return ErrClosed
	}
	if row < 0 || row >= t.maxRows {
		return ErrOutOfRange
	}
	return t.handle.addTimestamp(row, timestamp)
}

// SetBool assigns a BOOLEAN value by zero-based row and column.
func (t *Tablet) SetBool(row, column int, value bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.validateLocked(row, column, DataTypeBoolean); err != nil {
		return err
	}
	return t.handle.addBool(row, column, value)
}

// SetInt32 assigns an INT32 or DATE value.
func (t *Tablet) SetInt32(row, column int, value int32) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.validateLocked(row, column, DataTypeInt32, DataTypeDate); err != nil {
		return err
	}
	return t.handle.addInt32(row, column, value)
}

// SetInt64 assigns an INT64 or TIMESTAMP value.
func (t *Tablet) SetInt64(row, column int, value int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.validateLocked(row, column, DataTypeInt64, DataTypeTimestamp); err != nil {
		return err
	}
	return t.handle.addInt64(row, column, value)
}

// SetFloat32 assigns a FLOAT value.
func (t *Tablet) SetFloat32(row, column int, value float32) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.validateLocked(row, column, DataTypeFloat); err != nil {
		return err
	}
	return t.handle.addFloat32(row, column, value)
}

// SetFloat64 assigns a DOUBLE value.
func (t *Tablet) SetFloat64(row, column int, value float64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.validateLocked(row, column, DataTypeDouble); err != nil {
		return err
	}
	return t.handle.addFloat64(row, column, value)
}

// SetString assigns a TEXT or STRING value.
func (t *Tablet) SetString(row, column int, value string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.validateLocked(row, column, DataTypeText, DataTypeString); err != nil {
		return err
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: strings with embedded NUL are not supported", ErrInvalidArgument)
	}
	return t.handle.addString(row, column, value)
}

// Rows returns the number of rows currently present in the tablet. It returns
// zero after the tablet has been closed.
func (t *Tablet) Rows() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.handle == nil || t.handle.ptr == nil {
		return 0
	}
	return t.handle.rowCount()
}

// Close releases the native tablet. It is safe to call more than once.
func (t *Tablet) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.handle == nil {
		return nil
	}
	return t.handle.close()
}
