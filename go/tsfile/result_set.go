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
	"errors"
	"fmt"
	"sync"
)

// ErrNoCurrentRow indicates that a value was requested before a successful
// call to Next or after the result set reached its end.
var ErrNoCurrentRow = errors.New("tsfile: result set is not positioned on a row")

// ResultSet is a forward-only query result owned by its Reader.
type ResultSet struct {
	mu       sync.Mutex
	handle   *resultSetHandle
	reader   *Reader
	metadata []ColumnMetadata
	current  bool
}

func (rs *ResultSet) locked(fn func() error) error {
	reader := rs.reader
	if reader == nil {
		return ErrClosed
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.handle == nil || rs.handle.ptr == nil {
		return ErrClosed
	}
	return fn()
}

// Next advances to the next row. It returns false with a nil error at EOF.
func (rs *ResultSet) Next() (bool, error) {
	var next bool
	err := rs.locked(func() error {
		var err error
		next, err = rs.handle.next()
		rs.current = next
		return err
	})
	return next, err
}

// Metadata returns a copy of the result columns, including timestamp at zero.
// It returns nil after the result set has been closed.
func (rs *ResultSet) Metadata() []ColumnMetadata {
	reader := rs.reader
	if reader == nil {
		return nil
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.handle == nil || rs.handle.ptr == nil {
		return nil
	}
	return append([]ColumnMetadata(nil), rs.metadata...)
}

func (rs *ResultSet) validateColumn(column int, allowed ...DataType) error {
	if !rs.current {
		return ErrNoCurrentRow
	}
	if column < 0 || column >= len(rs.metadata) {
		return ErrOutOfRange
	}
	actual := rs.metadata[column].DataType
	for _, expected := range allowed {
		if actual == expected {
			return nil
		}
	}
	return fmt.Errorf("%w: column %d has data type %d", ErrTypeMismatch, column, actual)
}

// IsNull reports whether the zero-based column is null in the current row.
func (rs *ResultSet) IsNull(column int) (value bool, err error) {
	err = rs.locked(func() error {
		if !rs.current {
			return ErrNoCurrentRow
		}
		if column < 0 || column >= len(rs.metadata) {
			return ErrOutOfRange
		}
		value = rs.handle.isNull(column)
		return nil
	})
	return
}

func (rs *ResultSet) get(column int, allowed []DataType, fn func()) error {
	return rs.locked(func() error {
		if err := rs.validateColumn(column, allowed...); err != nil {
			return err
		}
		if rs.handle.isNull(column) {
			return ErrNullValue
		}
		fn()
		return nil
	})
}

// Bool returns a BOOLEAN column from the current row.
func (rs *ResultSet) Bool(column int) (value bool, err error) {
	err = rs.get(column, []DataType{DataTypeBoolean}, func() { value = rs.handle.bool(column) })
	return
}

// Int32 returns an INT32 or DATE column from the current row.
func (rs *ResultSet) Int32(column int) (value int32, err error) {
	err = rs.get(column, []DataType{DataTypeInt32, DataTypeDate}, func() { value = rs.handle.int32(column) })
	return
}

// Int64 returns an INT64 or TIMESTAMP column from the current row.
func (rs *ResultSet) Int64(column int) (value int64, err error) {
	err = rs.get(column, []DataType{DataTypeInt64, DataTypeTimestamp}, func() { value = rs.handle.int64(column) })
	return
}

// Float32 returns a FLOAT column from the current row.
func (rs *ResultSet) Float32(column int) (value float32, err error) {
	err = rs.get(column, []DataType{DataTypeFloat}, func() { value = rs.handle.float32(column) })
	return
}

// Float64 returns a DOUBLE column from the current row.
func (rs *ResultSet) Float64(column int) (value float64, err error) {
	err = rs.get(column, []DataType{DataTypeDouble}, func() { value = rs.handle.float64(column) })
	return
}

// String returns a TEXT or STRING column from the current row.
func (rs *ResultSet) String(column int) (value string, err error) {
	err = rs.get(column, []DataType{DataTypeText, DataTypeString}, func() { value = rs.handle.string(column) })
	return
}

// Close releases the native result. It is idempotent.
func (rs *ResultSet) Close() error {
	reader := rs.reader
	if reader == nil {
		return nil
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.handle == nil || rs.handle.ptr == nil {
		return nil
	}
	err := rs.handle.close()
	if err == nil {
		delete(reader.results, rs)
		rs.current = false
	}
	return err
}
