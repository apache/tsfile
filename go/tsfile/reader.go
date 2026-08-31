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
)

// TableQuery selects table-model columns over an inclusive time range.
type TableQuery struct {
	Table      string
	Columns    []string
	Start, End int64
}

// TreeQuery selects exact tree-model full paths over an inclusive time range.
type TreeQuery struct {
	Paths      []string
	Start, End int64
}

// TreeRowsQuery selects tree-model rows with global offset and limit.
type TreeRowsQuery struct {
	Devices, Measurements []string
	Offset, Limit         int
}

// TableRowsQuery selects table-model rows with offset and limit.
type TableRowsQuery struct {
	Table                    string
	Columns                  []string
	Offset, Limit, BatchSize int
}

// Reader queries one TsFile and owns all ResultSets created from it.
type Reader struct {
	mu      sync.Mutex
	handle  *readerHandle
	results map[*ResultSet]struct{}
}

// NewReader opens an existing TsFile for queries.
func NewReader(path string) (*Reader, error) {
	handle, err := newReaderHandle(path)
	if err != nil {
		return nil, err
	}
	return &Reader{handle: handle, results: make(map[*ResultSet]struct{})}, nil
}

func validateQueryStrings(op, field string, values []string) error {
	if len(values) == 0 {
		return fmt.Errorf("%w: %s must not be empty", ErrInvalidArgument, field)
	}
	if err := validateCInt32(op, field+" count", len(values)); err != nil {
		return err
	}
	if _, err := validateCStrings(op, field, values); err != nil {
		return err
	}
	return nil
}

func (r *Reader) query(fn func(*readerHandle) (*resultSetHandle, error)) (*ResultSet, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handle == nil || r.handle.ptr == nil {
		return nil, ErrClosed
	}
	handle, err := fn(r.handle)
	if err != nil {
		return nil, err
	}
	result := &ResultSet{handle: handle, reader: r}
	result.metadata = handle.metadata()
	r.results[result] = struct{}{}
	return result, nil
}

// QueryTable executes a time-range table-model query.
func (r *Reader) QueryTable(query TableQuery) (*ResultSet, error) {
	if err := validateCString("query table", "table", query.Table); err != nil {
		return nil, err
	}
	if err := validateQueryStrings("query table", "columns", query.Columns); err != nil {
		return nil, err
	}
	if query.End < query.Start {
		return nil, fmt.Errorf("%w: end must not precede start", ErrInvalidArgument)
	}
	return r.query(func(h *readerHandle) (*resultSetHandle, error) { return h.queryTable(query) })
}

// QueryTree executes a time-range query for exact full paths.
func (r *Reader) QueryTree(query TreeQuery) (*ResultSet, error) {
	if err := validateQueryStrings("query tree", "paths", query.Paths); err != nil {
		return nil, err
	}
	if query.End < query.Start {
		return nil, fmt.Errorf("%w: end must not precede start", ErrInvalidArgument)
	}
	return r.query(func(h *readerHandle) (*resultSetHandle, error) { return h.queryTree(query) })
}

func validateRows(offset, limit int, op string) error {
	if offset < 0 {
		return fmt.Errorf("%w: offset must be nonnegative", ErrInvalidArgument)
	}
	if err := validateCInt32(op, "offset", offset); err != nil {
		return err
	}
	return validateCInt32(op, "limit", limit)
}

// QueryTreeRows executes an offset/limit tree-model query.
func (r *Reader) QueryTreeRows(query TreeRowsQuery) (*ResultSet, error) {
	if err := validateQueryStrings("query tree rows", "devices", query.Devices); err != nil {
		return nil, err
	}
	if err := validateQueryStrings("query tree rows", "measurements", query.Measurements); err != nil {
		return nil, err
	}
	if err := validateRows(query.Offset, query.Limit, "query tree rows"); err != nil {
		return nil, err
	}
	return r.query(func(h *readerHandle) (*resultSetHandle, error) { return h.queryTreeRows(query) })
}

// QueryTableRows executes an offset/limit table-model query.
func (r *Reader) QueryTableRows(query TableRowsQuery) (*ResultSet, error) {
	if err := validateCString("query table rows", "table", query.Table); err != nil {
		return nil, err
	}
	if err := validateQueryStrings("query table rows", "columns", query.Columns); err != nil {
		return nil, err
	}
	if err := validateRows(query.Offset, query.Limit, "query table rows"); err != nil {
		return nil, err
	}
	if query.BatchSize < 0 {
		return nil, fmt.Errorf("%w: batch size must be nonnegative", ErrInvalidArgument)
	}
	if err := validateCInt32("query table rows", "batch size", query.BatchSize); err != nil {
		return nil, err
	}
	return r.query(func(h *readerHandle) (*resultSetHandle, error) { return h.queryTableRows(query) })
}

// Close releases all active result sets and then the reader. It is idempotent.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handle == nil || r.handle.ptr == nil {
		return nil
	}
	for result := range r.results {
		result.mu.Lock()
		if result.handle != nil {
			_ = result.handle.close()
		}
		result.current = false
		result.mu.Unlock()
		delete(r.results, result)
	}
	return r.handle.close()
}
