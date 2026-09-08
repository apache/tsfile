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
	"math"
	"sync"
)

type queryOptions struct {
	start, end               int64
	offset, limit, batchSize int
	tagFilter                TagFilter
}

// QueryOption customizes one table query.
type QueryOption func(*queryOptions) error

func WithTimeRange(start, end int64) QueryOption {
	return func(options *queryOptions) error {
		if end < start {
			return fmt.Errorf("%w: end must not precede start", ErrInvalidArgument)
		}
		options.start, options.end = start, end
		return nil
	}
}

func WithTagFilter(filter TagFilter) QueryOption {
	return func(options *queryOptions) error {
		if err := validateTagFilter(filter); err != nil {
			return err
		}
		options.tagFilter = filter
		return nil
	}
}

func WithOffset(offset int) QueryOption {
	return func(options *queryOptions) error {
		if offset < 0 {
			return fmt.Errorf("%w: offset must be nonnegative", ErrInvalidArgument)
		}
		if err := validateCInt32("query", "offset", offset); err != nil {
			return err
		}
		options.offset = offset
		return nil
	}
}

func WithLimit(limit int) QueryOption {
	return func(options *queryOptions) error {
		if limit < -1 {
			return fmt.Errorf("%w: limit must be -1 or nonnegative", ErrInvalidArgument)
		}
		if err := validateCInt32("query", "limit", limit); err != nil {
			return err
		}
		options.limit = limit
		return nil
	}
}

func WithBatchSize(batchSize int) QueryOption {
	return func(options *queryOptions) error {
		if batchSize <= 0 {
			options.batchSize = 0
			return nil
		}
		if err := validateCInt32("query", "batch size", batchSize); err != nil {
			return err
		}
		options.batchSize = batchSize
		return nil
	}
}

func buildQueryOptions(options ...QueryOption) (queryOptions, error) {
	settings := queryOptions{start: math.MinInt64, end: math.MaxInt64, limit: -1}
	for _, option := range options {
		if option == nil {
			return queryOptions{}, fmt.Errorf("%w: nil query option", ErrInvalidArgument)
		}
		if err := option(&settings); err != nil {
			return queryOptions{}, err
		}
	}
	return settings, nil
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

func (r *Reader) query(mode resultMode, fn func(*readerHandle) (*resultSetHandle, error)) (*ResultSet, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handle == nil || r.handle.ptr == nil {
		return nil, ErrClosed
	}
	handle, err := fn(r.handle)
	if err != nil {
		return nil, err
	}
	result := &ResultSet{handle: handle, reader: r, mode: mode}
	result.metadata = handle.metadata()
	r.results[result] = struct{}{}
	return result, nil
}

// Query executes a table query. Omitting options scans the full time range in
// row mode.
func (r *Reader) Query(table string, columns []string, options ...QueryOption) (*ResultSet, error) {
	if err := validateCString("query table", "table", table); err != nil {
		return nil, err
	}
	if err := validateQueryStrings("query table", "columns", columns); err != nil {
		return nil, err
	}
	settings, err := buildQueryOptions(options...)
	if err != nil {
		return nil, err
	}
	mode := resultModeRows
	if settings.batchSize > 0 {
		mode = resultModeBatches
	}
	table = normalizeIdentifier(table)
	normalizedColumns := make([]string, len(columns))
	for i, column := range columns {
		normalizedColumns[i] = normalizeIdentifier(column)
	}
	return r.query(mode, func(h *readerHandle) (*resultSetHandle, error) {
		return h.queryTableOptions(table, normalizedColumns, settings)
	})
}

// GetTableSchema returns an independent copy of one table schema.
func (r *Reader) GetTableSchema(table string) (TableSchema, error) {
	if err := validateCString("get table schema", "table", table); err != nil {
		return TableSchema{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handle == nil || r.handle.ptr == nil {
		return TableSchema{}, ErrClosed
	}
	return r.handle.tableSchema(normalizeIdentifier(table))
}

// GetAllTableSchemas returns independent copies of every table schema.
func (r *Reader) GetAllTableSchemas() ([]TableSchema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handle == nil || r.handle.ptr == nil {
		return nil, ErrClosed
	}
	return r.handle.allTableSchemas()
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
